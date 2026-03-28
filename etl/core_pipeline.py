"""
core_pipeline.py
────────────────────────────────────────────────────────────────────────────────
ETL Pipeline: API → Bronze → Silver → Gold (Data Lake) + PostgreSQL

Full flow per run:
  1.  Download CSV from source API
  2.  BRONZE  — Stage raw CSV to Azure Blob (immutable, timestamped)    [lake]
  3.  BRONZE  — Download staged blob back as authoritative source       [lake]
  4.  Schema validation                                          (unchanged)
  5.  Transform                                                  (unchanged)
  6.  SILVER  — Write validated+cleaned data as Parquet          [lake NEW]
  7.  Load billing_data with idempotency                         (unchanged)
  8.  Load aggregate tables with idempotency                     (unchanged)
  9.  GOLD    — Write analytics aggregates as Parquet            [lake NEW]

Only steps 6 and 9 are new. Everything else is byte-for-byte identical.
────────────────────────────────────────────────────────────────────────────────
"""

import requests
import base64
import binascii
import re
import io
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timedelta
import time
import os
import numpy as np
import aggregate
import azure_staging
from dotenv import load_dotenv

load_dotenv()

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


DB_CONFIG = {
    "host":     require_env("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": require_env("DB_NAME"),
    "user":     require_env("DB_USER"),
    "password": require_env("DB_PASSWORD"),
}


POSTGRES_COLUMNS = [
    "invoice", "storeInvoice", "orderDate", "time", "productId", "productName", "barcode",
    "quantity", "sellingPrice", "discountAmount", "totalProductPrice", "deliveryFee",
    "HSNCode", "GST", "GSTAmount", "CGSTRate", "CGSTAmount", "SGSTRate", "SGSTAmount",
    "acessAmount", "cess", "cessAmount", "orderAmountTax", "orderAmountNet", "cashAmount",
    "cardAmount", "upiAmount", "creditAmount", "costPrice", "description", "brandName",
    "categoryName", "subCategoryOf", "storeName", "GSTIN", "orderType", "paymentMethod",
    "customerName", "customerNumber", "orderFrom", "orderStatus"
]

INTEGER_COLUMNS = ["productId", "quantity"]
BIGINT_COLUMNS  = ["barcode"]
NUMERIC_COLUMNS = ["GST", "CGSTRate", "SGSTRate", "acessAmount", "cess"]

# -- Columns the CSV must contain for the pipeline to proceed --
REQUIRED_CSV_COLUMNS = [
    "invoice", "orderDate", "productId", "productName", "quantity",
    "totalProductPrice", "brandName", "subCategoryOf", "storeName", "orderType"
]

# -- Row-level validation rules: (column, label, predicate) --
ROW_VALIDATION_RULES = [
    ("totalProductPrice", "negative totalProductPrice",
        lambda v: pd.to_numeric(v, errors="coerce") < 0),
    ("quantity",          "zero/negative quantity",
        lambda v: pd.to_numeric(v, errors="coerce") <= 0),
    ("invoice",           "null/blank invoice",
        lambda v: v.isna() | (v.astype(str).str.strip() == "")),
]


# =============================================================================
# DOWNLOAD (unchanged)
# =============================================================================

class CSVDownloader:
    def __init__(self, base_url="https://api.example.in", username="user/pws", password="user/pws"):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.token    = None
        self.session  = requests.Session()

    def authenticate(self, retries=3, delay=5):
        for attempt in range(retries):
            print("Authenticating...")
            login_url = f"{self.base_url}/login"
            payload   = {"username": self.username, "password": self.password}
            try:
                response = self.session.post(login_url, json=payload, timeout=10)
            except requests.exceptions.RequestException as e:
                print("Request failed:", e)
                time.sleep(delay)
                continue

            if response.status_code in [200, 201]:
                data       = response.json()
                self.token = data.get("token")
                if self.token:
                    print("Authentication successful!")
                    return True
                else:
                    print("Error: No token received")
            else:
                print(f"Authentication failed: {response.status_code}")
                print(f"Response: {response.text}")

            print(f"Retrying in {delay} seconds... ({attempt+1}/{retries})")
            time.sleep(delay)

        print("Failed to authenticate after retries")
        return False

    def download_yesterday_csv(self, order_type="online"):
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Downloading yesterday's data: {yesterday}")
        return self.download_csv(order_type, yesterday, yesterday)

    def download_csv(self, order_type="online", from_date=None, to_date=None):
        if not self.token:
            if not self.authenticate():
                return None

        csv_url = f"{self.base_url}/orders/orderReportCSV"
        params  = {"orderType": order_type, "fromDate": from_date, "toDate": to_date}
        headers = {"accept": "*/*", "Authorization": self.token}

        print(f"Downloading CSV for {order_type} orders from {from_date} to {to_date}...")
        try:
            response = self.session.get(csv_url, params=params, headers=headers, timeout=30)
        except requests.exceptions.RequestException as e:
            print("Request failed:", e)
            return None

        if response.status_code != 200:
            print(f"Download failed: {response.status_code}")
            print(f"Response: {response.text}")
            return None

        content = response.content
        try:
            text_content = content.decode("utf-8")
        except UnicodeDecodeError:
            print("Binary content received, cannot decode as text")
            return None

        clean_content = re.sub(r"\s", "", text_content)
        try:
            decoded_bytes = base64.b64decode(clean_content, validate=True)
            decoded_text  = decoded_bytes.decode("utf-8")
            csv_data      = decoded_text
        except (binascii.Error, ValueError):
            csv_data = text_content

        df = pd.read_csv(io.StringIO(csv_data), low_memory=False)
        print(f"Downloaded {len(df)} rows")
        return df


# =============================================================================
# SCHEMA VALIDATION (unchanged)
# =============================================================================

def validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("SCHEMA VALIDATION")
    print("=" * 60)

    missing_cols = [c for c in REQUIRED_CSV_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"SCHEMA ERROR: Required columns missing from CSV:\n"
            f"  Missing : {missing_cols}\n"
            f"  Available: {df.columns.tolist()}"
        )
    print(f"✓ All {len(REQUIRED_CSV_COLUMNS)} required columns present.")

    total_rows      = len(df)
    bad_row_indices = set()

    for col, label, predicate in ROW_VALIDATION_RULES:
        if col not in df.columns:
            print(f"  ⚠ Skipping rule '{label}' — column '{col}' not found.")
            continue
        try:
            mask     = predicate(df[col])
            bad_rows = df[mask]
            count    = len(bad_rows)
            if count > 0:
                print(f"  ⚠ {count} row(s) flagged — {label}")
                print(f"    Sample bad values: {bad_rows[col].head(5).tolist()}")
                bad_row_indices.update(bad_rows.index.tolist())
            else:
                print(f"  ✓ No issues — {label}")
        except Exception as e:
            print(f"  ⚠ Rule '{label}' could not be evaluated: {e}")

    if bad_row_indices:
        df = df.drop(index=list(bad_row_indices)).reset_index(drop=True)
        print(f"\n  Dropped {len(bad_row_indices)} invalid row(s). "
              f"Remaining: {len(df)}/{total_rows} rows.")
    else:
        print(f"\n✓ All {total_rows} rows passed row-level validation.")

    if df.empty:
        raise ValueError(
            "SCHEMA ERROR: DataFrame is empty after validation — "
            "no valid rows to insert. Aborting pipeline."
        )

    print("✓ Schema validation passed.")
    print("=" * 60 + "\n")
    return df


# =============================================================================
# TRANSFORM (unchanged)
# =============================================================================

def debug_date_formats(df: pd.DataFrame):
    if "orderDate" in df.columns:
        print("\n=== DEBUGGING ORDERDATE COLUMN ===")
        print(f"Column exists: {'orderDate' in df.columns}")
        print(f"Total rows: {len(df)}")
        print(f"Non-null values: {df['orderDate'].notna().sum()}")
        print(f"Unique date formats (first 10):")
        sample_dates = df['orderDate'].dropna().head(10).tolist()
        for i, date_val in enumerate(sample_dates, 1):
            print(f"  {i}. '{date_val}' (type: {type(date_val)})")
        test_formats = ["%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]
        for fmt in test_formats:
            try:
                parsed = pd.to_datetime(df['orderDate'].dropna().iloc[0], format=fmt)
                print(f"✓ Format '{fmt}' works - parsed as: {parsed}")
            except:
                print(f"✗ Format '{fmt}' failed")
        print("=== END DEBUG ===\n")


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Starting data transformation...")
    debug_date_formats(df)

    if "productMrp" in df.columns:
        df = df.drop(columns=["productMrp"])
        print("Dropped column: productMrp")

    if "orderDate" in df.columns:
        print("Processing orderDate column...")
        original_count = df['orderDate'].notna().sum()
        print(f"Original non-null orderDate values: {original_count}")

        formats_to_try = ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]
        df['orderDate_parsed'] = None

        for fmt in formats_to_try:
            mask = df['orderDate_parsed'].isna() & df['orderDate'].notna()
            if mask.any():
                try:
                    parsed_dates      = pd.to_datetime(df.loc[mask, 'orderDate'], format=fmt, errors='coerce')
                    successful_parses = parsed_dates.notna().sum()
                    if successful_parses > 0:
                        print(f"Format '{fmt}' successfully parsed {successful_parses} dates")
                        df.loc[mask & parsed_dates.notna(), 'orderDate_parsed'] = parsed_dates[parsed_dates.notna()]
                except Exception as e:
                    print(f"Format '{fmt}' failed: {e}")

        if df['orderDate_parsed'].isna().all():
            print("Trying pandas auto-detection...")
            df['orderDate_parsed'] = pd.to_datetime(df['orderDate'], errors='coerce', infer_datetime_format=True)

        df['orderDate'] = df['orderDate_parsed'].apply(lambda x: x.date() if pd.notnull(x) else None)
        df = df.drop(columns=['orderDate_parsed'])

        final_count = df['orderDate'].notna().sum()
        print(f"Final non-null orderDate values: {final_count}")
        print(f"Successfully converted: {final_count}/{original_count} dates")
        sample_converted = df[df['orderDate'].notna()]['orderDate'].head(5).tolist()
        print(f"Sample converted dates: {sample_converted}")

    if "time" in df.columns:
        print("Processing time column...")
        df["time"] = df["time"].astype(str).str[:8]
        df["time"] = df["time"].replace(["nan", "NaT"], None)
        print(f"Sample time values: {df[df['time'].notna()]['time'].head(3).tolist()}")

    print("Processing numeric columns...")
    for col in INTEGER_COLUMNS + BIGINT_COLUMNS:
        if col in df.columns:
            original_count = df[col].notna().sum()
            df[col]        = pd.to_numeric(df[col], errors="coerce")
            df[col]        = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
            final_count    = df[col].notna().sum()
            print(f"  {col}: {final_count}/{original_count} values converted")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            original_count = df[col].notna().sum()
            df[col]        = pd.to_numeric(df[col], errors="coerce")
            df[col]        = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
            final_count    = df[col].notna().sum()
            print(f"  {col}: {final_count}/{original_count} values converted")

    print("Processing string columns...")
    string_cols = [col for col in df.columns
                   if col in POSTGRES_COLUMNS
                   and col not in INTEGER_COLUMNS + BIGINT_COLUMNS + NUMERIC_COLUMNS + ["orderDate", "time"]]
    for col in string_cols:
        df[col] = df[col].astype(str).replace("nan", None)

    existing_cols = [col for col in POSTGRES_COLUMNS if col in df.columns]
    df = df[existing_cols]
    for col in POSTGRES_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[POSTGRES_COLUMNS]

    print("Data transformation completed.")
    print(f"\nFINAL DATA SUMMARY:")
    print(f"Total rows: {len(df)}")
    print(f"orderDate not null: {df['orderDate'].notna().sum()}")
    print(f"time not null: {df['time'].notna().sum()}")
    return df


# =============================================================================
# IDEMPOTENCY HELPERS (unchanged)
# =============================================================================

def get_dates_in_dataframe(df: pd.DataFrame) -> list:
    return df['orderDate'].dropna().unique().tolist()


def delete_existing_billing_data_for_dates(cur, dates: list):
    if not dates:
        print("No dates found in data — skipping delete step.")
        return
    print(f"Checking and clearing existing billing_data for dates: {dates}")
    placeholders = ",".join(["%s"] * len(dates))
    cur.execute(
        f'DELETE FROM billing_data WHERE "orderDate" IN ({placeholders})',
        dates
    )
    deleted_rows = cur.rowcount
    if deleted_rows > 0:
        print(f"Deleted {deleted_rows} existing rows from billing_data (overwrite mode).")
    else:
        print("No existing rows found in billing_data for those dates — clean insert.")


# =============================================================================
# LOAD (unchanged)
# =============================================================================

def load_to_postgres_bulk(df: pd.DataFrame):
    """Optimized bulk insert using execute_values with idempotency check."""
    conn = None
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()

        dates_in_data = get_dates_in_dataframe(df)
        delete_existing_billing_data_for_dates(cur, dates_in_data)

        print("Preparing bulk insert...")
        cols        = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS])
        data_tuples = [tuple(row) for row in df.values]
        insert_sql  = f'INSERT INTO billing_data ({cols}) VALUES %s'
        print(f"Inserting {len(data_tuples)} rows in bulk...")

        psycopg2.extras.execute_values(
            cur, insert_sql, data_tuples, template=None, page_size=1000
        )
        conn.commit()
        print(f"Successfully inserted {len(df)} rows into billing_data")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Failed to insert data: {e}")
        if conn:
            conn.rollback()


# =============================================================================
# MAIN — Full Bronze / Silver / Gold + PostgreSQL pipeline
# =============================================================================

def main(
    order_type: str = "online",
    date_str: str = None,
    replay_from_blob: str = None,
):
    """
    Run the full ETL + Data Lake pipeline.

    Parameters
    ----------
    order_type       : "online" or "offline".
    date_str         : ISO date to process (default: yesterday).
    replay_from_blob : Bronze blob name to replay from — skips API call.
                       e.g. "bronze/year=2026/month=03/day=25/online_20260326_191310.csv"
    """
    start_time = time.time()

    if date_str is None:
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n{'#'*60}")
    print(f"  ETL PIPELINE START — {date_str} / {order_type}")
    print(f"{'#'*60}\n")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 1 — Obtain raw DataFrame (API or Bronze replay)
    # ──────────────────────────────────────────────────────────────────────────
    if replay_from_blob:
        print(f"[REPLAY MODE] Loading Bronze blob: {replay_from_blob}")
        df_raw           = azure_staging.bronze_download(replay_from_blob)
        bronze_blob_name = replay_from_blob
        t_download       = time.time()
    else:
        downloader = CSVDownloader()
        df_raw = downloader.download_csv(
            order_type=order_type,
            from_date=date_str,
            to_date=date_str,
        )
        if df_raw is None or df_raw.empty:
            print("No data downloaded from API — aborting pipeline.")
            return
        t_download = time.time()
        print(f"Download completed in {t_download - start_time:.2f} seconds")

        # ── STEP 2 — BRONZE: Stage raw CSV ────────────────────────────────────
        bronze_blob_name = azure_staging.bronze_upload(df_raw, order_type, date_str)
        t_bronze = time.time()
        print(f"Bronze upload completed in {t_bronze - t_download:.2f} seconds")

        # ── STEP 3 — BRONZE: Download back as authoritative source ────────────
        df_raw = azure_staging.bronze_download(bronze_blob_name)
        t_bronze_dl = time.time()
        print(f"Bronze round-trip completed in {t_bronze_dl - t_bronze:.2f} seconds")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 4 — Schema validation (unchanged)
    # ──────────────────────────────────────────────────────────────────────────
    df = validate_schema(df_raw)
    t_validate = time.time()
    print(f"Schema validation completed in {t_validate - t_download:.2f} seconds")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 5 — Transform (unchanged)
    # ──────────────────────────────────────────────────────────────────────────
    df = transform_data(df)
    t_transform = time.time()
    print(f"Transform completed in {t_transform - t_validate:.2f} seconds")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 6 — SILVER: Write validated + cleaned data as Parquet  ← NEW
    # ──────────────────────────────────────────────────────────────────────────
    silver_blob = azure_staging.silver_write(df, date_str)
    t_silver = time.time()
    print(f"Silver write completed in {t_silver - t_transform:.2f} seconds")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 7 — Load billing_data with idempotency (unchanged)
    # ──────────────────────────────────────────────────────────────────────────
    load_to_postgres_bulk(df)
    t_billing = time.time()
    print(f"Billing data load completed in {t_billing - t_silver:.2f} seconds")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 8 — Load aggregate tables with idempotency (unchanged)
    # ──────────────────────────────────────────────────────────────────────────
    aggregate.load_aggregates_to_postgres(df)
    t_agg = time.time()
    print(f"Aggregate inserts completed in {t_agg - t_billing:.2f} seconds")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 9 — GOLD: Write analytics aggregates as Parquet  ← NEW
    # ──────────────────────────────────────────────────────────────────────────
    gold_blobs = azure_staging.gold_write_aggregates(df, date_str)
    t_gold = time.time()
    print(f"Gold write completed in {t_gold - t_agg:.2f} seconds")

    # ──────────────────────────────────────────────────────────────────────────
    # SUMMARY
    # ──────────────────────────────────────────────────────────────────────────
    total_time = t_gold - start_time
    print(f"\n{'#'*60}")
    print(f"  ETL + DATA LAKE COMPLETE")
    print(f"  Bronze  : {bronze_blob_name}")
    print(f"  Silver  : {silver_blob}")
    print(f"  Gold    : {len(gold_blobs)} tables written")
    for tbl, blob in gold_blobs.items():
        print(f"            {tbl:<20} → {blob}")
    print(f"  Total   : {total_time:.2f} seconds")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the ETL + Data Lake pipeline")
    parser.add_argument("--order-type",  default="online",
                        help="online or offline (default: online)")
    parser.add_argument("--date",        default=None,
                        help="ISO date e.g. 2026-03-26 (default: yesterday)")
    parser.add_argument("--replay-blob", default=None,
                        help="Replay from a specific Bronze blob — skips API call")
    args = parser.parse_args()

    main(
        order_type       = args.order_type,
        date_str         = args.date,
        replay_from_blob = args.replay_blob,
    )