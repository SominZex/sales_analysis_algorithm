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
import agg_insert
from dotenv import load_dotenv

load_dotenv()

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


DB_CONFIG = {
    "host": require_env("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": require_env("DB_NAME"),
    "user": require_env("DB_USER"),
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
BIGINT_COLUMNS = ["barcode"]
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


class CSVDownloader:
    def __init__(self, base_url="https://api.example.in", username="user", password="pw"):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.token = None
        self.session = requests.Session()

    def authenticate(self, retries=3, delay=5):
        for attempt in range(retries):
            print("Authenticating...")
            login_url = f"{self.base_url}/login"
            payload = {"username": self.username, "password": self.password}
            try:
                response = self.session.post(login_url, json=payload, timeout=10)
            except requests.exceptions.RequestException as e:
                print("Request failed:", e)
                time.sleep(delay)
                continue

            if response.status_code in [200, 201]:
                data = response.json()
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
        params = {"orderType": order_type, "fromDate": from_date, "toDate": to_date}
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
            decoded_text = decoded_bytes.decode("utf-8")
            csv_data = decoded_text
        except (binascii.Error, ValueError):
            csv_data = text_content

        df = pd.read_csv(io.StringIO(csv_data))
        print(f"Downloaded {len(df)} rows")
        return df


# =============================================================================
# SCHEMA VALIDATION
# =============================================================================

def validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run before transform_data():
      1. Hard stop if any required columns are missing from the CSV.
      2. Drop rows that fail row-level rules and print a report.
      3. Hard stop if the DataFrame is empty after cleaning.
    Returns the cleaned DataFrame.
    """
    print("\n" + "=" * 60)
    print("SCHEMA VALIDATION")
    print("=" * 60)

    # 1. Required column presence
    missing_cols = [c for c in REQUIRED_CSV_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"SCHEMA ERROR: Required columns missing from CSV:\n"
            f"  Missing : {missing_cols}\n"
            f"  Available: {df.columns.tolist()}"
        )
    print(f"✓ All {len(REQUIRED_CSV_COLUMNS)} required columns present.")

    # 2. Row-level validation
    total_rows = len(df)
    bad_row_indices = set()

    for col, label, predicate in ROW_VALIDATION_RULES:
        if col not in df.columns:
            print(f"  ⚠ Skipping rule '{label}' — column '{col}' not found.")
            continue
        try:
            mask = predicate(df[col])
            bad_rows = df[mask]
            count = len(bad_rows)
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

    # 3. Hard stop if nothing left
    if df.empty:
        raise ValueError(
            "SCHEMA ERROR: DataFrame is empty after validation — "
            "no valid rows to insert. Aborting pipeline."
        )

    print("✓ Schema validation passed.")
    print("=" * 60 + "\n")
    return df


# =============================================================================
# TRANSFORM
# =============================================================================

def debug_date_formats(df: pd.DataFrame):
    """Debug function to understand the date formats in the data"""
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

    # Enhanced orderDate processing with multiple format support
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
                    parsed_dates = pd.to_datetime(df.loc[mask, 'orderDate'], format=fmt, errors='coerce')
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
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
            final_count = df[col].notna().sum()
            print(f"  {col}: {final_count}/{original_count} values converted")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            original_count = df[col].notna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
            final_count = df[col].notna().sum()
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
# IDEMPOTENCY HELPERS
# =============================================================================

def get_dates_in_dataframe(df: pd.DataFrame) -> list:
    """Extract the unique dates present in the DataFrame's orderDate column."""
    return df['orderDate'].dropna().unique().tolist()


def delete_existing_billing_data_for_dates(cur, dates: list):
    """
    Delete existing rows in billing_data for the given dates.
    This ensures re-runs don't duplicate data — existing data is overwritten.
    """
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
# LOAD
# =============================================================================

def load_to_postgres_bulk(df: pd.DataFrame):
    """Optimized bulk insert using execute_values with idempotency check."""
    conn = None
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # --- IDEMPOTENCY: Delete existing rows for the same dates before inserting ---
        dates_in_data = get_dates_in_dataframe(df)
        delete_existing_billing_data_for_dates(cur, dates_in_data)
        # -----------------------------------------------------------------------------

        print("Preparing bulk insert...")

        cols = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS])
        data_tuples = [tuple(row) for row in df.values]
        insert_sql = f'INSERT INTO billing_data ({cols}) VALUES %s'

        print(f"Inserting {len(data_tuples)} rows in bulk...")

        psycopg2.extras.execute_values(
            cur,
            insert_sql,
            data_tuples,
            template=None,
            page_size=1000
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
# MAIN
# =============================================================================

def main():
    start_time = time.time()

    downloader = CSVDownloader(
        base_url=require_env("API_BASE_URL"),
        username=require_env("API_USERNAME"),
        password=require_env("API_PASSWORD")
    )
    df = downloader.download_yesterday_csv(order_type="online")

    if df is not None and not df.empty:
        download_time = time.time()
        print(f"Download completed in {download_time - start_time:.2f} seconds")

        # 1. Schema validation (before transform)
        df = validate_schema(df)
        validate_time = time.time()
        print(f"Schema validation completed in {validate_time - download_time:.2f} seconds")

        # 2. Transform
        df = transform_data(df)
        transform_time = time.time()
        print(f"Transform completed in {transform_time - validate_time:.2f} seconds")

        # 3. Load billing_data (with idempotency)
        load_to_postgres_bulk(df)
        billing_insert_time = time.time()
        print(f"Billing data load completed in {billing_insert_time - transform_time:.2f} seconds")

        # 4. Load aggregate tables (with idempotency)
        agg_insert.load_aggregates_to_postgres(df)
        aggregates_insert_time = time.time()
        print(f"Aggregate inserts completed in {aggregates_insert_time - billing_insert_time:.2f} seconds")

        print(f"Total ETL execution time: {aggregates_insert_time - start_time:.2f} seconds")
    else:
        print("No data downloaded")

if __name__ == "__main__":
    main()