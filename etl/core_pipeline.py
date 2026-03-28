"""
core_pipeline.py
────────────────────────────────────────────────────────────────────────────────
ETL Pipeline: API → Bronze → Silver → Gold (Data Lake) + PostgreSQL

Full flow per run:
  1.  Authenticate + download CSV from source API
  2.  BRONZE  — Split into chunks, upload as Parquet in parallel     [lake]
  3.  BRONZE  — Download chunks back as authoritative source         [lake]
  4.  Schema validation
  5.  Transform
  6.  SILVER  — Write validated + cleaned data as Parquet            [lake]
  7.  Load billing_data via COPY (idempotent)
  8.  Load aggregate tables via DuckDB + COPY (idempotent)
  9.  GOLD    — Write analytics aggregates as Parquet                [lake]

Upgrades vs previous version:
  - Checkpoint-based resumability: crashed runs resume from last completed step
  - Bronze uploads chunked Parquet in parallel (ThreadPoolExecutor)
  - billing_data loaded via Postgres COPY (5-10x faster than execute_values)
  - Timing anchor corrected: validation time no longer absorbs bronze latency
  - load_to_postgres_bulk() raises on failure so Gold write is never reached
    when Postgres insert failed
────────────────────────────────────────────────────────────────────────────────
"""

import requests
import base64
import binascii
import re
import io
import sys
import duckdb
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import time
import os
import aggregate
import azure_staging
from checkpoint import Checkpoint
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

REQUIRED_CSV_COLUMNS = [
    "invoice", "orderDate", "productId", "productName", "quantity",
    "totalProductPrice", "brandName", "subCategoryOf", "storeName", "orderType"
]

ROW_VALIDATION_RULES = [
    ("totalProductPrice", "negative totalProductPrice",
        lambda v: pd.to_numeric(v, errors="coerce") < 0),
    ("quantity",          "zero/negative quantity",
        lambda v: pd.to_numeric(v, errors="coerce") <= 0),
    ("invoice",           "null/blank invoice",
        lambda v: v.isna() | (v.astype(str).str.strip() == "")),
]


# =============================================================================
# DOWNLOAD
# =============================================================================

class CSVDownloader:
    def __init__(
        self,
        base_url: str = "https://api.example.in",
        username: str = None,
        password: str = None,
    ):
        self.base_url = base_url
        self.username = username or require_env("API_USERNAME")
        self.password = password or require_env("API_PASSWORD")
        self.token    = None
        self.session  = requests.Session()

    def authenticate(self, retries=3, delay=5):
        for attempt in range(retries):
            print("Authenticating...")
            try:
                response = self.session.post(
                    f"{self.base_url}/login",
                    json={"username": self.username, "password": self.password},
                    timeout=10,
                )
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                time.sleep(delay)
                continue

            if response.status_code in [200, 201]:
                self.token = response.json().get("token")
                if self.token:
                    print("Authentication successful!")
                    return True
                print("Error: No token received")
            else:
                print(f"Authentication failed: {response.status_code} — {response.text}")

            print(f"Retrying in {delay}s... ({attempt+1}/{retries})")
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

        print(f"Downloading CSV for {order_type} orders from {from_date} to {to_date}...")
        try:
            response = self.session.get(
                f"{self.base_url}/orders/orderReportCSV",
                params={"orderType": order_type, "fromDate": from_date, "toDate": to_date},
                headers={"accept": "*/*", "Authorization": self.token},
                timeout=30,
            )
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None

        if response.status_code != 200:
            print(f"Download failed: {response.status_code} — {response.text}")
            return None

        try:
            text_content = response.content.decode("utf-8")
        except UnicodeDecodeError:
            print("Binary content received, cannot decode as UTF-8")
            return None

        clean = re.sub(r"\s", "", text_content)
        try:
            csv_data = base64.b64decode(clean, validate=True).decode("utf-8")
        except (binascii.Error, ValueError):
            csv_data = text_content

        df = pd.read_csv(io.StringIO(csv_data), low_memory=False)
        print(f"Downloaded {len(df):,} rows")
        return df


# =============================================================================
# SCHEMA VALIDATION
# =============================================================================

def validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("SCHEMA VALIDATION")
    print("=" * 60)

    missing_cols = [c for c in REQUIRED_CSV_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"SCHEMA ERROR: Required columns missing from CSV:\n"
            f"  Missing  : {missing_cols}\n"
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
                print(f"    Sample: {bad_rows[col].head(5).tolist()}")
                bad_row_indices.update(bad_rows.index.tolist())
            else:
                print(f"  ✓ No issues — {label}")
        except Exception as e:
            print(f"  ⚠ Rule '{label}' could not be evaluated: {e}")

    if bad_row_indices:
        df = df.drop(index=list(bad_row_indices)).reset_index(drop=True)
        print(f"\n  Dropped {len(bad_row_indices)} invalid row(s). "
              f"Remaining: {len(df):,}/{total_rows:,}")
    else:
        print(f"\n✓ All {total_rows:,} rows passed row-level validation.")

    # DuckDB: confirm non-empty after dropping bad rows
    con = duckdb.connect()
    con.register("_validated", df)
    remaining = con.execute("SELECT COUNT(*) FROM _validated").fetchone()[0]
    con.close()

    if remaining == 0:
        raise ValueError(
            "SCHEMA ERROR: DataFrame is empty after validation — "
            "no valid rows to insert. Aborting pipeline."
        )

    print("✓ Schema validation passed.")
    print("=" * 60 + "\n")
    return df


# =============================================================================
# TRANSFORM
# Date parsing requires multi-format pandas iteration — kept as-is.
# DuckDB used for the final summary stats block.
# =============================================================================

def debug_date_formats(df: pd.DataFrame):
    if "orderDate" not in df.columns:
        return
    print("\n=== DEBUGGING ORDERDATE COLUMN ===")
    con = duckdb.connect()
    con.register("_dbg", df)
    non_null = con.execute(
        "SELECT COUNT(*) FROM _dbg WHERE orderDate IS NOT NULL"
    ).fetchone()[0]
    con.close()
    print(f"Total rows: {len(df):,}  |  Non-null orderDate: {non_null:,}")
    sample_dates = df['orderDate'].dropna().head(10).tolist()
    for i, v in enumerate(sample_dates, 1):
        print(f"  {i}. '{v}' (type: {type(v).__name__})")
    for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]:
        try:
            parsed = pd.to_datetime(df['orderDate'].dropna().iloc[0], format=fmt)
            print(f"✓ Format '{fmt}' works → {parsed}")
        except Exception:
            print(f"✗ Format '{fmt}' failed")
    print("=== END DEBUG ===\n")


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Starting data transformation...")
    debug_date_formats(df)

    if "productMrp" in df.columns:
        df = df.drop(columns=["productMrp"])
        print("Dropped column: productMrp")

    # ── orderDate: multi-format parse ─────────────────────────────────────────
    if "orderDate" in df.columns:
        print("Processing orderDate column...")
        original_count = df['orderDate'].notna().sum()
        df['orderDate_parsed'] = None

        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]:
            mask = df['orderDate_parsed'].isna() & df['orderDate'].notna()
            if mask.any():
                try:
                    parsed = pd.to_datetime(df.loc[mask, 'orderDate'], format=fmt, errors='coerce')
                    ok     = parsed.notna().sum()
                    if ok > 0:
                        print(f"  Format '{fmt}' parsed {ok:,} dates")
                        df.loc[mask & parsed.notna(), 'orderDate_parsed'] = parsed[parsed.notna()]
                except Exception as e:
                    print(f"  Format '{fmt}' failed: {e}")

        if df['orderDate_parsed'].isna().all():
            print("  Trying pandas auto-detection...")
            df['orderDate_parsed'] = pd.to_datetime(
                df['orderDate'], errors='coerce', infer_datetime_format=True
            )

        df['orderDate'] = df['orderDate_parsed'].apply(
            lambda x: x.date() if pd.notnull(x) else None
        )
        df = df.drop(columns=['orderDate_parsed'])
        final = df['orderDate'].notna().sum()
        print(f"  Converted: {final:,}/{original_count:,} dates")
        print(f"  Sample   : {df[df['orderDate'].notna()]['orderDate'].head(5).tolist()}")

    # ── time column ───────────────────────────────────────────────────────────
    if "time" in df.columns:
        print("Processing time column...")
        df["time"] = df["time"].astype(str).str[:8]
        df["time"] = df["time"].replace(["nan", "NaT"], None)

    # ── Numeric type coercion ─────────────────────────────────────────────────
    print("Processing numeric columns...")
    for col in INTEGER_COLUMNS + BIGINT_COLUMNS:
        if col in df.columns:
            orig = df[col].notna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
            print(f"  {col}: {df[col].notna().sum():,}/{orig:,} converted")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            orig = df[col].notna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
            print(f"  {col}: {df[col].notna().sum():,}/{orig:,} converted")

    # ── String columns ────────────────────────────────────────────────────────
    print("Processing string columns...")
    skip = set(INTEGER_COLUMNS + BIGINT_COLUMNS + NUMERIC_COLUMNS + ["orderDate", "time"])
    for col in df.columns:
        if col in POSTGRES_COLUMNS and col not in skip:
            df[col] = df[col].astype(str).replace("nan", None)

    # ── Column alignment to Postgres schema ───────────────────────────────────
    existing = [c for c in POSTGRES_COLUMNS if c in df.columns]
    df = df[existing]
    for col in POSTGRES_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[POSTGRES_COLUMNS]

    # ── DuckDB: final summary stats ───────────────────────────────────────────
    con = duckdb.connect()
    con.register("_transformed", df)
    summary = con.execute("""
        SELECT
            COUNT(*)                                              AS total_rows,
            COUNT(orderDate)                                      AS orderdate_nn,
            COUNT(time)                                           AS time_nn,
            COUNT(DISTINCT storeName)                             AS unique_stores,
            ROUND(SUM(TRY_CAST(totalProductPrice AS DOUBLE)), 2) AS total_price
        FROM _transformed
    """).fetchone()
    con.close()

    total, odate, ttime, stores, price = summary
    print(f"\nFINAL DATA SUMMARY:")
    print(f"  Total rows        : {total:,}")
    print(f"  orderDate non-null: {odate:,}")
    print(f"  time non-null     : {ttime:,}")
    print(f"  Unique stores     : {stores:,}")
    print(f"  Total price sum   : {price:,}")
    print("Data transformation completed.\n")
    return df


# =============================================================================
# IDEMPOTENCY HELPERS
# =============================================================================

def get_dates_in_dataframe(df: pd.DataFrame) -> list:
    """DuckDB: extract distinct non-null orderDate values."""
    con = duckdb.connect()
    con.register("_dates", df)
    dates = [r[0] for r in con.execute(
        "SELECT DISTINCT orderDate FROM _dates WHERE orderDate IS NOT NULL"
    ).fetchall()]
    con.close()
    return dates


def delete_existing_billing_data_for_dates(cur, dates: list):
    if not dates:
        print("No dates found — skipping billing_data delete.")
        return
    print(f"Clearing existing billing_data for dates: {dates}")
    placeholders = ",".join(["%s"] * len(dates))
    cur.execute(
        f'DELETE FROM billing_data WHERE "orderDate" IN ({placeholders})', dates
    )
    n = cur.rowcount
    if n > 0:
        print(f"  Deleted {n:,} existing rows (overwrite mode).")
    else:
        print("  No existing rows found — clean insert.")


# =============================================================================
# LOAD — Postgres COPY (replaces execute_values)
# =============================================================================

def load_to_postgres_bulk(df: pd.DataFrame):
    """
    Bulk-load billing_data using Postgres COPY FROM STDIN.
    5-10x faster than execute_values for large datasets.
    Idempotent: deletes existing rows for the same dates before copying.
    Raises on failure so the caller (main) knows not to proceed to Gold.
    """
    conn = None
    try:
        print("Connecting to database (billing_data COPY load)...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()

        # Idempotency delete
        dates = get_dates_in_dataframe(df)
        delete_existing_billing_data_for_dates(cur, dates)
        conn.commit()

        # COPY load
        buf = io.StringIO()
        df[POSTGRES_COLUMNS].to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)

        col_sql = ", ".join([f'"{c}"' for c in POSTGRES_COLUMNS])
        print(f"COPY → billing_data ({len(df):,} rows)...")
        cur.copy_expert(
            f"COPY billing_data ({col_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
            buf,
        )
        conn.commit()
        print(f"✓ COPY complete — {cur.rowcount:,} rows inserted into billing_data")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ billing_data load failed: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        raise   # propagate — prevents Gold write on Postgres failure


# =============================================================================
# MAIN — Full pipeline with checkpoint resumability
# =============================================================================

def main(
    order_type: str = "online",
    date_str: str = None,
    replay_from_blob: str = None,
    force_rerun: bool = False,
):
    """
    Run the full ETL + Data Lake pipeline with checkpoint-based resumability.

    Parameters
    ----------
    order_type       : 'online' or 'offline'.
    date_str         : ISO date to process (default: yesterday).
    replay_from_blob : Bronze blob prefix to replay from — skips API + upload.
                       e.g. 'bronze/year=2026/month=03/day=26/online/20260326_191310/'
    force_rerun      : If True, clears all checkpoints and runs every step.
    """
    start_time = time.time()

    if date_str is None:
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n{'#'*60}")
    print(f"  ETL PIPELINE START — {date_str} / {order_type}")
    print(f"{'#'*60}")

    # ── Checkpoint setup ──────────────────────────────────────────────────────
    cp = Checkpoint(run_date=date_str, order_type=order_type)
    if force_rerun:
        print("  [force_rerun=True] Clearing all checkpoints...")
        cp.clear()

    # Carry state across checkpointed steps
    bronze_blob_names = None
    silver_blob       = None
    gold_blobs        = {}
    df_raw            = None
    df                = None

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 1 — API download  (skipped entirely in replay mode)
    # ──────────────────────────────────────────────────────────────────────────
    if replay_from_blob:
        print(f"\n[REPLAY MODE] Blob prefix: {replay_from_blob}")
        bronze_blob_names = replay_from_blob   # prefix string
    else:
        if not cp.is_done("bronze_upload"):
            print("\n── STEP 1: API Download + Bronze upload ─────────────────")
            downloader = CSVDownloader()
            df_raw = downloader.download_csv(
                order_type=order_type, from_date=date_str, to_date=date_str
            )
            if df_raw is None or df_raw.empty:
                print("No data downloaded from API — aborting.")
                sys.exit(1)
            t_dl = time.time()
            print(f"  Download: {time.time() - start_time:.2f}s")

            # ── STEP 2 — BRONZE: chunked parallel Parquet upload ──────────────
            bronze_blob_names = azure_staging.bronze_upload(df_raw, order_type, date_str)
            cp.mark_done("bronze_upload", detail=str(bronze_blob_names[0]))
            print(f"  Bronze upload: {time.time() - t_dl:.2f}s")
        else:
            print("\n  ⏭  bronze_upload — resuming, discovering latest blobs...")
            bronze_blob_names = azure_staging.bronze_get_latest(order_type, date_str)
            if not bronze_blob_names:
                raise RuntimeError("Checkpoint says bronze_upload done but no blobs found.")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 3 — BRONZE: download authoritative source
    # ──────────────────────────────────────────────────────────────────────────
    t3 = time.time()
    print("\n── STEP 3: Bronze download ──────────────────────────────")
    df_raw = azure_staging.bronze_download(bronze_blob_names)
    cp.mark_done("bronze_download")
    print(f"  Bronze download: {time.time() - t3:.2f}s")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 4 — Schema validation
    # ──────────────────────────────────────────────────────────────────────────
    if not cp.is_done("schema_validation"):
        print("\n── STEP 4: Schema validation ────────────────────────────")
        t4 = time.time()
        df = validate_schema(df_raw)
        cp.mark_done("schema_validation", detail=f"{len(df)} rows passed")
        print(f"  Validation: {time.time() - t4:.2f}s")
    else:
        print("\n  ⏭  schema_validation — running anyway (transform needs df)...")
        df = validate_schema(df_raw)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 5 — Transform
    # ──────────────────────────────────────────────────────────────────────────
    if not cp.is_done("transform"):
        print("\n── STEP 5: Transform ────────────────────────────────────")
        t5 = time.time()
        df = transform_data(df)
        cp.mark_done("transform", detail=f"{len(df)} rows transformed")
        print(f"  Transform: {time.time() - t5:.2f}s")
    else:
        print("\n  ⏭  transform — running anyway (Silver/Postgres need df)...")
        df = transform_data(df)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 6 — SILVER: write Parquet
    # ──────────────────────────────────────────────────────────────────────────
    if not cp.is_done("silver_write"):
        print("\n── STEP 6: Silver write ─────────────────────────────────")
        t6 = time.time()
        silver_blob = azure_staging.silver_write(df, date_str)
        cp.mark_done("silver_write", detail=silver_blob)
        print(f"  Silver write: {time.time() - t6:.2f}s")
    else:
        print(f"\n  ⏭  silver_write — already done")
        silver_blob = f"silver/sales/{date_str.replace('-','/')}/part-0.parquet"

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 7 — Load billing_data (COPY)
    # ──────────────────────────────────────────────────────────────────────────
    if not cp.is_done("billing_data_load"):
        print("\n── STEP 7: billing_data COPY load ───────────────────────")
        t7 = time.time()
        load_to_postgres_bulk(df)
        cp.mark_done("billing_data_load", detail=f"{len(df)} rows")
        print(f"  Billing load: {time.time() - t7:.2f}s")
    else:
        print(f"\n  ⏭  billing_data_load — already done")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 8 — Aggregate tables (DuckDB + COPY)
    # ──────────────────────────────────────────────────────────────────────────
    if not cp.is_done("aggregate_load"):
        print("\n── STEP 8: Aggregate tables load ────────────────────────")
        t8 = time.time()
        aggregate.load_aggregates_to_postgres(df)
        cp.mark_done("aggregate_load")
        print(f"  Aggregate load: {time.time() - t8:.2f}s")
    else:
        print(f"\n  ⏭  aggregate_load — already done")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 9 — GOLD: write Parquet aggregates
    # ──────────────────────────────────────────────────────────────────────────
    if not cp.is_done("gold_write"):
        print("\n── STEP 9: Gold write ───────────────────────────────────")
        t9 = time.time()
        gold_blobs = azure_staging.gold_write_aggregates(df, date_str)
        cp.mark_done("gold_write", detail=f"{len(gold_blobs)} tables")
        print(f"  Gold write: {time.time() - t9:.2f}s")
    else:
        print(f"\n  ⏭  gold_write — already done")

    # ──────────────────────────────────────────────────────────────────────────
    # SUMMARY
    # ──────────────────────────────────────────────────────────────────────────
    total_time = time.time() - start_time
    print(f"\n{'#'*60}")
    print(f"  ETL + DATA LAKE COMPLETE")
    print(f"  Date       : {date_str} / {order_type}")
    print(f"  Bronze     : {bronze_blob_names}")
    print(f"  Silver     : {silver_blob}")
    if gold_blobs:
        print(f"  Gold       : {len(gold_blobs)} tables written")
        for tbl, blob in gold_blobs.items():
            print(f"               {tbl:<20} → {blob}")
    print(f"  Total time : {total_time:.2f}s")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the ETL + Data Lake pipeline")
    parser.add_argument("--order-type",   default="online",
                        help="online or offline (default: online)")
    parser.add_argument("--date",         default=None,
                        help="ISO date e.g. 2026-03-26 (default: yesterday)")
    parser.add_argument("--replay-blob",  default=None,
                        help="Bronze blob prefix to replay from — skips API + upload")
    parser.add_argument("--force-rerun",  action="store_true",
                        help="Clear all checkpoints and re-run every step")
    args = parser.parse_args()

    main(
        order_type       = args.order_type,
        date_str         = args.date,
        replay_from_blob = args.replay_blob,
        force_rerun      = args.force_rerun,
    )