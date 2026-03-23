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

JDBC_URL = (
    f"jdbc:postgresql://{require_env('DB_HOST')}:"
    f"{os.getenv('DB_PORT', 5432)}/{require_env('DB_NAME')}"
)

JDBC_PROPERTIES = {
    "user": require_env("DB_USER"),
    "password": require_env("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}


# =============================================================================
# ENGINE SELECTOR
# -----------------------------------------------------------------------------
# Automatically decides whether to use Pandas or PySpark based on the current
# row count of billing_data in PostgreSQL.
#
# How it works:
#   1. At startup, query COUNT(*) from billing_data
#   2. If row count < PANDAS_ROW_THRESHOLD  → run full Pandas pipeline
#   3. If row count >= PANDAS_ROW_THRESHOLD → run full PySpark pipeline
#
# Configure the threshold in your .env file:
#   PANDAS_ROW_THRESHOLD=500000   (default: 500,000 rows)
#
# You can also force a specific engine by setting USE_ENGINE in .env:
#   USE_ENGINE=pandas    → always use Pandas regardless of row count
#   USE_ENGINE=pyspark   → always use PySpark regardless of row count
#   USE_ENGINE=auto      → automatic based on row count (default)
# =============================================================================

PANDAS_ROW_THRESHOLD = int(os.getenv("PANDAS_ROW_THRESHOLD", "500000"))


def get_billing_row_count() -> int:
    """Query current row count of billing_data for engine selection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM billing_data')
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"⚠️  Could not query billing_data row count: {e}")
        print("    Defaulting to Pandas engine.")
        return 0


def select_engine() -> str:
    """
    Determine which engine to use.
    Returns 'pandas' or 'pyspark'.
    """
    forced = os.getenv("USE_ENGINE", "auto").strip().lower()
    if forced in ("pandas", "pyspark"):
        print(f"\n{'='*60}")
        print(f"ENGINE: Forced to '{forced.upper()}' via USE_ENGINE env variable")
        print(f"{'='*60}\n")
        return forced

    row_count = get_billing_row_count()
    print(f"\n{'='*60}")
    print(f"ENGINE SELECTION")
    print(f"  Current billing_data rows : {row_count:,}")
    print(f"  Pandas threshold          : {PANDAS_ROW_THRESHOLD:,}")

    if row_count < PANDAS_ROW_THRESHOLD:
        engine = "pandas"
        print(f"  Decision                  : PANDAS ✅  (rows below threshold)")
    else:
        engine = "pyspark"
        print(f"  Decision                  : PYSPARK 🚀  (rows at or above threshold)")
    print(f"{'='*60}\n")
    return engine


# =============================================================================
# COLUMN DEFINITIONS
# =============================================================================

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

# -- Row-level validation rules --
# Pandas format: (column, label, pandas_predicate)
PANDAS_ROW_VALIDATION_RULES = [
    ("totalProductPrice", "negative totalProductPrice",
        lambda v: pd.to_numeric(v, errors="coerce") < 0),
    ("quantity",          "zero/negative quantity",
        lambda v: pd.to_numeric(v, errors="coerce") <= 0),
    ("invoice",           "null/blank invoice",
        lambda v: v.isna() | (v.astype(str).str.strip() == "")),
]


# =============================================================================
# DOWNLOADER  (unchanged — API interaction always stays in Python/requests)
# =============================================================================

class CSVDownloader:
    def __init__(self, base_url="https://api.example.in", username="username", password="pwd"):
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
            csv_data = decoded_text
        except (binascii.Error, ValueError):
            csv_data = text_content

        df = pd.read_csv(io.StringIO(csv_data))
        print(f"Downloaded {len(df)} rows")
        return df


# =============================================================================
# ████████████████████████████████████████████████████████████████████████████
#  PANDAS PATH
# ████████████████████████████████████████████████████████████████████████████
# =============================================================================

def pandas_validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Schema validation — Pandas path.
    1. Hard stop if required columns are missing.
    2. Drop rows that fail row-level rules, print report.
    3. Hard stop if DataFrame is empty after cleaning.
    """
    print("\n" + "=" * 60)
    print("SCHEMA VALIDATION  (Pandas)")
    print("=" * 60)

    missing_cols = [c for c in REQUIRED_CSV_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"SCHEMA ERROR: Required columns missing from CSV:\n"
            f"  Missing  : {missing_cols}\n"
            f"  Available: {df.columns.tolist()}"
        )
    print(f"✓ All {len(REQUIRED_CSV_COLUMNS)} required columns present.")

    total_rows    = len(df)
    bad_row_indices = set()

    for col, label, predicate in PANDAS_ROW_VALIDATION_RULES:
        if col not in df.columns:
            print(f"  ⚠ Skipping rule '{label}' — column '{col}' not found.")
            continue
        try:
            mask      = predicate(df[col])
            bad_rows  = df[mask]
            count     = len(bad_rows)
            if count > 0:
                print(f"  ⚠ {count} row(s) flagged — {label}")
                print(f"    Sample bad values: {bad_rows[col].head(5).tolist()}")
                bad_row_indices.update(bad_rows.index.tolist())
            else:
                print(f"  ✓ No issues — {label}")
        except Exception as e:
            print(f"  ⚠ Rule '{label}' could not be evaluated: {e}")

    if bad_row_indices:
        df          = df.drop(index=list(bad_row_indices)).reset_index(drop=True)
        clean_count = len(df)
        print(f"\n  Dropped {len(bad_row_indices)} invalid row(s). "
              f"Remaining: {clean_count}/{total_rows} rows.")
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


def pandas_debug_date_formats(df: pd.DataFrame):
    if "orderDate" in df.columns:
        print("\n=== DEBUGGING ORDERDATE COLUMN ===")
        print(f"Total rows: {len(df)}")
        print(f"Non-null values: {df['orderDate'].notna().sum()}")
        sample_dates = df['orderDate'].dropna().head(10).tolist()
        for i, v in enumerate(sample_dates, 1):
            print(f"  {i}. '{v}' (type: {type(v)})")
        test_formats = ["%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]
        for fmt in test_formats:
            try:
                parsed = pd.to_datetime(df['orderDate'].dropna().iloc[0], format=fmt)
                print(f"✓ Format '{fmt}' works - parsed as: {parsed}")
            except:
                print(f"✗ Format '{fmt}' failed")
        print("=== END DEBUG ===\n")


def pandas_transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Full transformation pipeline — Pandas path."""
    print("Starting data transformation (Pandas)...")

    pandas_debug_date_formats(df)

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
                    parsed_dates = pd.to_datetime(df.loc[mask, 'orderDate'], format=fmt, errors='coerce')
                    successful   = parsed_dates.notna().sum()
                    if successful > 0:
                        print(f"  Format '{fmt}' parsed {successful} dates")
                        df.loc[mask & parsed_dates.notna(), 'orderDate_parsed'] = parsed_dates[parsed_dates.notna()]
                except Exception as e:
                    print(f"  Format '{fmt}' failed: {e}")

        if df['orderDate_parsed'].isna().all():
            print("Trying pandas auto-detection...")
            df['orderDate_parsed'] = pd.to_datetime(df['orderDate'], errors='coerce', infer_datetime_format=True)

        df['orderDate'] = df['orderDate_parsed'].apply(lambda x: x.date() if pd.notnull(x) else None)
        df = df.drop(columns=['orderDate_parsed'])

        final_count = df['orderDate'].notna().sum()
        print(f"Final non-null orderDate values: {final_count}/{original_count}")
        print(f"Sample converted dates: {df[df['orderDate'].notna()]['orderDate'].head(5).tolist()}")

    if "time" in df.columns:
        print("Processing time column...")
        df["time"] = df["time"].astype(str).str[:8]
        df["time"] = df["time"].replace(["nan", "NaT"], None)
        print(f"Sample time values: {df[df['time'].notna()]['time'].head(3).tolist()}")

    print("Processing numeric columns...")
    for col in INTEGER_COLUMNS + BIGINT_COLUMNS:
        if col in df.columns:
            original = df[col].notna().sum()
            df[col]  = pd.to_numeric(df[col], errors="coerce")
            df[col]  = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
            print(f"  {col}: {df[col].notna().sum()}/{original} values converted")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            original = df[col].notna().sum()
            df[col]  = pd.to_numeric(df[col], errors="coerce")
            df[col]  = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
            print(f"  {col}: {df[col].notna().sum()}/{original} values converted")

    print("Processing string columns...")
    string_cols = [
        c for c in df.columns
        if c in POSTGRES_COLUMNS
        and c not in INTEGER_COLUMNS + BIGINT_COLUMNS + NUMERIC_COLUMNS + ["orderDate", "time"]
    ]
    for col in string_cols:
        df[col] = df[col].astype(str).replace("nan", None)

    existing_cols = [c for c in POSTGRES_COLUMNS if c in df.columns]
    df = df[existing_cols]
    for col in POSTGRES_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[POSTGRES_COLUMNS]

    print("Data transformation completed.")
    print(f"\nFINAL DATA SUMMARY:")
    print(f"  Total rows     : {len(df)}")
    print(f"  orderDate !null: {df['orderDate'].notna().sum()}")
    print(f"  time !null     : {df['time'].notna().sum()}")
    return df


def pandas_get_dates(df: pd.DataFrame) -> list:
    return df['orderDate'].dropna().unique().tolist()


def pandas_delete_existing_billing_data(cur, dates: list):
    if not dates:
        print("No dates found — skipping delete step.")
        return
    print(f"Checking and clearing existing billing_data for dates: {dates}")
    placeholders = ",".join(["%s"] * len(dates))
    cur.execute(
        f'DELETE FROM billing_data WHERE "orderDate" IN ({placeholders})',
        dates
    )
    deleted = cur.rowcount
    if deleted > 0:
        print(f"Deleted {deleted} existing rows from billing_data (overwrite mode).")
    else:
        print("No existing rows found for those dates — clean insert.")


def pandas_load_to_postgres(df: pd.DataFrame):
    """Bulk insert via psycopg2.extras.execute_values — Pandas path."""
    conn = None
    try:
        print("Connecting to database for idempotency check...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()

        # IDEMPOTENCY
        dates_in_data = pandas_get_dates(df)
        pandas_delete_existing_billing_data(cur, dates_in_data)

        print("Preparing bulk insert (Pandas)...")
        cols       = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS])
        data_tuples = [tuple(row) for row in df.values]
        insert_sql  = f'INSERT INTO billing_data ({cols}) VALUES %s'

        print(f"Inserting {len(data_tuples)} rows...")
        psycopg2.extras.execute_values(
            cur, insert_sql, data_tuples,
            template=None, page_size=1000
        )
        conn.commit()
        print(f"✓ Successfully inserted {len(df)} rows into billing_data")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Failed to insert data: {e}")
        if conn:
            conn.rollback()
        raise


# =============================================================================
# ████████████████████████████████████████████████████████████████████████████
#  PYSPARK PATH
# ████████████████████████████████████████████████████████████████████████████
# =============================================================================

def _import_spark():
    """Lazy import of PySpark — only loaded when PySpark path is selected."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, DateType
    return SparkSession, F, StringType, IntegerType, LongType, DoubleType, DateType


def get_spark():
    """
    PySpark session — single node local mode.
    To switch to distributed: change .master("local[*]") to
    .master(require_env("SPARK_MASTER_URL")) and set SPARK_MASTER_URL in .env.
    """
    SparkSession, *_ = _import_spark()
    return (
        SparkSession.builder
        .appName("SalesETL")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def spark_validate_schema(spark_df):
    """Schema validation — PySpark path."""
    SparkSession, F, StringType, *_ = _import_spark()
    from pyspark.sql import functions as F

    print("\n" + "=" * 60)
    print("SCHEMA VALIDATION  (PySpark)")
    print("=" * 60)

    missing_cols = [c for c in REQUIRED_CSV_COLUMNS if c not in spark_df.columns]
    if missing_cols:
        raise ValueError(
            f"SCHEMA ERROR: Required columns missing:\n"
            f"  Missing  : {missing_cols}\n"
            f"  Available: {spark_df.columns}"
        )
    print(f"✓ All {len(REQUIRED_CSV_COLUMNS)} required columns present.")

    total_rows = spark_df.count()
    spark_df   = spark_df.withColumn("__row_id__", F.monotonically_increasing_id())
    bad_ids    = None

    spark_rules = [
        ("totalProductPrice", "negative totalProductPrice",
            lambda df: df.filter(F.col("totalProductPrice").cast("double") < 0)),
        ("quantity",          "zero/negative quantity",
            lambda df: df.filter(F.col("quantity").cast("double") <= 0)),
        ("invoice",           "null/blank invoice",
            lambda df: df.filter(F.col("invoice").isNull() | (F.trim(F.col("invoice")) == ""))),
    ]

    for col, label, bad_filter_fn in spark_rules:
        if col not in spark_df.columns:
            print(f"  ⚠ Skipping rule '{label}' — column '{col}' not found.")
            continue
        try:
            bad_rows  = bad_filter_fn(spark_df)
            bad_count = bad_rows.count()
            if bad_count > 0:
                samples = [str(r[col]) for r in bad_rows.select(col).limit(5).collect()]
                print(f"  ⚠ {bad_count} row(s) flagged — {label}")
                print(f"    Sample bad values: {samples}")
                if bad_ids is None:
                    bad_ids = bad_rows.select("__row_id__")
                else:
                    bad_ids = bad_ids.union(bad_rows.select("__row_id__"))
            else:
                print(f"  ✓ No issues — {label}")
        except Exception as e:
            print(f"  ⚠ Rule '{label}' could not be evaluated: {e}")

    if bad_ids is not None:
        bad_ids     = bad_ids.distinct()
        spark_df    = spark_df.join(bad_ids, on="__row_id__", how="left_anti")
        clean_count = spark_df.count()
        print(f"\n  Dropped {total_rows - clean_count} invalid row(s). "
              f"Remaining: {clean_count}/{total_rows} rows.")
    else:
        print(f"\n✓ All {total_rows} rows passed row-level validation.")

    spark_df = spark_df.drop("__row_id__")

    if spark_df.count() == 0:
        raise ValueError(
            "SCHEMA ERROR: DataFrame is empty after validation — "
            "no valid rows to insert. Aborting pipeline."
        )

    print("✓ Schema validation passed.")
    print("=" * 60 + "\n")
    return spark_df


def spark_transform_data(spark_df):
    """Full transformation pipeline — PySpark path."""
    SparkSession, F, StringType, IntegerType, LongType, DoubleType, DateType = _import_spark()

    print("Starting data transformation (PySpark)...")

    if "orderDate" in spark_df.columns:
        print("\n=== DEBUGGING ORDERDATE COLUMN ===")
        non_null = spark_df.filter(F.col("orderDate").isNotNull()).count()
        print(f"Non-null values: {non_null}")
        samples = [r["orderDate"] for r in spark_df.select("orderDate").limit(5).collect()]
        print(f"Sample values: {samples}")
        print("=== END DEBUG ===\n")

    if "productMrp" in spark_df.columns:
        spark_df = spark_df.drop("productMrp")
        print("Dropped column: productMrp")

    if "orderDate" in spark_df.columns:
        print("Processing orderDate column...")
        original_count = spark_df.filter(F.col("orderDate").isNotNull()).count()
        formats_to_try = ["yyyy-MM-dd", "dd-MM-yyyy", "MM/dd/yyyy", "dd/MM/yyyy", "yyyy/MM/dd"]
        spark_df = spark_df.withColumn("orderDate_parsed", F.lit(None).cast(DateType()))
        for fmt in formats_to_try:
            spark_df = spark_df.withColumn(
                "orderDate_parsed",
                F.when(
                    F.col("orderDate_parsed").isNull() & F.col("orderDate").isNotNull(),
                    F.to_date(F.col("orderDate"), fmt)
                ).otherwise(F.col("orderDate_parsed"))
            )
            parsed = spark_df.filter(F.col("orderDate_parsed").isNotNull()).count()
            print(f"  After format '{fmt}': {parsed} dates parsed")
        spark_df = spark_df.withColumn("orderDate", F.col("orderDate_parsed")).drop("orderDate_parsed")
        final_count = spark_df.filter(F.col("orderDate").isNotNull()).count()
        print(f"Final non-null orderDate values: {final_count}/{original_count}")

    if "time" in spark_df.columns:
        print("Processing time column...")
        spark_df = spark_df.withColumn("time", F.col("time").cast(StringType()))
        spark_df = spark_df.withColumn("time", F.substring(F.col("time"), 1, 8))
        spark_df = spark_df.withColumn(
            "time",
            F.when(F.col("time").isin("nan", "NaT", "null", "None"), None).otherwise(F.col("time"))
        )

    print("Processing numeric columns...")
    for col in INTEGER_COLUMNS:
        if col in spark_df.columns:
            spark_df = spark_df.withColumn(col, F.col(col).cast(IntegerType()))
    for col in BIGINT_COLUMNS:
        if col in spark_df.columns:
            spark_df = spark_df.withColumn(col, F.col(col).cast(LongType()))
    for col in NUMERIC_COLUMNS:
        if col in spark_df.columns:
            spark_df = spark_df.withColumn(col, F.col(col).cast(DoubleType()))

    print("Processing string columns...")
    string_cols = [
        c for c in spark_df.columns
        if c in POSTGRES_COLUMNS
        and c not in INTEGER_COLUMNS + BIGINT_COLUMNS + NUMERIC_COLUMNS + ["orderDate", "time"]
    ]
    for col in string_cols:
        spark_df = spark_df.withColumn(
            col,
            F.when(F.col(col).cast(StringType()) == "nan", None)
             .otherwise(F.col(col).cast(StringType()))
        )

    for col in POSTGRES_COLUMNS:
        if col not in spark_df.columns:
            spark_df = spark_df.withColumn(col, F.lit(None).cast(StringType()))

    spark_df = spark_df.select(POSTGRES_COLUMNS)

    print("Data transformation completed.")
    total = spark_df.count()
    print(f"\nFINAL DATA SUMMARY:")
    print(f"  Total rows     : {total}")
    print(f"  orderDate !null: {spark_df.filter(F.col('orderDate').isNotNull()).count()}")
    print(f"  time !null     : {spark_df.filter(F.col('time').isNotNull()).count()}")
    return spark_df


def spark_get_dates(spark_df) -> list:
    return [
        r["orderDate"]
        for r in spark_df.select("orderDate").distinct().collect()
        if r["orderDate"] is not None
    ]


def spark_delete_existing_billing_data(cur, dates: list):
    if not dates:
        print("No dates found — skipping delete step.")
        return
    print(f"Checking and clearing existing billing_data for dates: {dates}")
    placeholders = ",".join(["%s"] * len(dates))
    cur.execute(
        f'DELETE FROM billing_data WHERE "orderDate" IN ({placeholders})',
        dates
    )
    deleted = cur.rowcount
    if deleted > 0:
        print(f"Deleted {deleted} existing rows from billing_data (overwrite mode).")
    else:
        print("No existing rows found for those dates — clean insert.")


def spark_load_to_postgres(spark_df):
    """Bulk insert via Spark JDBC — PySpark path."""
    conn = None
    try:
        print("Connecting to database for idempotency check...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()

        # IDEMPOTENCY
        dates_in_data = spark_get_dates(spark_df)
        spark_delete_existing_billing_data(cur, dates_in_data)
        conn.commit()
        cur.close()
        conn.close()

        print("Writing billing_data via Spark JDBC...")
        row_count = spark_df.count()
        (
            spark_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "billing_data")
            .option("user", JDBC_PROPERTIES["user"])
            .option("password", JDBC_PROPERTIES["password"])
            .option("driver", JDBC_PROPERTIES["driver"])
            .option("batchsize", 1000)
            .option("numPartitions", 1)
            .mode("append")
            .save()
        )
        print(f"✓ Successfully inserted {row_count} rows into billing_data")

    except Exception as e:
        print(f"Failed to insert data: {e}")
        if conn:
            conn.rollback()
        raise


# =============================================================================
# MAIN
# =============================================================================

def main():
    start_time = time.time()

    # ── Download (always Pandas/requests) ────────────────────────────────────
    downloader = CSVDownloader(
        base_url=require_env("API_BASE_URL"),
        username=require_env("API_USERNAME"),
        password=require_env("API_PASSWORD")
    )
    pandas_df = downloader.download_yesterday_csv(order_type="online")

    if pandas_df is None or pandas_df.empty:
        print("No data downloaded")
        return

    download_time = time.time()
    print(f"Download completed in {download_time - start_time:.2f} seconds")

    # ── Engine selection ──────────────────────────────────────────────────────
    engine = select_engine()

    if engine == "pandas":
        # ── PANDAS PATH ───────────────────────────────────────────────────────
        print("\n🐼  Running PANDAS pipeline...\n")

        df = pandas_validate_schema(pandas_df)
        validate_time = time.time()
        print(f"Schema validation completed in {validate_time - download_time:.2f} seconds")

        df = pandas_transform_data(df)
        transform_time = time.time()
        print(f"Transform completed in {transform_time - validate_time:.2f} seconds")

        pandas_load_to_postgres(df)
        billing_insert_time = time.time()
        print(f"Billing data load completed in {billing_insert_time - transform_time:.2f} seconds")

        agg_insert.load_aggregates_to_postgres(df)
        aggregates_insert_time = time.time()
        print(f"Aggregate inserts completed in {aggregates_insert_time - billing_insert_time:.2f} seconds")

    else:
        # ── PYSPARK PATH ──────────────────────────────────────────────────────
        print("\n🚀  Running PYSPARK pipeline...\n")

        spark = get_spark()

        pandas_df = pandas_df.astype(str).replace("nan", None)
        spark_df  = spark.createDataFrame(pandas_df)
        print(f"Converted to Spark DataFrame: {spark_df.count()} rows")

        spark_df = spark_validate_schema(spark_df)
        validate_time = time.time()
        print(f"Schema validation completed in {validate_time - download_time:.2f} seconds")

        spark_df = spark_transform_data(spark_df)
        transform_time = time.time()
        print(f"Transform completed in {transform_time - validate_time:.2f} seconds")

        spark_load_to_postgres(spark_df)
        billing_insert_time = time.time()
        print(f"Billing data load completed in {billing_insert_time - transform_time:.2f} seconds")

        pandas_out = spark_df.toPandas()
        agg_insert.load_aggregates_to_postgres(pandas_out)
        aggregates_insert_time = time.time()
        print(f"Aggregate inserts completed in {aggregates_insert_time - billing_insert_time:.2f} seconds")

        spark.stop()

    print(f"Total ETL execution time: {aggregates_insert_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()