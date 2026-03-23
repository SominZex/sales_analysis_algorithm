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

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, DateType
)

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
# SPARK SESSION
# -----------------------------------------------------------------------------
# TWO MODES — only one block should be active at a time.
#
# MODE 1 — SINGLE NODE (active by default)
#   Runs entirely on this VM. No cluster needed.
#   Uses all available CPU cores via local[*].
#
# MODE 2 — DISTRIBUTED CLUSTER (commented out)
#   Uncomment this block and comment out MODE 1 when you are ready to
#   point the pipeline at a real cluster.
#   Add the required .env variables before switching:
#     SPARK_MASTER_URL  — e.g. spark://10.0.0.1:7077  or  yarn  or  databricks
#     SPARK_EXECUTOR_MEMORY   — e.g. 4g
#     SPARK_EXECUTOR_CORES    — e.g. 2
#     SPARK_NUM_EXECUTORS     — e.g. 4
# =============================================================================

# -----------------------------------------------------------------------------
# MODE 1 — SINGLE NODE  ✅ ACTIVE
# -----------------------------------------------------------------------------
def get_spark() -> SparkSession:
    """Single-node local Spark session. Uses all CPU cores on this VM."""
    return (
        SparkSession.builder
        .appName("SalesETL")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

# -----------------------------------------------------------------------------
# MODE 2 — DISTRIBUTED CLUSTER  💤 COMMENTED OUT
# To switch: comment out MODE 1 above and uncomment the block below.
# Also add SPARK_MASTER_URL and related vars to your .env file.
# -----------------------------------------------------------------------------
# def get_spark() -> SparkSession:
#     """
#     Distributed Spark session — connects to an external cluster.
#     Supports: Spark Standalone, YARN, Databricks, EMR, GCP Dataproc.
#
#     Required .env variables:
#       SPARK_MASTER_URL        e.g. spark://10.0.0.1:7077 | yarn | local[*]
#       SPARK_EXECUTOR_MEMORY   e.g. 4g
#       SPARK_EXECUTOR_CORES    e.g. 2
#       SPARK_NUM_EXECUTORS     e.g. 4
#     """
#     return (
#         SparkSession.builder
#         .appName("SalesETL")
#         .master(require_env("SPARK_MASTER_URL"))
#         .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
#         .config("spark.sql.session.timeZone", "UTC")
#         .config("spark.executor.memory",   os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
#         .config("spark.executor.cores",    os.getenv("SPARK_EXECUTOR_CORES",  "2"))
#         .config("spark.executor.instances", os.getenv("SPARK_NUM_EXECUTORS", "4"))
#         .config("spark.sql.shuffle.partitions", "200")
#         .config("spark.default.parallelism",    "200")
#         .config("spark.network.timeout",        "600s")
#         .config("spark.executor.heartbeatInterval", "60s")
#         .getOrCreate()
#     )


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

INTEGER_COLUMNS  = ["productId", "quantity"]
BIGINT_COLUMNS   = ["barcode"]
NUMERIC_COLUMNS  = ["GST", "CGSTRate", "SGSTRate", "acessAmount", "cess"]

# -- Columns the CSV must contain for the pipeline to proceed --
REQUIRED_CSV_COLUMNS = [
    "invoice", "orderDate", "productId", "productName", "quantity",
    "totalProductPrice", "brandName", "subCategoryOf", "storeName", "orderType"
]

# -- Row-level validation rules: (column, label, spark filter condition) --
ROW_VALIDATION_RULES = [
    ("totalProductPrice", "negative totalProductPrice",
        lambda df: df.filter(F.col("totalProductPrice").cast("double") < 0)),
    ("quantity",          "zero/negative quantity",
        lambda df: df.filter(F.col("quantity").cast("double") <= 0)),
    ("invoice",           "null/blank invoice",
        lambda df: df.filter(F.col("invoice").isNull() | (F.trim(F.col("invoice")) == ""))),
]


# =============================================================================
# DOWNLOADER  (API interaction stays in Python/requests — unchanged)
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
# SCHEMA VALIDATION  (PySpark)
# =============================================================================

def validate_schema(spark_df):
    """
    Run before transform_data():
      1. Hard stop if any required columns are missing.
      2. Drop rows that fail row-level rules and print a report.
      3. Hard stop if the DataFrame is empty after cleaning.
    Returns the cleaned Spark DataFrame.
    """
    print("\n" + "=" * 60)
    print("SCHEMA VALIDATION  (PySpark)")
    print("=" * 60)

    # 1. Required column presence
    existing_cols = spark_df.columns
    missing_cols  = [c for c in REQUIRED_CSV_COLUMNS if c not in existing_cols]
    if missing_cols:
        raise ValueError(
            f"SCHEMA ERROR: Required columns missing from CSV:\n"
            f"  Missing  : {missing_cols}\n"
            f"  Available: {existing_cols}"
        )
    print(f"✓ All {len(REQUIRED_CSV_COLUMNS)} required columns present.")

    # 2. Row-level validation
    total_rows = spark_df.count()
    spark_df   = spark_df.withColumn("__row_id__", F.monotonically_increasing_id())
    bad_ids    = None

    for col, label, bad_filter_fn in ROW_VALIDATION_RULES:
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
        dropped     = total_rows - clean_count
        print(f"\n  Dropped {dropped} invalid row(s). Remaining: {clean_count}/{total_rows} rows.")
    else:
        print(f"\n✓ All {total_rows} rows passed row-level validation.")

    spark_df = spark_df.drop("__row_id__")

    # 3. Hard stop if nothing left
    if spark_df.count() == 0:
        raise ValueError(
            "SCHEMA ERROR: DataFrame is empty after validation — "
            "no valid rows to insert. Aborting pipeline."
        )

    print("✓ Schema validation passed.")
    print("=" * 60 + "\n")
    return spark_df


# =============================================================================
# TRANSFORM  (PySpark)
# =============================================================================

def debug_date_formats(spark_df):
    """Debug: show sample orderDate values."""
    if "orderDate" in spark_df.columns:
        print("\n=== DEBUGGING ORDERDATE COLUMN ===")
        print(f"Total rows: {spark_df.count()}")
        non_null = spark_df.filter(F.col("orderDate").isNotNull()).count()
        print(f"Non-null values: {non_null}")
        samples = [r["orderDate"] for r in spark_df.select("orderDate").limit(10).collect()]
        print("Sample values:")
        for i, v in enumerate(samples, 1):
            print(f"  {i}. '{v}'")
        print("=== END DEBUG ===\n")


def transform_data(spark_df):
    """
    Full transformation pipeline using PySpark.
    Returns a cleaned Spark DataFrame with exactly POSTGRES_COLUMNS.
    """
    print("Starting data transformation (PySpark)...")

    debug_date_formats(spark_df)

    # Drop productMrp if present
    if "productMrp" in spark_df.columns:
        spark_df = spark_df.drop("productMrp")
        print("Dropped column: productMrp")

    # ── orderDate: try multiple formats ──────────────────────────────────────
    if "orderDate" in spark_df.columns:
        print("Processing orderDate column...")
        original_count = spark_df.filter(F.col("orderDate").isNotNull()).count()
        print(f"Original non-null orderDate values: {original_count}")

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
            parsed_count = spark_df.filter(F.col("orderDate_parsed").isNotNull()).count()
            print(f"  After format '{fmt}': {parsed_count} dates parsed")

        spark_df = spark_df.withColumn("orderDate", F.col("orderDate_parsed")).drop("orderDate_parsed")

        final_count = spark_df.filter(F.col("orderDate").isNotNull()).count()
        print(f"Final non-null orderDate values: {final_count}/{original_count}")
        samples = [str(r["orderDate"]) for r in spark_df.filter(F.col("orderDate").isNotNull()).select("orderDate").limit(5).collect()]
        print(f"Sample converted dates: {samples}")

    # ── time column ───────────────────────────────────────────────────────────
    if "time" in spark_df.columns:
        print("Processing time column...")
        spark_df = spark_df.withColumn("time", F.col("time").cast(StringType()))
        spark_df = spark_df.withColumn("time", F.substring(F.col("time"), 1, 8))
        spark_df = spark_df.withColumn(
            "time",
            F.when(F.col("time").isin("nan", "NaT", "null", "None"), None).otherwise(F.col("time"))
        )
        samples = [r["time"] for r in spark_df.filter(F.col("time").isNotNull()).select("time").limit(3).collect()]
        print(f"Sample time values: {samples}")

    # ── Numeric columns ───────────────────────────────────────────────────────
    print("Processing numeric columns...")
    for col in INTEGER_COLUMNS:
        if col in spark_df.columns:
            spark_df = spark_df.withColumn(col, F.col(col).cast(IntegerType()))
            print(f"  {col}: cast to IntegerType")

    for col in BIGINT_COLUMNS:
        if col in spark_df.columns:
            spark_df = spark_df.withColumn(col, F.col(col).cast(LongType()))
            print(f"  {col}: cast to LongType")

    for col in NUMERIC_COLUMNS:
        if col in spark_df.columns:
            spark_df = spark_df.withColumn(col, F.col(col).cast(DoubleType()))
            print(f"  {col}: cast to DoubleType")

    # ── String columns: replace "nan" with null ───────────────────────────────
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

    # ── Add any missing POSTGRES_COLUMNS as null ──────────────────────────────
    for col in POSTGRES_COLUMNS:
        if col not in spark_df.columns:
            spark_df = spark_df.withColumn(col, F.lit(None).cast(StringType()))

    # ── Select only POSTGRES_COLUMNS in exact order ───────────────────────────
    spark_df = spark_df.select(POSTGRES_COLUMNS)

    print("Data transformation completed.")
    total          = spark_df.count()
    non_null_date  = spark_df.filter(F.col("orderDate").isNotNull()).count()
    non_null_time  = spark_df.filter(F.col("time").isNotNull()).count()
    print(f"\nFINAL DATA SUMMARY:")
    print(f"  Total rows     : {total}")
    print(f"  orderDate !null: {non_null_date}")
    print(f"  time !null     : {non_null_time}")

    return spark_df


# =============================================================================
# IDEMPOTENCY HELPERS  (psycopg2 — runs before Spark JDBC write)
# =============================================================================

def get_dates_in_spark_df(spark_df) -> list:
    """Extract unique orderDate values from the Spark DataFrame."""
    return [
        r["orderDate"]
        for r in spark_df.select("orderDate").distinct().collect()
        if r["orderDate"] is not None
    ]


def delete_existing_billing_data_for_dates(cur, dates: list):
    """
    Delete existing rows in billing_data for the given dates.
    Ensures re-runs don't duplicate data — existing data is overwritten.
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
# LOAD  (PySpark JDBC write)
# =============================================================================

def load_to_postgres_bulk(spark_df):
    """
    Write billing_data to PostgreSQL using Spark JDBC.
    Idempotency delete runs first via psycopg2, then Spark writes in bulk.
    """
    conn = None
    try:
        print("Connecting to database for idempotency check...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()

        # --- IDEMPOTENCY: Delete existing rows for the same dates before inserting ---
        dates_in_data = get_dates_in_spark_df(spark_df)
        delete_existing_billing_data_for_dates(cur, dates_in_data)
        conn.commit()
        cur.close()
        conn.close()
        # -----------------------------------------------------------------------------

        print("Writing billing_data via Spark JDBC...")
        row_count = spark_df.count()

        # ---------------------------------------------------------
        # JDBC WRITE — SINGLE NODE  ✅ ACTIVE
        # numPartitions=1 writes sequentially from driver.
        # Fine for single VM. For distributed, see block below.
        # ---------------------------------------------------------
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

        # ---------------------------------------------------------
        # JDBC WRITE — DISTRIBUTED  💤 COMMENTED OUT
        # Uses multiple parallel connections from executor nodes.
        # Uncomment when running on a cluster.
        # numPartitions controls how many parallel DB connections
        # are opened — tune based on your DB connection pool size.
        # ---------------------------------------------------------
        # (
        #     spark_df
        #     .repartition(int(os.getenv("SPARK_NUM_EXECUTORS", "4")))
        #     .write
        #     .format("jdbc")
        #     .option("url", JDBC_URL)
        #     .option("dbtable", "billing_data")
        #     .option("user", JDBC_PROPERTIES["user"])
        #     .option("password", JDBC_PROPERTIES["password"])
        #     .option("driver", JDBC_PROPERTIES["driver"])
        #     .option("batchsize", 5000)
        #     .option("numPartitions", int(os.getenv("SPARK_NUM_EXECUTORS", "4")))
        #     .mode("append")
        #     .save()
        # )

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

    # ── Download via requests (unchanged) ────────────────────────────────────
    downloader = CSVDownloader(
        base_url=require_env("API_BASE_URL"),
        username=require_env("API_USERNAME"),
        password=require_env("API_PASSWORD")
    )
    pandas_df = downloader.download_yesterday_csv(order_type="online")

    if pandas_df is not None and not pandas_df.empty:
        download_time = time.time()
        print(f"Download completed in {download_time - start_time:.2f} seconds")

        # ── Start Spark ───────────────────────────────────────────────────────
        print("Initialising Spark session...")
        spark    = get_spark()

        # Convert Pandas → Spark (all columns as string first for safe parsing)
        pandas_df = pandas_df.astype(str).replace("nan", None)
        spark_df  = spark.createDataFrame(pandas_df)
        print(f"Converted to Spark DataFrame: {spark_df.count()} rows")

        # ── 1. Schema validation ──────────────────────────────────────────────
        spark_df      = validate_schema(spark_df)
        validate_time = time.time()
        print(f"Schema validation completed in {validate_time - download_time:.2f} seconds")

        # ── 2. Transform ──────────────────────────────────────────────────────
        spark_df       = transform_data(spark_df)
        transform_time = time.time()
        print(f"Transform completed in {transform_time - validate_time:.2f} seconds")

        # ── 3. Load billing_data (idempotency + Spark JDBC write) ─────────────
        load_to_postgres_bulk(spark_df)
        billing_insert_time = time.time()
        print(f"Billing data load completed in {billing_insert_time - transform_time:.2f} seconds")

        # ── 4. Load aggregate tables ──────────────────────────────────────────
        pandas_out = spark_df.toPandas()
        agg_insert.load_aggregates_to_postgres(pandas_out)
        aggregates_insert_time = time.time()
        print(f"Aggregate inserts completed in {aggregates_insert_time - billing_insert_time:.2f} seconds")

        print(f"Total ETL execution time: {aggregates_insert_time - start_time:.2f} seconds")

        spark.stop()
    else:
        print("No data downloaded")

if __name__ == "__main__":
    main()