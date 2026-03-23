import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

load_dotenv()

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


JDBC_URL = (
    f"jdbc:postgresql://{require_env('DB_HOST')}:"
    f"{os.getenv('DB_PORT', 5432)}/{require_env('DB_NAME')}"
)

JDBC_PROPERTIES = {
    "user": require_env("DB_USER"),
    "password": require_env("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

DB_CONFIG = {
    "host": require_env("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": require_env("DB_NAME"),
    "user": require_env("DB_USER"),
    "password": require_env("DB_PASSWORD"),
}


# =============================================================================
# SPARK SESSION
# -----------------------------------------------------------------------------
# TWO MODES — only one block should be active at a time.
#
# MODE 1 — SINGLE NODE (active by default)
#   Runs entirely on this VM. No cluster needed.
#
# MODE 2 — DISTRIBUTED CLUSTER (commented out)
#   Uncomment and comment out MODE 1 when ready to scale.
#   Required .env variables:
#     SPARK_MASTER_URL        e.g. spark://10.0.0.1:7077 | yarn
#     SPARK_EXECUTOR_MEMORY   e.g. 4g
#     SPARK_EXECUTOR_CORES    e.g. 2
#     SPARK_NUM_EXECUTORS     e.g. 4
# =============================================================================

# -----------------------------------------------------------------------------
# MODE 1 — SINGLE NODE  ✅ ACTIVE
# -----------------------------------------------------------------------------
def get_spark() -> SparkSession:
    """
    Reuses existing Spark session if already started by etl_pip.py.
    Falls back to creating a new local session if running standalone.
    """
    return (
        SparkSession.builder
        .appName("SalesAggregates")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

# -----------------------------------------------------------------------------
# MODE 2 — DISTRIBUTED CLUSTER  💤 COMMENTED OUT
# To switch: comment out MODE 1 above and uncomment the block below.
# -----------------------------------------------------------------------------
# def get_spark() -> SparkSession:
#     """
#     Distributed Spark session — connects to an external cluster.
#     Reuses existing session if already started by etl_pip.py.
#     Supports: Spark Standalone, YARN, Databricks, EMR, GCP Dataproc.
#
#     Required .env variables:
#       SPARK_MASTER_URL        e.g. spark://10.0.0.1:7077 | yarn
#       SPARK_EXECUTOR_MEMORY   e.g. 4g
#       SPARK_EXECUTOR_CORES    e.g. 2
#       SPARK_NUM_EXECUTORS     e.g. 4
#     """
#     return (
#         SparkSession.builder
#         .appName("SalesAggregates")
#         .master(require_env("SPARK_MASTER_URL"))
#         .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
#         .config("spark.sql.session.timeZone", "UTC")
#         .config("spark.executor.memory",    os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
#         .config("spark.executor.cores",     os.getenv("SPARK_EXECUTOR_CORES",  "2"))
#         .config("spark.executor.instances", os.getenv("SPARK_NUM_EXECUTORS",   "4"))
#         .config("spark.sql.shuffle.partitions",     "200")
#         .config("spark.default.parallelism",        "200")
#         .config("spark.network.timeout",            "600s")
#         .config("spark.executor.heartbeatInterval", "60s")
#         .getOrCreate()
#     )


# =============================================================================
# IDEMPOTENCY  (psycopg2 — runs before Spark JDBC writes)
# =============================================================================

def delete_existing_aggregates_for_dates(cur, dates: list):
    """
    Delete existing rows from all 4 aggregate tables for the given dates.
    Called once before all inserts so re-runs cleanly overwrite the data.
    """
    if not dates:
        print("No dates found in data — skipping aggregate delete step.")
        return

    print(f"\nChecking and clearing existing aggregate data for dates: {dates}")
    placeholders = ",".join(["%s"] * len(dates))

    tables = ["brand_sales", "store_sales", "category_sales", "product_sales"]
    for table in tables:
        cur.execute(
            f'DELETE FROM {table} WHERE "orderdate" IN ({placeholders})',
            dates
        )
        deleted = cur.rowcount
        if deleted > 0:
            print(f"  Deleted {deleted} existing rows from {table} (overwrite mode).")
        else:
            print(f"  No existing rows found in {table} for those dates — clean insert.")


# =============================================================================
# AGG INPUT SCHEMA VALIDATION  (PySpark)
# =============================================================================

AGG_REQUIRED_COLUMNS = [
    "invoice", "orderDate", "totalProductPrice", "quantity",
    "brandName", "storeName", "subCategoryOf", "productName"
]

def validate_agg_input(spark_df) -> None:
    """
    Validate that the Spark DataFrame contains all columns needed
    for groupBy aggregations. Raises ValueError immediately if any are missing.
    """
    print("\n" + "=" * 60)
    print("AGG INPUT SCHEMA VALIDATION  (PySpark)")
    print("=" * 60)

    missing_cols = [c for c in AGG_REQUIRED_COLUMNS if c not in spark_df.columns]
    if missing_cols:
        raise ValueError(
            f"AGG SCHEMA ERROR: Missing required columns for aggregation:\n"
            f"  {missing_cols}\n"
            f"  Available columns: {spark_df.columns}"
        )
    print(f"✓ All {len(AGG_REQUIRED_COLUMNS)} required agg columns present.")

    valid_price_count = (
        spark_df
        .filter(F.col("totalProductPrice").cast(DoubleType()).isNotNull())
        .count()
    )
    if valid_price_count == 0:
        raise ValueError(
            "AGG SCHEMA ERROR: 'totalProductPrice' has no valid numeric values — "
            "aggregations would produce empty results. Aborting."
        )
    print(f"✓ 'totalProductPrice' has {valid_price_count} valid numeric values.")

    valid_date_count = spark_df.filter(F.col("orderDate").isNotNull()).count()
    if valid_date_count == 0:
        raise ValueError(
            "AGG SCHEMA ERROR: 'orderDate' has no valid values — "
            "aggregations cannot be grouped by date. Aborting."
        )
    print(f"✓ 'orderDate' has {valid_date_count} non-null values.")

    if spark_df.count() == 0:
        raise ValueError(
            "AGG SCHEMA ERROR: DataFrame is empty — nothing to aggregate. Aborting."
        )

    print("✓ Agg input validation passed.")
    print("=" * 60 + "\n")


# =============================================================================
# SPARK JDBC WRITE HELPER
# -----------------------------------------------------------------------------
# TWO MODES — only one write block should be active at a time.
#
# MODE 1 — SINGLE NODE (active): numPartitions=1, sequential write
# MODE 2 — DISTRIBUTED (commented): parallel write from executor nodes
# =============================================================================

def write_to_postgres(spark_df, table: str, row_label: str):
    """Write a Spark DataFrame to a PostgreSQL table via JDBC."""
    count = spark_df.count()
    if count == 0:
        print(f"No {row_label} data to insert")
        return

    # ---------------------------------------------------------
    # MODE 1 — SINGLE NODE  ✅ ACTIVE
    # Sequential write from driver. Safe for single VM.
    # ---------------------------------------------------------
    (
        spark_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver", JDBC_PROPERTIES["driver"])
        .option("batchsize", 1000)
        .option("numPartitions", 1)
        .mode("append")
        .save()
    )

    # ---------------------------------------------------------
    # MODE 2 — DISTRIBUTED  💤 COMMENTED OUT
    # Parallel write from executor nodes.
    # Uncomment when running on a cluster.
    # numPartitions opens parallel DB connections — tune to
    # match your PostgreSQL max_connections setting.
    # ---------------------------------------------------------
    # (
    #     spark_df
    #     .repartition(int(os.getenv("SPARK_NUM_EXECUTORS", "4")))
    #     .write
    #     .format("jdbc")
    #     .option("url", JDBC_URL)
    #     .option("dbtable", table)
    #     .option("user", JDBC_PROPERTIES["user"])
    #     .option("password", JDBC_PROPERTIES["password"])
    #     .option("driver", JDBC_PROPERTIES["driver"])
    #     .option("batchsize", 5000)
    #     .option("numPartitions", int(os.getenv("SPARK_NUM_EXECUTORS", "4")))
    #     .mode("append")
    #     .save()
    # )

    print(f"✓ Inserted {count} rows into {table} ({row_label})")


# =============================================================================
# MAIN AGGREGATE LOADER
# =============================================================================

def load_aggregates_to_postgres(pandas_df: pd.DataFrame):
    """
    Accepts a Pandas DataFrame (passed from etl_pip.py),
    converts it to Spark, runs all aggregations using PySpark,
    and writes results to PostgreSQL via JDBC.
    All existing logic preserved: Ho Marlboro exclusion, idempotency, validation.
    """
    conn = None
    try:
        # ── Quick Pandas schema check before starting Spark ───────────────────
        missing = [c for c in AGG_REQUIRED_COLUMNS if c not in pandas_df.columns]
        if missing:
            raise ValueError(f"AGG SCHEMA ERROR: Missing columns: {missing}")

        # ── Pre-process totalProductPrice ─────────────────────────────────────
        pandas_df['totalProductPrice'] = pd.to_numeric(pandas_df['totalProductPrice'], errors='coerce')
        pandas_df = pandas_df[pandas_df['totalProductPrice'].notna()]

        # ── CRITICAL: EXCLUDE Ho Marlboro store from aggregations ─────────────
        print(f"\n{'='*60}")
        print(f"EXCLUDING Ho Marlboro FROM AGGREGATE TABLES")
        print(f"{'='*60}")
        print(f"Total rows in billing_data (including Ho Marlboro): {len(pandas_df)}")

        if 'storeName' not in pandas_df.columns:
            print("ERROR: 'storeName' column not found in DataFrame!")
            print(f"Available columns: {pandas_df.columns.tolist()}")
            return

        ho_marlboro_count = len(pandas_df[pandas_df['storeName'] == 'Ho Marlboro'])
        print(f"Ho Marlboro rows in source data: {ho_marlboro_count}")

        pandas_df_agg = pandas_df[pandas_df['storeName'] != 'Ho Marlboro'].copy()

        rows_excluded = len(pandas_df) - len(pandas_df_agg)
        print(f"Rows excluded from aggregates: {rows_excluded}")
        print(f"Rows used for aggregates: {len(pandas_df_agg)}")

        if 'Ho Marlboro' in pandas_df_agg['storeName'].values:
            print("❌ ERROR: Ho Marlboro still in aggregates dataframe!")
            return
        else:
            print("✓ Ho Marlboro successfully excluded from aggregates")
        print(f"{'='*60}\n")

        if len(pandas_df_agg) == 0:
            print("WARNING: No data remaining after filtering!")
            return

        # ── Convert filtered Pandas → Spark ───────────────────────────────────
        print("Initialising Spark session for aggregations...")
        spark    = get_spark()
        spark_df = spark.createDataFrame(pandas_df_agg)

        # ── Full PySpark schema validation ────────────────────────────────────
        validate_agg_input(spark_df)

        # ── IDEMPOTENCY: Delete existing rows for these dates ─────────────────
        dates_in_data = [
            r["orderDate"]
            for r in spark_df.select("orderDate").distinct().collect()
            if r["orderDate"] is not None
        ]
        print("Connecting to database for idempotency check...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()
        delete_existing_aggregates_for_dates(cur, dates_in_data)
        conn.commit()
        cur.close()
        conn.close()
        conn = None
        # ─────────────────────────────────────────────────────────────────────

        # Ensure totalProductPrice is DoubleType in Spark
        spark_df = spark_df.withColumn("totalProductPrice", F.col("totalProductPrice").cast(DoubleType()))

        # ── Brand Sales ───────────────────────────────────────────────────────
        print("\nProcessing Brand Sales...")
        brand_df = (
            spark_df
            .groupBy("brandName", "orderDate")
            .agg(
                F.countDistinct("invoice").alias("nooforders"),
                F.sum("totalProductPrice").alias("sales")
            )
            .withColumn("sales", F.col("sales").cast(DoubleType()))
            .withColumn("aov", F.round(F.col("sales") / F.col("nooforders"), 2))
            .withColumnRenamed("brandName", "brandname")
            .withColumnRenamed("orderDate", "orderdate")
            .select("brandname", "nooforders", "sales", "aov", "orderdate")
        )

        # CRITICAL VERIFICATION: Ensure Ho Marlboro is NOT in brand aggregates
        ho_check = brand_df.filter(F.col("brandname") == "Ho Marlboro").count()
        if ho_check > 0:
            raise ValueError("❌ CRITICAL: Ho Marlboro found in brand_sales aggregates!")

        write_to_postgres(brand_df, "brand_sales", "Ho Marlboro excluded")

        # ── Store Sales ───────────────────────────────────────────────────────
        print("\nProcessing Store Sales...")
        store_df = (
            spark_df
            .groupBy("storeName", "orderDate")
            .agg(
                F.countDistinct("invoice").alias("nooforder"),
                F.sum("totalProductPrice").alias("sales")
            )
            .withColumn("sales", F.col("sales").cast(DoubleType()))
            .withColumn("aov", F.round(F.col("sales") / F.col("nooforder"), 2))
            .withColumnRenamed("storeName", "storename")
            .withColumnRenamed("orderDate", "orderdate")
            .select("storename", "nooforder", "sales", "aov", "orderdate")
        )

        # CRITICAL VERIFICATION: Ensure Ho Marlboro is NOT in store aggregates
        ho_check = store_df.filter(F.col("storename") == "Ho Marlboro").count()
        if ho_check > 0:
            print("❌ CRITICAL ERROR: Ho Marlboro found in store_sales aggregates!")
            stores = [r["storename"] for r in store_df.select("storename").distinct().collect()]
            print(f"Stores in aggregate: {sorted(stores)}")
            raise ValueError("Ho Marlboro found in store aggregates — aborting.")
        else:
            stores = [r["storename"] for r in store_df.select("storename").distinct().collect()]
            print(f"✓ Verified: Ho Marlboro NOT in store aggregates")
            print(f"  Stores included: {sorted(stores)}")

        write_to_postgres(store_df, "store_sales", "Ho Marlboro excluded")

        # ── Category Sales ────────────────────────────────────────────────────
        print("\nProcessing Category Sales...")
        category_df = (
            spark_df
            .groupBy("subCategoryOf", "orderDate")
            .agg(
                F.sum("totalProductPrice").alias("sales")
            )
            .withColumnRenamed("subCategoryOf", "subcategoryof")
            .withColumnRenamed("orderDate", "orderdate")
            .select("subcategoryof", "sales", "orderdate")
        )
        write_to_postgres(category_df, "category_sales", "Ho Marlboro excluded")

        # ── Product Sales ─────────────────────────────────────────────────────
        print("\nProcessing Product Sales...")
        product_df = (
            spark_df
            .groupBy("productName", "orderDate")
            .agg(
                F.countDistinct("invoice").alias("nooforders"),
                F.sum("totalProductPrice").alias("sales"),
                F.sum(F.col("quantity").cast(DoubleType())).alias("quantitysold")
            )
            .withColumnRenamed("productName", "productname")
            .withColumnRenamed("orderDate", "orderdate")
            .select("productname", "nooforders", "sales", "quantitysold", "orderdate")
        )
        write_to_postgres(product_df, "product_sales", "Ho Marlboro excluded")

        print(f"\n{'='*60}")
        print("✓ SUCCESS: All aggregate tables populated WITHOUT Ho Marlboro")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"\n{'='*60}")
        print(f"❌ ERROR: Failed to insert aggregates: {e}")
        print(f"{'='*60}\n")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()