import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

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

# Column names in DB (all lowercase)
POSTGRES_COLUMNS_BRAND    = ["brandname", "nooforders", "sales", "aov", "orderdate"]
POSTGRES_COLUMNS_STORE    = ["storename", "nooforder",  "sales", "aov", "orderdate"]
POSTGRES_COLUMNS_CATEGORY = ["subcategoryof", "sales", "orderdate"]
POSTGRES_COLUMNS_PRODUCT  = ["productname", "nooforders", "sales", "quantitysold", "orderdate"]

AGG_REQUIRED_COLUMNS = [
    "invoice", "orderDate", "totalProductPrice", "quantity",
    "brandName", "storeName", "subCategoryOf", "productName"
]

# Threshold — mirrors etl_pip.py
PANDAS_ROW_THRESHOLD = int(os.getenv("PANDAS_ROW_THRESHOLD", "500000"))


# =============================================================================
# ENGINE SELECTOR  (reads USE_ENGINE / PANDAS_ROW_THRESHOLD from .env)
# =============================================================================

def select_engine(pandas_df: pd.DataFrame) -> str:
    """
    Determine which engine to use for aggregations.
    Reads USE_ENGINE from .env — same logic as etl_pip.py.
    Falls back to row count of the incoming DataFrame if USE_ENGINE=auto.
    """
    forced = os.getenv("USE_ENGINE", "auto").strip().lower()
    if forced in ("pandas", "pyspark"):
        print(f"  Engine: {forced.upper()} (forced via USE_ENGINE)")
        return forced

    row_count = len(pandas_df)
    if row_count < PANDAS_ROW_THRESHOLD:
        print(f"  Engine: PANDAS ✅  ({row_count:,} rows < threshold {PANDAS_ROW_THRESHOLD:,})")
        return "pandas"
    else:
        print(f"  Engine: PYSPARK 🚀  ({row_count:,} rows >= threshold {PANDAS_ROW_THRESHOLD:,})")
        return "pyspark"


# =============================================================================
# IDEMPOTENCY  (shared — used by both Pandas and PySpark paths)
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
# SHARED VALIDATION  (runs on Pandas before engine split)
# =============================================================================

def validate_agg_input_pandas(df: pd.DataFrame) -> None:
    """
    Quick Pandas-level validation before any engine work starts.
    Checks required columns, non-null revenue, non-null dates.
    """
    print("\n" + "=" * 60)
    print("AGG INPUT SCHEMA VALIDATION")
    print("=" * 60)

    missing = [c for c in AGG_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"AGG SCHEMA ERROR: Missing columns: {missing}")
    print(f"✓ All {len(AGG_REQUIRED_COLUMNS)} required columns present.")

    valid_price = pd.to_numeric(df['totalProductPrice'], errors='coerce').notna().sum()
    if valid_price == 0:
        raise ValueError("AGG SCHEMA ERROR: 'totalProductPrice' has no valid numeric values.")
    print(f"✓ 'totalProductPrice' has {valid_price} valid numeric values.")

    valid_dates = df['orderDate'].dropna().shape[0]
    if valid_dates == 0:
        raise ValueError("AGG SCHEMA ERROR: 'orderDate' has no valid values.")
    print(f"✓ 'orderDate' has {valid_dates} non-null values.")

    if df.empty:
        raise ValueError("AGG SCHEMA ERROR: DataFrame is empty — nothing to aggregate.")

    print("✓ Agg input validation passed.")
    print("=" * 60 + "\n")


# =============================================================================
# HO MARLBORO EXCLUSION  (shared — runs on Pandas before engine split)
# =============================================================================

def exclude_ho_marlboro(pandas_df: pd.DataFrame) -> pd.DataFrame:
    """
    Exclude Ho Marlboro from aggregation data.
    Always runs in Pandas before engine split — fast and consistent.
    """
    print(f"\n{'='*60}")
    print(f"EXCLUDING Ho Marlboro FROM AGGREGATE TABLES")
    print(f"{'='*60}")
    print(f"Total rows (including Ho Marlboro): {len(pandas_df)}")

    if 'storeName' not in pandas_df.columns:
        print("ERROR: 'storeName' column not found!")
        print(f"Available columns: {pandas_df.columns.tolist()}")
        raise ValueError("storeName column missing — cannot exclude Ho Marlboro.")

    ho_count = len(pandas_df[pandas_df['storeName'] == 'Ho Marlboro'])
    print(f"Ho Marlboro rows: {ho_count}")

    df_agg = pandas_df[pandas_df['storeName'] != 'Ho Marlboro'].copy()
    print(f"Rows excluded: {len(pandas_df) - len(df_agg)}")
    print(f"Rows used for aggregates: {len(df_agg)}")

    if 'Ho Marlboro' in df_agg['storeName'].values:
        raise ValueError("❌ ERROR: Ho Marlboro still in aggregates dataframe!")

    print("✓ Ho Marlboro successfully excluded from aggregates")
    print(f"{'='*60}\n")

    if len(df_agg) == 0:
        raise ValueError("WARNING: No data remaining after Ho Marlboro exclusion!")

    return df_agg


# =============================================================================
# ████████████████████████████████████████████████████████████████████████████
#  PANDAS AGGREGATION PATH
# ████████████████████████████████████████████████████████████████████████████
# =============================================================================

def pandas_run_aggregations(df_agg: pd.DataFrame, conn):
    """
    Run all 4 aggregations and insert via psycopg2 execute_values — Pandas path.
    Receives the already Ho-Marlboro-filtered DataFrame.
    Idempotency delete already done before this is called.
    """
    cur = conn.cursor()

    # ── Brand Sales ───────────────────────────────────────────────────────────
    print("\nProcessing Brand Sales (Pandas)...")
    brand_df = df_agg.groupby(['brandName', 'orderDate'], as_index=False).agg(
        nooforders=('invoice', 'nunique'),
        sales=('totalProductPrice', 'sum')
    )
    brand_df['sales'] = pd.to_numeric(brand_df['sales'], errors='coerce')
    brand_df['aov']   = (brand_df['sales'] / brand_df['nooforders']).round(2)
    brand_df.rename(columns={'brandName': 'brandname', 'orderDate': 'orderdate'}, inplace=True)

    # CRITICAL VERIFICATION
    if 'Ho Marlboro' in brand_df['brandname'].values:
        raise ValueError("❌ CRITICAL: Ho Marlboro found in brand_sales aggregates!")

    brand_cols   = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_BRAND])
    brand_tuples = [tuple(row) for row in brand_df[POSTGRES_COLUMNS_BRAND].values]
    if brand_tuples:
        psycopg2.extras.execute_values(
            cur, f'INSERT INTO brand_sales ({brand_cols}) VALUES %s',
            brand_tuples, template=None, page_size=1000
        )
        print(f"✓ Inserted {len(brand_tuples)} rows into brand_sales")
    else:
        print("No brand data to insert")

    # ── Store Sales ───────────────────────────────────────────────────────────
    print("\nProcessing Store Sales (Pandas)...")
    store_df = df_agg.groupby(['storeName', 'orderDate'], as_index=False).agg(
        nooforder=('invoice', 'nunique'),
        sales=('totalProductPrice', 'sum')
    )
    store_df['sales'] = pd.to_numeric(store_df['sales'], errors='coerce')
    store_df['aov']   = (store_df['sales'] / store_df['nooforder']).round(2)
    store_df.rename(columns={'storeName': 'storename', 'orderDate': 'orderdate'}, inplace=True)

    # CRITICAL VERIFICATION
    if 'Ho Marlboro' in store_df['storename'].values:
        raise ValueError("❌ CRITICAL ERROR: Ho Marlboro found in store_sales aggregates!")
    print(f"✓ Verified: Ho Marlboro NOT in store aggregates")
    print(f"  Stores included: {sorted(store_df['storename'].unique())}")

    store_cols   = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_STORE])
    store_tuples = [tuple(row) for row in store_df[POSTGRES_COLUMNS_STORE].values]
    if store_tuples:
        psycopg2.extras.execute_values(
            cur, f'INSERT INTO store_sales ({store_cols}) VALUES %s',
            store_tuples, template=None, page_size=1000
        )
        print(f"✓ Inserted {len(store_tuples)} rows into store_sales")
    else:
        print("No store data to insert")

    # ── Category Sales ────────────────────────────────────────────────────────
    print("\nProcessing Category Sales (Pandas)...")
    category_df = df_agg.groupby(['subCategoryOf', 'orderDate'], as_index=False).agg(
        nooforder=('invoice', 'nunique'),
        sales=('totalProductPrice', 'sum')
    )
    category_df.rename(columns={'subCategoryOf': 'subcategoryof', 'orderDate': 'orderdate'}, inplace=True)

    category_cols   = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_CATEGORY])
    category_tuples = [tuple(row) for row in category_df[POSTGRES_COLUMNS_CATEGORY].values]
    if category_tuples:
        psycopg2.extras.execute_values(
            cur, f'INSERT INTO category_sales ({category_cols}) VALUES %s',
            category_tuples, template=None, page_size=1000
        )
        print(f"✓ Inserted {len(category_tuples)} rows into category_sales")
    else:
        print("No category data to insert")

    # ── Product Sales ─────────────────────────────────────────────────────────
    print("\nProcessing Product Sales (Pandas)...")
    product_df = df_agg.groupby(['productName', 'orderDate'], as_index=False).agg(
        nooforders=('invoice', 'nunique'),
        sales=('totalProductPrice', 'sum'),
        quantitysold=('quantity', 'sum')
    )
    product_df.rename(columns={'productName': 'productname', 'orderDate': 'orderdate'}, inplace=True)

    product_cols   = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_PRODUCT])
    product_tuples = [tuple(row) for row in product_df[POSTGRES_COLUMNS_PRODUCT].values]
    if product_tuples:
        psycopg2.extras.execute_values(
            cur, f'INSERT INTO product_sales ({product_cols}) VALUES %s',
            product_tuples, template=None, page_size=1000
        )
        print(f"✓ Inserted {len(product_tuples)} rows into product_sales")
    else:
        print("No product data to insert")

    cur.close()


# =============================================================================
# ████████████████████████████████████████████████████████████████████████████
#  PYSPARK AGGREGATION PATH
# ████████████████████████████████████████████████████████████████████████████
# =============================================================================

def _import_spark():
    """Lazy import — PySpark only loaded when PySpark path is selected."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType
    return SparkSession, F, DoubleType, StringType


def get_spark():
    SparkSession, *_ = _import_spark()
    return (
        SparkSession.builder
        .appName("SalesAggregates")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def write_to_postgres(spark_df, table: str, row_label: str):
    """Write Spark DataFrame to PostgreSQL via JDBC."""
    count = spark_df.count()
    if count == 0:
        print(f"No {row_label} data to insert")
        return
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
    print(f"✓ Inserted {count} rows into {table} ({row_label})")


def spark_run_aggregations(df_agg: pd.DataFrame):
    """
    Run all 4 aggregations via PySpark and write via JDBC — PySpark path.
    Receives the already Ho-Marlboro-filtered Pandas DataFrame.
    Converts to Spark, runs groupBy, writes via JDBC.
    Idempotency delete already done before this is called.
    """
    SparkSession, F, DoubleType, StringType = _import_spark()

    print("Initialising Spark session for aggregations...")
    spark    = get_spark()
    spark_df = spark.createDataFrame(df_agg)
    spark_df = spark_df.withColumn("totalProductPrice", F.col("totalProductPrice").cast(DoubleType()))

    # ── Brand Sales ───────────────────────────────────────────────────────────
    print("\nProcessing Brand Sales (PySpark)...")
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

    # CRITICAL VERIFICATION
    ho_check = brand_df.filter(F.col("brandname") == "Ho Marlboro").count()
    if ho_check > 0:
        raise ValueError("❌ CRITICAL: Ho Marlboro found in brand_sales aggregates!")

    write_to_postgres(brand_df, "brand_sales", "Ho Marlboro excluded")

    # ── Store Sales ───────────────────────────────────────────────────────────
    print("\nProcessing Store Sales (PySpark)...")
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

    # CRITICAL VERIFICATION
    ho_check = store_df.filter(F.col("storename") == "Ho Marlboro").count()
    if ho_check > 0:
        stores = [r["storename"] for r in store_df.select("storename").distinct().collect()]
        print(f"❌ CRITICAL ERROR: Ho Marlboro found in store_sales! Stores: {sorted(stores)}")
        raise ValueError("Ho Marlboro found in store aggregates — aborting.")
    else:
        stores = [r["storename"] for r in store_df.select("storename").distinct().collect()]
        print(f"✓ Verified: Ho Marlboro NOT in store aggregates")
        print(f"  Stores included: {sorted(stores)}")

    write_to_postgres(store_df, "store_sales", "Ho Marlboro excluded")

    # ── Category Sales ────────────────────────────────────────────────────────
    print("\nProcessing Category Sales (PySpark)...")
    category_df = (
        spark_df
        .groupBy("subCategoryOf", "orderDate")
        .agg(F.sum("totalProductPrice").alias("sales"))
        .withColumnRenamed("subCategoryOf", "subcategoryof")
        .withColumnRenamed("orderDate", "orderdate")
        .select("subcategoryof", "sales", "orderdate")
    )
    write_to_postgres(category_df, "category_sales", "Ho Marlboro excluded")

    # ── Product Sales ─────────────────────────────────────────────────────────
    print("\nProcessing Product Sales (PySpark)...")
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


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def load_aggregates_to_postgres(pandas_df: pd.DataFrame):
    """
    Main aggregation entry point — called from etl_pip.py.

    Flow (same for both engines):
      1. Shared Pandas validation (fast, before any engine work)
      2. Shared Ho Marlboro exclusion (Pandas, always)
      3. Shared idempotency delete (psycopg2, always)
      4. Engine-specific aggregation + insert
         - Pandas path : psycopg2.extras.execute_values
         - PySpark path: Spark JDBC write
    """
    conn = None
    try:
        # ── Step 1: Shared validation ─────────────────────────────────────────
        pandas_df['totalProductPrice'] = pd.to_numeric(
            pandas_df['totalProductPrice'], errors='coerce'
        )
        pandas_df = pandas_df[pandas_df['totalProductPrice'].notna()]
        validate_agg_input_pandas(pandas_df)

        # ── Step 2: Shared Ho Marlboro exclusion ──────────────────────────────
        df_agg = exclude_ho_marlboro(pandas_df)

        # ── Step 3: Engine selection ──────────────────────────────────────────
        engine = select_engine(df_agg)

        # ── Step 4: Shared idempotency delete (always psycopg2) ───────────────
        dates_in_data = df_agg['orderDate'].dropna().unique().tolist()
        print("Connecting to database for idempotency check...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()
        delete_existing_aggregates_for_dates(cur, dates_in_data)
        conn.commit()

        # ── Step 5: Engine-specific aggregation + insert ──────────────────────
        if engine == "pandas":
            print("\n🐼  Running PANDAS aggregations...\n")
            pandas_run_aggregations(df_agg, conn)
            conn.commit()
            conn.close()
            conn = None
        else:
            conn.close()
            conn = None
            print("\n🚀  Running PYSPARK aggregations...\n")
            spark_run_aggregations(df_agg)

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