import pandas as pd
import pdfkit
import plotly.graph_objects as go
import base64
import os
import time
from io import BytesIO
from datetime import date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContentSettings, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timezone, timedelta
import tempfile


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from llm_recommender import save_weekly_snapshot
from llm_recommender import (
    brand_recommendation,
    category_recommendation,
    product_recommendation,
    # ── Stock insight functions (appended to each AI recommendation section) ──
    brand_stock_insight,
    category_stock_insight,
    product_stock_insight,
    # ── RTV insight function ──────────────────────────────────────────────────
    rtv_insight,
)

# Directory where stock.py saves per-store CSVs
STOCK_DIR           = os.getenv("STOCK_DIR", "store_stocks")
LOW_STOCK_THRESHOLD = float(os.getenv("LOW_STOCK_THRESHOLD", "5"))
RTV_DIR             = os.getenv("RTV_DIR", "store_rtv")
AZURE_ACCOUNT_NAME      = os.getenv("AZURE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY       = os.getenv("AZURE_ACCOUNT_KEY", "")
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING", "")
AZURE_CONTAINER         = os.getenv("AZURE_CONTAINER", "weekly-reports")

SAS_EXPIRY_DAYS = 7

load_dotenv()

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


# =============================================================================
# DATABASE CONFIG
# =============================================================================

DB_URI = (
    f"postgresql+psycopg2://"
    f"{require_env('DB_USER')}:{require_env('DB_PASSWORD')}"
    f"@{require_env('DB_HOST')}:{os.getenv('DB_PORT', '5432')}"
    f"/{require_env('DB_NAME')}"
)

# SQLAlchemy engine with NullPool and keepalives — unchanged from original.
# Used only for short single-row metadata queries (date range, store list).
engine = create_engine(
    DB_URI,
    poolclass=NullPool,
    connect_args={
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)

JDBC_URL = (
    f"jdbc:postgresql://{require_env('DB_HOST')}:"
    f"{os.getenv('DB_PORT', '5432')}/{require_env('DB_NAME')}"
)

JDBC_PROPERTIES = {
    "user":     require_env("DB_USER"),
    "password": require_env("DB_PASSWORD"),
    "driver":   "org.postgresql.Driver"
}

PDFKIT_CONFIG = pdfkit.configuration(wkhtmltopdf="/usr/bin/wkhtmltopdf")


# =============================================================================
# SPARK SESSION
# -----------------------------------------------------------------------------
# TWO MODES — only one block should be active at a time.
#
# MODE 1 — SINGLE NODE  ✅ ACTIVE (default)
#   Runs on this VM. No cluster needed.
#
# MODE 2 — DISTRIBUTED CLUSTER  💤 COMMENTED OUT
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
    """Single-node local Spark session. Reuses existing session if running."""
    return (
        SparkSession.builder
        .appName("WeeklyReports")
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
#     Reuses existing session if already started.
#
#     Required .env variables:
#       SPARK_MASTER_URL        e.g. spark://10.0.0.1:7077 | yarn
#       SPARK_EXECUTOR_MEMORY   e.g. 4g
#       SPARK_EXECUTOR_CORES    e.g. 2
#       SPARK_NUM_EXECUTORS     e.g. 4
#     """
#     return (
#         SparkSession.builder
#         .appName("WeeklyReports")
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
# METADATA QUERIES  (SQLAlchemy — short single-row queries, no Spark needed)
# safe_read_sql preserved exactly from original including exponential backoff
# and engine.dispose() on EOF/closed connection errors.
# =============================================================================

def safe_read_sql(query, params=None, retries=5, delay=3):
    """Executes SQL query with retries for transient DB disconnects."""
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                result = pd.read_sql(query, conn, params=params)
                return result

        except OperationalError as e:
            error_str = str(e)
            print(f"⚠️ Database error (attempt {attempt+1}/{retries}): {error_str[:100]}")

            if attempt == retries - 1:
                raise RuntimeError(f"❌ Query failed after {retries} retries: {error_str[:200]}")

            wait_time = delay * (2 ** attempt)
            print(f"   ⏳ Waiting {wait_time}s before retry...")
            time.sleep(wait_time)

            if "EOF" in error_str or "closed" in error_str.lower() or "terminated" in error_str.lower():
                print("   🔄 Recreating database connection pool...")
                engine.dispose()

        except Exception as e:
            print(f"⚠️ Unexpected error (attempt {attempt+1}/{retries}): {e}")
            if attempt == retries - 1:
                raise RuntimeError(f"❌ Query failed: {e}")
            time.sleep(delay)

    raise RuntimeError("❌ Query failed after multiple retries.")


def get_unique_stores():
    """Fetch all unique store names."""
    query = 'SELECT DISTINCT "storeName" FROM "billing_data" ORDER BY "storeName";'
    df = safe_read_sql(query)
    return df["storeName"].dropna().tolist()


def get_week_dates(store_name: str):
    """
    Resolve week_start and week_end for a store.
    Mirrors original date_range_query exactly:
      week_end   = MAX(orderDate)
      week_start = week_end - 6 days
    Returns (week_start, week_end) as Python date objects, or (None, None).
    """
    date_range_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        )
        SELECT
            (max_date - INTERVAL '6 days')::date AS week_start,
            max_date AS week_end
        FROM latest_date;
    """
    date_info = safe_read_sql(date_range_query, params=(store_name,))
    if date_info.empty or date_info['week_start'].iloc[0] is None:
        return None, None
    week_start = date_info['week_start'].iloc[0]
    week_end   = date_info['week_end'].iloc[0]
    # Ensure Python date objects
    if hasattr(week_start, 'date'):
        week_start = week_start.date()
    if hasattr(week_end, 'date'):
        week_end = week_end.date()
    return week_start, week_end


# =============================================================================
# STOCK HELPERS  (Pandas — unchanged, operates on local CSV files)
# =============================================================================

# Short display names for all table columns — unchanged
COLUMN_LABELS = {
    "brandName":       "Brand",
    "categoryName":    "Category",
    "productName":     "Product",
    "total_sales":     "Sales (₹)",
    "quantity_sold":   "Qty Sold",
    "current_stock":   "Stock",
    "contrib_percent": "Contrib%",
    "profit_margin":   "Margin%",
}


def load_stock_lookups(store_name: str):
    """
    Load the store's stock CSV once and return three dicts for O(1) lookup:
        brand_stock    : {brandName    -> sum(quantity)}
        category_stock : {categoryName -> sum(quantity)}
        product_stock  : {productName  -> quantity}
    Returns three empty dicts if the CSV is missing (non-fatal).
    All quantities are kept as raw numbers (can be negative = no GRN).
    """
    safe_name = store_name.replace("/", "_")
    candidates = [
        os.path.join(STOCK_DIR, f"{safe_name}.csv"),
        os.path.join(STOCK_DIR, f"{store_name}.csv"),
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

                brand_stock = (
                    df.groupby("brand")["quantity"].sum().to_dict()
                    if "brand" in df.columns else {}
                )
                category_stock = (
                    df.groupby("categoryName")["quantity"].sum().to_dict()
                    if "categoryName" in df.columns else {}
                )
                product_stock = (
                    df.set_index("productName")["quantity"].to_dict()
                    if "productName" in df.columns else {}
                )
                print(f"  📦 Stock CSV loaded for {store_name} "
                      f"({len(df)} SKUs, {len(brand_stock)} brands, "
                      f"{len(category_stock)} categories)")
                return brand_stock, category_stock, product_stock
            except Exception as e:
                print(f"  ⚠️  Stock CSV read error for {store_name}: {e}")
                return {}, {}, {}
    print(f"  ℹ️  No stock CSV for {store_name} — 'Stock' column will show N/A")
    return {}, {}, {}


def inject_stock_column(df: pd.DataFrame, name_col: str, stock_lookup: dict) -> pd.DataFrame:
    """
    Insert a 'current_stock' column right after 'quantity_sold', then
    rename all columns to short display labels.
    Values not found in the lookup are shown as 'N/A'.
    """
    if df.empty:
        return df
    df = df.copy()
    df["current_stock"] = df[name_col].map(stock_lookup)
    cols = list(df.columns)
    cols.remove("current_stock")
    if "quantity_sold" in cols:
        pos = cols.index("quantity_sold") + 1
        cols.insert(pos, "current_stock")
    df = df[cols]
    return df


def df_to_html_with_stock(df: pd.DataFrame) -> str:
    """
    Render DataFrame to HTML with:
      - Short column headers (via COLUMN_LABELS)
      - Negative 'current_stock' cells highlighted red
      - N/A for missing stock values
    """
    if df.empty:
        return ""

    df = df.copy()
    headers = "".join(
        f"<th>{COLUMN_LABELS.get(c, c)}</th>" for c in df.columns
    )

    rows_html = []
    for _, row in df.iterrows():
        cells = []
        for col in df.columns:
            val = row[col]
            if col == "current_stock":
                if pd.isna(val):
                    cells.append("<td>N/A</td>")
                else:
                    val_num = float(val)
                    if val_num < 0:
                        cells.append(
                            f'<td style="color:#dc3545;font-weight:bold;">{val_num:.0f}</td>'
                        )
                    else:
                        cells.append(f"<td>{val_num:.0f}</td>")
            else:
                cells.append(f"<td>{val}</td>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")

    return (
        '<table class="styled-table">'
        "<thead><tr>" + headers + "</tr></thead>"
        "<tbody>" + "".join(rows_html) + "</tbody>"
        "</table>"
    )


# =============================================================================
# CHART GENERATION  (Plotly — unchanged, operates on Pandas DataFrames)
# =============================================================================

def plot_chart(df, x_col, y_col, title, top_n=10):
    """Generate a modern Plotly bar chart and return base64 image string."""
    if df.empty:
        return ""
    df_plot = df.head(top_n)

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_plot[x_col],
                y=df_plot[y_col],
                text=[f"₹{v:,.0f}" for v in df_plot[y_col]],
                textposition="outside",
                marker=dict(color="#0078d7"),
            )
        ]
    )
    fig.update_layout(
        title=dict(text=title, font=dict(size=18, color="#0078d7", family="Segoe UI")),
        xaxis=dict(title="", tickangle=-45, automargin=True),
        yaxis=dict(title="Sales (₹)", gridcolor="rgba(200,200,200,0.3)"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=40, r=40, t=50, b=80),
        height=500,
    )

    buffer = BytesIO()
    fig.write_image(buffer, format="png", scale=2)
    buffer.seek(0)
    img_base64 = base64.b64encode(buffer.read()).decode("utf-8")
    return f'<img src="data:image/png;base64,{img_base64}" style="display:block;margin:auto;width:90%;max-height:500px;">'


# =============================================================================
# SPARK DATA LOADING
# =============================================================================

def load_store_week_data(spark, store_name: str, week_start: date, week_end: date):
    """
    Load all billing_data rows for one store and one week via Spark JDBC.

    Mirrors original week filter exactly:
      orderDate > (max_date - 7 days) AND orderDate <= max_date
    Which is equivalent to: orderDate >= week_start AND orderDate <= week_end
    (since week_start = week_end - 6 days = max_date - 6 days,
     and max_date - 7 days is strictly less than week_start)

    Uses SQL subquery as dbtable to push filter into PostgreSQL —
    only relevant rows transferred. No partitionColumn (DATE type not supported).
    Result is .cache()'d — reused across all 3 aggregations + totals.
    """
    print(f"  📥 Loading data for {store_name} ({week_start} → {week_end}) via Spark JDBC...")

    subquery = f"""(
        SELECT *
        FROM billing_data
        WHERE "storeName" = '{store_name.replace("'", "''")}'
          AND "orderDate" >= DATE '{week_start}'
          AND "orderDate" <= DATE '{week_end}'
    ) AS store_week_data"""

    df = (
        spark.read
        .format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  subquery)
        .option("user",     JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver",   JDBC_PROPERTIES["driver"])
        .load()
        .withColumn("totalProductPrice", F.col("totalProductPrice").cast(DoubleType()))
        .withColumn("costPrice",         F.col("costPrice").cast(DoubleType()))
        .withColumn("quantity",          F.col("quantity").cast(DoubleType()))
        .cache()
    )

    row_count = df.count()
    print(f"  Loaded {row_count} rows for {store_name}.")
    return df


# =============================================================================
# SPARK AGGREGATIONS
# Each function mirrors the original SQL query logic exactly.
# All receive the cached df_week Spark DataFrame.
# All return Pandas DataFrames (needed for stock injection, LLM calls, HTML).
# =============================================================================

def compute_comparison(spark, store_name: str, week_end: date) -> dict:
    """
    Compute current week sales vs previous 2-week average.

    Mirrors original comparison_query exactly:
      current_week : orderDate > week_end - 7d  AND orderDate <= week_end
      week_2       : orderDate > week_end - 14d AND orderDate <= week_end - 7d
      week_3       : orderDate > week_end - 21d AND orderDate <= week_end - 14d
      prev_2_weeks_avg = (week_2_sales + week_3_sales) / 2.0
      Always divides by 2.0 — matching original SQL exactly.
    """
    # Load 21 days of data for this store (3 weeks)
    three_weeks_start = week_end - timedelta(days=21)

    subquery = f"""(
        SELECT "totalProductPrice", "orderDate"
        FROM billing_data
        WHERE "storeName" = '{store_name.replace("'", "''")}'
          AND "orderDate" >  DATE '{three_weeks_start}'
          AND "orderDate" <= DATE '{week_end}'
    ) AS comparison_data"""

    df = (
        spark.read
        .format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  subquery)
        .option("user",     JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver",   JDBC_PROPERTIES["driver"])
        .load()
        .withColumn("totalProductPrice", F.col("totalProductPrice").cast(DoubleType()))
        .withColumn("orderDate", F.col("orderDate").cast("date"))
    )

    # Bucket each row into the correct week period using the same
    # exclusive-lower / inclusive-upper logic as the original SQL
    w_end       = week_end
    w_end_minus7  = week_end - timedelta(days=7)
    w_end_minus14 = week_end - timedelta(days=14)
    w_end_minus21 = week_end - timedelta(days=21)

    df = df.withColumn(
        "week_bucket",
        F.when(
            (F.col("orderDate") >  F.lit(str(w_end_minus7)))  &
            (F.col("orderDate") <= F.lit(str(w_end))),
            F.lit("current")
        ).when(
            (F.col("orderDate") >  F.lit(str(w_end_minus14))) &
            (F.col("orderDate") <= F.lit(str(w_end_minus7))),
            F.lit("week2")
        ).when(
            (F.col("orderDate") >  F.lit(str(w_end_minus21))) &
            (F.col("orderDate") <= F.lit(str(w_end_minus14))),
            F.lit("week3")
        ).otherwise(F.lit(None))
    ).filter(F.col("week_bucket").isNotNull())

    agg = (
        df.groupBy("week_bucket")
        .agg(F.sum("totalProductPrice").alias("sales"))
        .toPandas()
    )

    def get_bucket_sales(bucket: str) -> float:
        rows = agg[agg["week_bucket"] == bucket]["sales"]
        return float(rows.iloc[0]) if len(rows) > 0 else 0.0

    current_week_sales = get_bucket_sales("current")
    week_2_sales       = get_bucket_sales("week2")
    week_3_sales       = get_bucket_sales("week3")

    # Original SQL: (week_2_sales + week_3_sales) / 2.0
    prev_2_weeks_avg = (week_2_sales + week_3_sales) / 2.0

    return {
        "current_week_sales": current_week_sales,
        "prev_2_weeks_avg":   prev_2_weeks_avg,
    }


def compute_brand_agg(df_week) -> pd.DataFrame:
    """
    Brand-level aggregation — mirrors original brand_query SQL exactly.
    Output columns: brandName, total_sales, quantity_sold,
                    contrib_percent, profit_margin
    Top 50 by total_sales.
    Note: margin column is 'profit_margin' (not 'PROFIT_MARGIN' as in monthly).
    """
    total_sales_val = df_week.agg(
        F.sum("totalProductPrice").alias("t")
    ).collect()[0]["t"] or 1.0

    brand_df = (
        df_week
        .groupBy("brandName")
        .agg(
            F.round(F.sum("totalProductPrice"), 2).alias("total_sales"),
            F.sum("quantity").alias("quantity_sold"),
            F.round(
                F.sum("totalProductPrice") / total_sales_val * 100, 2
            ).alias("contrib_percent"),
            F.round(
                F.when(
                    F.sum("totalProductPrice") > 0,
                    (
                        F.sum("totalProductPrice")
                        - F.sum(F.coalesce(F.col("costPrice"), F.lit(0.0)) * F.col("quantity"))
                    ) / F.sum("totalProductPrice") * 100
                ).otherwise(F.lit(0.0)),
                2
            ).alias("profit_margin")
        )
        .orderBy(F.col("total_sales").desc())
        .limit(50)
        .toPandas()
    )
    return brand_df


def compute_category_agg(df_week) -> pd.DataFrame:
    """
    Category-level aggregation — mirrors original category_query SQL exactly.
    Output columns: categoryName, total_sales, quantity_sold,
                    contrib_percent, profit_margin
    Top 50 by total_sales.
    """
    total_sales_val = df_week.agg(
        F.sum("totalProductPrice").alias("t")
    ).collect()[0]["t"] or 1.0

    category_df = (
        df_week
        .groupBy("categoryName")
        .agg(
            F.round(F.sum("totalProductPrice"), 2).alias("total_sales"),
            F.sum("quantity").alias("quantity_sold"),
            F.round(
                F.sum("totalProductPrice") / total_sales_val * 100, 2
            ).alias("contrib_percent"),
            F.round(
                F.when(
                    F.sum("totalProductPrice") > 0,
                    (
                        F.sum("totalProductPrice")
                        - F.sum(F.coalesce(F.col("costPrice"), F.lit(0.0)) * F.col("quantity"))
                    ) / F.sum("totalProductPrice") * 100
                ).otherwise(F.lit(0.0)),
                2
            ).alias("profit_margin")
        )
        .orderBy(F.col("total_sales").desc())
        .limit(50)
        .toPandas()
    )
    return category_df


def compute_product_agg(df_week) -> pd.DataFrame:
    """
    Product-level aggregation — mirrors original product_query SQL exactly.
    Output columns: productName, total_sales, quantity_sold,
                    contrib_percent, profit_margin
    Top 100 by total_sales.
    """
    total_sales_val = df_week.agg(
        F.sum("totalProductPrice").alias("t")
    ).collect()[0]["t"] or 1.0

    product_df = (
        df_week
        .groupBy("productName")
        .agg(
            F.round(F.sum("totalProductPrice"), 2).alias("total_sales"),
            F.sum("quantity").alias("quantity_sold"),
            F.round(
                F.sum("totalProductPrice") / total_sales_val * 100, 2
            ).alias("contrib_percent"),
            F.round(
                F.when(
                    F.sum("totalProductPrice") > 0,
                    (
                        F.sum("totalProductPrice")
                        - F.sum(F.coalesce(F.col("costPrice"), F.lit(0.0)) * F.col("quantity"))
                    ) / F.sum("totalProductPrice") * 100
                ).otherwise(F.lit(0.0)),
                2
            ).alias("profit_margin")
        )
        .orderBy(F.col("total_sales").desc())
        .limit(100)
        .toPandas()
    )
    return product_df


def compute_total_sales_profit(df_week) -> dict:
    """
    Compute total weekly sales, cost, profit, and avg profit margin.

    Mirrors original total_sales_profit_query SQL exactly.
    Per-row item_cost and item_profit_margin computed via withColumn first
    (matching the item_margins CTE), then aggregated.
    """
    df_with_margin = df_week.withColumn(
        "item_cost",
        F.coalesce(F.col("costPrice"), F.lit(0.0)) * F.col("quantity")
    ).withColumn(
        "item_profit_margin",
        F.when(
            F.col("totalProductPrice") > 0,
            (F.col("totalProductPrice") - F.coalesce(F.col("costPrice"), F.lit(0.0)) * F.col("quantity"))
            / F.col("totalProductPrice") * 100
        ).otherwise(F.lit(0.0))
    )

    result = df_with_margin.agg(
        F.round(F.sum("totalProductPrice"),   2).alias("total_weekly_sales"),
        F.round(F.sum("item_cost"),           2).alias("total_weekly_cost"),
        F.round(
            F.sum("totalProductPrice") - F.sum("item_cost"), 2
        ).alias("total_weekly_profit"),
        F.round(F.avg("item_profit_margin"),  2).alias("avg_profit_margin_percent")
    ).collect()[0]

    return {
        "total_weekly_sales":       float(result["total_weekly_sales"]       or 0.0),
        "total_weekly_cost":        float(result["total_weekly_cost"]        or 0.0),
        "total_weekly_profit":      float(result["total_weekly_profit"]      or 0.0),
        "avg_profit_margin_percent": float(result["avg_profit_margin_percent"] or 0.0),
    }


# =============================================================================
# REPORT GENERATOR
# =============================================================================

def generate_store_report(spark, store_name: str):
    """
    Generate weekly PDF report for one store.

    Layer split:
      - Date resolution:        SQLAlchemy (single-row metadata query)
      - Data fetch + agg:       PySpark JDBC (all heavy computation)
      - Stock CSV injection:    Pandas (local CSV, unchanged)
      - LLM + stock + RTV calls: Python (Pandas DataFrames, unchanged)
      - Charts + HTML + PDF:    Plotly + pdfkit (unchanged)
    """

    # ── Step 1: Resolve week dates (SQLAlchemy metadata query) ────────────────
    week_start, week_end = get_week_dates(store_name)

    if week_start is None:
        print(f"  ⚠️ Could not determine week dates for {store_name} — skipping.")
        return

    week_start_str = week_start.strftime('%d %b %Y')
    week_end_str   = week_end.strftime('%d %b %Y')

    # ── Step 2: Load store's weekly data into Spark (one JDBC read, cached) ───
    df_week = load_store_week_data(spark, store_name, week_start, week_end)

    if df_week.count() == 0:
        print(f"  ⚠️ No data found for {store_name} in week {week_start_str} — skipping.")
        df_week.unpersist()
        return

    # ── Step 3: Comparison stats (current week vs prev 2-week avg) ───────────
    comparison = compute_comparison(spark, store_name, week_end)
    current_week_sales = comparison["current_week_sales"]
    prev_2_weeks_avg   = comparison["prev_2_weeks_avg"]

    if current_week_sales > 0:
        if prev_2_weeks_avg > 0:
            percentage_diff = ((current_week_sales - prev_2_weeks_avg) / prev_2_weeks_avg) * 100
            diff_sign  = "+" if percentage_diff >= 0 else ""
            diff_color = "#28a745" if percentage_diff >= 0 else "#dc3545"
            comparison_text = f"""
                <div style="text-align: center; margin-top: 10px;">
                    <span style="font-size: 18px; color: #666;">Previous 2 Weeks Average: ₹{prev_2_weeks_avg:,.2f}</span><br>
                    <span style="font-size: 20px; font-weight: bold; color: {diff_color};">
                        {diff_sign}{percentage_diff:.2f}%
                    </span>
                </div>
            """
        else:
            comparison_text = '<div style="text-align: center; margin-top: 10px;"><span style="font-size: 18px; color: #666;">Insufficient data for comparison</span></div>'
    else:
        comparison_text = '<div style="text-align: center; margin-top: 10px;"><span style="font-size: 18px; color: #666;">No sales data available</span></div>'

    # ── Step 4: All aggregations via PySpark (reusing cached df_week) ─────────
    brand_df    = compute_brand_agg(df_week)
    category_df = compute_category_agg(df_week)
    product_df  = compute_product_agg(df_week)
    financials  = compute_total_sales_profit(df_week)

    total_weekly_sales        = financials["total_weekly_sales"]
    total_weekly_cost         = financials["total_weekly_cost"]
    total_weekly_profit       = financials["total_weekly_profit"]
    avg_profit_margin_percent = financials["avg_profit_margin_percent"]

    # ── Step 5: Release cached Spark data — all aggregations done ─────────────
    df_week.unpersist()

    # ── Step 6: Load stock CSV, inject current_stock column (Pandas — unchanged)
    brand_stock_lookup, category_stock_lookup, product_stock_lookup = load_stock_lookups(store_name)
    brand_df    = inject_stock_column(brand_df,    "brandName",    brand_stock_lookup)
    category_df = inject_stock_column(category_df, "categoryName", category_stock_lookup)
    product_df  = inject_stock_column(product_df,  "productName",  product_stock_lookup)

    # ── Step 7: LLM recommendations (Pandas DataFrames — unchanged) ──────────
    print(f"  🤖 Generating LLM recommendations for {store_name}...")
    brand_rec    = brand_recommendation(store_name, brand_df,    total_weekly_sales, week_start=week_start, engine=engine, report_type="weekly")
    category_rec = category_recommendation(store_name, category_df, total_weekly_sales, week_start=week_start, engine=engine, report_type="weekly")
    product_rec  = product_recommendation(store_name, product_df,  total_weekly_sales, week_start=week_start, engine=engine, report_type="weekly")

    # ── Step 8: Stock insight calls (reads per-store CSV — unchanged) ─────────
    print(f"  📦 Generating stock insights for {store_name}...")
    brand_stock_rec    = brand_stock_insight(store_name,    STOCK_DIR, LOW_STOCK_THRESHOLD)
    category_stock_rec = category_stock_insight(store_name, STOCK_DIR, LOW_STOCK_THRESHOLD)
    product_stock_rec  = product_stock_insight(store_name,  STOCK_DIR, LOW_STOCK_THRESHOLD)

    # ── Step 9: RTV insight call (reads per-store CSV — unchanged) ────────────
    print(f"  🔄 Generating RTV insights for {store_name}...")
    rtv_rec = rtv_insight(store_name, RTV_DIR)

    save_weekly_snapshot(store_name, week_start, brand_df, category_df, product_df, engine)

    # ── Step 10: Add % suffix AFTER LLM calls, skip current_stock (unchanged) ─
    for df in [brand_df, category_df, product_df]:
        if not df.empty:
            if 'contrib_percent' in df.columns:
                df['contrib_percent'] = df['contrib_percent'].astype(str) + '%'
            if 'profit_margin' in df.columns:
                df['profit_margin'] = df['profit_margin'].astype(str) + '%'

    # ── Step 11: Charts (Plotly on Pandas — unchanged) ────────────────────────
    brand_chart    = plot_chart(brand_df,    "brandName",    "total_sales", "Top 10 Brands by Sales")
    category_chart = plot_chart(category_df, "categoryName", "total_sales", "Top 10 Categories by Sales")
    product_chart  = plot_chart(product_df,  "productName",  "total_sales", "Top 10 Products by Sales")

    # ── Step 12: Render tables with stock column + colour coding (unchanged) ───
    brand_table_html    = df_to_html_with_stock(brand_df)
    category_table_html = df_to_html_with_stock(category_df)
    product_table_html  = df_to_html_with_stock(product_df)

    # ── Step 13: HTML template (unchanged) ───────────────────────────────────
    html_template = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{store_name} - Weekly Store Report</title>
        <style>
            body {{
                font-family: 'Segoe UI', sans-serif;
                margin: 20px;
                background-color: #f6f8fa;
                position: relative;
            }}
            .logo {{
                position: absolute;
                top: 40px;
                right: 20px;
                width: 100px;
                height: auto;
            }}
            h1 {{
                text-align: center;
                color: #333;
                margin-bottom: 10px;
                padding-top: 0px;
            }}
            h2 {{
                text-align: center;
                color: #0078d7;
                margin-bottom: 5px;
            }}
            .date-range {{
                text-align: center;
                color: #666;
                font-size: 16px;
                margin-bottom: 20px;
            }}
            .profit-section {{
                text-align: center;
                margin: 15px 0;
            }}
            .profit-label {{
                font-size: 18px;
                color: #666;
                display: inline-block;
                margin-right: 10px;
            }}
            .profit-value {{
                font-size: 20px;
                font-weight: bold;
                color: #28a745;
            }}
            .profit-margin {{
                font-size: 20px;
                font-weight: bold;
                color: #0078d7;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
                background-color: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
            }}
            th, td {{
                padding: 10px 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }}
            th {{
                background-color: #0078d7;
                color: white;
                text-transform: uppercase;
            }}
            tr:hover {{ background-color: #f1f1f1; }}
            .table-title {{
                color: #0078d7;
                font-size: 22px;
                font-weight: bold;
                text-align: center;
                margin: 20px 0 10px;
            }}
        </style>
    </head>
    <body>
        <img src="file:///home/azureuser/azure_analysis_algorithm/tns.png" class="logo" alt="Company Logo">
        <h1>📊 Weekly Store Report – {store_name}</h1>
        <div class="date-range">Week: {week_start_str} to {week_end_str}</div>
        <h2>Total Weekly Sales: ₹{total_weekly_sales:,.2f}</h2>

        <div class="profit-section">
            <span class="profit-label">Total Profit:</span>
            <span class="profit-value">₹{total_weekly_profit:,.2f}</span>
            <span style="margin: 0 15px;">|</span>
            <span class="profit-label">Average Profit Margin:</span>
            <span class="profit-margin">{avg_profit_margin_percent:.2f}%</span>
        </div>

        {comparison_text}

        <div class="table-title">Top 50 Brands (by Sales)</div>
        {brand_table_html}
        {brand_chart}
        {brand_rec}
        {brand_stock_rec}


        <div style="height: 240px;"></div>
        <div class="table-title">Top 50 Categories (by Sales)</div>
        {category_table_html}
        {category_chart}
        {category_rec}
        {category_stock_rec}

        <div class="table-title">Top 100 Products (by Sales)</div>
        {product_table_html}
        {product_chart}
        {product_rec}
        {product_stock_rec}
        {rtv_rec}
    </body>
    </html>
    """

    # ── Step 14: Save PDF (unchanged) ─────────────────────────────────────────
    # ── Upload PDF to Azure Blob Storage ─────────────────────────────

    blob_name = f"{store_name.replace(' ', '_')}_weekly_report.pdf"

    # Create temp file instead of saving locally
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Generate PDF in temp file
        pdfkit.from_string(
            html_template,
            tmp_path,
            configuration=PDFKIT_CONFIG,
            options={"enable-local-file-access": ""}
        )

        # Upload to Azure Blob
        service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = service_client.get_container_client(AZURE_CONTAINER)

        try:
            container_client.create_container()
        except Exception:
            pass  # already exists

        blob_client = container_client.get_blob_client(blob_name)

        with open(tmp_path, "rb") as f:
            blob_client.upload_blob(
                f,
                overwrite=True,
                content_settings=ContentSettings(content_type="application/pdf")
            )

        # Generate SAS URL
        sas_token = generate_blob_sas(
            account_name=AZURE_ACCOUNT_NAME,
            container_name=AZURE_CONTAINER,
            blob_name=blob_name,
            account_key=AZURE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(days=SAS_EXPIRY_DAYS),
        )

        shareable_url = (
            f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net/"
            f"{AZURE_CONTAINER}/{blob_name}?{sas_token}"
        )

        print(f"✅ Uploaded {store_name} → {shareable_url}")

    finally:
        os.remove(tmp_path)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    store_names = get_unique_stores()
    print(f"Found {len(store_names)} stores.\nGenerating reports...\n")

    # Start Spark once — reused across all store reports
    spark = get_spark()

    for store in store_names:
        try:
            generate_store_report(spark, store)
            time.sleep(1)
        except Exception as e:
            print(f"❌ Error generating report for {store}: {e}")

    spark.stop()
    print("\n✅ All store reports generated successfully inside azure Blob Storage")