import pandas as pd
import pdfkit
import plotly.graph_objects as go
import base64
import os
import time
from io import BytesIO
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── LLM recommender ───────────────────────────────────────────────────────────
from llm_recommender import (
    brand_recommendation,
    category_recommendation,
    product_recommendation,
    save_monthly_snapshot,
)

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

# SQLAlchemy engine — used only for short metadata queries (store list,
# date range). These are single-row lookups that don't benefit from Spark.
engine = create_engine(DB_URI, pool_pre_ping=True, pool_recycle=300)

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
        .appName("MonthlyReports")
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
#         .appName("MonthlyReports")
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
# =============================================================================

def safe_read_sql(query, params=None, retries=3, delay=3):
    """Executes a metadata SQL query with retries for transient DB disconnects."""
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                return pd.read_sql(query, conn, params=params)
        except OperationalError as e:
            print(f"⚠️ Database error (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
        except Exception as e:
            print(f"⚠️ Unexpected error (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("❌ Query failed after multiple retries.")


def get_unique_stores():
    """Fetch all unique store names."""
    query = 'SELECT DISTINCT "storeName" FROM "billing_data" ORDER BY "storeName";'
    df = safe_read_sql(query)
    return df["storeName"].dropna().tolist()


def get_report_month(store_name: str):
    """
    Determine the target report month for a store — mirrors original SQL logic.
    Returns (month_start, month_end) as Python date objects, or (None, None).
    Uses SQLAlchemy — single-row metadata query, no Spark needed.
    """
    date_range_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        ),
        target_month AS (
            SELECT
                CASE
                    WHEN (SELECT DATE_TRUNC('month', max_date) FROM latest_date)
                         >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                    THEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                    ELSE DATE_TRUNC('month', (SELECT max_date FROM latest_date))
                END AS report_month
        )
        SELECT
            (SELECT report_month FROM target_month)::date AS month_start,
            ((SELECT report_month FROM target_month)
             + INTERVAL '1 month' - INTERVAL '1 day')::date AS month_end;
    """
    date_info = safe_read_sql(date_range_query, params=(store_name,))
    if date_info.empty or date_info['month_start'].iloc[0] is None:
        return None, None
    # Ensure Python date objects (SQLAlchemy may return Timestamp or date)
    month_start = date_info['month_start'].iloc[0]
    month_end   = date_info['month_end'].iloc[0]
    if hasattr(month_start, 'date'):
        month_start = month_start.date()
    if hasattr(month_end, 'date'):
        month_end = month_end.date()
    return month_start, month_end


# =============================================================================
# SPARK DATA LOADING
# =============================================================================

def load_store_month_data(spark, store_name: str, month_start: date, month_end: date):
    """
    Load all billing_data rows for one store and one complete month via Spark JDBC.
    Uses a subquery as dbtable to push the store + date filter into PostgreSQL —
    only the relevant rows are transferred, not the full table.

    NOTE: Spark JDBC partitionColumn requires a numeric column. Since orderDate
    is a DATE, we use a simple single-partition read (no partitionColumn).
    For very large tables on a cluster, consider a numeric surrogate key instead.

    The result is .cache()'d — reused across all aggregations for this store
    so PostgreSQL is queried only once per store per report run.
    """
    print(f"  📥 Loading data for {store_name} ({month_start} → {month_end}) via Spark JDBC...")

    # Push filter into PostgreSQL via subquery in dbtable
    subquery = f"""(
        SELECT *
        FROM billing_data
        WHERE "storeName" = '{store_name.replace("'", "''")}'
          AND "orderDate" >= DATE '{month_start}'
          AND "orderDate" <= DATE '{month_end}'
    ) AS store_month_data"""

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
        .cache()  # cached — reused by brand/category/product/totals aggregations
    )

    row_count = df.count()
    print(f"  Loaded {row_count} rows for {store_name}.")
    return df


# =============================================================================
# SPARK AGGREGATIONS
# Each function mirrors the original SQL query logic exactly.
# All receive the cached df_month Spark DataFrame.
# All return Pandas DataFrames ready for LLM calls and HTML rendering.
# =============================================================================

def compute_comparison(spark, store_name: str, month_start: date) -> dict:
    """
    Compute current month sales and previous 3-month average.

    Mirrors original comparison_query exactly:
      - current_month_sales  = SUM of totalProductPrice in report month
      - prev_3_months_avg    = (M-1 + M-2 + M-3) / 3.0
        Always divides by 3.0 regardless of data availability —
        matching the original SQL behaviour precisely.
    """
    # month_start is a Python date — compute prior month boundaries safely
    m1_start = (month_start - relativedelta(months=1)).replace(day=1)
    m2_start = (month_start - relativedelta(months=2)).replace(day=1)
    m3_start = (month_start - relativedelta(months=3)).replace(day=1)
    # Last day of month_start - 1 day = last day of 3 months ago
    four_months_ago_start = m3_start
    month_end_exclusive   = month_start + relativedelta(months=1)  # first day of next month

    subquery = f"""(
        SELECT "totalProductPrice", "orderDate"
        FROM billing_data
        WHERE "storeName" = '{store_name.replace("'", "''")}'
          AND "orderDate" >= DATE '{four_months_ago_start}'
          AND "orderDate" <  DATE '{month_end_exclusive}'
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
        .withColumn("month_trunc",
                    F.trunc(F.col("orderDate").cast("date"), "month"))
    )

    # Aggregate per month
    agg = (
        df.groupBy("month_trunc")
        .agg(F.sum("totalProductPrice").alias("month_sales"))
        .toPandas()
    )
    agg["month_trunc"] = pd.to_datetime(agg["month_trunc"]).dt.date

    month_start_ts = month_start

    def get_month_sales(target_start: date) -> float:
        rows = agg[agg["month_trunc"] == target_start]["month_sales"]
        return float(rows.iloc[0]) if len(rows) > 0 else 0.0

    current_month_sales = get_month_sales(month_start)
    m1_sales            = get_month_sales(m1_start)
    m2_sales            = get_month_sales(m2_start)
    m3_sales            = get_month_sales(m3_start)

    # Original SQL: (month_1_sales + month_2_sales + month_3_sales) / 3.0
    # Always divides by 3.0 — not by count of non-zero months
    prev_3_months_avg = (m1_sales + m2_sales + m3_sales) / 3.0

    return {
        "current_month_sales": current_month_sales,
        "prev_3_months_avg":   prev_3_months_avg,
    }


def compute_brand_agg(df_month) -> pd.DataFrame:
    """
    Brand-level aggregation — mirrors original brand_query SQL exactly.
    Output columns: brandName, total_sales, quantity_sold,
                    contrib_percent, PROFIT_MARGIN
    Top 50 by total_sales.
    """
    total_sales_val = df_month.agg(
        F.sum("totalProductPrice").alias("t")
    ).collect()[0]["t"] or 1.0

    brand_df = (
        df_month
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
            ).alias("PROFIT_MARGIN")
        )
        .orderBy(F.col("total_sales").desc())
        .limit(50)
        .toPandas()
    )
    return brand_df


def compute_category_agg(df_month) -> pd.DataFrame:
    """
    Category-level aggregation — mirrors original category_query SQL exactly.
    Output columns: categoryName, total_sales, quantity_sold,
                    contrib_percent, PROFIT_MARGIN
    Top 50 by total_sales.
    """
    total_sales_val = df_month.agg(
        F.sum("totalProductPrice").alias("t")
    ).collect()[0]["t"] or 1.0

    category_df = (
        df_month
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
            ).alias("PROFIT_MARGIN")
        )
        .orderBy(F.col("total_sales").desc())
        .limit(50)
        .toPandas()
    )
    return category_df


def compute_product_agg(df_month) -> pd.DataFrame:
    """
    Product-level aggregation — mirrors original product_query SQL exactly.
    Output columns: productName, total_sales, quantity_sold,
                    contrib_percent, PROFIT_MARGIN
    Top 100 by total_sales.
    """
    total_sales_val = df_month.agg(
        F.sum("totalProductPrice").alias("t")
    ).collect()[0]["t"] or 1.0

    product_df = (
        df_month
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
            ).alias("PROFIT_MARGIN")
        )
        .orderBy(F.col("total_sales").desc())
        .limit(100)
        .toPandas()
    )
    return product_df


def compute_total_sales_profit(df_month) -> dict:
    """
    Compute total sales, cost, profit, and avg profit margin.

    Mirrors original total_sales_profit_query SQL exactly.
    Key fix: avg_profit_margin is computed as AVG of per-row margin,
    not as a single-pass aggregation — matching the original SQL's
    item_margins CTE pattern.
    """
    # Compute per-row item_cost and item_profit_margin first (mirrors item_margins CTE)
    df_with_margin = df_month.withColumn(
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
        F.round(F.sum("totalProductPrice"), 2).alias("total_monthly_sales"),
        F.round(F.sum("item_cost"),          2).alias("total_monthly_cost"),
        F.round(
            F.sum("totalProductPrice") - F.sum("item_cost"), 2
        ).alias("total_monthly_profit"),
        F.round(F.avg("item_profit_margin"),  2).alias("avg_profit_margin_percent")
    ).collect()[0]

    return {
        "total_monthly_sales":       float(result["total_monthly_sales"]       or 0.0),
        "total_monthly_cost":        float(result["total_monthly_cost"]        or 0.0),
        "total_monthly_profit":      float(result["total_monthly_profit"]      or 0.0),
        "avg_profit_margin_percent": float(result["avg_profit_margin_percent"] or 0.0),
    }


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
# REPORT GENERATOR
# =============================================================================

def generate_store_report(spark, store_name: str):
    """
    Generate monthly PDF report for one store — previous complete month.

    Layer split:
      - Date resolution:   SQLAlchemy (single-row metadata query)
      - Data fetch + agg:  PySpark JDBC (all heavy computation)
      - LLM calls:         Python (operates on Pandas DataFrames)
      - Charts + HTML+PDF: Plotly + pdfkit (unchanged)
    """

    # ── Step 1: Resolve report month (SQLAlchemy metadata query) ─────────────
    month_start, month_end = get_report_month(store_name)

    if month_start is None:
        print(f"  ⚠️ Could not determine report month for {store_name} — skipping.")
        return

    month_start_str = month_start.strftime('%d %b %Y')
    month_end_str   = month_end.strftime('%d %b %Y')

    # ── Step 2: Load store's monthly data into Spark (one JDBC read, cached) ──
    df_month = load_store_month_data(spark, store_name, month_start, month_end)

    if df_month.count() == 0:
        print(f"  ⚠️ No data found for {store_name} in {month_start_str} — skipping.")
        df_month.unpersist()
        return

    # ── Step 3: Comparison stats (current month vs prev 3-month avg) ─────────
    comparison = compute_comparison(spark, store_name, month_start)
    current_month_sales = comparison["current_month_sales"]
    prev_3_months_avg   = comparison["prev_3_months_avg"]

    if current_month_sales > 0:
        if prev_3_months_avg > 0:
            percentage_diff = ((current_month_sales - prev_3_months_avg) / prev_3_months_avg) * 100
            diff_sign  = "+" if percentage_diff >= 0 else ""
            diff_color = "#28a745" if percentage_diff >= 0 else "#dc3545"
            comparison_text = f"""
                <div style="text-align: center; margin-top: 10px;">
                    <span style="font-size: 18px; color: #666;">Previous 3 Months Average: ₹{prev_3_months_avg:,.2f}</span><br>
                    <span style="font-size: 20px; font-weight: bold; color: {diff_color};">
                        {diff_sign}{percentage_diff:.2f}%
                    </span>
                </div>
            """
        else:
            comparison_text = '<div style="text-align: center; margin-top: 10px;"><span style="font-size: 18px; color: #666;">Insufficient data for comparison</span></div>'
    else:
        comparison_text = '<div style="text-align: center; margin-top: 10px;"><span style="font-size: 18px; color: #666;">No sales data available</span></div>'

    # ── Step 4: All aggregations via PySpark (reusing cached df_month) ────────
    brand_df    = compute_brand_agg(df_month)
    category_df = compute_category_agg(df_month)
    product_df  = compute_product_agg(df_month)
    financials  = compute_total_sales_profit(df_month)

    total_monthly_sales       = financials["total_monthly_sales"]
    total_monthly_cost        = financials["total_monthly_cost"]
    total_monthly_profit      = financials["total_monthly_profit"]
    avg_profit_margin_percent = financials["avg_profit_margin_percent"]

    # ── Step 5: Release cached Spark data — all aggregations done ─────────────
    df_month.unpersist()

    # ── Step 6: LLM recommendations (Pandas DataFrames — unchanged) ──────────
    print(f"  🤖 Generating LLM recommendations for {store_name}...")
    brand_rec    = brand_recommendation(store_name, brand_df,    total_monthly_sales, month_start=month_start, engine=engine, report_type="monthly")
    category_rec = category_recommendation(store_name, category_df, total_monthly_sales, month_start=month_start, engine=engine, report_type="monthly")
    product_rec  = product_recommendation(store_name, product_df,  total_monthly_sales, month_start=month_start, engine=engine, report_type="monthly")

    save_monthly_snapshot(store_name, month_start, brand_df, category_df, product_df, engine)

    # ── Step 7: Add % suffix AFTER LLM calls (unchanged) ─────────────────────
    for df in [brand_df, category_df, product_df]:
        if not df.empty:
            if 'contrib_percent' in df.columns:
                df['contrib_percent'] = df['contrib_percent'].astype(str) + '%'
            if 'profit_margin' in df.columns:
                df['profit_margin'] = df['profit_margin'].astype(str) + '%'
            if 'PROFIT_MARGIN' in df.columns:
                df['PROFIT_MARGIN'] = df['PROFIT_MARGIN'].astype(str) + '%'

    # ── Step 8: Charts (Plotly on Pandas — unchanged) ─────────────────────────
    brand_chart    = plot_chart(brand_df,    "brandName",    "total_sales", "Top 10 Brands by Sales")
    category_chart = plot_chart(category_df, "categoryName", "total_sales", "Top 10 Categories by Sales")
    product_chart  = plot_chart(product_df,  "productName",  "total_sales", "Top 10 Products by Sales")

    # ── Step 9: HTML template (unchanged) ────────────────────────────────────
    html_template = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{store_name} - Monthly Store Report</title>
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
        <img src="file:///base/dir/sales_analysis_algorithm/tns.png" class="logo" alt="Company Logo">
        <h1>📊 Monthly Store Report – {store_name}</h1>
        <div class="date-range">Month: {month_start_str} to {month_end_str}</div>
        <h2>Total Monthly Sales: ₹{total_monthly_sales:,.2f}</h2>

        <div class="profit-section">
            <span class="profit-label">Total Profit:</span>
            <span class="profit-value">₹{total_monthly_profit:,.2f}</span>
            <span style="margin: 0 15px;">|</span>
            <span class="profit-label">Average Profit Margin:</span>
            <span class="profit-margin">{avg_profit_margin_percent:.2f}%</span>
        </div>

        {comparison_text}

        <div class="table-title">Top 50 Brands (by Sales)</div>
        {brand_df.to_html(index=False, classes="styled-table")}
        {brand_chart}
        {brand_rec}

        <div class="table-title">Top 50 Categories (by Sales)</div>
        {category_df.to_html(index=False, classes="styled-table")}
        {category_chart}
        {category_rec}

        <div class="table-title">Top 100 Products (by Sales)</div>
        {product_df.to_html(index=False, classes="styled-table")}
        {product_chart}
        {product_rec}
    </body>
    </html>
    """

    # ── Step 10: Save PDF (unchanged) ─────────────────────────────────────────
    os.makedirs("/base/dir/sales_analysis_algorithm/monthly_reports", exist_ok=True)
    pdf_path = os.path.join(
        "/base/dir/sales_analysis_algorithm/monthly_reports",
        f"{store_name.replace(' ', '_')}_monthly_report.pdf"
    )
    pdfkit.from_string(
        html_template, pdf_path,
        configuration=PDFKIT_CONFIG,
        options={"enable-local-file-access": ""}
    )
    print(f"✅ Saved {store_name} report → {pdf_path}")


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
    print("\n✅ All store reports generated successfully inside /monthly_reports/")