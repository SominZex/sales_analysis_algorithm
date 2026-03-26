import pandas as pd
import pdfkit
import plotly.graph_objects as go
import base64
import os
import time
import tempfile
from io import BytesIO
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from llm_recommender import save_weekly_snapshot
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContentSettings, generate_blob_sas, BlobSasPermissions

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

# Directory where stock.py saves per-store CSVs (relative or absolute path)
# ───────────────────────── PATH CONFIG (CRITICAL FIX) ─────────────────────────

BASE_DIR = "/home/azureuser/azure_analysis_algorithm"

STOCK_DIR = os.getenv(
    "STOCK_DIR",
    os.path.join(BASE_DIR, "store_stocks")
)

RTV_DIR = os.getenv(
    "RTV_DIR",
    os.path.join(BASE_DIR, "store_rtv")
)

LOW_STOCK_THRESHOLD = float(os.getenv("LOW_STOCK_THRESHOLD", "5"))

# ── Azure Blob Storage ────────────────────────────────────────────────────────
AZURE_ACCOUNT_NAME      = os.getenv("AZURE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY       = os.getenv("AZURE_ACCOUNT_KEY", "")
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING", "")
AZURE_CONTAINER         = os.getenv("AZURE_CONTAINER", "weekly-reports")
SAS_EXPIRY_DAYS = 7

PARTNER_FILE = os.path.join(BASE_DIR, "partner.csv")

print(f"📂 STOCK_DIR → {STOCK_DIR}")
print(f"📂 RTV_DIR   → {RTV_DIR}")

load_dotenv()

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

DB_URI = (
    f"postgresql+psycopg2://"
    f"{require_env('DB_USER')}:{require_env('DB_PASSWORD')}"
    f"@{require_env('DB_HOST')}:{os.getenv('DB_PORT', '5432')}"
    f"/{require_env('DB_NAME')}"
)

# Use NullPool to avoid connection reuse issues
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


PDFKIT_CONFIG = pdfkit.configuration(wkhtmltopdf="/usr/bin/wkhtmltopdf")


def safe_read_sql(query, params=None, retries=5, delay=3):
    """Executes SQL query with retries for transient DB disconnects"""
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
    """Fetch all unique store names"""
    query = 'SELECT DISTINCT "storeName" FROM "billing_data" ORDER BY "storeName";'
    df = safe_read_sql(query)
    return df["storeName"].dropna().tolist()


# ── Short display names for all table columns ────────────────────────────────
COLUMN_LABELS = {
    "brandName":    "Brand",
    "categoryName": "Category",
    "productName":  "Product",
    "total_sales":  "Sales (₹)",
    "quantity_sold":"Qty Sold",
    "current_stock":"Stock",
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
                # product: use the raw row quantity (one row per SKU in stock CSV)
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

    # Map stock values; keep numeric so we can colour-code negatives later
    df["current_stock"] = df[name_col].map(stock_lookup)

    # Reorder: insert current_stock right after quantity_sold
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

    # Build header row with short labels
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


def plot_chart(df, x_col, y_col, title, top_n=10):
    """Generate a modern Plotly bar chart and return base64 image string"""
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


def generate_store_report(store_name):
    """Generate weekly PDF report for one store"""

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

    if date_info.empty:
        week_start_str = "N/A"
        week_end_str = "N/A"
    else:
        week_start = date_info['week_start'].iloc[0]
        week_end   = date_info['week_end'].iloc[0]
        week_start_str = week_start.strftime('%d %b %Y')
        week_end_str   = week_end.strftime('%d %b %Y')

    # === Get Previous 2 Weeks Average and Current Week Sales ===
    comparison_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        ),
        sales_periods AS (
            SELECT
                SUM(CASE
                    WHEN "orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
                         AND "orderDate" <= (SELECT max_date FROM latest_date)
                    THEN "totalProductPrice"
                    ELSE 0
                END) AS current_week_sales,
                SUM(CASE
                    WHEN "orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '14 days'
                         AND "orderDate" <= (SELECT max_date FROM latest_date) - INTERVAL '7 days'
                    THEN "totalProductPrice"
                    ELSE 0
                END) AS week_2_sales,
                SUM(CASE
                    WHEN "orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '21 days'
                         AND "orderDate" <= (SELECT max_date FROM latest_date) - INTERVAL '14 days'
                    THEN "totalProductPrice"
                    ELSE 0
                END) AS week_3_sales
            FROM "billing_data"
            WHERE "storeName" = %s
        )
        SELECT
            current_week_sales,
            (week_2_sales + week_3_sales) / 2.0 AS prev_2_weeks_avg
        FROM sales_periods;
    """

    comparison_df = safe_read_sql(comparison_query, params=(store_name, store_name))

    if not comparison_df.empty and comparison_df['current_week_sales'].iloc[0] is not None:
        current_week_sales = float(comparison_df['current_week_sales'].iloc[0])
        prev_2_weeks_avg   = float(comparison_df['prev_2_weeks_avg'].iloc[0]) if comparison_df['prev_2_weeks_avg'].iloc[0] is not None else 0.0

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
        current_week_sales = 0.0
        comparison_text    = '<div style="text-align: center; margin-top: 10px;"><span style="font-size: 18px; color: #666;">No sales data available</span></div>'

    # === Queries with Contribution % and Profit Margin %
    brand_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        ),
        total_sales AS (
            SELECT SUM(b."totalProductPrice") AS total
            FROM "billing_data" b
            WHERE b."storeName" = %s
              AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
              AND b."orderDate" <= (SELECT max_date FROM latest_date)
        )
        SELECT
            b."brandName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2) AS total_sales,
            SUM(b."quantity") AS quantity_sold,
            ROUND((SUM(b."totalProductPrice") / NULLIF((SELECT total FROM total_sales), 0) * 100)::numeric, 2) AS contrib_percent,
            ROUND(
                CASE
                    WHEN SUM(b."totalProductPrice") > 0 THEN
                        ((SUM(b."totalProductPrice") - SUM(COALESCE(b."costPrice", 0) * b."quantity")) / SUM(b."totalProductPrice") * 100)::numeric
                    ELSE 0
                END, 2
            ) AS profit_margin
        FROM "billing_data" b
        WHERE b."storeName" = %s
          AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
          AND b."orderDate" <= (SELECT max_date FROM latest_date)
        GROUP BY b."brandName"
        ORDER BY total_sales DESC
        LIMIT 50;
    """

    category_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        ),
        total_sales AS (
            SELECT SUM(b."totalProductPrice") AS total
            FROM "billing_data" b
            WHERE b."storeName" = %s
              AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
              AND b."orderDate" <= (SELECT max_date FROM latest_date)
        )
        SELECT
            b."categoryName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2) AS total_sales,
            SUM(b."quantity") AS quantity_sold,
            ROUND((SUM(b."totalProductPrice") / NULLIF((SELECT total FROM total_sales), 0) * 100)::numeric, 2) AS contrib_percent,
            ROUND(
                CASE
                    WHEN SUM(b."totalProductPrice") > 0 THEN
                        ((SUM(b."totalProductPrice") - SUM(COALESCE(b."costPrice", 0) * b."quantity")) / SUM(b."totalProductPrice") * 100)::numeric
                    ELSE 0
                END, 2
            ) AS profit_margin
        FROM "billing_data" b
        WHERE b."storeName" = %s
          AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
          AND b."orderDate" <= (SELECT max_date FROM latest_date)
        GROUP BY b."categoryName"
        ORDER BY total_sales DESC
        LIMIT 50;
    """

    product_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        ),
        total_sales AS (
            SELECT SUM(b."totalProductPrice") AS total
            FROM "billing_data" b
            WHERE b."storeName" = %s
              AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
              AND b."orderDate" <= (SELECT max_date FROM latest_date)
        )
        SELECT
            b."productName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2) AS total_sales,
            SUM(b."quantity") AS quantity_sold,
            ROUND((SUM(b."totalProductPrice") / NULLIF((SELECT total FROM total_sales), 0) * 100)::numeric, 2) AS contrib_percent,
            ROUND(
                CASE
                    WHEN SUM(b."totalProductPrice") > 0 THEN
                        ((SUM(b."totalProductPrice") - SUM(COALESCE(b."costPrice", 0) * b."quantity")) / SUM(b."totalProductPrice") * 100)::numeric
                    ELSE 0
                END, 2
            ) AS profit_margin
        FROM "billing_data" b
        WHERE b."storeName" = %s
          AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
          AND b."orderDate" <= (SELECT max_date FROM latest_date)
        GROUP BY b."productName"
        ORDER BY total_sales DESC
        LIMIT 100;
    """

    # Fetch data
    brand_df    = safe_read_sql(brand_query,    params=(store_name, store_name, store_name))
    category_df = safe_read_sql(category_query, params=(store_name, store_name, store_name))
    product_df  = safe_read_sql(product_query,  params=(store_name, store_name, store_name))

    # ── Load stock CSV once, build three O(1) lookups ────────────────────────
    brand_stock_lookup, category_stock_lookup, product_stock_lookup = load_stock_lookups(store_name)

    # Inject current_stock column right after quantity_sold (numeric, pre-% labels)
    brand_df    = inject_stock_column(brand_df,    "brandName",    brand_stock_lookup)
    category_df = inject_stock_column(category_df, "categoryName", category_stock_lookup)
    product_df  = inject_stock_column(product_df,  "productName",  product_stock_lookup)

    # === MODIFIED: Query for Total Sales, Total Cost, Total Profit, and AVERAGE Profit Margin%
    total_sales_profit_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        ),
        item_margins AS (
            SELECT
                "totalProductPrice",
                COALESCE("costPrice", 0) * "quantity" AS item_cost,
                CASE
                    WHEN "totalProductPrice" > 0 THEN
                        (("totalProductPrice" - COALESCE("costPrice", 0) * "quantity") / "totalProductPrice" * 100)
                    ELSE 0
                END AS item_profit_margin
            FROM "billing_data"
            WHERE "storeName" = %s
              AND "orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
              AND "orderDate" <= (SELECT max_date FROM latest_date)
        )
        SELECT
            ROUND(SUM("totalProductPrice")::numeric, 2) AS total_weekly_sales,
            ROUND(SUM(item_cost)::numeric, 2) AS total_weekly_cost,
            ROUND((SUM("totalProductPrice") - SUM(item_cost))::numeric, 2) AS total_weekly_profit,
            ROUND(AVG(item_profit_margin)::numeric, 2) AS avg_profit_margin_percent
        FROM item_margins;
    """

    total_sales_profit_df = safe_read_sql(total_sales_profit_query, params=(store_name, store_name))

    if not total_sales_profit_df.empty and total_sales_profit_df["total_weekly_sales"].iloc[0] is not None:
        total_weekly_sales        = float(total_sales_profit_df["total_weekly_sales"].iloc[0])
        total_weekly_cost         = float(total_sales_profit_df["total_weekly_cost"].iloc[0]) if total_sales_profit_df["total_weekly_cost"].iloc[0] is not None else 0.0
        total_weekly_profit       = float(total_sales_profit_df["total_weekly_profit"].iloc[0]) if total_sales_profit_df["total_weekly_profit"].iloc[0] is not None else 0.0
        avg_profit_margin_percent = float(total_sales_profit_df["avg_profit_margin_percent"].iloc[0]) if total_sales_profit_df["avg_profit_margin_percent"].iloc[0] is not None else 0.0
    else:
        total_weekly_sales        = 0.0
        total_weekly_cost         = 0.0
        total_weekly_profit       = 0.0
        avg_profit_margin_percent = 0.0

    # ── LLM calls (numeric dfs, before % suffix is added) ────────────────────
    print(f"  🤖 Generating LLM recommendations for {store_name}...")
    brand_rec    = brand_recommendation(store_name, brand_df,    total_weekly_sales, week_start=week_start, engine=engine, report_type="weekly")
    category_rec = category_recommendation(store_name, category_df, total_weekly_sales, week_start=week_start, engine=engine, report_type="weekly")
    product_rec  = product_recommendation(store_name, product_df,  total_weekly_sales, week_start=week_start, engine=engine, report_type="weekly")

    # ── Stock insight calls (reads per-store CSV from STOCK_DIR) ─────────────
    # Returns "" silently if the CSV doesn't exist for this store, so safe to
    # call even when stock.py hasn't been run yet.
    print(f"  📦 Generating stock insights for {store_name}...")
    brand_stock_rec    = brand_stock_insight(store_name,    STOCK_DIR, LOW_STOCK_THRESHOLD)
    category_stock_rec = category_stock_insight(store_name, STOCK_DIR, LOW_STOCK_THRESHOLD)
    product_stock_rec  = product_stock_insight(store_name,  STOCK_DIR, LOW_STOCK_THRESHOLD)

    # ── RTV insight call (reads per-store CSV from RTV_DIR) ───────────────────
    # Returns "" silently if rtv.py hasn't been run or no returns today.
    print(f"  🔄 Generating RTV insights for {store_name}...")
    rtv_rec = rtv_insight(store_name, RTV_DIR)

    save_weekly_snapshot(store_name, week_start, brand_df, category_df, product_df, engine)

    # Add percentage symbols (AFTER LLM calls, skip current_stock)
    for df in [brand_df, category_df, product_df]:
        if not df.empty:
            if 'contrib_percent' in df.columns:
                df['contrib_percent'] = df['contrib_percent'].astype(str) + '%'
            if 'profit_margin' in df.columns:
                df['profit_margin'] = df['profit_margin'].astype(str) + '%'

    # --- Charts ---
    brand_chart    = plot_chart(brand_df,    "brandName",    "total_sales", "Top 10 Brands by Sales")
    category_chart = plot_chart(category_df, "categoryName", "total_sales", "Top 10 Categories by Sales")
    product_chart  = plot_chart(product_df,  "productName",  "total_sales", "Top 10 Products by Sales")

    # --- Render tables (short headers + colour-coded stock column) ---
    brand_table_html    = df_to_html_with_stock(brand_df)
    category_table_html = df_to_html_with_stock(category_df)
    product_table_html  = df_to_html_with_stock(product_df)

    # --- HTML Template ---
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

    # ── Upload PDF to Azure Blob Storage ─────────────────────────────────────
    blob_name = f"{store_name.replace(' ', '_')}_weekly_report.pdf"

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        pdfkit.from_string(html_template, tmp_path, configuration=PDFKIT_CONFIG, options={"enable-local-file-access": ""})

        service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = service_client.get_container_client(AZURE_CONTAINER)
        try:
            container_client.create_container()
        except Exception:
            pass  # container already exists

        blob_client = container_client.get_blob_client(blob_name)
        with open(tmp_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True, content_settings=ContentSettings(content_type="application/pdf"))

        # Generate SAS link valid for SAS_EXPIRY_DAYS days
        sas_token = generate_blob_sas(
            account_name=AZURE_ACCOUNT_NAME,
            container_name=AZURE_CONTAINER,
            blob_name=blob_name,
            account_key=AZURE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(days=SAS_EXPIRY_DAYS),
        )
        shareable_url = (
            f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
            f"/{AZURE_CONTAINER}/{blob_name}?{sas_token}"
        )
        print(f"✅ Uploaded {store_name} → {shareable_url}")

        # ── Write shareable link to pdf_link column in partner.csv ───────────
        if os.path.exists(PARTNER_FILE):
            partner_df = pd.read_csv(PARTNER_FILE)
            if "pdf_link" not in partner_df.columns:
                partner_df["pdf_link"] = ""
            partner_df.loc[partner_df["storeName"] == store_name, "pdf_link"] = shareable_url
            partner_df.to_csv(PARTNER_FILE, index=False)
            print(f"   💾 Link saved to partner.csv")
        else:
            print(f"   ⚠️  partner.csv not found at {PARTNER_FILE} — link not saved.")

    finally:
        os.remove(tmp_path)


if __name__ == "__main__":
    store_names = get_unique_stores()
    print(f"Found {len(store_names)} stores.\nGenerating reports...\n")

    for store in store_names:
        try:
            generate_store_report(store)
            time.sleep(1)
        except Exception as e:
            print(f"❌ Error generating report for {store}: {e}")

    print("\n✅ All store reports uploaded to Azure Blob and links saved to partner.csv")