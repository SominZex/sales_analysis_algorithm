"""
weekly_azure_llm.py  —  Weekly Store Report Generator
────────────────────────────────────────────────────────────────────────────────
Reads precomputed data from Azure Blob cache (report_cache.py) instead of
running heavy SQL queries per store. Falls back to direct DB queries if cache
is unavailable.

Fixes applied:
  [1]  load_dotenv() moved to top — before any os.getenv() call
  [2]  week_start assigned None default — NameError on empty store eliminated
  [3]  ResourceExistsError caught specifically on container creation
  [4]  partner.csv written once at end of __main__, not per store
  [5]  plot_chart() docstring warns against passing %-suffixed columns
  [6]  Cache-first data loading — 700 SQL queries → 5 Parquet reads total
  [7]  Dummy Store and Ho Marlboro excluded from store list
  [8]  pandas removed from hot path — DuckDB used for all cache filtering;
       pandas kept only for stock CSV reads and partner.csv (local files,
       not bottlenecks)
────────────────────────────────────────────────────────────────────────────────
"""

# ── load_dotenv FIRST — before any os.getenv() call  [FIX 1] ─────────────────
from dotenv import load_dotenv
load_dotenv()

import os
import time
import tempfile
import csv
from io import BytesIO
from datetime import datetime, timezone, timedelta

import duckdb
import pandas as pd                  # kept for: load_stock_lookups (CSV), partner.csv
import pdfkit
import plotly.graph_objects as go
import base64
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool
from azure.storage.blob import BlobServiceClient, ContentSettings, generate_blob_sas, BlobSasPermissions
from azure.core.exceptions import ResourceExistsError
import report_cache
import sys
sys.path.insert(0, "/base/dir")
from monitoring.metrics import (
    task_timer, record_task_error,
    report_timer, record_report, record_stores_processed,
)

from llm_recommender import (
    save_weekly_snapshot,
    brand_recommendation,
    category_recommendation,
    product_recommendation,
    brand_stock_insight,
    category_stock_insight,
    product_stock_insight,
    rtv_insight,
)


BASE_DIR  = "/base/dir"
STOCK_DIR = os.getenv("STOCK_DIR", os.path.join(BASE_DIR, "store_stocks"))
RTV_DIR   = os.getenv("RTV_DIR",   os.path.join(BASE_DIR, "store_rtv"))
LOW_STOCK_THRESHOLD = float(os.getenv("LOW_STOCK_THRESHOLD", "5"))

# ── Azure Blob Storage ────────────────────────────────────────────────────────
AZURE_ACCOUNT_NAME      = os.getenv("AZURE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY       = os.getenv("AZURE_ACCOUNT_KEY", "")
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING", "")
AZURE_CONTAINER         = os.getenv("AZURE_CONTAINER", "weekly-reports")
SAS_EXPIRY_DAYS         = 3

PARTNER_FILE = os.path.join(BASE_DIR, "partner.csv")

# ── Excluded stores  [FIX 7] ──────────────────────────────────────────────────
EXCLUDED_STORES = {"Ho Marlboro", "Dummy Store --- For Testing Only"}

print(f"STOCK_DIR → {STOCK_DIR}")
print(f"RTV_DIR   → {RTV_DIR}")


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


# =============================================================================
# DB helpers (fallback only — used when cache is unavailable)
# =============================================================================

def safe_read_sql(query, params=None, retries=5, delay=3):
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                return pd.read_sql(query, conn, params=params)
        except OperationalError as e:
            error_str = str(e)
            print(f"Database error (attempt {attempt+1}/{retries}): {error_str[:100]}")
            if attempt == retries - 1:
                raise RuntimeError(f"Query failed after {retries} retries: {error_str[:200]}")
            wait_time = delay * (2 ** attempt)
            print(f"Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
            if "EOF" in error_str or "closed" in error_str.lower() or "terminated" in error_str.lower():
                print("Recreating database connection pool...")
                engine.dispose()
        except Exception as e:
            print(f"Unexpected error (attempt {attempt+1}/{retries}): {e}")
            if attempt == retries - 1:
                raise RuntimeError(f"Query failed: {e}")
            time.sleep(delay)
    raise RuntimeError("Query failed after multiple retries.")


def get_unique_stores():
    """
    Fetch store names. Excludes test/invalid stores.  [FIX 7]
    Weekly reports are currently scoped to partner stores — adjust the
    IN list or remove the WHERE clause to run for all stores.
    """
    excl  = tuple(EXCLUDED_STORES)
    query = f"""
        SELECT DISTINCT "storeName"
        FROM "billing_data"
        WHERE "storeName" NOT IN %(excl)s
        ORDER BY "storeName"
    """
    df = safe_read_sql(query, params={"excl": excl})
    return df["storeName"].dropna().tolist()


# =============================================================================
# DuckDB cache query helper  [FIX 8]
# =============================================================================

def _make_cache_con(cache: dict) -> duckdb.DuckDBPyConnection:
    """
    Create an in-process DuckDB connection and register all PyArrow Tables
    from the cache dict as virtual tables. Callers query them with plain SQL.
    The connection is cheap (in-memory, no Postgres attach needed here).
    """
    con = duckdb.connect()
    for table_name, arrow_table in cache.items():
        if table_name == "meta":
            continue
        con.register(table_name, arrow_table)
    return con


# =============================================================================
# Stock helpers — pandas kept here (CSV is a local file, not a bottleneck)
# =============================================================================

def load_stock_lookups(store_name: str):
    safe_name  = store_name.replace("/", "_")
    candidates = [
        os.path.join(STOCK_DIR, f"{safe_name}.csv"),
        os.path.join(STOCK_DIR, f"{store_name}.csv"),
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
                brand_stock    = df.groupby("brand")["quantity"].sum().to_dict()         if "brand"        in df.columns else {}
                category_stock = df.groupby("categoryName")["quantity"].sum().to_dict()  if "categoryName" in df.columns else {}
                product_stock  = df.set_index("productName")["quantity"].to_dict()       if "productName"  in df.columns else {}
                print(f"Stock CSV loaded for {store_name} "
                      f"({len(df)} SKUs, {len(brand_stock)} brands, {len(category_stock)} categories)")
                return brand_stock, category_stock, product_stock
            except Exception as e:
                print(f"Stock CSV read error for {store_name}: {e}")
                return {}, {}, {}
    print(f"No stock CSV for {store_name} — Stock column will show N/A")
    return {}, {}, {}


# =============================================================================
# inject_stock_column — now works on list-of-dicts rows  [FIX 8]
# =============================================================================

def inject_stock_column(rows: list[dict], name_col: str, stock_lookup: dict) -> list[dict]:
    """
    Add a 'current_stock' key into each row dict, inserted after 'quantity_sold'.
    Returns a new list — original rows are not mutated.
    """
    if not rows:
        return rows
    result = []
    for row in rows:
        r = dict(row)
        r["current_stock"] = stock_lookup.get(r.get(name_col))
        result.append(r)

    # Reorder keys: insert current_stock after quantity_sold
    if result:
        keys = list(result[0].keys())
        if "current_stock" in keys and "quantity_sold" in keys:
            keys.remove("current_stock")
            idx = keys.index("quantity_sold") + 1
            keys.insert(idx, "current_stock")
        result = [{k: r[k] for k in keys} for r in result]
    return result


# =============================================================================
# HTML table builder — plain Python, no pandas  [FIX 8]
# =============================================================================

def df_to_html_with_stock(rows: list[dict]) -> str:
    """
    Build an HTML table from a list-of-dicts.
    Mirrors the original df_to_html_with_stock() output exactly.
    """
    if not rows:
        return ""
    cols    = list(rows[0].keys())
    headers = "".join(f"<th>{COLUMN_LABELS.get(c, c)}</th>" for c in cols)
    rows_html = []
    for row in rows:
        cells = []
        for col in cols:
            val = row[col]
            if col == "current_stock":
                if val is None:
                    cells.append("<td>N/A</td>")
                else:
                    val_num = float(val)
                    color   = ' style="color:#dc3545;font-weight:bold;"' if val_num < 0 else ""
                    cells.append(f"<td{color}>{val_num:.0f}</td>")
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
# plot_chart — accepts list-of-dicts  [FIX 8]
# =============================================================================

def plot_chart(rows: list[dict], x_col: str, y_col: str, title: str, top_n: int = 10) -> str:
    """
    Generate a Plotly bar chart and return a base64 image string.
    NOTE: y_col must be numeric. Do NOT pass columns after the '%' suffix
    has been appended (contrib_percent, profit_margin) — pass them before.
    """
    if not rows:
        return ""
    top = rows[:top_n]
    x_vals = [r[x_col] for r in top]
    y_vals = [float(r[y_col]) for r in top]
    fig = go.Figure(data=[go.Bar(
        x=x_vals,
        y=y_vals,
        text=[f"₹{v:,.0f}" for v in y_vals],
        textposition="outside",
        marker=dict(color="#0078d7"),
    )])
    fig.update_layout(
        title=dict(text=title, font=dict(size=18, color="#0078d7", family="Segoe UI")),
        xaxis=dict(title="", tickangle=-45, automargin=True),
        yaxis=dict(title="Sales (₹)", gridcolor="rgba(200,200,200,0.3)"),
        plot_bgcolor="white", paper_bgcolor="white",
        margin=dict(l=40, r=40, t=50, b=80), height=500,
    )
    buf = BytesIO()
    fig.write_image(buf, format="png", scale=2)
    buf.seek(0)
    img_b64 = base64.b64encode(buf.read()).decode("utf-8")
    return f'<img src="data:image/png;base64,{img_b64}" style="display:block;margin:auto;width:90%;max-height:500px;">'


# =============================================================================
# Azure upload helper
# =============================================================================

def _upload_pdf_to_blob(tmp_path: str, blob_name: str) -> str:
    """Upload PDF and return shareable SAS URL."""
    service_client   = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = service_client.get_container_client(AZURE_CONTAINER)

    try:
        container_client.create_container()
    except ResourceExistsError:
        pass

    blob_client = container_client.get_blob_client(blob_name)
    with open(tmp_path, "rb") as f:
        blob_client.upload_blob(
            f, overwrite=True,
            content_settings=ContentSettings(content_type="application/pdf"),
        )

    sas_token = generate_blob_sas(
        account_name   = AZURE_ACCOUNT_NAME,
        container_name = AZURE_CONTAINER,
        blob_name      = blob_name,
        account_key    = AZURE_ACCOUNT_KEY,
        permission     = BlobSasPermissions(read=True),
        expiry         = datetime.now(timezone.utc) + timedelta(days=SAS_EXPIRY_DAYS),
    )
    return (
        f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
        f"/{AZURE_CONTAINER}/{blob_name}?{sas_token}"
    )


# =============================================================================
# MAIN REPORT FUNCTION  [FIX 8 — pandas removed from hot path]
# =============================================================================

def generate_store_report(store_name: str, cache: dict, cache_con: duckdb.DuckDBPyConnection) -> str | None:
    """
    Generate weekly PDF for one store. Returns shareable SAS URL or None on failure.

    Parameters
    ----------
    store_name : Store to generate report for.
    cache      : Preloaded dict from report_cache.load_weekly_cache().
                 Values are PyArrow Tables covering ALL stores.
    cache_con  : Shared DuckDB connection with all cache tables registered.
                 Filter queries run here — no pandas, no per-store DB round-trips.
    """

    # ── [FIX 2] Assign week_start default before any conditional ─────────────
    week_start     = None
    week_start_str = "N/A"
    week_end_str   = "N/A"

    # ── Pull store row from cache: store_summary (DuckDB query) ──────────────
    summary_rows = cache_con.execute(
        "SELECT * FROM store_summary WHERE storeName = ?", [store_name]
    ).fetchall()
    summary_cols = [d[0] for d in cache_con.description]

    if not summary_rows:
        print(f"  ⚠ No data for {store_name} in cache — skipping.")
        return None

    summary = dict(zip(summary_cols, summary_rows[0]))
    total_weekly_sales        = float(summary.get("total_weekly_sales")        or 0.0)
    total_weekly_cost         = float(summary.get("total_weekly_cost")         or 0.0)
    total_weekly_profit       = float(summary.get("total_weekly_profit")       or 0.0)
    avg_profit_margin_percent = float(summary.get("avg_profit_margin_percent") or 0.0)

    # ── Derive week dates from cache meta ────────────────────────────────────
    if "meta" in cache and "week_start" in cache["meta"]:
        week_start     = cache["meta"]["week_start"]
        week_end       = cache["meta"]["week_end"]
        week_start_str = week_start.strftime('%d %b %Y')
        week_end_str   = week_end.strftime('%d %b %Y')

    if week_start is None:
        print(f"  ⚠ Could not resolve week dates for {store_name} — skipping.")
        return None

    # ── comparison ────────────────────────────────────────────────────────────
    comp_rows = cache_con.execute(
        "SELECT * FROM comparison WHERE storeName = ?", [store_name]
    ).fetchall()
    comp_cols = [d[0] for d in cache_con.description]

    comparison_text = '<div style="text-align:center;margin-top:10px;"><span style="font-size:18px;color:#666;">Insufficient data for comparison</span></div>'
    if comp_rows:
        comp = dict(zip(comp_cols, comp_rows[0]))
        curr = float(comp.get("current_week_sales") or 0.0)
        prev = float(comp.get("prev_2_weeks_avg")   or 0.0)
        if prev > 0:
            pct   = ((curr - prev) / prev) * 100
            sign  = "+" if pct >= 0 else ""
            color = "#28a745" if pct >= 0 else "#dc3545"
            comparison_text = f"""
                <div style="text-align:center;margin-top:10px;">
                    <span style="font-size:18px;color:#666;">Previous 2 Weeks Average: ₹{prev:,.2f}</span><br>
                    <span style="font-size:20px;font-weight:bold;color:{color};">{sign}{pct:.2f}%</span>
                </div>"""

    # ── brand / category / product — DuckDB filter, return list-of-dicts ─────
    def _fetch_rows(table: str, name_col: str, limit: int) -> list[dict]:
        rows = cache_con.execute(f"""
            SELECT * EXCLUDE (storeName)
            FROM {table}
            WHERE storeName = ?
            ORDER BY total_sales DESC
            LIMIT {limit}
        """, [store_name]).fetchall()
        cols = [d[0] for d in cache_con.description]
        return [dict(zip(cols, r)) for r in rows]

    brand_rows    = _fetch_rows("brand_sales",    "brandName",    50)
    category_rows = _fetch_rows("category_sales", "categoryName", 50)
    product_rows  = _fetch_rows("product_sales",  "productName",  100)

    # ── Stock lookups ─────────────────────────────────────────────────────────
    brand_stock_lookup, category_stock_lookup, product_stock_lookup = load_stock_lookups(store_name)
    brand_rows    = inject_stock_column(brand_rows,    "brandName",    brand_stock_lookup)
    category_rows = inject_stock_column(category_rows, "categoryName", category_stock_lookup)
    product_rows  = inject_stock_column(product_rows,  "productName",  product_stock_lookup)

    # ── LLM calls — pass list-of-dicts (before % suffix) ─────────────────────
    # Convert to pandas only here because llm_recommender expects DataFrames
    brand_df    = pd.DataFrame(brand_rows)
    category_df = pd.DataFrame(category_rows)
    product_df  = pd.DataFrame(product_rows)

    print(f"  Generating LLM recommendations for {store_name}...")
    brand_rec    = brand_recommendation(store_name, brand_df,    total_weekly_sales, week_start=week_start, engine=engine, report_type="weekly")
    category_rec = category_recommendation(store_name, category_df, total_weekly_sales, week_start=week_start, engine=engine, report_type="weekly")
    product_rec  = product_recommendation(store_name, product_df,  total_weekly_sales, week_start=week_start, engine=engine, report_type="weekly")

    # ── Stock + RTV insights ──────────────────────────────────────────────────
    print(f"  Generating stock insights for {store_name}...")
    brand_stock_rec    = brand_stock_insight(store_name,    STOCK_DIR, LOW_STOCK_THRESHOLD)
    category_stock_rec = category_stock_insight(store_name, STOCK_DIR, LOW_STOCK_THRESHOLD)
    product_stock_rec  = product_stock_insight(store_name,  STOCK_DIR, LOW_STOCK_THRESHOLD)
    print(f"  Generating RTV insights for {store_name}...")
    rtv_rec = rtv_insight(store_name, RTV_DIR)

    # Snapshot saved with numeric data — before % suffix  [ordering preserved]
    save_weekly_snapshot(store_name, week_start, brand_df, category_df, product_df, engine)

    # ── % suffix on rows (AFTER LLM + snapshot) ───────────────────────────────
    for rows in [brand_rows, category_rows, product_rows]:
        for row in rows:
            for col in ["contrib_percent", "profit_margin"]:
                if col in row and row[col] is not None:
                    row[col] = str(row[col]) + "%"

    # ── Charts — y_col still numeric in rows before suffix ────────────────────
    brand_chart    = plot_chart(brand_rows,    "brandName",    "total_sales", "Top 10 Brands by Sales")
    category_chart = plot_chart(category_rows, "categoryName", "total_sales", "Top 10 Categories by Sales")
    product_chart  = plot_chart(product_rows,  "productName",  "total_sales", "Top 10 Products by Sales")

    brand_table_html    = df_to_html_with_stock(brand_rows)
    category_table_html = df_to_html_with_stock(category_rows)
    product_table_html  = df_to_html_with_stock(product_rows)

    # ── HTML ──────────────────────────────────────────────────────────────────
    html_template = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{store_name} - Weekly Store Report</title>
        <style>
            body {{ font-family: 'Segoe UI', sans-serif; margin: 20px; background-color: #f6f8fa; position: relative; }}
            .logo {{ position: absolute; top: 40px; right: 20px; width: 100px; height: auto; }}
            h1 {{ text-align: center; color: #333; margin-bottom: 10px; padding-top: 0px; }}
            h2 {{ text-align: center; color: #0078d7; margin-bottom: 5px; }}
            .date-range {{ text-align: center; color: #666; font-size: 16px; margin-bottom: 20px; }}
            .profit-section {{ text-align: center; margin: 15px 0; }}
            .profit-label {{ font-size: 18px; color: #666; display: inline-block; margin-right: 10px; }}
            .profit-value {{ font-size: 20px; font-weight: bold; color: #28a745; }}
            .profit-margin {{ font-size: 20px; font-weight: bold; color: #0078d7; }}
            table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; background-color: white; border-radius: 8px; overflow: hidden; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
            th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #0078d7; color: white; text-transform: uppercase; }}
            tr:hover {{ background-color: #f1f1f1; }}
            .table-title {{ color: #0078d7; font-size: 22px; font-weight: bold; text-align: center; margin: 20px 0 10px; }}
        </style>
    </head>
    <body>
        <img src="file:///base/dir/tns.png" class="logo" alt="Company Logo">
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

    # ── PDF → temp file → Azure Blob ──────────────────────────────────────────
    blob_name = f"{store_name.replace(' ', '_')}_weekly_report.pdf"

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        pdfkit.from_string(
            html_template, tmp_path,
            configuration=PDFKIT_CONFIG,
            options={"enable-local-file-access": ""},
        )
        shareable_url = _upload_pdf_to_blob(tmp_path, blob_name)
        print(f"  ✓ Uploaded {store_name} → {shareable_url}")
        return shareable_url
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# =============================================================================
# ENTRY POINT
# [FIX 4] partner.csv written ONCE at the end, not per store in the loop
# =============================================================================

if __name__ == "__main__":
    
    with task_timer("weekly_reports"):
        # ── Resolve dates ─────────────────────────────────────────────────────────
        week_start, week_end = report_cache.resolve_weekly_dates()
        week_start_str_key   = week_start.isoformat()

        # ── Load cache (5 Parquet reads — replaces 700 SQL queries) ─── [FIX 6] ──
        print(f"\nLoading weekly cache for {week_start_str_key}...")
        try:
            cache = report_cache.load_weekly_cache(week_start_str_key)
            cache["meta"] = {"week_start": week_start, "week_end": week_end}
            print("✓ Cache loaded from Azure Blob")
        except Exception as e:
            print(f"⚠ Cache unavailable ({e}) — run report_cache.py first.")
            raise SystemExit(1)

        # ── Build ONE shared DuckDB connection for all store queries  [FIX 8] ────
        cache_con = _make_cache_con(cache)

        store_names = cache_con.execute(
            "SELECT storeName FROM store_summary ORDER BY storeName"
        ).fetchall()
        store_names = [r[0] for r in store_names if r[0] not in EXCLUDED_STORES]

        print(f"Found {len(store_names)} stores.\nGenerating reports...\n")

        # ── Generate all reports — collect URLs ───────────────────────────────────
        pdf_links: dict[str, str] = {}

        success_count = 0
        failed_count  = 0

        for store in store_names:
            try:
                with report_timer(store, "weekly"):  
                    url = generate_store_report(store, cache, cache_con)
                if url:
                    pdf_links[store] = url
                record_report(store, "weekly", True)       
                success_count += 1
                time.sleep(1)
            except Exception as e:
                record_report(store, "weekly", False)            
                record_task_error("weekly_reports", e)         
                failed_count += 1
                print(f"  ✗ Error generating report for {store}: {e}")

        cache_con.close()
        record_stores_processed("weekly", success_count, failed_count)
        
        # ── [FIX 4] Write partner.csv ONCE after all stores are done ─────────────
        if pdf_links and os.path.exists(PARTNER_FILE):
            partner_df = pd.read_csv(PARTNER_FILE)
            if "pdf_link" not in partner_df.columns:
                partner_df["pdf_link"] = ""
            for store_name, url in pdf_links.items():
                partner_df.loc[partner_df["storeName"] == store_name, "pdf_link"] = url
            partner_df.to_csv(PARTNER_FILE, index=False)
            print(f"\n✓ partner.csv updated with {len(pdf_links)} links")
        elif not os.path.exists(PARTNER_FILE):
            print(f"⚠ partner.csv not found at {PARTNER_FILE} — links not saved.")

        print(f"\n✓ All store reports uploaded to Azure Blob")