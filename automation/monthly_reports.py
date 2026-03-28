"""
monthly_llm.py  —  Monthly Store Report Generator
────────────────────────────────────────────────────────────────────────────────
Reads precomputed data from Azure Blob cache (report_cache.py) instead of
running heavy SQL queries per store.

Fixes applied:
  [1]  load_dotenv() moved to top — before any os.getenv() call
  [2]  month_start assigned None default — NameError on empty store eliminated
  [3]  NullPool used instead of pool_recycle (correct for batch scripts)
  [4]  PDF uploaded to Azure Blob + SAS URL written to partner_month.csv (monthly_pdf_link)
  [5]  Dummy Store and Ho Marlboro excluded from store list
  [6]  PROFIT_MARGIN SQL alias normalised to profit_margin (lowercase)
       Dead 'profit_margin' check removed — single consistent check
  [7]  tempfile + finally guard on PDF write — no corrupt file left on crash
  [8]  partner_month.csv written once at end, not per store
  [9]  Cache-first data loading — replaces per-store SQL queries
────────────────────────────────────────────────────────────────────────────────
"""

# ── load_dotenv FIRST — before any os.getenv() call  [FIX 1] ─────────────────
from dotenv import load_dotenv
load_dotenv()

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
from sqlalchemy.pool import NullPool                             # [FIX 3]
from azure.storage.blob import BlobServiceClient, ContentSettings, generate_blob_sas, BlobSasPermissions
from azure.core.exceptions import ResourceExistsError
import report_cache                                              # [FIX 9]

from llm_recommender import (
    brand_recommendation,
    category_recommendation,
    product_recommendation,
    save_monthly_snapshot,
)


BASE_DIR = "/base/dir/"

# ── Azure Blob Storage  [FIX 4] ───────────────────────────────────────────────
AZURE_ACCOUNT_NAME      = os.getenv("AZURE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY       = os.getenv("AZURE_ACCOUNT_KEY", "")
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING", "")
AZURE_CONTAINER         = os.getenv("AZURE_MONTHLY_CONTAINER", "monthly-reports")
SAS_EXPIRY_DAYS         = 7    # monthly links last longer than weekly (3 days)

PARTNER_FILE = os.path.join(BASE_DIR, "partner_month.csv")

# ── Excluded stores  [FIX 5] ─────────────────────────────────────────────────
EXCLUDED_STORES = {"Ho Marlboro", "Dummy Store --- For Testing Only"}


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

# [FIX 3] NullPool — correct for a long-running batch that runs once per store
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


# =============================================================================
# DB helpers (fallback only — used when cache is unavailable)
# =============================================================================

def safe_read_sql(query, params=None, retries=3, delay=3):
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


# =============================================================================
# Chart helper (unchanged)
# =============================================================================

def plot_chart(df, x_col, y_col, title, top_n=10):
    """
    Generate a Plotly bar chart and return a base64 image string.
    NOTE: y_col must be numeric. Do NOT call with %-suffixed columns.
    """
    if df.empty:
        return ""
    df_plot = df.head(top_n)
    fig = go.Figure(data=[go.Bar(
        x=df_plot[x_col],
        y=df_plot[y_col],
        text=[f"₹{v:,.0f}" for v in df_plot[y_col]],
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
# Azure upload helper  [FIX 4]
# =============================================================================

def _upload_pdf_to_blob(tmp_path: str, blob_name: str) -> str:
    """Upload PDF to the monthly-reports container and return a SAS URL."""
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
# MAIN REPORT FUNCTION
# =============================================================================

def generate_store_report(store_name: str, cache: dict) -> str | None:
    """
    Generate monthly PDF for one store. Returns shareable SAS URL or None.

    Parameters
    ----------
    store_name : Store to generate report for.
    cache      : Preloaded dict from report_cache.load_monthly_cache().
    """

    # ── [FIX 2] Default month_start before any conditional ───────────────────
    month_start     = None
    month_start_str = "N/A"
    month_end_str   = "N/A"

    # ── store_summary from cache ──────────────────────────────────────────────
    summary_all = cache["store_summary"]
    summary_row = summary_all[summary_all["storeName"] == store_name]

    if summary_row.empty:
        print(f"  ⚠ No data for {store_name} in cache — skipping.")
        return None

    total_monthly_sales       = float(summary_row["total_monthly_sales"].iloc[0]       or 0.0)
    total_monthly_cost        = float(summary_row["total_monthly_cost"].iloc[0]        or 0.0)
    total_monthly_profit      = float(summary_row["total_monthly_profit"].iloc[0]      or 0.0)
    avg_profit_margin_percent = float(summary_row["avg_profit_margin_percent"].iloc[0] or 0.0)

    # ── Resolve month dates from cache meta ───────────────────────────────────
    if "meta" in cache and "month_start" in cache["meta"]:
        month_start     = cache["meta"]["month_start"]
        month_end       = cache["meta"]["month_end"]
        month_start_str = month_start.strftime('%d %b %Y')
        month_end_str   = month_end.strftime('%d %b %Y')

    # [FIX 2] Guard — if month_start is still None, skip safely
    if month_start is None:
        print(f"  ⚠ Could not resolve month dates for {store_name} — skipping.")
        return None

    # ── comparison ────────────────────────────────────────────────────────────
    comp_all = cache["comparison"]
    comp_row = comp_all[comp_all["storeName"] == store_name]

    comparison_text = '<div style="text-align:center;margin-top:10px;"><span style="font-size:18px;color:#666;">Insufficient data for comparison</span></div>'
    if not comp_row.empty:
        curr = float(comp_row["current_month_sales"].iloc[0] or 0.0)
        prev = float(comp_row["prev_3_months_avg"].iloc[0]   or 0.0)
        if prev > 0:
            pct   = ((curr - prev) / prev) * 100
            sign  = "+" if pct >= 0 else ""
            color = "#28a745" if pct >= 0 else "#dc3545"
            comparison_text = f"""
                <div style="text-align:center;margin-top:10px;">
                    <span style="font-size:18px;color:#666;">Previous 3 Months Average: ₹{prev:,.2f}</span><br>
                    <span style="font-size:20px;font-weight:bold;color:{color};">{sign}{pct:.2f}%</span>
                </div>"""

    # ── brand / category / product — filtered from full-cache DataFrames ──────
    brand_df    = cache["brand_sales"][cache["brand_sales"]["storeName"]       == store_name].drop(columns=["storeName"]).head(50).reset_index(drop=True)
    category_df = cache["category_sales"][cache["category_sales"]["storeName"] == store_name].drop(columns=["storeName"]).head(50).reset_index(drop=True)
    product_df  = cache["product_sales"][cache["product_sales"]["storeName"]   == store_name].drop(columns=["storeName"]).head(100).reset_index(drop=True)

    # ── LLM calls (before % suffix — numeric columns required) ───────────────
    print(f"  🤖 Generating LLM recommendations for {store_name}...")
    brand_rec    = brand_recommendation(store_name, brand_df,    total_monthly_sales, month_start=month_start, engine=engine, report_type="monthly")
    category_rec = category_recommendation(store_name, category_df, total_monthly_sales, month_start=month_start, engine=engine, report_type="monthly")
    product_rec  = product_recommendation(store_name, product_df,  total_monthly_sales, month_start=month_start, engine=engine, report_type="monthly")

    # Snapshot with numeric data — before % suffix  [ordering preserved]
    save_monthly_snapshot(store_name, month_start, brand_df, category_df, product_df, engine)

    # ── % suffix (AFTER LLM + snapshot)  [FIX 6] ─────────────────────────────
    # Column is now consistently 'profit_margin' (lowercase) — dead PROFIT_MARGIN
    # check removed. Single loop, single check per column.
    for df in [brand_df, category_df, product_df]:
        if not df.empty:
            for col in ["contrib_percent", "profit_margin"]:   # [FIX 6]
                if col in df.columns:
                    df[col] = df[col].astype(str) + "%"

    # ── Charts (total_sales still numeric at this point) ──────────────────────
    brand_chart    = plot_chart(brand_df,    "brandName",    "total_sales", "Top 10 Brands by Sales")
    category_chart = plot_chart(category_df, "categoryName", "total_sales", "Top 10 Categories by Sales")
    product_chart  = plot_chart(product_df,  "productName",  "total_sales", "Top 10 Products by Sales")

    # ── HTML ──────────────────────────────────────────────────────────────────
    html_template = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{store_name} - Monthly Store Report</title>
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
        <img src="file:///base/dir//tns.png" class="logo" alt="Company Logo">
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

    # ── [FIX 7] tempfile + finally — no corrupt PDF left on crash ────────────
    blob_name = f"{store_name.replace(' ', '_')}_monthly_report.pdf"

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        pdfkit.from_string(
            html_template, tmp_path,
            configuration=PDFKIT_CONFIG,
            options={"enable-local-file-access": ""},
        )
        shareable_url = _upload_pdf_to_blob(tmp_path, blob_name)   # [FIX 4]
        print(f"  ✅ Uploaded {store_name} → {shareable_url}")
        return shareable_url
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# =============================================================================
# ENTRY POINT
# [FIX 8] partner_month.csv written ONCE at the end, not per store in the loop
# =============================================================================

if __name__ == "__main__":
    # ── Resolve dates ─────────────────────────────────────────────────────────
    month_start, month_end = report_cache.resolve_monthly_dates()
    month_start_str_key    = month_start.isoformat()

    # ── Load cache (5 Parquet reads — replaces hundreds of SQL queries) ───────
    print(f"\nLoading monthly cache for {month_start_str_key}...")
    try:
        cache = report_cache.load_monthly_cache(month_start_str_key)
        cache["meta"] = {"month_start": month_start, "month_end": month_end}
        print("✓ Cache loaded from Azure Blob")
    except Exception as e:
        print(f"⚠ Cache unavailable ({e}) — run: python report_cache.py --mode monthly")
        raise SystemExit(1)

    store_names = [
        s for s in cache["store_summary"]["storeName"].tolist()
        if s not in EXCLUDED_STORES                              # [FIX 5]
    ]
    print(f"Found {len(store_names)} stores.\nGenerating reports...\n")

    # ── Generate all reports — collect URLs ───────────────────────────────────
    pdf_links: dict[str, str] = {}

    # store_names = [
    #     s for s in cache["store_summary"]["storeName"].tolist()
    #     if s not in EXCLUDED_STORES
    # ]

    # print(f"Found {len(store_names)} stores.\nGenerating reports...\n")

    # # --- TEST MODE ---
    # TEST_STORE = "Adchini Aurobindo Marg"   # 👈 put real store name

    # if TEST_STORE:
    #     print(f"\n⚠ TEST MODE: Running only for → {TEST_STORE}\n")
    #     store_names = [TEST_STORE] if TEST_STORE in store_names else []

    for store in store_names:
        try:
            url = generate_store_report(store, cache)
            if url:
                pdf_links[store] = url
            time.sleep(1)
        except Exception as e:
            print(f"  ❌ Error generating report for {store}: {e}")

    # ── [FIX 8] Write partner_month.csv ONCE after all stores are done ─────────────
    if pdf_links and os.path.exists(PARTNER_FILE):
        partner_df = pd.read_csv(PARTNER_FILE)
        if "monthly_pdf_link" not in partner_df.columns:
            partner_df["monthly_pdf_link"] = ""
        for store_name, url in pdf_links.items():
            partner_df.loc[partner_df["storeName"] == store_name, "monthly_pdf_link"] = url
        partner_df.to_csv(PARTNER_FILE, index=False)
        print(f"\n✓ partner_month.csv updated with {len(pdf_links)} monthly links")
    elif not os.path.exists(PARTNER_FILE):
        print(f"⚠ partner_month.csv not found at {PARTNER_FILE} — links not saved.")

    print(f"\n✅ All monthly store reports uploaded to Azure Blob")