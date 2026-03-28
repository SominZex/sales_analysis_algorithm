"""
report_cache.py
────────────────────────────────────────────────────────────────────────────────
Precompute layer — runs once after the daily ETL, caches all report data
to Azure Blob Storage as Parquet files.

Both azure_blob_report.py (weekly) and monthly_llm.py (monthly) load their
data from these cache files instead of running heavy SQL queries at report time.

WHY THIS IS FASTER
──────────────────
Current situation (without cache):
  - Weekly report: 5 SQL queries × 140 stores = 700 sequential DB round-trips
  - Each query scans billing_data (340k+ rows) with GROUP BY + window functions
  - Total DB time: ~15–25 minutes just in queries

With this cache:
  - This script runs ONE set of queries across ALL stores at once using
    GROUP BY storeName — the DB scans the table once, not 140 times
  - Results written to Parquet in blob storage (one file per report type)
  - Report scripts read Parquet (~50ms per store) instead of hitting the DB
  - Total DB time for report generation: ~0 seconds
  - Estimated speedup: 10–20x

BLOB LAYOUT (container: report-cache)
──────────────────────────────────────
  weekly/
    week_start=2026-03-20/
      brand_sales.parquet        ← all stores, all brands for this week
      category_sales.parquet
      product_sales.parquet
      store_summary.parquet      ← total_sales, profit, margin per store
      comparison.parquet         ← current vs prev 2 weeks per store

  monthly/
    month_start=2026-03-01/
      brand_sales.parquet
      category_sales.parquet
      product_sales.parquet
      store_summary.parquet
      comparison.parquet         ← current vs prev 3 months per store

USAGE
─────
# Run after ETL completes (add to cron after core_pipeline.py):
  python report_cache.py --mode weekly
  python report_cache.py --mode monthly
  python report_cache.py --mode both      ← default

# Report scripts load cache automatically — no changes needed to call sites.
────────────────────────────────────────────────────────────────────────────────
"""

import io
import os
import argparse
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import OperationalError
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError
from dotenv import load_dotenv

# ── load_dotenv FIRST — before any os.getenv() call ──────────────────────────
load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING", "")
CACHE_CONTAINER         = os.getenv("AZURE_CACHE_CONTAINER", "report-cache")

EXCLUDED_STORES = ["Ho Marlboro", "Dummy Store --- For Testing Only"]

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

# ── Blob helpers ──────────────────────────────────────────────────────────────

def _get_blob_client():
    if not AZURE_CONNECTION_STRING:
        raise RuntimeError("AZURE_CONNECTION_STRING not set in .env")
    return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)


def _ensure_container(sc, name: str):
    try:
        sc.create_container(name)
        print(f"  Created container: {name}")
    except ResourceExistsError:
        pass


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf   = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    return buf.read()


def _upload(sc, blob_name: str, df: pd.DataFrame):
    data = _df_to_parquet_bytes(df)
    sc.get_blob_client(container=CACHE_CONTAINER, blob=blob_name).upload_blob(
        data,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/octet-stream"),
    )
    print(f"  ✓ {blob_name}  ({len(df):,} rows, {len(data)/1024:.0f} KB)")


def _download(blob_name: str) -> pd.DataFrame:
    """Public helper — called by report scripts to load cached data."""
    sc   = _get_blob_client()
    data = sc.get_blob_client(
        container=CACHE_CONTAINER, blob=blob_name
    ).download_blob().readall()
    return pq.read_table(io.BytesIO(data)).to_pandas()


# ── SQL helpers ───────────────────────────────────────────────────────────────

def safe_read_sql(query: str, params=None, retries: int = 3, delay: int = 3) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                return pd.read_sql(query, conn, params=params)
        except OperationalError as e:
            print(f"  DB error (attempt {attempt+1}/{retries}): {str(e)[:80]}")
            if attempt == retries - 1:
                raise
            time.sleep(delay * (2 ** attempt))
            engine.dispose()
        except Exception as e:
            print(f"  Unexpected DB error (attempt {attempt+1}/{retries}): {e}")
            if attempt == retries - 1:
                raise
            time.sleep(delay)
    raise RuntimeError("Query failed after retries")


# =============================================================================
# WEEKLY CACHE
# Runs ONE query per table across ALL stores — DB scans billing_data once.
# =============================================================================

def build_weekly_cache(week_start: datetime.date, week_end: datetime.date) -> str:
    """
    Compute all weekly report data for all stores in a single pass per table.
    Writes Parquet files to blob. Returns the cache prefix string.
    """
    prefix = f"weekly/week_start={week_start.isoformat()}"
    ws_str = week_start.isoformat()
    we_str = week_end.isoformat()

    print(f"\n{'='*60}")
    print(f"WEEKLY CACHE BUILD  —  {ws_str} to {we_str}")
    print(f"{'='*60}")

    sc = _get_blob_client()
    _ensure_container(sc, CACHE_CONTAINER)

    excl = tuple(EXCLUDED_STORES)

    # ── store_summary: total sales, profit, margin per store ─────────────────
    print("\n[1/5] Computing store_summary...")
    q = f"""
        SELECT
            "storeName",
            ROUND(SUM("totalProductPrice")::numeric, 2)                         AS total_weekly_sales,
            ROUND(SUM(COALESCE("costPrice",0) * "quantity")::numeric, 2)        AS total_weekly_cost,
            ROUND((SUM("totalProductPrice")
                   - SUM(COALESCE("costPrice",0) * "quantity"))::numeric, 2)    AS total_weekly_profit,
            ROUND(AVG(
                CASE WHEN "totalProductPrice" > 0
                     THEN ("totalProductPrice"
                           - COALESCE("costPrice",0) * "quantity")
                          / "totalProductPrice" * 100
                     ELSE 0 END
            )::numeric, 2)                                                       AS avg_profit_margin_percent
        FROM "billing_data"
        WHERE "orderDate" BETWEEN %(ws)s AND %(we)s
          AND "storeName" NOT IN %(excl)s
        GROUP BY "storeName"
    """
    store_summary = safe_read_sql(q, params={"ws": ws_str, "we": we_str, "excl": excl})
    _upload(sc, f"{prefix}/store_summary.parquet", store_summary)

    # ── comparison: current week vs prev 2 weeks avg per store ───────────────
    print("\n[2/5] Computing comparison...")
    q = f"""
        WITH periods AS (
            SELECT
                "storeName",
                SUM(CASE WHEN "orderDate" BETWEEN %(ws)s AND %(we)s
                         THEN "totalProductPrice" ELSE 0 END) AS current_week_sales,
                SUM(CASE WHEN "orderDate" BETWEEN %(w2s)s AND %(w2e)s
                         THEN "totalProductPrice" ELSE 0 END) AS week_2_sales,
                SUM(CASE WHEN "orderDate" BETWEEN %(w3s)s AND %(w3e)s
                         THEN "totalProductPrice" ELSE 0 END) AS week_3_sales
            FROM "billing_data"
            WHERE "orderDate" BETWEEN %(w3s)s AND %(we)s
              AND "storeName" NOT IN %(excl)s
            GROUP BY "storeName"
        )
        SELECT
            "storeName",
            current_week_sales,
            ROUND(((week_2_sales + week_3_sales) / 2.0)::numeric, 2) AS prev_2_weeks_avg
        FROM periods
    """
    w2_start = (week_start - timedelta(days=7)).isoformat()
    w2_end   = (week_start - timedelta(days=1)).isoformat()
    w3_start = (week_start - timedelta(days=14)).isoformat()
    w3_end   = (week_start - timedelta(days=8)).isoformat()
    comparison = safe_read_sql(q, params={
        "ws": ws_str, "we": we_str,
        "w2s": w2_start, "w2e": w2_end,
        "w3s": w3_start, "w3e": w3_end,
        "excl": excl,
    })
    _upload(sc, f"{prefix}/comparison.parquet", comparison)

    # ── brand_sales ───────────────────────────────────────────────────────────
    print("\n[3/5] Computing brand_sales...")
    q = f"""
        WITH totals AS (
            SELECT "storeName", SUM("totalProductPrice") AS store_total
            FROM "billing_data"
            WHERE "orderDate" BETWEEN %(ws)s AND %(we)s
              AND "storeName" NOT IN %(excl)s
            GROUP BY "storeName"
        )
        SELECT
            b."storeName",
            b."brandName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2)                        AS total_sales,
            SUM(b."quantity")                                                     AS quantity_sold,
            ROUND((SUM(b."totalProductPrice")
                   / NULLIF(t.store_total,0) * 100)::numeric, 2)                AS contrib_percent,
            ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                       THEN ((SUM(b."totalProductPrice")
                             - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                            / SUM(b."totalProductPrice") * 100)::numeric
                       ELSE 0 END, 2)                                            AS profit_margin
        FROM "billing_data" b
        JOIN totals t ON t."storeName" = b."storeName"
        WHERE b."orderDate" BETWEEN %(ws)s AND %(we)s
          AND b."storeName" NOT IN %(excl)s
        GROUP BY b."storeName", b."brandName", t.store_total
        ORDER BY b."storeName", total_sales DESC
    """
    brand_sales = safe_read_sql(q, params={"ws": ws_str, "we": we_str, "excl": excl})
    _upload(sc, f"{prefix}/brand_sales.parquet", brand_sales)

    # ── category_sales ────────────────────────────────────────────────────────
    print("\n[4/5] Computing category_sales...")
    q = f"""
        WITH totals AS (
            SELECT "storeName", SUM("totalProductPrice") AS store_total
            FROM "billing_data"
            WHERE "orderDate" BETWEEN %(ws)s AND %(we)s
              AND "storeName" NOT IN %(excl)s
            GROUP BY "storeName"
        )
        SELECT
            b."storeName",
            b."categoryName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2)                        AS total_sales,
            SUM(b."quantity")                                                     AS quantity_sold,
            ROUND((SUM(b."totalProductPrice")
                   / NULLIF(t.store_total,0) * 100)::numeric, 2)                AS contrib_percent,
            ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                       THEN ((SUM(b."totalProductPrice")
                             - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                            / SUM(b."totalProductPrice") * 100)::numeric
                       ELSE 0 END, 2)                                            AS profit_margin
        FROM "billing_data" b
        JOIN totals t ON t."storeName" = b."storeName"
        WHERE b."orderDate" BETWEEN %(ws)s AND %(we)s
          AND b."storeName" NOT IN %(excl)s
        GROUP BY b."storeName", b."categoryName", t.store_total
        ORDER BY b."storeName", total_sales DESC
    """
    category_sales = safe_read_sql(q, params={"ws": ws_str, "we": we_str, "excl": excl})
    _upload(sc, f"{prefix}/category_sales.parquet", category_sales)

    # ── product_sales ─────────────────────────────────────────────────────────
    print("\n[5/5] Computing product_sales...")
    q = f"""
        WITH totals AS (
            SELECT "storeName", SUM("totalProductPrice") AS store_total
            FROM "billing_data"
            WHERE "orderDate" BETWEEN %(ws)s AND %(we)s
              AND "storeName" NOT IN %(excl)s
            GROUP BY "storeName"
        )
        SELECT
            b."storeName",
            b."productName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2)                        AS total_sales,
            SUM(b."quantity")                                                     AS quantity_sold,
            ROUND((SUM(b."totalProductPrice")
                   / NULLIF(t.store_total,0) * 100)::numeric, 2)                AS contrib_percent,
            ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                       THEN ((SUM(b."totalProductPrice")
                             - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                            / SUM(b."totalProductPrice") * 100)::numeric
                       ELSE 0 END, 2)                                            AS profit_margin
        FROM "billing_data" b
        JOIN totals t ON t."storeName" = b."storeName"
        WHERE b."orderDate" BETWEEN %(ws)s AND %(we)s
          AND b."storeName" NOT IN %(excl)s
        GROUP BY b."storeName", b."productName", t.store_total
        ORDER BY b."storeName", total_sales DESC
    """
    product_sales = safe_read_sql(q, params={"ws": ws_str, "we": we_str, "excl": excl})
    _upload(sc, f"{prefix}/product_sales.parquet", product_sales)

    print(f"\n  ✓ Weekly cache complete → {prefix}")
    return prefix


# =============================================================================
# MONTHLY CACHE
# Same single-pass approach for monthly data.
# =============================================================================

def build_monthly_cache(month_start: datetime.date, month_end: datetime.date) -> str:
    """
    Compute all monthly report data for all stores in a single pass per table.
    """
    prefix  = f"monthly/month_start={month_start.isoformat()}"
    ms_str  = month_start.isoformat()
    me_str  = month_end.isoformat()

    print(f"\n{'='*60}")
    print(f"MONTHLY CACHE BUILD  —  {ms_str} to {me_str}")
    print(f"{'='*60}")

    sc = _get_blob_client()
    _ensure_container(sc, CACHE_CONTAINER)

    excl = tuple(EXCLUDED_STORES)

    # Prev 3 month boundaries
    from dateutil.relativedelta import relativedelta
    m1s = (month_start - relativedelta(months=1)).isoformat()
    m1e = (month_start - timedelta(days=1)).isoformat()
    m2s = (month_start - relativedelta(months=2)).isoformat()
    m2e = (month_start - relativedelta(months=1) - timedelta(days=1)).isoformat()
    m3s = (month_start - relativedelta(months=3)).isoformat()
    m3e = (month_start - relativedelta(months=2) - timedelta(days=1)).isoformat()

    # ── store_summary ─────────────────────────────────────────────────────────
    print("\n[1/5] Computing store_summary...")
    q = f"""
        SELECT
            "storeName",
            ROUND(SUM("totalProductPrice")::numeric, 2)                         AS total_monthly_sales,
            ROUND(SUM(COALESCE("costPrice",0) * "quantity")::numeric, 2)        AS total_monthly_cost,
            ROUND((SUM("totalProductPrice")
                   - SUM(COALESCE("costPrice",0) * "quantity"))::numeric, 2)    AS total_monthly_profit,
            ROUND(AVG(
                CASE WHEN "totalProductPrice" > 0
                     THEN ("totalProductPrice"
                           - COALESCE("costPrice",0) * "quantity")
                          / "totalProductPrice" * 100
                     ELSE 0 END
            )::numeric, 2)                                                       AS avg_profit_margin_percent
        FROM "billing_data"
        WHERE "orderDate" BETWEEN %(ms)s AND %(me)s
          AND "storeName" NOT IN %(excl)s
        GROUP BY "storeName"
    """
    store_summary = safe_read_sql(q, params={"ms": ms_str, "me": me_str, "excl": excl})
    _upload(sc, f"{prefix}/store_summary.parquet", store_summary)

    # ── comparison: current month vs prev 3 months avg ────────────────────────
    print("\n[2/5] Computing comparison...")
    q = f"""
        WITH periods AS (
            SELECT
                "storeName",
                SUM(CASE WHEN "orderDate" BETWEEN %(ms)s  AND %(me)s  THEN "totalProductPrice" ELSE 0 END) AS current_month_sales,
                SUM(CASE WHEN "orderDate" BETWEEN %(m1s)s AND %(m1e)s THEN "totalProductPrice" ELSE 0 END) AS month_1_sales,
                SUM(CASE WHEN "orderDate" BETWEEN %(m2s)s AND %(m2e)s THEN "totalProductPrice" ELSE 0 END) AS month_2_sales,
                SUM(CASE WHEN "orderDate" BETWEEN %(m3s)s AND %(m3e)s THEN "totalProductPrice" ELSE 0 END) AS month_3_sales
            FROM "billing_data"
            WHERE "orderDate" BETWEEN %(m3s)s AND %(me)s
              AND "storeName" NOT IN %(excl)s
            GROUP BY "storeName"
        )
        SELECT
            "storeName",
            current_month_sales,
            ROUND(((month_1_sales + month_2_sales + month_3_sales) / 3.0)::numeric, 2) AS prev_3_months_avg
        FROM periods
    """
    comparison = safe_read_sql(q, params={
        "ms": ms_str, "me": me_str,
        "m1s": m1s, "m1e": m1e,
        "m2s": m2s, "m2e": m2e,
        "m3s": m3s, "m3e": m3e,
        "excl": excl,
    })
    _upload(sc, f"{prefix}/comparison.parquet", comparison)

    # ── brand_sales ───────────────────────────────────────────────────────────
    print("\n[3/5] Computing brand_sales...")
    q = f"""
        WITH totals AS (
            SELECT "storeName", SUM("totalProductPrice") AS store_total
            FROM "billing_data"
            WHERE "orderDate" BETWEEN %(ms)s AND %(me)s
              AND "storeName" NOT IN %(excl)s
            GROUP BY "storeName"
        )
        SELECT
            b."storeName",
            b."brandName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2)                        AS total_sales,
            SUM(b."quantity")                                                     AS quantity_sold,
            ROUND((SUM(b."totalProductPrice")
                   / NULLIF(t.store_total,0) * 100)::numeric, 2)                AS contrib_percent,
            ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                       THEN ((SUM(b."totalProductPrice")
                             - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                            / SUM(b."totalProductPrice") * 100)::numeric
                       ELSE 0 END, 2)                                            AS profit_margin
        FROM "billing_data" b
        JOIN totals t ON t."storeName" = b."storeName"
        WHERE b."orderDate" BETWEEN %(ms)s AND %(me)s
          AND b."storeName" NOT IN %(excl)s
        GROUP BY b."storeName", b."brandName", t.store_total
        ORDER BY b."storeName", total_sales DESC
    """
    brand_sales = safe_read_sql(q, params={"ms": ms_str, "me": me_str, "excl": excl})
    _upload(sc, f"{prefix}/brand_sales.parquet", brand_sales)

    # ── category_sales ────────────────────────────────────────────────────────
    print("\n[4/5] Computing category_sales...")
    q = f"""
        WITH totals AS (
            SELECT "storeName", SUM("totalProductPrice") AS store_total
            FROM "billing_data"
            WHERE "orderDate" BETWEEN %(ms)s AND %(me)s
              AND "storeName" NOT IN %(excl)s
            GROUP BY "storeName"
        )
        SELECT
            b."storeName",
            b."categoryName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2)                        AS total_sales,
            SUM(b."quantity")                                                     AS quantity_sold,
            ROUND((SUM(b."totalProductPrice")
                   / NULLIF(t.store_total,0) * 100)::numeric, 2)                AS contrib_percent,
            ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                       THEN ((SUM(b."totalProductPrice")
                             - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                            / SUM(b."totalProductPrice") * 100)::numeric
                       ELSE 0 END, 2)                                            AS profit_margin
        FROM "billing_data" b
        JOIN totals t ON t."storeName" = b."storeName"
        WHERE b."orderDate" BETWEEN %(ms)s AND %(me)s
          AND b."storeName" NOT IN %(excl)s
        GROUP BY b."storeName", b."categoryName", t.store_total
        ORDER BY b."storeName", total_sales DESC
    """
    category_sales = safe_read_sql(q, params={"ms": ms_str, "me": me_str, "excl": excl})
    _upload(sc, f"{prefix}/category_sales.parquet", category_sales)

    # ── product_sales ─────────────────────────────────────────────────────────
    print("\n[5/5] Computing product_sales...")
    q = f"""
        WITH totals AS (
            SELECT "storeName", SUM("totalProductPrice") AS store_total
            FROM "billing_data"
            WHERE "orderDate" BETWEEN %(ms)s AND %(me)s
              AND "storeName" NOT IN %(excl)s
            GROUP BY "storeName"
        )
        SELECT
            b."storeName",
            b."productName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2)                        AS total_sales,
            SUM(b."quantity")                                                     AS quantity_sold,
            ROUND((SUM(b."totalProductPrice")
                   / NULLIF(t.store_total,0) * 100)::numeric, 2)                AS contrib_percent,
            ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                       THEN ((SUM(b."totalProductPrice")
                             - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                            / SUM(b."totalProductPrice") * 100)::numeric
                       ELSE 0 END, 2)                                            AS profit_margin
        FROM "billing_data" b
        JOIN totals t ON t."storeName" = b."storeName"
        WHERE b."orderDate" BETWEEN %(ms)s AND %(me)s
          AND b."storeName" NOT IN %(excl)s
        GROUP BY b."storeName", b."productName", t.store_total
        ORDER BY b."storeName", total_sales DESC
    """
    product_sales = safe_read_sql(q, params={"ms": ms_str, "me": me_str, "excl": excl})
    _upload(sc, f"{prefix}/product_sales.parquet", product_sales)

    print(f"\n  ✓ Monthly cache complete → {prefix}")
    return prefix


# =============================================================================
# PUBLIC READ API — called by report scripts
# =============================================================================

def load_weekly_cache(week_start_str: str) -> dict:
    """
    Load all weekly cache tables for a given week_start (ISO string).
    Returns a dict of DataFrames keyed by table name.

    Usage in azure_blob_report.py:
        cache = report_cache.load_weekly_cache("2026-03-20")
        brand_df = cache["brand_sales"][cache["brand_sales"]["storeName"] == store_name]
    """
    prefix = f"weekly/week_start={week_start_str}"
    tables = ["store_summary", "comparison", "brand_sales", "category_sales", "product_sales"]
    result = {}
    for t in tables:
        result[t] = _download(f"{prefix}/{t}.parquet")
    return result


def load_monthly_cache(month_start_str: str) -> dict:
    """
    Load all monthly cache tables for a given month_start (ISO string).
    Returns a dict of DataFrames keyed by table name.

    Usage in monthly_llm.py:
        cache = report_cache.load_monthly_cache("2026-03-01")
        brand_df = cache["brand_sales"][cache["brand_sales"]["storeName"] == store_name]
    """
    prefix = f"monthly/month_start={month_start_str}"
    tables = ["store_summary", "comparison", "brand_sales", "category_sales", "product_sales"]
    result = {}
    for t in tables:
        result[t] = _download(f"{prefix}/{t}.parquet")
    return result


# =============================================================================
# DATE RESOLUTION — figure out the right week/month from DB
# =============================================================================

def resolve_weekly_dates() -> tuple:
    """Return (week_start, week_end) as date objects based on MAX(orderDate)."""
    q = 'SELECT MAX("orderDate")::date AS max_date FROM "billing_data"'
    df = safe_read_sql(q)
    max_date   = df["max_date"].iloc[0]
    week_end   = max_date
    week_start = max_date - timedelta(days=6)
    return week_start, week_end


def resolve_monthly_dates() -> tuple:
    """Return (month_start, month_end) for the previous complete calendar month."""
    from dateutil.relativedelta import relativedelta
    today       = datetime.now().date()
    month_start = (today.replace(day=1) - relativedelta(months=1))
    month_end   = today.replace(day=1) - timedelta(days=1)
    return month_start, month_end


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build report cache in Azure Blob Storage")
    parser.add_argument("--mode", choices=["weekly", "monthly", "both"], default="both",
                        help="Which cache to build (default: both)")
    args = parser.parse_args()

    start = time.time()

    ws, we = resolve_weekly_dates()
    build_weekly_cache(ws, we)

    # Monthly cache disabled — uncomment to re-enable
    # if args.mode in ("monthly", "both"):
    #     ms, me = resolve_monthly_dates()
    #     build_monthly_cache(ms, me)

    print(f"\n✓ Cache build complete in {time.time() - start:.1f} seconds")