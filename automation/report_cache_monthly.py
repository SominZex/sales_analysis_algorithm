"""
report_cache_monthly.py
────────────────────────────────────────────────────────────────────────────────
Precompute layer (MONTHLY) — runs once after the monthly ETL, caches monthly
report data to Azure Blob Storage as Parquet files.

monthly_llm.py loads its data from these cache files instead of running heavy
SQL queries at report time.

WHY THIS IS FASTER
──────────────────
Current situation (without cache):
  - Monthly report: 5 SQL queries × 140 stores = 700 sequential DB round-trips
  - Each query scans billing_data (340k+ rows) with GROUP BY + window functions
  - Total DB time: ~15–25 minutes just in queries

With this cache (DuckDB edition):
  - DuckDB connects directly to Postgres via its postgres extension and pushes
    all aggregation down — the DB sends only pre-grouped rows, not raw rows
  - DuckDB runs the GROUP BY / CASE logic in its own columnar engine using
    parallel threads — no Python/pandas overhead at all
  - Results written to Parquet via DuckDB's native writer (columnar, streamed)
  - Report scripts read Parquet (~50ms per store) instead of hitting the DB
  - Total DB time for report generation: ~0 seconds
  - Handles billions of rows: DuckDB spills to disk automatically when memory
    is insufficient, so it never OOMs regardless of dataset size
  - Estimated speedup over pandas: 5–15x additional on top of the 10–20x
    already gained by the single-pass cache strategy

BLOB LAYOUT (container: report-cache)
──────────────────────────────────────
  monthly/
    month_start=2026-03-01/
      brand_sales.parquet
      category_sales.parquet
      product_sales.parquet
      store_summary.parquet
      comparison.parquet         ← current vs prev 3 months per store

USAGE
─────
# Run after monthly ETL completes (add to cron after core_pipeline.py):
  python report_cache_monthly.py

# Report scripts load cache automatically — no changes needed to call sites.
────────────────────────────────────────────────────────────────────────────────
"""

import io
import os
import time
import tempfile
from datetime import datetime, timedelta
import psutil

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
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

DB_HOST     = require_env("DB_HOST")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = require_env("DB_NAME")
DB_USER     = require_env("DB_USER")
DB_PASSWORD = require_env("DB_PASSWORD")

_PG_CONN_STR = (
    f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
    f"user={DB_USER} password={DB_PASSWORD}"
)

# Set to 0 to let DuckDB auto-detect (uses all logical CPUs)
DUCKDB_THREADS = int(os.getenv("DUCKDB_THREADS", "0"))


# ── DuckDB connection factory ─────────────────────────────────────────────────

def _new_duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()

    if DUCKDB_THREADS > 0:
        con.execute(f"SET threads TO {DUCKDB_THREADS}")

    # Compute 80% of system RAM and express it as an explicit GiB value
    total_ram_gb = psutil.virtual_memory().total / (1024 ** 3)
    memory_limit_gb = max(1, int(total_ram_gb * 0.8))
    con.execute(f"SET memory_limit = '{memory_limit_gb}GiB'")
    con.execute("SET temp_directory = '/tmp/duckdb_spill'")

    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(
        f"ATTACH '{_PG_CONN_STR}' AS pg (TYPE POSTGRES, READ_ONLY)"
    )
    return con


# ── Blob helpers ──────────────────────────────────────────────────────────────

def _get_blob_client() -> BlobServiceClient:
    if not AZURE_CONNECTION_STRING:
        raise RuntimeError("AZURE_CONNECTION_STRING not set in .env")
    return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)


def _ensure_container(sc: BlobServiceClient, name: str):
    try:
        sc.create_container(name)
        print(f"  Created container: {name}")
    except ResourceExistsError:
        pass


def _upload_parquet(sc: BlobServiceClient, blob_name: str, parquet_path: str):
    size = os.path.getsize(parquet_path)
    with open(parquet_path, "rb") as f:
        sc.get_blob_client(container=CACHE_CONTAINER, blob=blob_name).upload_blob(
            f,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/octet-stream"),
        )
    meta = pq.read_metadata(parquet_path)
    rows = meta.num_rows
    print(f"  ✓ {blob_name}  ({rows:,} rows, {size / 1024:.0f} KB)")


def _duck_to_blob(
    con: duckdb.DuckDBPyConnection,
    query: str,
    sc: BlobServiceClient,
    blob_name: str,
):
    """
    Execute *query* in DuckDB, write result to a temp Parquet file using
    DuckDB's native columnar writer, upload to blob, then delete the temp file.
    Never materialises the full result in Python — safe for billions of rows.
    """
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tf:
        tmp_path = tf.name
    try:
        con.execute(f"""
            COPY ({query})
            TO '{tmp_path}'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        _upload_parquet(sc, blob_name, tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _download(blob_name: str) -> pa.Table:
    """
    Download a Parquet blob and return a PyArrow Table.
    monthly_azure_llm.py registers this into DuckDB and queries it directly —
    no pandas materialisation needed.
    """
    sc   = _get_blob_client()
    data = sc.get_blob_client(
        container=CACHE_CONTAINER, blob=blob_name
    ).download_blob().readall()
    return pq.read_table(io.BytesIO(data))


def _excl_clause() -> str:
    quoted = ", ".join(f"'{s}'" for s in EXCLUDED_STORES)
    return f'"storeName" NOT IN ({quoted})'


# =============================================================================
# MONTHLY CACHE
# =============================================================================

def build_monthly_cache(month_start, month_end) -> str:
    """
    Compute all monthly report data for all stores in a single pass per table.
    Writes Parquet files to blob via DuckDB's native writer.
    Returns the cache prefix string.
    """
    ms_str = month_start.isoformat()
    me_str = month_end.isoformat()
    prefix = f"monthly/month_start={ms_str}"

    print(f"\n{'='*60}")
    print(f"MONTHLY CACHE BUILD  —  {ms_str} to {me_str}")
    print(f"{'='*60}")

    sc   = _get_blob_client()
    _ensure_container(sc, CACHE_CONTAINER)
    excl = _excl_clause()
    con  = _new_duck()

    # Prev 3 month boundaries
    from dateutil.relativedelta import relativedelta
    m1s = (month_start - relativedelta(months=1)).isoformat()
    m1e = (month_start - timedelta(days=1)).isoformat()
    m2s = (month_start - relativedelta(months=2)).isoformat()
    m2e = (month_start - relativedelta(months=1) - timedelta(days=1)).isoformat()
    m3s = (month_start - relativedelta(months=3)).isoformat()
    m3e = (month_start - relativedelta(months=2) - timedelta(days=1)).isoformat()

    try:
        # ── store_summary ─────────────────────────────────────────────────────
        print("\n[1/5] Computing store_summary...")
        _duck_to_blob(con, f"""
            SELECT
                "storeName",
                ROUND(SUM("totalProductPrice")::DECIMAL(18,2), 2)                  AS total_monthly_sales,
                ROUND(SUM(COALESCE("costPrice",0) * "quantity")::DECIMAL(18,2), 2) AS total_monthly_cost,
                ROUND((SUM("totalProductPrice")
                       - SUM(COALESCE("costPrice",0) * "quantity"))::DECIMAL(18,2), 2) AS total_monthly_profit,
                ROUND(AVG(
                    CASE WHEN "totalProductPrice" > 0
                         THEN ("totalProductPrice"
                               - COALESCE("costPrice",0) * "quantity")
                              / "totalProductPrice" * 100
                         ELSE 0 END
                )::DECIMAL(18,2), 2)                                               AS avg_profit_margin_percent
            FROM pg.billing_data
            WHERE "orderDate" BETWEEN '{ms_str}' AND '{me_str}'
              AND {excl}
            GROUP BY "storeName"
        """, sc, f"{prefix}/store_summary.parquet")

        # ── comparison ────────────────────────────────────────────────────────
        print("\n[2/5] Computing comparison...")
        _duck_to_blob(con, f"""
            WITH periods AS (
                SELECT
                    "storeName",
                    SUM(CASE WHEN "orderDate" BETWEEN '{ms_str}' AND '{me_str}'
                             THEN "totalProductPrice" ELSE 0 END) AS current_month_sales,
                    SUM(CASE WHEN "orderDate" BETWEEN '{m1s}' AND '{m1e}'
                             THEN "totalProductPrice" ELSE 0 END) AS month_1_sales,
                    SUM(CASE WHEN "orderDate" BETWEEN '{m2s}' AND '{m2e}'
                             THEN "totalProductPrice" ELSE 0 END) AS month_2_sales,
                    SUM(CASE WHEN "orderDate" BETWEEN '{m3s}' AND '{m3e}'
                             THEN "totalProductPrice" ELSE 0 END) AS month_3_sales
                FROM pg.billing_data
                WHERE "orderDate" BETWEEN '{m3s}' AND '{me_str}'
                  AND {excl}
                GROUP BY "storeName"
            )
            SELECT
                "storeName",
                current_month_sales,
                ROUND(((month_1_sales + month_2_sales + month_3_sales) / 3.0)::DECIMAL(18,2), 2) AS prev_3_months_avg
            FROM periods
        """, sc, f"{prefix}/comparison.parquet")

        # ── brand_sales ───────────────────────────────────────────────────────
        print("\n[3/5] Computing brand_sales...")
        _duck_to_blob(con, f"""
            WITH totals AS (
                SELECT "storeName", SUM("totalProductPrice") AS store_total
                FROM pg.billing_data
                WHERE "orderDate" BETWEEN '{ms_str}' AND '{me_str}'
                  AND {excl}
                GROUP BY "storeName"
            )
            SELECT
                b."storeName",
                b."brandName",
                ROUND(SUM(b."totalProductPrice")::DECIMAL(18,2), 2)               AS total_sales,
                SUM(b."quantity")                                                   AS quantity_sold,
                ROUND((SUM(b."totalProductPrice")
                       / NULLIF(t.store_total, 0) * 100)::DECIMAL(18,2), 2)       AS contrib_percent,
                ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                           THEN ((SUM(b."totalProductPrice")
                                 - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                                / SUM(b."totalProductPrice") * 100)::DECIMAL(18,2)
                           ELSE 0 END, 2)                                          AS profit_margin
            FROM pg.billing_data b
            JOIN totals t USING ("storeName")
            WHERE b."orderDate" BETWEEN '{ms_str}' AND '{me_str}'
              AND {excl}
            GROUP BY b."storeName", b."brandName", t.store_total
            ORDER BY b."storeName", total_sales DESC
        """, sc, f"{prefix}/brand_sales.parquet")

        # ── category_sales ────────────────────────────────────────────────────
        print("\n[4/5] Computing category_sales...")
        _duck_to_blob(con, f"""
            WITH totals AS (
                SELECT "storeName", SUM("totalProductPrice") AS store_total
                FROM pg.billing_data
                WHERE "orderDate" BETWEEN '{ms_str}' AND '{me_str}'
                  AND {excl}
                GROUP BY "storeName"
            )
            SELECT
                b."storeName",
                b."categoryName",
                ROUND(SUM(b."totalProductPrice")::DECIMAL(18,2), 2)               AS total_sales,
                SUM(b."quantity")                                                   AS quantity_sold,
                ROUND((SUM(b."totalProductPrice")
                       / NULLIF(t.store_total, 0) * 100)::DECIMAL(18,2), 2)       AS contrib_percent,
                ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                           THEN ((SUM(b."totalProductPrice")
                                 - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                                / SUM(b."totalProductPrice") * 100)::DECIMAL(18,2)
                           ELSE 0 END, 2)                                          AS profit_margin
            FROM pg.billing_data b
            JOIN totals t USING ("storeName")
            WHERE b."orderDate" BETWEEN '{ms_str}' AND '{me_str}'
              AND {excl}
            GROUP BY b."storeName", b."categoryName", t.store_total
            ORDER BY b."storeName", total_sales DESC
        """, sc, f"{prefix}/category_sales.parquet")

        # ── product_sales ─────────────────────────────────────────────────────
        print("\n[5/5] Computing product_sales...")
        _duck_to_blob(con, f"""
            WITH totals AS (
                SELECT "storeName", SUM("totalProductPrice") AS store_total
                FROM pg.billing_data
                WHERE "orderDate" BETWEEN '{ms_str}' AND '{me_str}'
                  AND {excl}
                GROUP BY "storeName"
            )
            SELECT
                b."storeName",
                b."productName",
                ROUND(SUM(b."totalProductPrice")::DECIMAL(18,2), 2)               AS total_sales,
                SUM(b."quantity")                                                   AS quantity_sold,
                ROUND((SUM(b."totalProductPrice")
                       / NULLIF(t.store_total, 0) * 100)::DECIMAL(18,2), 2)       AS contrib_percent,
                ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                           THEN ((SUM(b."totalProductPrice")
                                 - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                                / SUM(b."totalProductPrice") * 100)::DECIMAL(18,2)
                           ELSE 0 END, 2)                                          AS profit_margin
            FROM pg.billing_data b
            JOIN totals t USING ("storeName")
            WHERE b."orderDate" BETWEEN '{ms_str}' AND '{me_str}'
              AND {excl}
            GROUP BY b."storeName", b."productName", t.store_total
            ORDER BY b."storeName", total_sales DESC
        """, sc, f"{prefix}/product_sales.parquet")

    finally:
        con.close()

    print(f"\n  ✓ Monthly cache complete → {prefix}")
    return prefix


# =============================================================================
# PUBLIC READ API — called by monthly_llm.py
# =============================================================================

def load_monthly_cache(month_start_str: str) -> dict:
    """
    Load all monthly cache tables for a given month_start (ISO string).
    Returns a dict of PyArrow Tables keyed by table name.

    monthly_azure_llm.py registers these into a DuckDB connection and queries
    them with SQL — no pandas materialisation in the hot path.
    """
    prefix = f"monthly/month_start={month_start_str}"
    tables = ["store_summary", "comparison", "brand_sales", "category_sales", "product_sales"]
    return {t: _download(f"{prefix}/{t}.parquet") for t in tables}


# =============================================================================
# DATE RESOLUTION
# =============================================================================

def resolve_monthly_dates() -> tuple:
    """Return (month_start, month_end) for the previous complete calendar month."""
    from dateutil.relativedelta import relativedelta
    today       = datetime.now().date()
    month_start = today.replace(day=1) - relativedelta(months=1)
    month_end   = today.replace(day=1) - timedelta(days=1)
    return month_start, month_end


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    start = time.time()
    ms, me = resolve_monthly_dates()
    build_monthly_cache(ms, me)
    print(f"\n✓ Monthly cache build complete in {time.time() - start:.1f} seconds")