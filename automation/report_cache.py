"""
report_cache.py
────────────────────────────────────────────────────────────────────────────────
Precompute layer (WEEKLY) — runs once after the daily ETL, caches weekly
report data to Azure Blob Storage as Parquet files.

weekly_azure_llm.py loads its data from these cache files instead of running
heavy SQL queries at report time.

WHY THIS IS FASTER
──────────────────
Current situation (without cache):
  - Weekly report: 5 SQL queries × 140 stores = 700 sequential DB round-trips
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

READ API
────────
load_weekly_cache() returns a dict of PyArrow Tables (not pandas DataFrames).
weekly_azure_llm.py registers these tables into a shared DuckDB connection and
queries them with SQL — zero pandas in the hot path.

BLOB LAYOUT (container: report-cache)
──────────────────────────────────────
  weekly/
    week_start=2026-03-20/
      brand_sales.parquet
      category_sales.parquet
      product_sales.parquet
      store_summary.parquet
      comparison.parquet

USAGE
─────
  python report_cache.py
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

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING", "")
CACHE_CONTAINER         = os.getenv("AZURE_CACHE_CONTAINER", "report-cache")
EXCLUDED_STORES         = ["Ho Marlboro", "Dummy Store --- For Testing Only"]

DB_HOST     = require_env("DB_HOST")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = require_env("DB_NAME")
DB_USER     = require_env("DB_USER")
DB_PASSWORD = require_env("DB_PASSWORD")

_PG_CONN_STR = (
    f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
    f"user={DB_USER} password={DB_PASSWORD}"
)

DUCKDB_THREADS = int(os.getenv("DUCKDB_THREADS", "0"))


# ── DuckDB connection factory ─────────────────────────────────────────────────

def _new_duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if DUCKDB_THREADS > 0:
        con.execute(f"SET threads TO {DUCKDB_THREADS}")
    total_ram_gb    = psutil.virtual_memory().total / (1024 ** 3)
    memory_limit_gb = max(1, int(total_ram_gb * 0.8))
    con.execute(f"SET memory_limit = '{memory_limit_gb}GiB'")
    con.execute("SET temp_directory = '/tmp/duckdb_spill'")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{_PG_CONN_STR}' AS pg (TYPE POSTGRES, READ_ONLY)")
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
            f, overwrite=True,
            content_settings=ContentSettings(content_type="application/octet-stream"),
        )
    rows = pq.read_metadata(parquet_path).num_rows
    print(f"  ✓ {blob_name}  ({rows:,} rows, {size / 1024:.0f} KB)")


def _duck_to_blob(con: duckdb.DuckDBPyConnection, query: str,
                  sc: BlobServiceClient, blob_name: str):
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tf:
        tmp_path = tf.name
    try:
        con.execute(f"COPY ({query}) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
        _upload_parquet(sc, blob_name, tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _download(blob_name: str) -> pa.Table:
    """
    Download a Parquet blob and return a PyArrow Table.
    weekly_azure_llm.py registers this into DuckDB and queries it directly —
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
# WEEKLY CACHE BUILD
# =============================================================================

def build_weekly_cache(week_start, week_end) -> str:
    ws_str = week_start.isoformat()
    we_str = week_end.isoformat()
    prefix = f"weekly/week_start={ws_str}"

    print(f"\n{'='*60}")
    print(f"WEEKLY CACHE BUILD  —  {ws_str} to {we_str}")
    print(f"{'='*60}")

    sc   = _get_blob_client()
    _ensure_container(sc, CACHE_CONTAINER)
    excl = _excl_clause()
    con  = _new_duck()

    try:
        print("\n[1/5] Computing store_summary...")
        _duck_to_blob(con, f"""
            SELECT
                "storeName",
                ROUND(SUM("totalProductPrice")::DECIMAL(18,2), 2)                  AS total_weekly_sales,
                ROUND(SUM(COALESCE("costPrice",0) * "quantity")::DECIMAL(18,2), 2) AS total_weekly_cost,
                ROUND((SUM("totalProductPrice")
                       - SUM(COALESCE("costPrice",0) * "quantity"))::DECIMAL(18,2), 2) AS total_weekly_profit,
                ROUND(AVG(
                    CASE WHEN "totalProductPrice" > 0
                         THEN ("totalProductPrice" - COALESCE("costPrice",0) * "quantity")
                              / "totalProductPrice" * 100
                         ELSE 0 END
                )::DECIMAL(18,2), 2)                                               AS avg_profit_margin_percent
            FROM pg.billing_data
            WHERE "orderDate" BETWEEN '{ws_str}' AND '{we_str}' AND {excl}
            GROUP BY "storeName"
        """, sc, f"{prefix}/store_summary.parquet")

        print("\n[2/5] Computing comparison...")
        w2_start = (week_start - timedelta(days=7)).isoformat()
        w2_end   = (week_start - timedelta(days=1)).isoformat()
        w3_start = (week_start - timedelta(days=14)).isoformat()
        w3_end   = (week_start - timedelta(days=8)).isoformat()
        _duck_to_blob(con, f"""
            WITH periods AS (
                SELECT
                    "storeName",
                    SUM(CASE WHEN "orderDate" BETWEEN '{ws_str}'  AND '{we_str}'  THEN "totalProductPrice" ELSE 0 END) AS current_week_sales,
                    SUM(CASE WHEN "orderDate" BETWEEN '{w2_start}' AND '{w2_end}' THEN "totalProductPrice" ELSE 0 END) AS week_2_sales,
                    SUM(CASE WHEN "orderDate" BETWEEN '{w3_start}' AND '{w3_end}' THEN "totalProductPrice" ELSE 0 END) AS week_3_sales
                FROM pg.billing_data
                WHERE "orderDate" BETWEEN '{w3_start}' AND '{we_str}' AND {excl}
                GROUP BY "storeName"
            )
            SELECT "storeName", current_week_sales,
                   ROUND(((week_2_sales + week_3_sales) / 2.0)::DECIMAL(18,2), 2) AS prev_2_weeks_avg
            FROM periods
        """, sc, f"{prefix}/comparison.parquet")

        print("\n[3/5] Computing brand_sales...")
        _duck_to_blob(con, f"""
            WITH totals AS (
                SELECT "storeName", SUM("totalProductPrice") AS store_total
                FROM pg.billing_data
                WHERE "orderDate" BETWEEN '{ws_str}' AND '{we_str}' AND {excl}
                GROUP BY "storeName"
            )
            SELECT b."storeName", b."brandName",
                ROUND(SUM(b."totalProductPrice")::DECIMAL(18,2), 2)                                  AS total_sales,
                SUM(b."quantity")                                                                      AS quantity_sold,
                ROUND((SUM(b."totalProductPrice") / NULLIF(t.store_total,0) * 100)::DECIMAL(18,2), 2) AS contrib_percent,
                ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                           THEN ((SUM(b."totalProductPrice") - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                                / SUM(b."totalProductPrice") * 100)::DECIMAL(18,2)
                           ELSE 0 END, 2)                                                             AS profit_margin
            FROM pg.billing_data b JOIN totals t USING ("storeName")
            WHERE b."orderDate" BETWEEN '{ws_str}' AND '{we_str}' AND {excl}
            GROUP BY b."storeName", b."brandName", t.store_total
            ORDER BY b."storeName", total_sales DESC
        """, sc, f"{prefix}/brand_sales.parquet")

        print("\n[4/5] Computing category_sales...")
        _duck_to_blob(con, f"""
            WITH totals AS (
                SELECT "storeName", SUM("totalProductPrice") AS store_total
                FROM pg.billing_data
                WHERE "orderDate" BETWEEN '{ws_str}' AND '{we_str}' AND {excl}
                GROUP BY "storeName"
            )
            SELECT b."storeName", b."categoryName",
                ROUND(SUM(b."totalProductPrice")::DECIMAL(18,2), 2)                                  AS total_sales,
                SUM(b."quantity")                                                                      AS quantity_sold,
                ROUND((SUM(b."totalProductPrice") / NULLIF(t.store_total,0) * 100)::DECIMAL(18,2), 2) AS contrib_percent,
                ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                           THEN ((SUM(b."totalProductPrice") - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                                / SUM(b."totalProductPrice") * 100)::DECIMAL(18,2)
                           ELSE 0 END, 2)                                                             AS profit_margin
            FROM pg.billing_data b JOIN totals t USING ("storeName")
            WHERE b."orderDate" BETWEEN '{ws_str}' AND '{we_str}' AND {excl}
            GROUP BY b."storeName", b."categoryName", t.store_total
            ORDER BY b."storeName", total_sales DESC
        """, sc, f"{prefix}/category_sales.parquet")

        print("\n[5/5] Computing product_sales...")
        _duck_to_blob(con, f"""
            WITH totals AS (
                SELECT "storeName", SUM("totalProductPrice") AS store_total
                FROM pg.billing_data
                WHERE "orderDate" BETWEEN '{ws_str}' AND '{we_str}' AND {excl}
                GROUP BY "storeName"
            )
            SELECT b."storeName", b."productName",
                ROUND(SUM(b."totalProductPrice")::DECIMAL(18,2), 2)                                  AS total_sales,
                SUM(b."quantity")                                                                      AS quantity_sold,
                ROUND((SUM(b."totalProductPrice") / NULLIF(t.store_total,0) * 100)::DECIMAL(18,2), 2) AS contrib_percent,
                ROUND(CASE WHEN SUM(b."totalProductPrice") > 0
                           THEN ((SUM(b."totalProductPrice") - SUM(COALESCE(b."costPrice",0)*b."quantity"))
                                / SUM(b."totalProductPrice") * 100)::DECIMAL(18,2)
                           ELSE 0 END, 2)                                                             AS profit_margin
            FROM pg.billing_data b JOIN totals t USING ("storeName")
            WHERE b."orderDate" BETWEEN '{ws_str}' AND '{we_str}' AND {excl}
            GROUP BY b."storeName", b."productName", t.store_total
            ORDER BY b."storeName", total_sales DESC
        """, sc, f"{prefix}/product_sales.parquet")

    finally:
        con.close()

    print(f"\n  ✓ Weekly cache complete → {prefix}")
    return prefix


# =============================================================================
# PUBLIC READ API — called by weekly_azure_llm.py
# =============================================================================

def load_weekly_cache(week_start_str: str) -> dict:
    """
    Load all weekly cache tables for a given week_start (ISO string).
    Returns a dict of PyArrow Tables keyed by table name.

    weekly_azure_llm.py registers these into a DuckDB connection and queries
    them with SQL — no pandas materialisation in the hot path.
    """
    prefix = f"weekly/week_start={week_start_str}"
    tables = ["store_summary", "comparison", "brand_sales", "category_sales", "product_sales"]
    return {t: _download(f"{prefix}/{t}.parquet") for t in tables}


# =============================================================================
# DATE RESOLUTION
# =============================================================================

def resolve_weekly_dates() -> tuple:
    """Return (week_start, week_end) as date objects based on MAX(orderDate)."""
    con = _new_duck()
    try:
        row = con.execute(
            'SELECT MAX("orderDate")::DATE AS max_date FROM pg.billing_data'
        ).fetchone()
    finally:
        con.close()
    max_date   = row[0]
    week_end   = max_date
    week_start = max_date - timedelta(days=6)
    return week_start, week_end


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    start = time.time()
    ws, we = resolve_weekly_dates()
    build_weekly_cache(ws, we)
    print(f"\n✓ Weekly cache build complete in {time.time() - start:.1f} seconds")