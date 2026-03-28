"""
azure_staging.py  —  Bronze / Silver / Gold Data Lake
────────────────────────────────────────────────────────────────────────────────
Blob layout (single container: etl-data-lake):

  bronze/                         ← Raw API dump (Parquet chunks, immutable)
    year=2026/month=03/day=26/
      online/
        20260326_191310/
          chunk-001.parquet
          chunk-002.parquet

  silver/                         ← Validated + cleaned (Parquet, partitioned)
    sales/year=2026/month=03/day=26/part-0.parquet

  gold/                           ← Analytics-ready aggregates (Parquet)
    daily_metrics/year=2026/month=03/day=26/metrics.parquet
    brand_sales/year=2026/month=03/day=26/part-0.parquet
    store_sales/year=2026/month=03/day=26/part-0.parquet
    category_sales/year=2026/month=03/day=26/part-0.parquet
    product_sales/year=2026/month=03/day=26/part-0.parquet

────────────────────────────────────────────────────────────────────────────────
Upgrades in this version:
  1. Bronze → chunked Parquet (not CSV): smaller, faster re-read, column-aware
  2. Parallel chunk uploads via ThreadPoolExecutor
  3. Gold aggregations via DuckDB on Silver data — columnar, low RAM
  4. copy_dataframe_to_postgres() — COPY-based loader, 5-10x faster than
     execute_values for large tables
  5. All backwards-compat aliases preserved
────────────────────────────────────────────────────────────────────────────────
"""

import io
import os
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Optional, List

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
    BlobSasPermissions,
)
from dotenv import load_dotenv

load_dotenv()

# ── Azure config ──────────────────────────────────────────────────────────────
AZURE_ACCOUNT_NAME      = os.getenv("AZURE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY       = os.getenv("AZURE_ACCOUNT_KEY", "")
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING", "")

LAKE_CONTAINER     = os.getenv("AZURE_LAKE_CONTAINER", "etl-data-lake")
SAS_EXPIRY_DAYS    = int(os.getenv("SAS_EXPIRY_DAYS", "7"))
BRONZE_CHUNK_SIZE  = int(os.getenv("BRONZE_CHUNK_SIZE", "50000"))
MAX_UPLOAD_WORKERS = int(os.getenv("MAX_UPLOAD_WORKERS", "4"))

# ── Postgres config (used by COPY loader) ─────────────────────────────────────
def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

_DB_CONFIG = {
    "host":     _require_env("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": _require_env("DB_NAME"),
    "user":     _require_env("DB_USER"),
    "password": _require_env("DB_PASSWORD"),
}


# ── Partition helper ──────────────────────────────────────────────────────────

def _partition_path(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"


# ── Internal Azure helpers ────────────────────────────────────────────────────

def _get_service_client() -> BlobServiceClient:
    if AZURE_CONNECTION_STRING:
        return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    raise RuntimeError("AZURE_CONNECTION_STRING is not set in .env")


def _ensure_container(service_client: BlobServiceClient, container_name: str) -> None:
    from azure.core.exceptions import ResourceExistsError
    try:
        service_client.create_container(container_name)
        print(f"  Created container: {container_name}")
    except ResourceExistsError:
        pass


def _upload_bytes(sc: BlobServiceClient, blob_name: str, data: bytes,
                  content_type: str = "application/octet-stream") -> None:
    sc.get_blob_client(container=LAKE_CONTAINER, blob=blob_name).upload_blob(
        data, overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )


def _download_bytes(sc: BlobServiceClient, blob_name: str) -> bytes:
    return sc.get_blob_client(
        container=LAKE_CONTAINER, blob=blob_name
    ).download_blob().readall()


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buf, compression="snappy")
    buf.seek(0)
    return buf.read()


def _parquet_bytes_to_df(data: bytes) -> pd.DataFrame:
    return pq.read_table(io.BytesIO(data)).to_pandas()


def _list_blobs(prefix: str) -> List[str]:
    try:
        sc = _get_service_client()
        return sorted([b.name for b in
                       sc.get_container_client(LAKE_CONTAINER)
                         .list_blobs(name_starts_with=prefix)])
    except Exception as exc:
        print(f"  Warning: could not list blobs '{prefix}': {exc}")
        return []


# =============================================================================
# BRONZE LAYER — chunked Parquet, parallel upload, immutable + timestamped
#
# Layout:
#   bronze/{partition}/{order_type}/{timestamp}/chunk-001.parquet ...
# =============================================================================

def _upload_one_chunk(args) -> tuple:
    """Upload a single chunk blob. Runs inside ThreadPoolExecutor."""
    sc, blob_name, data = args
    _upload_bytes(sc, blob_name, data)
    return blob_name, len(data)


def bronze_upload(df: pd.DataFrame, order_type: str, date_str: str) -> List[str]:
    """
    Split df into BRONZE_CHUNK_SIZE chunks and upload each as Parquet in parallel.
    Returns sorted list of blob names written (one per chunk).
    Re-runs create a new timestamped folder — original chunks are never overwritten.
    """
    partition  = _partition_path(date_str)
    timestamp  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_path  = f"bronze/{partition}/{order_type}/{timestamp}"
    total_rows = len(df)
    n_chunks   = max(1, (total_rows + BRONZE_CHUNK_SIZE - 1) // BRONZE_CHUNK_SIZE)

    print(f"\n{'='*60}")
    print(f"BRONZE — Chunked Parquet upload  ({order_type} / {date_str})")
    print(f"  Rows   : {total_rows:,}  →  {n_chunks} chunk(s) of ≤{BRONZE_CHUNK_SIZE:,} rows")
    print(f"  Workers: {MAX_UPLOAD_WORKERS}")

    sc = _get_service_client()
    _ensure_container(sc, LAKE_CONTAINER)

    tasks = []
    for i in range(n_chunks):
        chunk     = df.iloc[i * BRONZE_CHUNK_SIZE : (i + 1) * BRONZE_CHUNK_SIZE]
        blob_name = f"{base_path}/chunk-{i+1:03d}.parquet"
        tasks.append((sc, blob_name, _df_to_parquet_bytes(chunk)))

    blob_names  = []
    total_bytes = 0
    with ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as pool:
        futures = {pool.submit(_upload_one_chunk, t): t[1] for t in tasks}
        for fut in as_completed(futures):
            name, size = fut.result()   # raises if upload failed
            blob_names.append(name)
            total_bytes += size

    blob_names.sort()
    print(f"  ✓ {n_chunks} chunk(s) uploaded  ({total_bytes/1024/1024:.2f} MB Parquet)")
    print(f"{'='*60}\n")
    return blob_names


def bronze_download(blob_input) -> pd.DataFrame:
    """
    Download Bronze Parquet chunk(s) → single concatenated DataFrame.

    Accepts:
      - List[str]  — explicit list of blob names
      - str ending with '/'  — prefix, auto-discovers all chunks underneath
      - str  — single blob name (legacy single-chunk)
    """
    print(f"\n{'='*60}")
    print(f"BRONZE — Parquet download")

    sc = _get_service_client()

    if isinstance(blob_input, list):
        names = sorted(blob_input)
    elif isinstance(blob_input, str) and blob_input.endswith("/"):
        names = _list_blobs(blob_input)
        print(f"  Discovered {len(names)} chunk(s) under: {blob_input}")
    else:
        names = [blob_input]

    if not names:
        raise RuntimeError(f"No Bronze blobs found for input: {blob_input}")

    chunks, total_bytes = [], 0
    for name in names:
        data = _download_bytes(sc, name)
        total_bytes += len(data)
        chunks.append(_parquet_bytes_to_df(data))

    df = pd.concat(chunks, ignore_index=True) if len(chunks) > 1 else chunks[0]
    print(f"  ✓ {len(names)} chunk(s)  "
          f"{total_bytes/1024/1024:.2f} MB  →  {len(df):,} rows, {len(df.columns)} cols")
    print(f"{'='*60}\n")
    return df


def bronze_get_latest(order_type: str, date_str: str) -> Optional[str]:
    """
    Return the prefix (ending with '/') of the most recent Bronze run folder.
    Pass the returned prefix directly to bronze_download().
    Returns None if no blobs exist for this date.
    """
    partition = _partition_path(date_str)
    prefix    = f"bronze/{partition}/{order_type}/"
    blobs     = _list_blobs(prefix)
    if not blobs:
        return None
    # Each blob: bronze/{partition}/{order_type}/{timestamp}/chunk-NNN.parquet
    # Extract unique run folders (everything up to and including timestamp/)
    run_folders = sorted(set(
        "/".join(b.split("/")[:-1]) + "/"
        for b in blobs
    ), reverse=True)
    return run_folders[0] if run_folders else None


# =============================================================================
# SILVER LAYER — validated + cleaned Parquet, one file per date
# =============================================================================

def silver_write(df: pd.DataFrame, date_str: str) -> str:
    """Write validated + transformed DataFrame to Silver layer. Returns blob name."""
    partition = _partition_path(date_str)
    blob_name = f"silver/sales/{partition}/part-0.parquet"

    print(f"\n{'='*60}")
    print(f"SILVER — Cleaned data write")
    print(f"  Blob  : {blob_name}")
    print(f"  Rows  : {len(df):,}   Columns: {len(df.columns)}")

    parquet_bytes = _df_to_parquet_bytes(df)
    sc = _get_service_client()
    _ensure_container(sc, LAKE_CONTAINER)
    _upload_bytes(sc, blob_name, parquet_bytes)

    print(f"  ✓ Silver write complete  ({len(parquet_bytes)/1024/1024:.2f} MB Parquet)")
    print(f"{'='*60}\n")
    return blob_name


def silver_read(date_str: str) -> pd.DataFrame:
    """Read back the Silver partition for a given date."""
    partition = _partition_path(date_str)
    blob_name = f"silver/sales/{partition}/part-0.parquet"
    sc   = _get_service_client()
    data = _download_bytes(sc, blob_name)
    df   = _parquet_bytes_to_df(data)
    print(f"  ✓ Silver read: {len(df):,} rows from {blob_name}")
    return df


# =============================================================================
# GOLD LAYER — DuckDB aggregations, Parquet output
# DuckDB operates on the registered Silver DataFrame with columnar execution.
# =============================================================================

def _gold_write_table(df: pd.DataFrame, table_name: str,
                      date_str: str, filename: str = "part-0.parquet") -> str:
    partition     = _partition_path(date_str)
    blob_name     = f"gold/{table_name}/{partition}/{filename}"
    parquet_bytes = _df_to_parquet_bytes(df)
    sc = _get_service_client()
    _ensure_container(sc, LAKE_CONTAINER)
    _upload_bytes(sc, blob_name, parquet_bytes)
    print(f"  ✓ gold/{table_name:<20}  {len(df):>6,} rows  "
          f"({len(parquet_bytes)/1024:>6.0f} KB)")
    return blob_name


def gold_write_aggregates(
    df_silver: pd.DataFrame,
    date_str: str,
    exclude_stores: Optional[list] = None,
) -> dict:
    """
    Compute + write all 5 Gold tables using DuckDB on the Silver DataFrame.
    Same logic and exclusions as aggregate.py / PostgreSQL tables.
    Returns dict of table_name → blob_name.
    """
    if exclude_stores is None:
        exclude_stores = ["Ho Marlboro"]

    print(f"\n{'='*60}")
    print(f"GOLD — DuckDB aggregate write  ({date_str})")
    print(f"{'='*60}")

    con = duckdb.connect()
    con.register("silver_raw", df_silver)

    exclude_list = ", ".join([f"'{s}'" for s in exclude_stores])
    con.execute(f"""
        CREATE TABLE gold_base AS
        SELECT *, TRY_CAST(totalProductPrice AS DOUBLE) AS _price
        FROM silver_raw
        WHERE storeName NOT IN ({exclude_list})
          AND TRY_CAST(totalProductPrice AS DOUBLE) IS NOT NULL
    """)

    for store in exclude_stores:
        n = con.execute(
            f"SELECT COUNT(*) FROM silver_raw WHERE storeName = '{store}'"
        ).fetchone()[0]
        if n:
            print(f"  Excluded {n:,} rows for store: {store}")

    written = {}

    # daily_metrics
    r = con.execute("""
        SELECT ROUND(SUM(_price),2), COUNT(DISTINCT invoice), COUNT(*),
               COUNT(DISTINCT storeName), COUNT(DISTINCT brandName),
               COUNT(DISTINCT subCategoryOf), COUNT(DISTINCT productName)
        FROM gold_base
    """).fetchone()
    ts, to_, tr, st, br, ca, pr = r
    written["daily_metrics"] = _gold_write_table(pd.DataFrame([{
        "date": date_str,
        "total_sales":       float(ts or 0),
        "total_orders":      int(to_ or 0),
        "total_rows":        int(tr or 0),
        "stores_active":     int(st or 0),
        "brands_active":     int(br or 0),
        "categories_active": int(ca or 0),
        "products_active":   int(pr or 0),
        "avg_order_value":   round(float(ts or 0) / max(int(to_ or 1), 1), 2),
    }]), "daily_metrics", date_str, "metrics.parquet")

    # brand_sales
    written["brand_sales"] = _gold_write_table(con.execute("""
        SELECT brandName, orderDate,
               COUNT(DISTINCT invoice)                          AS nooforders,
               ROUND(SUM(_price), 2)                           AS sales,
               ROUND(SUM(_price)/COUNT(DISTINCT invoice), 2)   AS aov
        FROM gold_base GROUP BY brandName, orderDate
    """).df(), "brand_sales", date_str)

    # store_sales
    written["store_sales"] = _gold_write_table(con.execute("""
        SELECT storeName, orderDate,
               COUNT(DISTINCT invoice)                          AS nooforder,
               ROUND(SUM(_price), 2)                           AS sales,
               ROUND(SUM(_price)/COUNT(DISTINCT invoice), 2)   AS aov
        FROM gold_base GROUP BY storeName, orderDate
    """).df(), "store_sales", date_str)

    # category_sales
    written["category_sales"] = _gold_write_table(con.execute("""
        SELECT subCategoryOf, orderDate,
               COUNT(DISTINCT invoice)  AS nooforder,
               ROUND(SUM(_price), 2)   AS sales
        FROM gold_base GROUP BY subCategoryOf, orderDate
    """).df(), "category_sales", date_str)

    # product_sales
    written["product_sales"] = _gold_write_table(con.execute("""
        SELECT productName, orderDate,
               COUNT(DISTINCT invoice)  AS nooforders,
               ROUND(SUM(_price), 2)   AS sales,
               SUM(quantity)           AS quantitysold
        FROM gold_base GROUP BY productName, orderDate
    """).df(), "product_sales", date_str)

    con.close()
    print(f"\n  ✓ Gold layer complete — {len(written)} tables written")
    print(f"{'='*60}\n")
    return written


def gold_read(table_name: str, date_str: str) -> pd.DataFrame:
    """Read back any Gold table for a given date."""
    partition = _partition_path(date_str)
    filename  = "metrics.parquet" if table_name == "daily_metrics" else "part-0.parquet"
    blob_name = f"gold/{table_name}/{partition}/{filename}"
    sc   = _get_service_client()
    data = _download_bytes(sc, blob_name)
    df   = _parquet_bytes_to_df(data)
    print(f"  ✓ Gold read [{table_name}]: {len(df):,} rows")
    return df


# =============================================================================
# POSTGRES COPY LOADER
# Uses Postgres COPY FROM STDIN — 5-10x faster than execute_values.
# Streams data through an in-memory CSV buffer, no temp files on disk.
# =============================================================================

def copy_dataframe_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    columns: list,
    conn=None,
) -> int:
    """
    Load df into a Postgres table using COPY FROM STDIN (CSV mode).

    Parameters
    ----------
    df         : DataFrame. Only `columns` are written.
    table_name : Target table (e.g. 'billing_data').
    columns    : Ordered list of column names matching the table schema.
    conn       : Existing psycopg2 connection, or None to open + close one.

    Returns
    -------
    Number of rows copied.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = psycopg2.connect(**_DB_CONFIG)
    try:
        cur = conn.cursor()
        buf = io.StringIO()
        df[columns].to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)
        col_sql = ", ".join([f'"{c}"' for c in columns])
        cur.copy_expert(
            f"COPY {table_name} ({col_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
            buf,
        )
        row_count = cur.rowcount
        if owns_conn:
            conn.commit()
        cur.close()
        return row_count
    except Exception:
        if owns_conn:
            conn.rollback()
        raise
    finally:
        if owns_conn:
            conn.close()


# =============================================================================
# SAS URL
# =============================================================================

def generate_sas_url(blob_name: str, expiry_days: int = SAS_EXPIRY_DAYS) -> str:
    """Generate a time-limited read-only SAS URL for any blob in the lake."""
    sas_token = generate_blob_sas(
        account_name   = AZURE_ACCOUNT_NAME,
        container_name = LAKE_CONTAINER,
        blob_name      = blob_name,
        account_key    = AZURE_ACCOUNT_KEY,
        permission     = BlobSasPermissions(read=True),
        expiry         = datetime.now(timezone.utc) + timedelta(days=expiry_days),
    )
    return (
        f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
        f"/{LAKE_CONTAINER}/{blob_name}?{sas_token}"
    )


# =============================================================================
# BACKWARDS COMPATIBILITY ALIASES
# =============================================================================

def upload_raw_csv(df: pd.DataFrame, order_type: str, date_str: str):
    """Alias → bronze_upload(). Returns list of blob names."""
    return bronze_upload(df, order_type, date_str)

def download_raw_csv(blob_input) -> pd.DataFrame:
    """Alias → bronze_download()."""
    return bronze_download(blob_input)

def get_latest_blob_for_date(order_type: str, date_str: str) -> Optional[str]:
    """Alias → bronze_get_latest(). Returns latest run prefix string."""
    return bronze_get_latest(order_type, date_str)