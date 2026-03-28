"""
azure_staging.py  —  Bronze / Silver / Gold Data Lake
────────────────────────────────────────────────────────────────────────────────
Blob layout (single container: etl-data-lake):

  bronze/                         ← Raw API dump (CSV, immutable, timestamped)
    year=2026/month=03/day=26/
      online_20260326_191310.csv

  silver/                         ← Validated + cleaned (Parquet, partitioned)
    sales/
      year=2026/month=03/day=26/
        part-0.parquet

  gold/                           ← Analytics-ready aggregates (Parquet)
    daily_metrics/
      year=2026/month=03/day=26/
        metrics.parquet
    brand_sales/
      year=2026/month=03/day=26/
        part-0.parquet
    store_sales/
      year=2026/month=03/day=26/
        part-0.parquet
    category_sales/
      year=2026/month=03/day=26/
        part-0.parquet
    product_sales/
      year=2026/month=03/day=26/
        part-0.parquet

────────────────────────────────────────────────────────────────────────────────
IMPORTANT: core_pipeline.py and aggregate.py are NOT modified.
  - All PostgreSQL inserts, schema validation, idempotency logic are unchanged.
  - This module is purely additive — it writes to blob alongside Postgres.
  - Backwards-compatible aliases (upload_raw_csv / download_raw_csv /
    get_latest_blob_for_date) mean core_pipeline.py needs zero changes.
────────────────────────────────────────────────────────────────────────────────
"""

import io
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

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

LAKE_CONTAINER  = os.getenv("AZURE_LAKE_CONTAINER", "etl-data-lake")
SAS_EXPIRY_DAYS = int(os.getenv("SAS_EXPIRY_DAYS", "7"))


# ── Partition helper ──────────────────────────────────────────────────────────

def _partition_path(date_str: str) -> str:
    """
    '2026-03-26'  →  'year=2026/month=03/day=26'
    Used as the directory prefix for every Silver and Gold blob.
    Hive-style partitioning — compatible with Spark, Athena, Synapse, DuckDB.
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"


# ── Internal helpers ──────────────────────────────────────────────────────────

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


def _upload_bytes(
    service_client: BlobServiceClient,
    blob_name: str,
    data: bytes,
    content_type: str,
) -> None:
    """Upload bytes to a blob, overwriting if it exists."""
    blob_client = service_client.get_blob_client(
        container=LAKE_CONTAINER,
        blob=blob_name,
    )
    blob_client.upload_blob(
        data,
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialise a DataFrame to Parquet (Snappy compressed) in memory."""
    buf   = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    return buf.read()


def _parquet_bytes_to_df(data: bytes) -> pd.DataFrame:
    """Deserialise Parquet bytes to a DataFrame."""
    return pq.read_table(io.BytesIO(data)).to_pandas()


def _download_bytes(service_client: BlobServiceClient, blob_name: str) -> bytes:
    return service_client.get_blob_client(
        container=LAKE_CONTAINER,
        blob=blob_name,
    ).download_blob().readall()


# =============================================================================
# BRONZE LAYER
# Raw API dump — CSV, immutable, timestamped. Nothing is modified here.
# bronze/year=2026/month=03/day=26/online_20260326_191310.csv
# =============================================================================

def bronze_upload(df: pd.DataFrame, order_type: str, date_str: str) -> str:
    """
    Write the exact API response to the Bronze layer as CSV.
    Timestamped so re-runs produce a new file — original is never overwritten.

    Returns the full blob name written.
    """
    partition = _partition_path(date_str)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    blob_name = f"bronze/{partition}/{order_type}_{timestamp}.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    print(f"\n{'='*60}")
    print(f"BRONZE — Raw upload")
    print(f"  Blob  : {blob_name}")
    print(f"  Rows  : {len(df):,}   Size: {len(csv_bytes)/1024/1024:.2f} MB")

    try:
        sc = _get_service_client()
        _ensure_container(sc, LAKE_CONTAINER)
        _upload_bytes(sc, blob_name, csv_bytes, "text/csv")
        print(f"  ✓ Bronze upload complete")
        print(f"{'='*60}\n")
        return blob_name
    except Exception as exc:
        raise RuntimeError(f"Bronze upload failed ({blob_name}): {exc}") from exc


def bronze_download(blob_name: str) -> pd.DataFrame:
    """
    Download a Bronze CSV blob and return it as a raw DataFrame.
    Used for replay — skips the API entirely.
    """
    print(f"\n{'='*60}")
    print(f"BRONZE — Replay download")
    print(f"  Blob : {blob_name}")
    try:
        sc   = _get_service_client()
        data = _download_bytes(sc, blob_name)
        df   = pd.read_csv(io.BytesIO(data), low_memory=False)
        print(f"  ✓ {len(data)/1024/1024:.2f} MB → {len(df):,} rows, {len(df.columns)} columns")
        print(f"{'='*60}\n")
        return df
    except Exception as exc:
        raise RuntimeError(f"Bronze download failed ({blob_name}): {exc}") from exc


def bronze_get_latest(order_type: str, date_str: str) -> Optional[str]:
    """Return the most recent Bronze blob for (order_type, date_str), or None."""
    partition = _partition_path(date_str)
    prefix    = f"bronze/{partition}/{order_type}_"
    try:
        sc    = _get_service_client()
        cc    = sc.get_container_client(LAKE_CONTAINER)
        blobs = sorted(
            [b.name for b in cc.list_blobs(name_starts_with=prefix)],
            reverse=True,
        )
        return blobs[0] if blobs else None
    except Exception as exc:
        print(f"  Warning: could not list Bronze blobs: {exc}")
        return None


# =============================================================================
# SILVER LAYER
# Validated + cleaned, Parquet + Snappy, partitioned by date.
# silver/sales/year=2026/month=03/day=26/part-0.parquet
# =============================================================================

def silver_write(df: pd.DataFrame, date_str: str) -> str:
    """
    Write the validated and transformed DataFrame to the Silver layer.

    Called AFTER validate_schema() + transform_data() in core_pipeline.py,
    so the data is clean, correctly typed, and ready for analytics.

    Parquet + Snappy is typically 3–5x smaller than the equivalent Bronze CSV.
    Returns the blob name written.
    """
    partition = _partition_path(date_str)
    blob_name = f"silver/sales/{partition}/part-0.parquet"

    print(f"\n{'='*60}")
    print(f"SILVER — Cleaned data write")
    print(f"  Blob  : {blob_name}")
    print(f"  Rows  : {len(df):,}   Columns: {len(df.columns)}")

    try:
        sc            = _get_service_client()
        _ensure_container(sc, LAKE_CONTAINER)
        parquet_bytes = _df_to_parquet_bytes(df)
        _upload_bytes(sc, blob_name, parquet_bytes, "application/octet-stream")
        print(f"  ✓ Silver write complete  ({len(parquet_bytes)/1024/1024:.2f} MB Parquet)")
        print(f"{'='*60}\n")
        return blob_name
    except Exception as exc:
        raise RuntimeError(f"Silver write failed ({blob_name}): {exc}") from exc


def silver_read(date_str: str) -> pd.DataFrame:
    """
    Read back the Silver partition for a given date.

    Usage:
        df = azure_staging.silver_read("2026-03-26")
    """
    partition = _partition_path(date_str)
    blob_name = f"silver/sales/{partition}/part-0.parquet"
    try:
        sc   = _get_service_client()
        data = _download_bytes(sc, blob_name)
        df   = _parquet_bytes_to_df(data)
        print(f"  ✓ Silver read: {len(df):,} rows from {blob_name}")
        return df
    except Exception as exc:
        raise RuntimeError(f"Silver read failed ({blob_name}): {exc}") from exc


# =============================================================================
# GOLD LAYER
# Analytics-ready aggregates, Parquet, partitioned by date.
# Mirrors the exact same groupby logic as aggregate.py / PostgreSQL tables.
# gold/<table>/year=2026/month=03/day=26/part-0.parquet
# gold/daily_metrics/year=2026/month=03/day=26/metrics.parquet
# =============================================================================

def _gold_write_table(
    df: pd.DataFrame,
    table_name: str,
    date_str: str,
    filename: str = "part-0.parquet",
) -> str:
    """Internal: write one Gold table partition."""
    partition     = _partition_path(date_str)
    blob_name     = f"gold/{table_name}/{partition}/{filename}"
    parquet_bytes = _df_to_parquet_bytes(df)
    sc            = _get_service_client()
    _ensure_container(sc, LAKE_CONTAINER)
    _upload_bytes(sc, blob_name, parquet_bytes, "application/octet-stream")
    print(f"  ✓ gold/{table_name:<20}  {len(df):>6,} rows  "
          f"({len(parquet_bytes)/1024:>6.0f} KB)")
    return blob_name


def gold_write_aggregates(
    df_silver: pd.DataFrame,
    date_str: str,
    exclude_stores: Optional[list] = None,
) -> dict:
    """
    Compute and write all Gold aggregate tables from the Silver DataFrame.

    Uses the same groupby logic and exclusions as aggregate.py so the Gold
    layer is always consistent with what is in PostgreSQL.

    Parameters
    ----------
    df_silver      : Validated + transformed DataFrame (output of silver_write).
    date_str       : ISO date string used for Hive partitioning.
    exclude_stores : Stores excluded from aggregates. Defaults to ["Ho Marlboro"].

    Returns
    -------
    Dict mapping table_name → blob_name written.
    """
    if exclude_stores is None:
        exclude_stores = ["Ho Marlboro"]

    print(f"\n{'='*60}")
    print(f"GOLD — Aggregate write  ({date_str})")
    print(f"{'='*60}")

    df = df_silver.copy()
    df['totalProductPrice'] = pd.to_numeric(df['totalProductPrice'], errors='coerce')
    df = df[df['totalProductPrice'].notna()]

    # Apply same store exclusions as aggregate.py
    for store in exclude_stores:
        before = len(df)
        df     = df[df['storeName'] != store]
        if before - len(df):
            print(f"  Excluded {before - len(df):,} rows for store: {store}")

    written: dict = {}

    # ── daily_metrics ─────────────────────────────────────────────────────────
    total_sales  = float(df['totalProductPrice'].sum())
    total_orders = int(df['invoice'].nunique())
    metrics = pd.DataFrame([{
        "date":              date_str,
        "total_sales":       round(total_sales, 2),
        "total_orders":      total_orders,
        "total_rows":        len(df),
        "stores_active":     int(df['storeName'].nunique()),
        "brands_active":     int(df['brandName'].nunique()),
        "categories_active": int(df['subCategoryOf'].nunique()),
        "products_active":   int(df['productName'].nunique()),
        "avg_order_value":   round(total_sales / total_orders, 2) if total_orders else 0.0,
    }])
    written["daily_metrics"] = _gold_write_table(
        metrics, "daily_metrics", date_str, "metrics.parquet"
    )

    # ── brand_sales ───────────────────────────────────────────────────────────
    brand_df = df.groupby(['brandName', 'orderDate'], as_index=False).agg(
        nooforders=('invoice',           'nunique'),
        sales     =('totalProductPrice', 'sum'),
    )
    brand_df['sales'] = brand_df['sales'].round(2)
    brand_df['aov']   = (brand_df['sales'] / brand_df['nooforders']).round(2)
    written["brand_sales"] = _gold_write_table(brand_df, "brand_sales", date_str)

    # ── store_sales ───────────────────────────────────────────────────────────
    store_df = df.groupby(['storeName', 'orderDate'], as_index=False).agg(
        nooforder=('invoice',           'nunique'),
        sales    =('totalProductPrice', 'sum'),
    )
    store_df['sales'] = store_df['sales'].round(2)
    store_df['aov']   = (store_df['sales'] / store_df['nooforder']).round(2)
    written["store_sales"] = _gold_write_table(store_df, "store_sales", date_str)

    # ── category_sales ────────────────────────────────────────────────────────
    cat_df = df.groupby(['subCategoryOf', 'orderDate'], as_index=False).agg(
        nooforder=('invoice',           'nunique'),
        sales    =('totalProductPrice', 'sum'),
    )
    cat_df['sales'] = cat_df['sales'].round(2)
    written["category_sales"] = _gold_write_table(cat_df, "category_sales", date_str)

    # ── product_sales ─────────────────────────────────────────────────────────
    prod_df = df.groupby(['productName', 'orderDate'], as_index=False).agg(
        nooforders  =('invoice',           'nunique'),
        sales       =('totalProductPrice', 'sum'),
        quantitysold=('quantity',          'sum'),
    )
    prod_df['sales'] = prod_df['sales'].round(2)
    written["product_sales"] = _gold_write_table(prod_df, "product_sales", date_str)

    print(f"\n  ✓ Gold layer complete — {len(written)} tables written")
    print(f"{'='*60}\n")
    return written


def gold_read(table_name: str, date_str: str) -> pd.DataFrame:
    """
    Read back any Gold table for a given date.

    Usage:
        df = azure_staging.gold_read("brand_sales",   "2026-03-26")
        df = azure_staging.gold_read("daily_metrics", "2026-03-26")
        df = azure_staging.gold_read("store_sales",   "2026-03-26")
    """
    partition = _partition_path(date_str)
    filename  = "metrics.parquet" if table_name == "daily_metrics" else "part-0.parquet"
    blob_name = f"gold/{table_name}/{partition}/{filename}"
    try:
        sc   = _get_service_client()
        data = _download_bytes(sc, blob_name)
        df   = _parquet_bytes_to_df(data)
        print(f"  ✓ Gold read [{table_name}]: {len(df):,} rows")
        return df
    except Exception as exc:
        raise RuntimeError(f"Gold read failed ({blob_name}): {exc}") from exc


# =============================================================================
# SAS URL — shareable read-only link for any layer
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
# BACKWARDS COMPATIBILITY
# core_pipeline.py calls upload_raw_csv / download_raw_csv / get_latest_blob_for_date.
# These aliases mean core_pipeline.py needs ZERO changes to work with the lake.
# =============================================================================

def upload_raw_csv(df: pd.DataFrame, order_type: str, date_str: str) -> str:
    """Alias → bronze_upload(). Keeps core_pipeline.py unchanged."""
    return bronze_upload(df, order_type, date_str)


def download_raw_csv(blob_name: str) -> pd.DataFrame:
    """Alias → bronze_download(). Keeps core_pipeline.py unchanged."""
    return bronze_download(blob_name)


def get_latest_blob_for_date(order_type: str, date_str: str) -> Optional[str]:
    """Alias → bronze_get_latest(). Keeps core_pipeline.py unchanged."""
    return bronze_get_latest(order_type, date_str)