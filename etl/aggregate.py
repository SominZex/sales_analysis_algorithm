"""
aggregate.py
────────────────────────────────────────────────────────────────────────────────
Computes and loads all 4 aggregate tables into PostgreSQL.

Upgrades in this version:
  - All GROUP BY aggregations run via DuckDB (columnar, multi-threaded)
  - Postgres inserts use COPY FROM STDIN via azure_staging.copy_dataframe_to_postgres
    (5-10x faster than execute_values for large aggregate result sets)
  - Ho Marlboro exclusion verified in DuckDB before any DB work
  - Idempotency (delete-before-insert) retained exactly as before
────────────────────────────────────────────────────────────────────────────────
"""

import duckdb
import psycopg2
import pandas as pd
import io
import os
from dotenv import load_dotenv
import azure_staging

load_dotenv()


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


# Column names in DB (all lowercase) — order must match COPY target schema
POSTGRES_COLUMNS_BRAND    = ["brandname", "nooforders", "sales", "aov", "orderdate"]
POSTGRES_COLUMNS_STORE    = ["storename", "nooforder", "sales", "aov", "orderdate"]
POSTGRES_COLUMNS_CATEGORY = ["subcategoryof", "sales", "orderdate"]
POSTGRES_COLUMNS_PRODUCT  = ["productname", "nooforders", "sales", "quantitysold", "orderdate"]


def delete_existing_aggregates_for_dates(cur, dates: list):
    """
    Delete existing rows from all 4 aggregate tables for the given dates.
    Called once before all inserts so re-runs cleanly overwrite the data.
    """
    if not dates:
        print("No dates found in data — skipping aggregate delete step.")
        return

    print(f"\nChecking and clearing existing aggregate data for dates: {dates}")
    placeholders = ",".join(["%s"] * len(dates))
    tables = ["brand_sales", "store_sales", "category_sales", "product_sales"]
    for table in tables:
        cur.execute(
            f'DELETE FROM {table} WHERE "orderdate" IN ({placeholders})',
            dates
        )
        deleted = cur.rowcount
        if deleted > 0:
            print(f"  Deleted {deleted} existing rows from {table} (overwrite mode).")
        else:
            print(f"  No existing rows found in {table} for those dates — clean insert.")


# ── Columns required for all 4 aggregate tables ───────────────────────────────
AGG_REQUIRED_COLUMNS = [
    "invoice", "orderDate", "totalProductPrice", "quantity",
    "brandName", "storeName", "subCategoryOf", "productName"
]


def validate_agg_input(df: pd.DataFrame) -> None:
    """
    Validate that the DataFrame contains all columns needed for aggregation.
    Raises ValueError immediately — prevents silent KeyError mid-aggregation.
    """
    print("\n" + "=" * 60)
    print("AGG INPUT SCHEMA VALIDATION")
    print("=" * 60)

    missing_cols = [c for c in AGG_REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"AGG SCHEMA ERROR: Missing required columns for aggregation:\n"
            f"  {missing_cols}\n"
            f"  Available columns: {df.columns.tolist()}"
        )
    print(f"✓ All {len(AGG_REQUIRED_COLUMNS)} required agg columns present.")

    # DuckDB: count valid numeric prices
    con = duckdb.connect()
    con.register("_val_df", df)
    valid_price_count = con.execute(
        "SELECT COUNT(*) FROM _val_df WHERE TRY_CAST(totalProductPrice AS DOUBLE) IS NOT NULL"
    ).fetchone()[0]
    con.close()

    if valid_price_count == 0:
        raise ValueError(
            "AGG SCHEMA ERROR: 'totalProductPrice' has no valid numeric values — "
            "aggregations would produce empty results. Aborting."
        )
    print(f"✓ 'totalProductPrice' has {valid_price_count:,} valid numeric values.")

    valid_date_count = df['orderDate'].dropna().shape[0]
    if valid_date_count == 0:
        raise ValueError(
            "AGG SCHEMA ERROR: 'orderDate' has no valid values — "
            "aggregations cannot be grouped by date. Aborting."
        )
    print(f"✓ 'orderDate' has {valid_date_count:,} non-null values.")

    if df.empty:
        raise ValueError("AGG SCHEMA ERROR: DataFrame is empty — nothing to aggregate.")

    print("✓ Agg input validation passed.")
    print("=" * 60 + "\n")


def load_aggregates_to_postgres(df: pd.DataFrame):
    """
    Compute all 4 aggregate tables via DuckDB and load them into Postgres
    using COPY FROM STDIN (fastest bulk load method available in psycopg2).

    Idempotency: existing rows for the same dates are deleted before insert.
    Ho Marlboro: excluded from all aggregates, verified in DuckDB before any
                 database work begins.
    """
    conn = None
    con  = None
    try:
        # ── Fail-fast schema validation ────────────────────────────────────────
        validate_agg_input(df)

        # ── Build DuckDB filtered base table ──────────────────────────────────
        con = duckdb.connect()
        con.register("billing_raw", df)

        print(f"\n{'='*60}")
        print(f"EXCLUDING Ho Marlboro FROM AGGREGATE TABLES")
        print(f"{'='*60}")
        print(f"Total rows in billing_data (including Ho Marlboro): {len(df):,}")

        if 'storeName' not in df.columns:
            print("ERROR: 'storeName' column not found in DataFrame!")
            print(f"Available columns: {df.columns.tolist()}")
            return

        ho_count = con.execute(
            "SELECT COUNT(*) FROM billing_raw WHERE storeName = 'Ho Marlboro'"
        ).fetchone()[0]
        print(f"Ho Marlboro rows in source data: {ho_count:,}")

        con.execute("""
            CREATE TABLE billing_agg AS
            SELECT *, TRY_CAST(totalProductPrice AS DOUBLE) AS _price
            FROM billing_raw
            WHERE storeName != 'Ho Marlboro'
              AND TRY_CAST(totalProductPrice AS DOUBLE) IS NOT NULL
        """)

        rows_in_agg   = con.execute("SELECT COUNT(*) FROM billing_agg").fetchone()[0]
        rows_excluded = len(df) - rows_in_agg
        print(f"Rows excluded from aggregates : {rows_excluded:,}")
        print(f"Rows used for aggregates      : {rows_in_agg:,}")

        ho_check = con.execute(
            "SELECT COUNT(*) FROM billing_agg WHERE storeName = 'Ho Marlboro'"
        ).fetchone()[0]
        if ho_check > 0:
            print("❌ ERROR: Ho Marlboro still present in billing_agg — aborting.")
            return
        print("✓ Ho Marlboro successfully excluded from aggregates")
        print(f"{'='*60}\n")

        if rows_in_agg == 0:
            print("WARNING: No data remaining after filtering — nothing to insert.")
            return

        # ── Run all 4 GROUP BY aggregations via DuckDB ────────────────────────
        print("Computing aggregations via DuckDB...")

        brand_df = con.execute("""
            SELECT
                brandName                                           AS brandname,
                COUNT(DISTINCT invoice)                             AS nooforders,
                ROUND(SUM(_price), 2)                              AS sales,
                ROUND(SUM(_price) / COUNT(DISTINCT invoice), 2)    AS aov,
                orderDate                                           AS orderdate
            FROM billing_agg
            GROUP BY brandName, orderDate
        """).df()
        print(f"  ✓ brand_sales    : {len(brand_df):,} rows")

        store_df = con.execute("""
            SELECT
                storeName                                           AS storename,
                COUNT(DISTINCT invoice)                             AS nooforder,
                ROUND(SUM(_price), 2)                              AS sales,
                ROUND(SUM(_price) / COUNT(DISTINCT invoice), 2)    AS aov,
                orderDate                                           AS orderdate
            FROM billing_agg
            GROUP BY storeName, orderDate
        """).df()

        # Critical verification: Ho Marlboro must not appear in store aggregates
        if 'Ho Marlboro' in store_df['storename'].values:
            print("❌ CRITICAL ERROR: Ho Marlboro found in store_sales aggregates — aborting.")
            print(f"  Stores in aggregate: {sorted(store_df['storename'].unique())}")
            return
        print(f"  ✓ store_sales    : {len(store_df):,} rows "
              f"(verified: Ho Marlboro absent)")

        category_df = con.execute("""
            SELECT
                subCategoryOf               AS subcategoryof,
                ROUND(SUM(_price), 2)       AS sales,
                orderDate                   AS orderdate
            FROM billing_agg
            GROUP BY subCategoryOf, orderDate
        """).df()
        print(f"  ✓ category_sales : {len(category_df):,} rows")

        product_df = con.execute("""
            SELECT
                productName                 AS productname,
                COUNT(DISTINCT invoice)     AS nooforders,
                ROUND(SUM(_price), 2)       AS sales,
                CAST(SUM(quantity) AS BIGINT) AS quantitysold,
                orderDate                   AS orderdate
            FROM billing_agg
            GROUP BY productName, orderDate
        """).df()
        print(f"  ✓ product_sales  : {len(product_df):,} rows")

        # ── Open one shared Postgres connection for all inserts ────────────────
        print("\nConnecting to database...")
        conn = psycopg2.connect(
            host=require_env("DB_HOST"),
            port=int(os.getenv("DB_PORT", 5432)),
            database=require_env("DB_NAME"),
            user=require_env("DB_USER"),
            password=require_env("DB_PASSWORD"),
        )
        cur = conn.cursor()

        # ── IDEMPOTENCY: delete existing rows for these dates ─────────────────
        dates_in_data = df['orderDate'].dropna().unique().tolist()
        delete_existing_aggregates_for_dates(cur, dates_in_data)
        conn.commit()   # commit the deletes before COPY inserts begin

        # ── COPY inserts — 5-10x faster than execute_values ──────────────────
        print("\nLoading aggregates via COPY FROM STDIN...")

        def _copy(df_agg, table, cols):
            buf = io.StringIO()
            df_agg[cols].to_csv(buf, index=False, header=False, na_rep="\\N")
            buf.seek(0)
            col_sql = ", ".join([f'"{c}"' for c in cols])
            cur.copy_expert(
                f"COPY {table} ({col_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                buf,
            )
            print(f"  ✓ COPY → {table:<20}  {cur.rowcount:,} rows")

        _copy(brand_df,    "brand_sales",    POSTGRES_COLUMNS_BRAND)
        _copy(store_df,    "store_sales",    POSTGRES_COLUMNS_STORE)
        _copy(category_df, "category_sales", POSTGRES_COLUMNS_CATEGORY)
        _copy(product_df,  "product_sales",  POSTGRES_COLUMNS_PRODUCT)

        conn.commit()
        cur.close()
        conn.close()
        con.close()

        print(f"\n{'='*60}")
        print("✓ SUCCESS: All aggregate tables populated WITHOUT Ho Marlboro")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"\n{'='*60}")
        print(f"❌ ERROR in load_aggregates_to_postgres: {e}")
        print(f"{'='*60}\n")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        if con:
            con.close()
        raise