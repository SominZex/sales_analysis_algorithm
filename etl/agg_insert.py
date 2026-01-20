import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

load_dotenv()

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


# Column names in DB (all lowercase)
POSTGRES_COLUMNS_BRAND = ["brandname", "nooforders", "sales", "aov", "orderdate"]
POSTGRES_COLUMNS_STORE = ["storename", "nooforder", "sales", "aov", "orderdate"]
POSTGRES_COLUMNS_CATEGORY = ["subcategoryof", "sales", "orderdate"]
POSTGRES_COLUMNS_PRODUCT = ["productname", "nooforders", "sales", "quantitysold", "orderdate"]

def load_aggregates_to_postgres(df: pd.DataFrame):
    conn = None
    try:
        df['totalProductPrice'] = pd.to_numeric(df['totalProductPrice'], errors='coerce')
        df = df[df['totalProductPrice'].notna()]
        
        # CRITICAL: EXCLUDE Ho Marlboro store from aggregations ONLY
        print(f"\n{'='*60}")
        print(f"EXCLUDING Ho Marlboro FROM AGGREGATE TABLES")
        print(f"{'='*60}")
        print(f"Total rows in billing_data (including Ho Marlboro): {len(df)}")
        
        # Check if storeName column exists
        if 'storeName' not in df.columns:
            print("ERROR: 'storeName' column not found in DataFrame!")
            print(f"Available columns: {df.columns.tolist()}")
            return
        
        # Show unique store names before filtering
        unique_stores_all = df['storeName'].unique()
        ho_marlboro_count = len(df[df['storeName'] == 'Ho Marlboro'])
        print(f"Ho Marlboro rows in source data: {ho_marlboro_count}")
        
        # Filter out Ho Marlboro for aggregations
        df_for_aggregates = df[df['storeName'] != 'Ho Marlboro'].copy()
        
        # Show results
        rows_excluded = len(df) - len(df_for_aggregates)
        print(f"Rows excluded from aggregates: {rows_excluded}")
        print(f"Rows used for aggregates: {len(df_for_aggregates)}")
        
        if 'Ho Marlboro' in df_for_aggregates['storeName'].values:
            print("❌ ERROR: Ho Marlboro still in aggregates dataframe!")
            return
        else:
            print("✓ Ho Marlboro successfully excluded from aggregates")
        print(f"{'='*60}\n")
        
        # Use filtered dataframe for all aggregations
        df_agg = df_for_aggregates
        
        if len(df_agg) == 0:
            print("WARNING: No data remaining after filtering!")
            return
        
        print("Connecting to database...")
        conn = psycopg2.connect(
            host=require_env("DB_HOST"),
            port=int(os.getenv("DB_PORT", 5432)),
            database=require_env("DB_NAME"),
            user=require_env("DB_USER"),
            password=require_env("DB_PASSWORD"),
        )
        cur = conn.cursor()


        # -------- Brand Sales (Ho Marlboro excluded) --------
        print("\nProcessing Brand Sales...")
        brand_df = df_agg.groupby(['brandName', 'orderDate'], as_index=False).agg(
            nooforders=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum')
        )
        brand_df['sales'] = pd.to_numeric(brand_df['sales'], errors='coerce')
        brand_df['aov'] = (brand_df['sales'] / brand_df['nooforders']).round(2)
        brand_df.rename(columns={'brandName': 'brandname', 'orderDate': 'orderdate'}, inplace=True)

        brand_cols = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_BRAND])
        brand_tuples = [tuple(row) for row in brand_df[POSTGRES_COLUMNS_BRAND].values]

        if len(brand_tuples) > 0:
            psycopg2.extras.execute_values(
                cur,
                f'INSERT INTO brand_sales ({brand_cols}) VALUES %s',
                brand_tuples,
                template=None,
                page_size=1000
            )
            print(f"✓ Inserted {len(brand_tuples)} rows into brand_sales (Ho Marlboro excluded)")
        else:
            print("No brand data to insert")

        # -------- Store Sales (Ho Marlboro excluded) --------
        print("\nProcessing Store Sales...")
        store_df = df_agg.groupby(['storeName', 'orderDate'], as_index=False).agg(
            nooforder=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum')
        )
        store_df['sales'] = pd.to_numeric(store_df['sales'], errors='coerce')
        store_df['aov'] = (store_df['sales'] / store_df['nooforder']).round(2)
        store_df.rename(columns={'storeName': 'storename', 'orderDate': 'orderdate'}, inplace=True)
        
        # CRITICAL VERIFICATION: Ensure Ho Marlboro is NOT in store aggregates
        if 'Ho Marlboro' in store_df['storename'].values:
            print("❌ CRITICAL ERROR: Ho Marlboro found in store_sales aggregates!")
            print(f"Stores in aggregate: {sorted(store_df['storename'].unique())}")
            conn.rollback()
            cur.close()
            conn.close()
            return
        else:
            print(f"✓ Verified: Ho Marlboro NOT in store aggregates")
            print(f"  Stores included: {sorted(store_df['storename'].unique())}")

        store_cols = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_STORE])
        store_tuples = [tuple(row) for row in store_df[POSTGRES_COLUMNS_STORE].values]

        if len(store_tuples) > 0:
            psycopg2.extras.execute_values(
                cur,
                f'INSERT INTO store_sales ({store_cols}) VALUES %s',
                store_tuples,
                template=None,
                page_size=1000
            )
            print(f"✓ Inserted {len(store_tuples)} rows into store_sales")
        else:
            print("No store data to insert")

        # -------- Category Sales (Ho Marlboro excluded) --------
        print("\nProcessing Category Sales...")
        category_df = df_agg.groupby(['subCategoryOf', 'orderDate'], as_index=False).agg(
            nooforder=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum')
        )
        category_df.rename(columns={'subCategoryOf': 'subcategoryof', 'orderDate': 'orderdate'}, inplace=True)

        category_cols = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_CATEGORY])
        category_tuples = [tuple(row) for row in category_df[POSTGRES_COLUMNS_CATEGORY].values]

        if len(category_tuples) > 0:
            psycopg2.extras.execute_values(
                cur,
                f'INSERT INTO category_sales ({category_cols}) VALUES %s',
                category_tuples,
                template=None,
                page_size=1000
            )
            print(f"✓ Inserted {len(category_tuples)} rows into category_sales (Ho Marlboro excluded)")
        else:
            print("No category data to insert")

        # -------- Product Sales (Ho Marlboro excluded) --------
        print("\nProcessing Product Sales...")
        product_df = df_agg.groupby(['productName', 'orderDate'], as_index=False).agg(
            nooforders=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum'),
            quantitysold=('quantity', 'sum')
        )
        product_df.rename(columns={'productName': 'productname', 'orderDate': 'orderdate'}, inplace=True)

        product_cols = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_PRODUCT])
        product_tuples = [tuple(row) for row in product_df[POSTGRES_COLUMNS_PRODUCT].values]

        if len(product_tuples) > 0:
            psycopg2.extras.execute_values(
                cur,
                f'INSERT INTO product_sales ({product_cols}) VALUES %s',
                product_tuples,
                template=None,
                page_size=1000
            )
            print(f"✓ Inserted {len(product_tuples)} rows into product_sales (Ho Marlboro excluded)")
        else:
            print("No product data to insert")

        conn.commit()
        cur.close()
        conn.close()
        
        print(f"\n{'='*60}")
        print("✓ SUCCESS: All aggregate tables populated WITHOUT Ho Marlboro")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"\n{'='*60}")
        print(f"❌ ERROR: Failed to insert aggregates: {e}")
        print(f"{'='*60}\n")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()