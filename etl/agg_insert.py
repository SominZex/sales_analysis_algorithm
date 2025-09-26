import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import datetime, timedelta
import time

# Column names in DB (all lowercase)
POSTGRES_COLUMNS_BRAND = ["brandname", "nooforders", "sales", "aov", "orderdate"]
POSTGRES_COLUMNS_STORE = ["storename", "nooforder", "sales", "aov", "orderdate"]
POSTGRES_COLUMNS_CATEGORY = ["subcategoryof", "sales", "orderdate"]
POSTGRES_COLUMNS_PRODUCT = ["productname", "nooforders", "sales", "quantitysold", "orderdate"]

def load_aggregates_to_postgres(df: pd.DataFrame):
    try:
        df['totalProductPrice'] = pd.to_numeric(df['totalProductPrice'], errors='coerce')
        df = df[df['totalProductPrice'].notna()]
        print("Connecting to database...")
        conn = psycopg2.connect(
            host="server_ip",
            port="port",
            database="db_name",
            user="user_name",
            password="pw"
        )
        cur = conn.cursor()

        # -------- Brand Sales --------
        brand_df = df.groupby(['brandName', 'orderDate'], as_index=False).agg(
            nooforders=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum')
        )
        brand_df['sales'] = pd.to_numeric(brand_df['sales'], errors='coerce')
        brand_df['aov'] = (brand_df['sales'] / brand_df['nooforders']).round(2)
        brand_df.rename(columns={'brandName': 'brandname', 'orderDate': 'orderdate'}, inplace=True)

        brand_cols = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_BRAND])
        brand_tuples = [tuple(row) for row in brand_df[POSTGRES_COLUMNS_BRAND].values]

        psycopg2.extras.execute_values(
            cur,
            f'INSERT INTO brand_sales ({brand_cols}) VALUES %s',
            brand_tuples,
            template=None,
            page_size=1000
        )
        print(f"Inserted {len(brand_tuples)} rows into brand_sales")

        # -------- Store Sales --------
        store_df = df.groupby(['storeName', 'orderDate'], as_index=False).agg(
            nooforder=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum')
        )
        store_df['sales'] = pd.to_numeric(store_df['sales'], errors='coerce')
        store_df['aov'] = (store_df['sales'] / store_df['nooforder']).round(2)
        store_df.rename(columns={'storeName': 'storename', 'orderDate': 'orderdate'}, inplace=True)

        store_cols = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_STORE])
        store_tuples = [tuple(row) for row in store_df[POSTGRES_COLUMNS_STORE].values]

        psycopg2.extras.execute_values(
            cur,
            f'INSERT INTO store_sales ({store_cols}) VALUES %s',
            store_tuples,
            template=None,
            page_size=1000
        )
        print(f"Inserted {len(store_tuples)} rows into store_sales")

        # -------- Category Sales --------
        category_df = df.groupby(['subCategoryOf', 'orderDate'], as_index=False).agg(
            nooforder=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum')
        )
        category_df.rename(columns={'subCategoryOf': 'subcategoryof', 'orderDate': 'orderdate'}, inplace=True)

        category_cols = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_CATEGORY])
        category_tuples = [tuple(row) for row in category_df[POSTGRES_COLUMNS_CATEGORY].values]

        psycopg2.extras.execute_values(
            cur,
            f'INSERT INTO category_sales ({category_cols}) VALUES %s',
            category_tuples,
            template=None,
            page_size=1000
        )
        print(f"Inserted {len(category_tuples)} rows into category_sales")

        # -------- Product Sales --------
        product_df = df.groupby(['productName', 'orderDate'], as_index=False).agg(
            nooforders=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum'),
            quantitysold=('quantity', 'sum')
        )
        product_df.rename(columns={'productName': 'productname', 'orderDate': 'orderdate'}, inplace=True)

        product_cols = ",".join([f'"{c}"' for c in POSTGRES_COLUMNS_PRODUCT])
        product_tuples = [tuple(row) for row in product_df[POSTGRES_COLUMNS_PRODUCT].values]

        psycopg2.extras.execute_values(
            cur,
            f'INSERT INTO product_sales ({product_cols}) VALUES %s',
            product_tuples,
            template=None,
            page_size=1000
        )
        print(f"Inserted {len(product_tuples)} rows into product_sales")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Failed to insert aggregates: {e}")
        if conn:
            conn.rollback()
