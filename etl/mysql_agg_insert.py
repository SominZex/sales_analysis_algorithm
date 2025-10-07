import pandas as pd
from datetime import datetime, timedelta
import time
import pymysql

# Column names in DB (all lowercase)
POSTGRES_COLUMNS_BRAND = ["brandname", "nooforders", "sales", "aov", "orderdate"]
POSTGRES_COLUMNS_STORE = ["storename", "nooforder", "sales", "aov", "orderdate"]
POSTGRES_COLUMNS_CATEGORY = ["subcategoryof", "sales", "orderdate"]
POSTGRES_COLUMNS_PRODUCT = ["productname", "nooforders", "sales", "quantitysold", "orderdate"]

def load_aggregates_to_mysql(df: pd.DataFrame):
    conn = None  # Initialize conn before try block
    cur = None   # Initialize cursor as well
    
    try:
        # Clean the data first
        df['totalProductPrice'] = pd.to_numeric(df['totalProductPrice'], errors='coerce')
        df = df[df['totalProductPrice'].notna()]
        
        print("Connecting to database...")
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            database="sales_data",
            user="root",
            password="root"
        )
        cur = conn.cursor()

        # -------- Brand Sales --------
        print("Processing brand sales data...")
        brand_df = df.groupby(['brandName', 'orderDate'], as_index=False).agg(
            nooforders=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum')
        )
        brand_df['sales'] = pd.to_numeric(brand_df['sales'], errors='coerce')
        brand_df['aov'] = (brand_df['sales'] / brand_df['nooforders']).round(2)
        brand_df.rename(columns={'brandName': 'brandname', 'orderDate': 'orderdate'}, inplace=True)

        # Use MySQL syntax (no quotes around column names, use backticks if needed)
        brand_cols = ",".join(POSTGRES_COLUMNS_BRAND)
        brand_tuples = [tuple(row) for row in brand_df[POSTGRES_COLUMNS_BRAND].values]
        
        # Use MySQL placeholders (%s)
        brand_placeholders = ",".join(["%s"] * len(POSTGRES_COLUMNS_BRAND))
        brand_sql = f"INSERT INTO brand_sales ({brand_cols}) VALUES ({brand_placeholders})"
        
        cur.executemany(brand_sql, brand_tuples)
        print(f"Inserted {len(brand_tuples)} rows into brand_sales")

        # -------- Store Sales --------
        print("Processing store sales data...")
        store_df = df.groupby(['storeName', 'orderDate'], as_index=False).agg(
            nooforder=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum')
        )
        store_df['sales'] = pd.to_numeric(store_df['sales'], errors='coerce')
        store_df['aov'] = (store_df['sales'] / store_df['nooforder']).round(2)
        store_df.rename(columns={'storeName': 'storename', 'orderDate': 'orderdate'}, inplace=True)

        store_cols = ",".join(POSTGRES_COLUMNS_STORE)
        store_tuples = [tuple(row) for row in store_df[POSTGRES_COLUMNS_STORE].values]
        
        store_placeholders = ",".join(["%s"] * len(POSTGRES_COLUMNS_STORE))
        store_sql = f"INSERT INTO store_sales ({store_cols}) VALUES ({store_placeholders})"
        
        cur.executemany(store_sql, store_tuples)
        print(f"Inserted {len(store_tuples)} rows into store_sales")

        # -------- Category Sales --------
        print("Processing category sales data...")
        category_df = df.groupby(['subCategoryOf', 'orderDate'], as_index=False).agg(
            sales=('totalProductPrice', 'sum')
        )
        category_df.rename(columns={'subCategoryOf': 'subcategoryof', 'orderDate': 'orderdate'}, inplace=True)

        category_cols = ",".join(POSTGRES_COLUMNS_CATEGORY)
        category_tuples = [tuple(row) for row in category_df[POSTGRES_COLUMNS_CATEGORY].values]
        
        category_placeholders = ",".join(["%s"] * len(POSTGRES_COLUMNS_CATEGORY))
        category_sql = f"INSERT INTO category_sales ({category_cols}) VALUES ({category_placeholders})"
        
        cur.executemany(category_sql, category_tuples)
        print(f"Inserted {len(category_tuples)} rows into category_sales")

        # -------- Product Sales --------
        print("Processing product sales data...")
        product_df = df.groupby(['productName', 'orderDate'], as_index=False).agg(
            nooforders=('invoice', 'nunique'),
            sales=('totalProductPrice', 'sum'),
            quantitysold=('quantity', 'sum')
        )
        product_df.rename(columns={'productName': 'productname', 'orderDate': 'orderdate'}, inplace=True)

        product_cols = ",".join(POSTGRES_COLUMNS_PRODUCT)
        product_tuples = [tuple(row) for row in product_df[POSTGRES_COLUMNS_PRODUCT].values]
        
        product_placeholders = ",".join(["%s"] * len(POSTGRES_COLUMNS_PRODUCT))
        product_sql = f"INSERT INTO product_sales ({product_cols}) VALUES ({product_placeholders})"
        
        cur.executemany(product_sql, product_tuples)
        print(f"Inserted {len(product_tuples)} rows into product_sales")

        # Commit all changes
        conn.commit()
        print("All aggregates inserted successfully")

    except Exception as e:
        print(f"Failed to insert aggregates: {e}")
        if conn:
            conn.rollback()
        raise  # Re-raise the exception for debugging
        
    finally:
        # Clean up resources
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Database connection closed")