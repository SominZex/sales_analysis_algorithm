import requests
import base64
import binascii
import re
import io
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timedelta
import time
import numpy as np
import mysql_agg_insert
import pymysql

POSTGRES_COLUMNS = [
    "invoice", "storeInvoice", "orderDate", "time", "productId", "productName", "barcode",
    "quantity","productMrp", "sellingPrice", "discountAmount", "totalProductPrice", "deliveryFee",
    "HSNCode", "GST", "GSTAmount", "CGSTRate", "CGSTAmount", "SGSTRate", "SGSTAmount",
    "acessAmount", "cess", "cessAmount", "orderAmountTax", "orderAmountNet", "cashAmount",
    "cardAmount", "upiAmount", "creditAmount", "costPrice", "description", "brandName",
    "categoryName", "subCategoryOf", "storeName", "GSTIN", "orderType", "paymentMethod",
    "customerName", "customerNumber", "orderFrom", "orderStatus"
]

INTEGER_COLUMNS = ["productId", "quantity"]
BIGINT_COLUMNS = ["barcode"]
NUMERIC_COLUMNS = ["GST", "CGSTRate", "SGSTRate", "acessAmount", "cess"]

class CSVDownloader:
    def __init__(self, base_url="https://api.thenewshop.in", username="api_username", password="api_pw"):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.token = None
        self.session = requests.Session()

    def authenticate(self, retries=3, delay=5):
        for attempt in range(retries):
            print("Authenticating...")
            login_url = f"{self.base_url}/login"
            payload = {"username": self.username, "password": self.password}
            try:
                response = self.session.post(login_url, json=payload, timeout=10)
            except requests.exceptions.RequestException as e:
                print("Request failed:", e)
                time.sleep(delay)
                continue

            if response.status_code in [200, 201]:
                data = response.json()
                self.token = data.get("token")
                if self.token:
                    print("Authentication successful!")
                    return True
                else:
                    print("Error: No token received")
            else:
                print(f"Authentication failed: {response.status_code}")
                print(f"Response: {response.text}")

            print(f"Retrying in {delay} seconds... ({attempt+1}/{retries})")
            time.sleep(delay)

        print("Failed to authenticate after retries")
        return False

    def download_yesterday_csv(self, order_type="online"):
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Downloading yesterday's data: {yesterday}")
        return self.download_csv(order_type, yesterday, yesterday)

    def download_csv(self, order_type="online", from_date=None, to_date=None):
        if not self.token:
            if not self.authenticate():
                return None

        csv_url = f"{self.base_url}/orders/orderReportCSV"
        params = {"orderType": order_type, "fromDate": from_date, "toDate": to_date}
        headers = {"accept": "*/*", "Authorization": self.token}

        print(f"Downloading CSV for {order_type} orders from {from_date} to {to_date}...")
        try:
            response = self.session.get(csv_url, params=params, headers=headers, timeout=30)
        except requests.exceptions.RequestException as e:
            print("Request failed:", e)
            return None

        if response.status_code != 200:
            print(f"Download failed: {response.status_code}")
            print(f"Response: {response.text}")
            return None

        content = response.content
        try:
            text_content = content.decode("utf-8")
        except UnicodeDecodeError:
            print("Binary content received, cannot decode as text")
            return None

        clean_content = re.sub(r"\s", "", text_content)
        try:
            decoded_bytes = base64.b64decode(clean_content, validate=True)
            decoded_text = decoded_bytes.decode("utf-8")
            csv_data = decoded_text
        except (binascii.Error, ValueError):
            csv_data = text_content

        df = pd.read_csv(io.StringIO(csv_data))
        print(f"Downloaded {len(df)} rows")
        return df

def debug_date_formats(df: pd.DataFrame):
    """Debug function to understand the date formats in the data"""
    if "orderDate" in df.columns:
        print("\n=== DEBUGGING ORDERDATE COLUMN ===")
        print(f"Column exists: {'orderDate' in df.columns}")
        print(f"Total rows: {len(df)}")
        print(f"Non-null values: {df['orderDate'].notna().sum()}")
        print(f"Unique date formats (first 10):")
        
        # Show sample values
        sample_dates = df['orderDate'].dropna().head(10).tolist()
        for i, date_val in enumerate(sample_dates, 1):
            print(f"  {i}. '{date_val}' (type: {type(date_val)})")
        
        # Check different possible formats
        test_formats = ["%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]
        for fmt in test_formats:
            try:
                parsed = pd.to_datetime(df['orderDate'].dropna().iloc[0], format=fmt)
                print(f"✓ Format '{fmt}' works - parsed as: {parsed}")
            except:
                print(f"✗ Format '{fmt}' failed")
        
        print("=== END DEBUG ===\n")

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Starting data transformation...")
    
    # Debug the orderDate column first
    debug_date_formats(df)
    
    # Enhanced orderDate processing with multiple format support
    if "orderDate" in df.columns:
        print("Processing orderDate column...")
        original_count = df['orderDate'].notna().sum()
        print(f"Original non-null orderDate values: {original_count}")
        
        # Try multiple date formats, prioritizing YYYY-MM-DD since database expects this format
        formats_to_try = ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]
        df['orderDate_parsed'] = None
        
        for fmt in formats_to_try:
            mask = df['orderDate_parsed'].isna() & df['orderDate'].notna()
            if mask.any():
                try:
                    parsed_dates = pd.to_datetime(df.loc[mask, 'orderDate'], format=fmt, errors='coerce')
                    successful_parses = parsed_dates.notna().sum()
                    if successful_parses > 0:
                        print(f"Format '{fmt}' successfully parsed {successful_parses} dates")
                        df.loc[mask & parsed_dates.notna(), 'orderDate_parsed'] = parsed_dates[parsed_dates.notna()]
                except Exception as e:
                    print(f"Format '{fmt}' failed: {e}")
        
        # If no format worked, try pandas auto-detection
        if df['orderDate_parsed'].isna().all():
            print("Trying pandas auto-detection...")
            df['orderDate_parsed'] = pd.to_datetime(df['orderDate'], errors='coerce', infer_datetime_format=True)
        
        # Convert to date and handle NaT
        df['orderDate'] = df['orderDate_parsed'].apply(lambda x: x.date() if pd.notnull(x) else None)
        df = df.drop(columns=['orderDate_parsed'])
        
        final_count = df['orderDate'].notna().sum()
        print(f"Final non-null orderDate values: {final_count}")
        print(f"Successfully converted: {final_count}/{original_count} dates")
        
        # Show sample of converted dates
        sample_converted = df[df['orderDate'].notna()]['orderDate'].head(5).tolist()
        print(f"Sample converted dates: {sample_converted}")

    # time -> HH:MM:SS string or None (take first 8 characters)
    if "time" in df.columns:
        print("Processing time column...")
        df["time"] = df["time"].astype(str).str[:8]  # Take first 8 characters (HH:MM:SS)
        df["time"] = df["time"].replace(["nan", "NaT"], None)
        print(f"Sample time values: {df[df['time'].notna()]['time'].head(3).tolist()}")

    # Process numeric columns more efficiently
    print("Processing numeric columns...")
    
    # Integers / bigints - Convert directly to Python int, NaN -> None
    for col in INTEGER_COLUMNS + BIGINT_COLUMNS:
        if col in df.columns:
            original_count = df[col].notna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Convert NaN to None explicitly for MySQL compatibility
            df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) and not pd.isna(x) else None)
            final_count = df[col].notna().sum()
            print(f"  {col}: {final_count}/{original_count} values converted")

    # Numeric columns - Convert to Python float, NaN -> None
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            original_count = df[col].notna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Convert NaN to None explicitly for MySQL compatibility
            df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) and not pd.isna(x) else None)
            final_count = df[col].notna().sum()
            print(f"  {col}: {final_count}/{original_count} values converted")

    # Process all other numeric columns that might contain NaN
    print("Processing all other numeric columns...")
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col not in INTEGER_COLUMNS + BIGINT_COLUMNS + NUMERIC_COLUMNS:
            # Convert any remaining NaN values to None
            df[col] = df[col].apply(lambda x: x if pd.notnull(x) and not pd.isna(x) else None)

    print("Processing string columns...")
    # All other columns -> string, NaN -> None (vectorized)
    string_cols = [col for col in df.columns 
                   if col in POSTGRES_COLUMNS 
                   and col not in INTEGER_COLUMNS + BIGINT_COLUMNS + NUMERIC_COLUMNS + ["orderDate", "time"]]
    
    for col in string_cols:
        # Handle NaN values more thoroughly
        df[col] = df[col].astype(str)
        df[col] = df[col].replace(["nan", "NaN", "NaT", "<NA>"], None)
        # Also handle actual NaN values
        df[col] = df[col].apply(lambda x: x if x != "nan" and pd.notnull(x) else None)

    # Keep only required columns and add missing ones
    existing_cols = [col for col in POSTGRES_COLUMNS if col in df.columns]
    df = df[existing_cols]
    
    # Add missing columns
    for col in POSTGRES_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[POSTGRES_COLUMNS]
    
    # FINAL CLEANUP: Replace any remaining NaN/inf values with None
    print("Final NaN cleanup...")
    df = df.replace([np.nan, np.inf, -np.inf], None)
    
    # Double-check for any remaining problematic values
    for col in df.columns:
        nan_count = df[col].isna().sum()
        if nan_count > 0:
            print(f"  {col}: {nan_count} null values")
    
    print("Data transformation completed.")
    
    # Final summary
    print(f"\nFINAL DATA SUMMARY:")
    print(f"Total rows: {len(df)}")
    print(f"orderDate not null: {df['orderDate'].notna().sum()}")
    print(f"time not null: {df['time'].notna().sum()}")
    
    return df

def load_to_postgres_bulk(df: pd.DataFrame):
    """Fixed bulk insert using PyMySQL (not psycopg2)"""
    conn = None
    cur = None
    
    try:
        print("Connecting to database...")
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            database="sales_data",
            user="root",
            password="root"
        )
        cur = conn.cursor()
        
        print("Preparing bulk insert...")
        
        # Create column names string (no quotes needed for MySQL)
        cols = ",".join(POSTGRES_COLUMNS)
        
        # Create placeholders for MySQL (%s)
        placeholders = ",".join(["%s"] * len(POSTGRES_COLUMNS))
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in df.values]
        
        # Use executemany for bulk insert (PyMySQL way)
        insert_sql = f'INSERT INTO sales_data ({cols}) VALUES ({placeholders})'
        
        print(f"Inserting {len(data_tuples)} rows in bulk...")
        
        cur.executemany(insert_sql, data_tuples)
        
        conn.commit()
        print(f"Successfully inserted {len(df)} rows into sales_data")
        
    except Exception as e:
        print(f"Failed to insert data: {e}")
        if conn:
            conn.rollback()
        raise  # Re-raise for debugging
        
    finally:

        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Database connection closed")

def main():
    start_time = time.time()
    
    downloader = CSVDownloader(username="nssomin", password="nssomin")
    df = downloader.download_yesterday_csv(order_type="online")
    
    if df is not None and not df.empty:
        download_time = time.time()
        print(f"Download completed in {download_time - start_time:.2f} seconds")
        
        df = transform_data(df)
        transform_time = time.time()
        print(f"Transform completed in {transform_time - download_time:.2f} seconds")
        
        # Bulk insert into sales_data
        load_to_postgres_bulk(df)
        billing_insert_time = time.time()
        print(f"Billing data load completed in {billing_insert_time - transform_time:.2f} seconds")
        
        # Insert aggregates into brand, store, category, product tables
        mysql_agg_insert.load_aggregates_to_mysql(df)
        aggregates_insert_time = time.time()
        print(f"Aggregate inserts completed in {aggregates_insert_time - billing_insert_time:.2f} seconds")
        
        print(f"Total ETL execution time: {aggregates_insert_time - start_time:.2f} seconds")
    else:
        print("No data downloaded")

if __name__ == "__main__":
    main()