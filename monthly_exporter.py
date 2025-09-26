import pandas as pd
import pymysql
from sqlalchemy import create_engine
import math

# ─── Config local MySQL
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "sales_data",
    "port": 3306
}

CHUNK_SIZE = 1_000_000 
OUTPUT_FILE = "july_2025_sales.xlsx"

# ─── SQL Connection ────────────────────────────────────────────────────────
def get_connection():
    url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(url)

# ─── Query and Export ──────────────────────────────────────────────────────
def export_july_sales_to_excel():
    engine = get_connection()

    # Step 1: Query July 2025 sales
    query = """
        SELECT *
        FROM sales_data
        WHERE orderDate between "2025-07-01" and "2025-07-31"
    """

    print("📦 Querying July 2025 sales data from MySQL...")
    df = pd.read_sql(query, con=engine)
    print(f"✅ Retrieved {len(df)} rows.")

    if df.empty:
        print("⚠️ No data found for July 2025.")
        return

    # Step 2: Split and write to Excel
    total_rows = len(df)
    num_sheets = math.ceil(total_rows / CHUNK_SIZE)

    print(f"📁 Writing to Excel file ({num_sheets} sheets)...")

    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        for i in range(num_sheets):
            start = i * CHUNK_SIZE
            end = (i + 1) * CHUNK_SIZE
            sheet_df = df.iloc[start:end]
            sheet_name = f"Sheet_{i+1}"
            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"   ✅ Sheet {i+1} → Rows {start} to {end-1}")

    print(f"\n✅ Export complete: {OUTPUT_FILE}")

# ─── Run ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    export_july_sales_to_excel()
