import mysql.connector
import pandas as pd
from datetime import datetime

# --- Database Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'sales_data'
}

# --- Define Date Range (modify if needed) ---
start_date = '2024-12-01'
end_date = '2025-05-31'

# --- SQL Query ---
query = f"""
    SELECT 
        t.tiers,
        sd.storeName,
        sd.brandName,
        sd.productName,
        DATE_FORMAT(sd.orderDate, '%m-%Y') AS sales_month,
        SUM(CAST(sd.totalProductPrice AS DECIMAL(10,2))) AS monthly_sales
    FROM sales_data sd
    JOIN tiers t ON sd.storeName = t.storeName
    WHERE sd.orderDate BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY t.tiers, sd.storeName, sd.brandName, sd.productName, sales_month
    ORDER BY t.tiers, sd.storeName, sd.brandName, sd.productName, sales_month;
"""

# --- Export to Excel ---
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    df = pd.read_sql(query, conn)

    output_file = "tiers_store_brand_product_monthly_sales.xlsx"
    df.to_excel(output_file, index=False)

    print(f"✅ Exported to {output_file}")

except mysql.connector.Error as err:
    print(f"❌ MySQL Error: {err}")

finally:
    if conn.is_connected():
        conn.close()
