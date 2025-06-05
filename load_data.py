import pandas as pd
import numpy as np
from decimal import Decimal
from mysql.connector import Error as MySQLError
from connector import get_db_connection

csv_file_path = "./datasets/dec_sales.csv"

# Define expected columns
expected_columns = [
    "invoice", "storeInvoice", "orderDate", "time", "productId", "productName", "barcode", "quantity",
    "sellingPrice", "discountAmount", "totalProductPrice", "deliveryFee", "HSNCode", "GST", "GSTAmount",
    "CGSTRate", "CGSTAmount", "SGSTRate", "SGSTAmount", "acessAmount", "cess", "cessAmount",
    "orderAmountTax", "orderAmountNet", "costPrice", "description", "brandName", "categoryName",
    "subCategoryOf", "storeName", "GSTIN", "orderType", "paymentMethod", "commision", "customerName",
    "customerNumber", "orderFrom", "orderStatus", "cashAmount", "cardAmount", "upiAmount", "creditAmount"
]

dtype_mapping = {col: str for col in expected_columns}
dtype_mapping.update({
    "productId": pd.Int64Dtype(),
    "quantity": pd.Int64Dtype(),
    "sellingPrice": float,
    "discountAmount": float,
    "totalProductPrice": float,
    "deliveryFee": float,
    "GST": float,
    "GSTAmount": float,
    "CGSTRate": float,
    "CGSTAmount": float,
    "SGSTRate": float,
    "SGSTAmount": float,
    "acessAmount": float,
    "cess": float,
    "cessAmount": float,
    "orderAmountTax": float,
    "orderAmountNet": float,
    "cashAmount": float,
    "cardAmount": float,
    "upiAmount": float,
    "creditAmount": float,
    "costPrice": float,
    "commision": float
})

default_values = {col: "NA" for col in expected_columns}
default_values.update({
    "orderDate": '1900-01-01',
    "time": '00:00:00',
    "productId": 0,
    "quantity": 0,
    "sellingPrice": Decimal('0.00'),
    "discountAmount": Decimal('0.00'),
    "totalProductPrice": Decimal('0.00'),
    "deliveryFee": Decimal('0.00'),
    "GST": Decimal('0.00'),
    "GSTAmount": Decimal('0.00'),
    "CGSTRate": Decimal('0.00'),
    "CGSTAmount": Decimal('0.00'),
    "SGSTRate": Decimal('0.00'),
    "SGSTAmount": Decimal('0.00'),
    "acessAmount": Decimal('0.00'),
    "cess": Decimal('0.00'),
    "cessAmount": Decimal('0.00'),
    "orderAmountTax": Decimal('0.00'),
    "orderAmountNet": Decimal('0.00'),
    "cashAmount": Decimal('0.00'),
    "cardAmount": Decimal('0.00'),
    "upiAmount": Decimal('0.00'),
    "creditAmount": Decimal('0.00'),
    "costPrice": Decimal('0.00'),
    "commision": Decimal('0.00')
})

# Read CSV file safely
df = pd.read_csv(csv_file_path, dtype=dtype_mapping, low_memory=False)

# Add missing columns with default "NA"
for col in expected_columns:
    if col not in df.columns:
        df[col] = default_values[col]

# Convert orderDate and time
df['orderDate'] = pd.to_datetime(df['orderDate'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S', errors='coerce').dt.strftime('%H:%M:%S')

# Fill missing values
for column in expected_columns:
    df[column] = df[column].fillna(default_values[column])

# Ensure correct column order
df = df[expected_columns]

def convert_row_types(row):
    """Convert row data types to match MySQL schema."""
    converted = []
    for value, column in zip(row, expected_columns):
        if pd.isna(value) or value == "NA":
            converted.append(default_values[column])
        elif isinstance(value, (int, np.int64)):
            converted.append(int(value))
        elif isinstance(value, float):
            converted.append(Decimal(str(value)).quantize(Decimal('0.00')))
        elif isinstance(value, str):
            converted.append(value.strip())
        else:
            converted.append(value)
    return tuple(converted)

def validate_row(row):
    """Ensure row has correct length for MySQL insert."""
    return len(row) == len(expected_columns)

# Convert data
data_to_insert = [convert_row_types(row) for row in df.itertuples(index=False, name=None) if validate_row(row)]

insert_query = """
INSERT INTO sales_data (
    invoice, storeInvoice, orderDate, time, productId, productName, barcode, quantity, 
    sellingPrice, discountAmount, totalProductPrice, deliveryFee, HSNCode, GST, GSTAmount, 
    CGSTRate, CGSTAmount, SGSTRate, SGSTAmount, acessAmount, cess, cessAmount, 
    orderAmountTax, orderAmountNet, costPrice, description, brandName, categoryName, 
    subCategoryOf, storeName, GSTIN, orderType, paymentMethod, commision, customerName, 
    customerNumber, orderFrom, orderStatus, cashAmount, cardAmount, upiAmount, creditAmount
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
          %s, %s, %s)
"""

# Debugging
print(f"SQL Placeholders: {insert_query.count('%s')}")
print(f"First row length: {len(data_to_insert[0])}")
print(f"Sample row: {data_to_insert[0]}")
print(f"Total rows to insert: {len(data_to_insert)}")

# Insert into MySQL
conn = get_db_connection()
cursor = conn.cursor()
batch_size = 500
rows_inserted = 0

try:
    for i in range(0, len(data_to_insert), batch_size):
        batch = data_to_insert[i:i+batch_size]
        try:
            cursor.executemany(insert_query, batch)
            conn.commit()
            rows_inserted += len(batch)
            print(f"Inserted {rows_inserted} rows out of {len(data_to_insert)}...")
        except MySQLError as err:
            print(f"❌ Error inserting batch at row {i}: {err}")
            conn.rollback()
except Exception as e:
    print(f"❌ Data loading failed! Error: {e}")
else:
    print(f"✅ Data loaded successfully! {rows_inserted} rows inserted.")
finally:
    cursor.close()
    conn.close()
