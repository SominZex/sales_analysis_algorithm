import mysql.connector
import pandas as pd
import psycopg2
import psycopg2.extras
import time


# --- MySQL connection ---
def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",
        user="user_name",
        password="root",
        database="db_name"
    )


# --- PostgreSQL connection ---
def get_pg_connection():
    return psycopg2.connect(
        host="server_ip_address",
        port="port_no",
        database="db_name",
        user="user_name",
        password="pw"
    )


def migrate_sales(start_date, end_date):
    start_time = time.time()

    # SQL query (exclude productMrp)
    query = f"""
        SELECT 
            invoice, storeInvoice, orderDate, time, productId, productName, barcode,
            quantity, sellingPrice, discountAmount, totalProductPrice, deliveryFee,
            HSNCode, GST, GSTAmount, CGSTRate, CGSTAmount, SGSTRate, SGSTAmount,
            acessAmount, cess, cessAmount, orderAmountTax, orderAmountNet, cashAmount,
            cardAmount, upiAmount, creditAmount, costPrice, description, brandName,
            categoryName, subCategoryOf, storeName, GSTIN, orderType, paymentMethod,
            customerName, customerNumber, orderFrom, orderStatus
        FROM sales_data
        WHERE orderDate BETWEEN '{start_date}' AND '{end_date}';
    """

    try:
        # --- Extract from MySQL ---
        mysql_conn = get_mysql_connection()
        print("⏳ Fetching data from MySQL...")
        df = pd.read_sql(query, mysql_conn)
        mysql_conn.close()
        print(f"✅ Retrieved {len(df)} rows from MySQL")

        # --- Transform ---
        if "time" in df.columns:
            df["time"] = df["time"].apply(
                lambda x: str(x).split()[-1] if pd.notnull(x) else None
            )

        if "orderDate" in df.columns:
            df["orderDate"] = pd.to_datetime(
                df["orderDate"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        # --- Load into PostgreSQL ---
        pg_conn = get_pg_connection()
        cur = pg_conn.cursor()

        columns = list(df.columns)
        cols_str = ",".join([f'"{col}"' for col in columns])

        data_tuples = [tuple(row) for row in df.values]

        insert_sql = f'INSERT INTO billing_data ({cols_str}) VALUES %s'

        print(f"⏳ Inserting {len(data_tuples)} rows into PostgreSQL...")
        psycopg2.extras.execute_values(
            cur, insert_sql, data_tuples, page_size=2000
        )
        pg_conn.commit()
        cur.close()
        pg_conn.close()

        end_time = time.time()
        print(f"✅ Migration completed in {end_time - start_time:.2f} seconds")
        print(f"Successfully migrated {len(df)} rows into billing_data")

    except Exception as e:
        print("❌ Error during migration:", e)


if __name__ == "__main__":
    migrate_sales("2024-12-01", "2024-12-31") # change the date range as needed
