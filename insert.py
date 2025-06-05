import pandas as pd
from sqlalchemy import create_engine, text

# Create your DB connection (edit credentials if needed)
engine = create_engine("mysql+pymysql://root:root@localhost/sales_data")

with engine.connect() as conn:
    result = conn.execute(text("SELECT DISTINCT orderDate FROM sales_data WHERE orderDate IS NOT NULL"))
    order_dates = [row[0] for row in result]

queries = {
    "brand_sales": """
        INSERT INTO brand_sales (brandname, nooforders, sales, aov, orderdate)
        SELECT 
            brandname,  
            COUNT(DISTINCT invoice), 
            SUM(totalProductPrice), 
            ROUND(SUM(totalProductPrice) / NULLIF(COUNT(DISTINCT invoice), 0), 2),
            orderDate
        FROM sales_data
        WHERE orderDate = :date
        GROUP BY brandname, orderDate
    """,
    "category_sales": """
        INSERT INTO category_sales (subcategoryof, sales, orderdate)
        SELECT 
            subcategoryof, 
            SUM(totalProductPrice),
            orderDate
        FROM sales_data
        WHERE orderDate = :date
        GROUP BY subcategoryof, orderDate
    """,
    "product_sales": """
        INSERT INTO product_sales (productname, nooforders, sales, quantitysold, orderdate)
        SELECT 
            productname,  
            COUNT(DISTINCT invoice), 
            SUM(totalProductPrice), 
            SUM(quantity),
            orderDate
        FROM sales_data
        WHERE orderDate = :date
        GROUP BY productname, orderDate
    """,
    "store_sales": """
        INSERT INTO store_sales (storename, nooforder, sales, aov, orderdate)
        SELECT 
            storename, 
            COUNT(DISTINCT invoice),
            SUM(totalProductPrice), 
            ROUND(SUM(totalProductPrice) / NULLIF(COUNT(DISTINCT invoice), 0), 2), 
            orderDate
        FROM sales_data
        WHERE orderDate = :date
        GROUP BY storename, orderDate
    """
}

# Step 3: Loop over each date and run the insert queries
with engine.begin() as conn:
    for date in order_dates:
        for name, query in queries.items():
            print(f"Inserting data into {name} for date {date}")
            conn.execute(text(query), {"date": date})
