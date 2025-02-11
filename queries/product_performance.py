import pandas as pd
from connector import get_db_connection
from queries.trend import get_trend_arrow

def fetch_product_data(default_start_date, default_end_date):
    """Fetch product sales data directly from product_sales table, limited to top 100 products by sales."""
    if not default_start_date or not default_end_date:
        return pd.DataFrame()  # Return empty DataFrame if dates are missing

    engine = get_db_connection()

    query = """
    WITH last_day_sales AS (
        -- Fetch total sales for the last available date (default_end_date)
        SELECT 
            productName,
            Sales AS sales_today,
            QuantitySold AS quantity_today
        FROM product_sales
        WHERE orderDate = %(default_end_date)s
    ),
    previous_period AS (
        -- Calculate the average sales for the selected previous date range
        SELECT 
            productName,
            ROUND(AVG(Sales), 2) AS avg_sales_previous_days,
            ROUND(AVG(QuantitySold), 2) AS avg_quantity_previous_days
        FROM product_sales
        WHERE orderDate BETWEEN %(default_start_date)s AND DATE(%(default_end_date)s - INTERVAL 1 DAY)
        GROUP BY productName
    )
    SELECT 
        COALESCE(l.productName, p.productName) AS productName,
        COALESCE(l.sales_today, 0) AS sales_today,     -- Fixed to Last Day
        COALESCE(l.quantity_today, 0) AS quantity_today, -- Fixed to Last Day
        COALESCE(p.avg_sales_previous_days, 0) AS avg_sales_previous_days,
        COALESCE(p.avg_quantity_previous_days, 0) AS avg_quantity_previous_days
    FROM last_day_sales l
    LEFT JOIN previous_period p ON l.productName = p.productName
    
    UNION
    
    SELECT 
        p.productName,
        0 AS sales_today,
        0 AS quantity_today,
        p.avg_sales_previous_days,
        p.avg_quantity_previous_days
    FROM previous_period p
    LEFT JOIN last_day_sales l ON p.productName = l.productName
    WHERE l.productName IS NULL
    
    ORDER BY sales_today DESC
    LIMIT 100;
    """

    df = pd.read_sql(query, engine, params={'default_start_date': default_start_date, 'default_end_date': default_end_date})
    engine.dispose()

    if df.empty:
        return pd.DataFrame()

    # Calculate trend comparison
    df["salesTrend"] = df.apply(lambda row: get_trend_arrow(row["sales_today"], row["avg_sales_previous_days"]), axis=1)
    df["quantityTrend"] = df.apply(lambda row: get_trend_arrow(row["quantity_today"], row["avg_quantity_previous_days"]), axis=1)

    # Format display values with trends
    df["sales_display"] = df["sales_today"].astype(str) + " " + df["salesTrend"]
    df["quantity_display"] = df["quantity_today"].astype(str) + " " + df["quantityTrend"]

    # Add Serial Number column (1 to N)
    df.insert(0, "S.No", range(1, len(df) + 1))

    # Select and rename columns for display (Removed "Number of Orders")
    df = df[["S.No", "productName", "sales_display", "quantity_display"]]
    df.columns = ["S.No", "Product Name", "Sales", "Quantity Sold"]

    return df
