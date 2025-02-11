import pandas as pd
from connector import get_db_connection
from queries.trend import get_trend_arrow

def fetch_brand_data(default_start_date, default_end_date):
    """Fetch brand sales data directly from brand_sales table."""
    if not default_start_date or not default_end_date:
        return pd.DataFrame()  # Return empty DataFrame if dates are missing

    engine = get_db_connection()

    query = """
    WITH last_day_sales AS (
        -- Fetch total sales for the last available date (default_end_date)
        SELECT 
            brandName,
            NoOfOrders AS orders_today,
            Sales AS sales_today,
            AOV AS AOV_today
        FROM brand_sales
        WHERE orderDate = %(default_end_date)s
    ),
    previous_period AS (
        -- Calculate the average sales for the selected previous date range
        SELECT 
            brandName,
            ROUND(AVG(NoOfOrders), 2) AS avg_orders_previous_days,
            ROUND(AVG(Sales), 2) AS avg_sales_previous_days,
            ROUND(AVG(AOV), 2) AS avg_AOV_previous_days
        FROM brand_sales
        WHERE orderDate BETWEEN %(default_start_date)s AND DATE(%(default_end_date)s - INTERVAL 1 DAY)
        GROUP BY brandName
    )
    SELECT 
        COALESCE(l.brandName, p.brandName) AS brandName,
        COALESCE(l.orders_today, 0) AS orders_today,  -- Fixed to Last Day
        COALESCE(l.sales_today, 0) AS sales_today,    -- Fixed to Last Day
        COALESCE(l.AOV_today, 0) AS AOV_today,        -- Fixed to Last Day
        COALESCE(p.avg_orders_previous_days, 0) AS avg_orders_previous_days,
        COALESCE(p.avg_sales_previous_days, 0) AS avg_sales_previous_days,
        COALESCE(p.avg_AOV_previous_days, 0) AS avg_AOV_previous_days
    FROM last_day_sales l
    LEFT JOIN previous_period p ON l.brandName = p.brandName
    
    UNION
    
    SELECT 
        p.brandName,
        0 AS orders_today,
        0 AS sales_today,
        0 AS AOV_today,
        p.avg_orders_previous_days,
        p.avg_sales_previous_days,
        p.avg_AOV_previous_days
    FROM previous_period p
    LEFT JOIN last_day_sales l ON p.brandName = l.brandName
    WHERE l.brandName IS NULL
    
    ORDER BY sales_today DESC
    LIMIT 100;  -- Ensure only the top 100 brands are displayed
    """

    df = pd.read_sql(query, engine, params={'default_start_date': default_start_date, 'default_end_date': default_end_date})
    engine.dispose()

    if df.empty:
        return pd.DataFrame()

    # Calculate trend comparison
    df["ordersTrend"] = df.apply(lambda row: get_trend_arrow(row["orders_today"], row["avg_orders_previous_days"]), axis=1)
    df["salesTrend"] = df.apply(lambda row: get_trend_arrow(row["sales_today"], row["avg_sales_previous_days"]), axis=1)
    df["AOVTrend"] = df.apply(lambda row: get_trend_arrow(row["AOV_today"], row["avg_AOV_previous_days"]), axis=1)

    # Format the display values with trend arrows
    df["orders_today"] = df["orders_today"].astype(str) + " " + df["ordersTrend"]
    df["sales_today"] = df["sales_today"].astype(str) + " " + df["salesTrend"]
    df["AOV_today"] = df["AOV_today"].astype(str) + " " + df["AOVTrend"]

    # Add Serial Number Column Before Brand Name
    df.insert(0, "S.No", range(1, len(df) + 1))

    df = df[["S.No", "brandName", "orders_today", "sales_today", "AOV_today"]]
    df.columns = ["S.No", "Brand Name", "Number of Orders", "Sales", "Average Order Value"]

    return df
