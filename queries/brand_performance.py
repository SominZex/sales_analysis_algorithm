import pandas as pd
from connector import get_db_connection
from queries.trend import get_trend_arrow

def fetch_brand_data(default_start_date=None, default_end_date=None):
    """Fetch brand sales data dynamically from brand_sales table."""
    engine = get_db_connection()

    # Get the latest available orderDate dynamically
    last_date_query = "SELECT MAX(orderDate) FROM brand_sales;"
    last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]

    if last_date is None:
        return pd.DataFrame(columns=["S.No", "Brand Name", "Number of Orders", "Sales", "Average Order Value"])

    # Set default_end_date to the latest available orderDate
    default_end_date = last_date if default_end_date is None else default_end_date
    default_start_date = pd.to_datetime(default_end_date) - pd.DateOffset(days=7) if default_start_date is None else default_start_date

    query = """
    WITH last_day_sales AS (
        SELECT 
            brandName,
            NoOfOrders AS orders_today,
            Sales AS sales_today,
            AOV AS AOV_today
        FROM brand_sales
        WHERE orderDate = %(default_end_date)s
    ),
    previous_period AS (
        SELECT 
            brandName,
            ROUND(AVG(NoOfOrders), 2) AS avg_orders_previous_days,
            ROUND(AVG(Sales), 2) AS avg_sales_previous_days,
            ROUND(AVG(AOV), 2) AS avg_AOV_previous_days
        FROM brand_sales
        WHERE orderDate BETWEEN DATE_SUB(%(default_end_date)s, INTERVAL 7 DAY) AND DATE_SUB(%(default_end_date)s, INTERVAL 1 DAY)
        GROUP BY brandName
    )
    SELECT 
        COALESCE(l.brandName, p.brandName) AS brandName,
        COALESCE(l.orders_today, 0) AS orders_today,
        COALESCE(l.sales_today, 0) AS sales_today,
        COALESCE(l.AOV_today, 0) AS AOV_today,
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
    LIMIT 50;
    """

    df = pd.read_sql(query, engine, params={'default_end_date': default_end_date})
    engine.dispose()

    if df.empty:
        return pd.DataFrame(columns=["S.No", "Brand Name", "Number of Orders", "Sales", "Average Order Value"])

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
