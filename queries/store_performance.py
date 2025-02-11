import pandas as pd
from connector import get_db_connection
from queries.trend import get_trend_arrow

def fetch_sales_data(default_start_date, default_end_date):
    """Fetch store sales data from store_sales table based on selected date range."""
    if not default_start_date or not default_end_date:
        return pd.DataFrame()  # Return empty DataFrame if dates are missing

    engine = get_db_connection()

    query = """
    WITH last_day_sales AS (
        -- Fetch data for the last available date (default_end_date)
        SELECT 
            storeName,
            NoOfOrder AS orders_today,
            sales AS totalSales,
            AOV AS AOV_today
        FROM store_sales
        WHERE orderDate = %(default_end_date)s
    ),
    previous_7_days AS (
        -- Dynamically calculate the 7-day average before `default_start_date`
        SELECT 
            storeName,
            ROUND(AVG(NoOfOrder), 2) AS avg_orders_last_7_days,
            ROUND(AVG(sales), 2) AS avg_sales_last_7_days,
            ROUND(AVG(AOV), 2) AS avg_AOV_last_7_days
        FROM store_sales
        WHERE orderDate BETWEEN DATE(%(default_start_date)s - INTERVAL 7 DAY) AND DATE(%(default_start_date)s - INTERVAL 1 DAY)
        GROUP BY storeName
    )
    SELECT 
        COALESCE(l.storeName, p.storeName) AS storeName,
        COALESCE(l.orders_today, 0) AS orders_today,  -- Fixed to Last Day
        COALESCE(l.totalSales, 0) AS totalSales,      -- Fixed to Last Day
        COALESCE(l.AOV_today, 0) AS AOV_today,        -- Fixed to Last Day
        COALESCE(p.avg_orders_last_7_days, 0) AS avg_orders_last_7_days,
        COALESCE(p.avg_sales_last_7_days, 0) AS avg_sales_last_7_days,
        COALESCE(p.avg_AOV_last_7_days, 0) AS avg_AOV_last_7_days
    FROM last_day_sales l
    LEFT JOIN previous_7_days p ON l.storeName = p.storeName
    
    UNION
    
    SELECT 
        p.storeName,
        0 AS orders_today,
        0 AS totalSales,
        0 AS AOV_today,
        p.avg_orders_last_7_days,
        p.avg_sales_last_7_days,
        p.avg_AOV_last_7_days
    FROM previous_7_days p
    LEFT JOIN last_day_sales l ON p.storeName = l.storeName
    WHERE l.storeName IS NULL
    ORDER BY totalSales DESC;
    """

    # Execute query with parameters
    df = pd.read_sql(query, engine, params={'default_start_date': default_start_date, 'default_end_date': default_end_date})
    engine.dispose()

    if df.empty:
        # Return empty DataFrame with correct columns
        return pd.DataFrame(columns=["S.No", "Store Name", "Number of Orders", "Sales", "Average Order Value"])

    # Calculate trends using existing get_trend_arrow function
    df["ordersTrend"] = df.apply(lambda row: get_trend_arrow(row["orders_today"], row["avg_orders_last_7_days"]), axis=1)
    df["salesTrend"] = df.apply(lambda row: get_trend_arrow(row["totalSales"], row["avg_sales_last_7_days"]), axis=1)
    df["AOVTrend"] = df.apply(lambda row: get_trend_arrow(row["AOV_today"], row["avg_AOV_last_7_days"]), axis=1)

    # Format numbers with commas for thousands
    df["orders_today"] = df["orders_today"].apply(lambda x: f"{int(x):,}")
    df["totalSales"] = df["totalSales"].apply(lambda x: f"{float(x):,.2f}")
    df["AOV_today"] = df["AOV_today"].apply(lambda x: f"{float(x):,.2f}")

    # Format the display values with trend arrows
    df["Number of Orders"] = df["orders_today"] + " " + df["ordersTrend"]
    df["Sales"] = df["totalSales"] + " " + df["salesTrend"]
    df["Average Order Value"] = df["AOV_today"] + " " + df["AOVTrend"]

    # Add serial number column (1-based index)
    df.insert(0, "S.No", range(1, len(df) + 1))

    # Select only the required columns with proper names
    result_df = df[["S.No", "storeName", "Number of Orders", "Sales", "Average Order Value"]]
    result_df.columns = ["S.No", "Store Name", "Number of Orders", "Sales", "Average Order Value"]

    return result_df
