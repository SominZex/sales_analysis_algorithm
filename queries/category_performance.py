import pandas as pd
from connector import get_db_connection
from queries.trend import get_trend_arrow

def fetch_subcategory_data(default_start_date, default_end_date):
    """Fetch subcategory sales data directly from category_sales table for the selected date range."""
    if not default_start_date or not default_end_date:
        return pd.DataFrame()  # Return empty DataFrame if dates are missing

    engine = get_db_connection()

    query = """
    WITH last_day_sales AS (
        -- Fetch total sales for the last available date (default_end_date)
        SELECT 
            subCategoryOf,
            SUM(Sales) AS sales_today
        FROM category_sales
        WHERE orderDate = %(default_end_date)s
        GROUP BY subCategoryOf
    ),
    previous_period AS (
        -- Calculate the average sales for the selected previous date range
        SELECT 
            subCategoryOf,
            ROUND(AVG(Sales), 2) AS avg_sales_previous_days
        FROM category_sales
        WHERE orderDate BETWEEN %(default_start_date)s AND DATE(%(default_end_date)s - INTERVAL 1 DAY)
        GROUP BY subCategoryOf
    )
    SELECT 
        COALESCE(l.subCategoryOf, p.subCategoryOf) AS subCategoryOf,
        COALESCE(l.sales_today, 0) AS sales_today,  -- Always from `default_end_date`
        COALESCE(p.avg_sales_previous_days, 0) AS avg_sales_previous_days
    FROM last_day_sales l
    LEFT JOIN previous_period p ON l.subCategoryOf = p.subCategoryOf
    
    UNION
    
    SELECT 
        p.subCategoryOf,
        0 AS sales_today,  -- Ensures missing categories still appear
        p.avg_sales_previous_days
    FROM previous_period p
    LEFT JOIN last_day_sales l ON p.subCategoryOf = l.subCategoryOf
    WHERE l.subCategoryOf IS NULL
    
    ORDER BY sales_today DESC;
    """

    df = pd.read_sql(query, engine, params={'default_start_date': default_start_date, 'default_end_date': default_end_date})
    engine.dispose()

    if df.empty:
        return pd.DataFrame()

    # Calculate trend comparison
    df["salesTrend"] = df.apply(lambda row: get_trend_arrow(row["sales_today"], row["avg_sales_previous_days"]), axis=1)
    df["sales_today"] = df["sales_today"].astype(str) + " " + df["salesTrend"]

    # Add Serial Number column
    df.insert(0, "S.No", df.index + 1)

    # Rename columns
    df = df[["S.No", "subCategoryOf", "sales_today"]]
    df.columns = ["S.No", "Subcategory", "Sales"]

    return df
