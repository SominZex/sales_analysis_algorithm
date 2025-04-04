import pandas as pd
from connector import get_db_connection
from queries.trend import get_trend_arrow

def fetch_subcategory_data(default_start_date=None, default_end_date=None):
    """Fetch subcategory sales data dynamically from category_sales table."""
    engine = get_db_connection()

    # Get the latest available orderDate dynamically
    last_date_query = "SELECT MAX(orderDate) FROM category_sales;"
    last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]

    if last_date is None:
        return pd.DataFrame(columns=["S.No", "Subcategory", "Sales"])

    # Set default_end_date to the latest available orderDate
    default_end_date = last_date if default_end_date is None else default_end_date
    default_start_date = pd.to_datetime(default_end_date) - pd.DateOffset(days=7) if default_start_date is None else default_start_date

    query = """
    WITH last_day_sales AS (
        SELECT 
            subCategoryOf,
            SUM(Sales) AS sales_today
        FROM category_sales
        WHERE orderDate = %(default_end_date)s
        GROUP BY subCategoryOf
    ),
    previous_period AS (
        SELECT 
            subCategoryOf,
            ROUND(AVG(Sales), 2) AS avg_sales_previous_days
        FROM category_sales
        WHERE orderDate BETWEEN DATE_SUB(%(default_end_date)s, INTERVAL 7 DAY) AND DATE_SUB(%(default_end_date)s, INTERVAL 1 DAY)
        GROUP BY subCategoryOf
    )
    SELECT 
        COALESCE(l.subCategoryOf, p.subCategoryOf) AS subCategoryOf,
        COALESCE(l.sales_today, 0) AS sales_today,
        COALESCE(p.avg_sales_previous_days, 0) AS avg_sales_previous_days
    FROM last_day_sales l
    LEFT JOIN previous_period p ON l.subCategoryOf = p.subCategoryOf
    
    UNION
    
    SELECT 
        p.subCategoryOf,
        0 AS sales_today,
        p.avg_sales_previous_days
    FROM previous_period p
    LEFT JOIN last_day_sales l ON p.subCategoryOf = l.subCategoryOf
    WHERE l.subCategoryOf IS NULL
    
    ORDER BY sales_today DESC;
    """

    df = pd.read_sql(query, engine, params={'default_end_date': default_end_date})
    engine.dispose()

    if df.empty:
        return pd.DataFrame(columns=["S.No", "Subcategory", "Sales"])

    # Calculate trend comparison
    df["salesTrend"] = df.apply(lambda row: get_trend_arrow(row["sales_today"], row["avg_sales_previous_days"]), axis=1)
    df["sales_today"] = df["sales_today"].astype(str) + " " + df["salesTrend"]

    # Add Serial Number column
    df.insert(0, "S.No", df.index + 1)

    # Rename columns
    df = df[["S.No", "subCategoryOf", "sales_today"]]
    df.columns = ["S.No", "Subcategory", "Sales"]

    return df