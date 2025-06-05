import pandas as pd
from connector import get_db_connection
from queries.trend import get_monthly_trend_arrow
from monthly_query.date_utils import CURRENT_MONTH, PREVIOUS_TWO_MONTHS


def fetch_subcategory_data_monthly():
    """Fetch subcategory sales data for February 2025 and compare to the average of the last two months (December 2024 and January 2025)."""
    engine = get_db_connection()

    query = """
    WITH monthly_sales AS (
        SELECT 
            subCategoryOf,
            SUM(totalProductPrice) AS total_sales
        FROM sales_data
        WHERE DATE_FORMAT(orderDate, '%%Y-%%m') = %(current_month)s
        GROUP BY subCategoryOf
    ),
    previous_two_months AS (
        SELECT 
            subCategoryOf,
            SUM(totalProductPrice) AS total_sales
        FROM sales_data
        WHERE DATE_FORMAT(orderDate, '%%Y-%%m') IN %(previous_two_months)s
        GROUP BY subCategoryOf
    )
    SELECT 
        COALESCE(m.subCategoryOf, p.subCategoryOf) AS subCategoryOf,
        COALESCE(m.total_sales, 0) AS total_sales,
        COALESCE(p.total_sales / 2, 0) AS avg_sales_previous_two_months
    FROM monthly_sales m
    LEFT JOIN previous_two_months p ON m.subCategoryOf = p.subCategoryOf
    
    UNION
    
    SELECT 
        p.subCategoryOf,
        0 AS total_sales,
        p.total_sales / 2 AS avg_sales_previous_two_months
    FROM previous_two_months p
    LEFT JOIN monthly_sales m ON p.subCategoryOf = m.subCategoryOf
    WHERE m.subCategoryOf IS NULL
    
    ORDER BY total_sales DESC;
    """ 

    df = pd.read_sql(query, engine, params={'current_month': CURRENT_MONTH, 'previous_two_months': tuple(PREVIOUS_TWO_MONTHS)})
    engine.dispose()

    if df.empty:
        return pd.DataFrame(columns=["S.No", "Subcategory", "Sales"])

    # Debugging: Print the total sales and average sales for comparison
    print("Total Sales and Average Sales for Comparison:")
    print(df[['subCategoryOf', 'total_sales', 'avg_sales_previous_two_months']])

    # Calculate trend comparison
    def calculate_growth_arrow(row):
        if row["avg_sales_previous_two_months"] == 0:
            return "→ (N/A)"  # No growth calculation possible
        change_percent = ((row["total_sales"] - row["avg_sales_previous_two_months"]) / row["avg_sales_previous_two_months"]) * 100
        arrow = "🡅" if change_percent > 0 else "🡇" if change_percent < 0 else "→"
        return f"{arrow} ({change_percent:.1f}%)"

    df["salesTrend"] = df.apply(calculate_growth_arrow, axis=1)
    df["total_sales"] = df["total_sales"].round(2).astype(str) + " " + df["salesTrend"]

    # Add Serial Number column
    df.insert(0, "S.No", df.index + 1)

    # Rename columns
    df = df[["S.No", "subCategoryOf", "total_sales"]]
    df.columns = ["S.No", "Subcategory", "Sales"]

    return df