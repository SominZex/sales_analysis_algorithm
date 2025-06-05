import pandas as pd
from connector import get_db_connection
from queries.trend import get_monthly_trend_arrow  
from monthly_query.date_utils import CURRENT_MONTH, PREVIOUS_TWO_MONTHS

def brand_sales():
    """Fetch brand sales data for February 2025 and compare to average sales of previous two months."""
    engine = get_db_connection()

    last_month = "2025-05"

    # Fetch total sales for February 2025
    query_sales = """
        SELECT brandName, SUM(totalProductPrice) AS total_sales 
        FROM sales_data 
        WHERE DATE_FORMAT(orderDate, '%%Y-%%m') = %(current_month)s
        GROUP BY brandName;
    """
    sales_df = pd.read_sql(query_sales, engine, params={'current_month': CURRENT_MONTH})


    # Fetch total sales for the previous two months (December 2024, January 2025)
    query_sales_previous_months = """
        SELECT brandName, SUM(totalProductPrice) AS total_sales 
        FROM sales_data 
        WHERE DATE_FORMAT(orderDate, '%%Y-%%m') IN %(previous_two_months)s
        GROUP BY brandName;
    """
    sales_previous_months_df = pd.read_sql(query_sales_previous_months, engine, params={'previous_two_months': tuple(PREVIOUS_TWO_MONTHS)})
    # Calculate average sales for the previous two months by dividing by 2
    avg_sales_previous_months = sales_previous_months_df.groupby('brandName')['total_sales'].sum().reset_index()
    avg_sales_previous_months['avg_monthly_sales'] = avg_sales_previous_months['total_sales'] / 2
    avg_sales_previous_months.drop(columns=['total_sales'], inplace=True)

    engine.dispose()

    # Merge current and previous sales data
    sales_df = sales_df.merge(avg_sales_previous_months, on="brandName", how="left").fillna(0)

    # Calculate growth trend using the specified formula
    def calculate_growth_arrow(row):
        if row["avg_monthly_sales"] == 0:
            return "→ (N/A)"  # No growth calculation possible
        change_percent = ((row["total_sales"] - row["avg_monthly_sales"]) / row["avg_monthly_sales"]) * 100
        arrow = "🡅" if change_percent > 0 else "🡇" if change_percent < 0 else "→"
        return f"{arrow} ({change_percent:.1f}%)"

    sales_df["growth_arrow"] = sales_df.apply(calculate_growth_arrow, axis=1)
    
    # Sort by total sales in descending order and keep the top 100 brands
    sales_df = sales_df.sort_values(by="total_sales", ascending=False).head(100)

    # Prepare the final DataFrame with serial numbers
    sales_df.insert(0, "S.No", range(1, len(sales_df) + 1))
    sales_df["Sales & Trend"] = sales_df.apply(lambda row: f"{row['total_sales']:,.2f} {row['growth_arrow']}", axis=1)
    result_df = sales_df[["S.No", "brandName", "Sales & Trend"]].rename(columns={"brandName": "Brand"})


    return result_df