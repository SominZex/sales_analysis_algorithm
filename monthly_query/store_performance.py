import pandas as pd
from connector import get_db_connection
from queries.trend import get_monthly_trend_arrow  
from monthly_query.date_utils import CURRENT_MONTH, PREVIOUS_TWO_MONTHS


def fetch_monthly_sales():
    """Fetch total sales for February 2025 and calculate average monthly growth percentage from the previous two months (December 2024 and January 2025)."""
    engine = get_db_connection()

    query_sales_current_month = """
        SELECT 
            COUNT(DISTINCT invoice) AS unique_invoices,
            SUM(totalProductPrice) AS total_sales
        FROM sales_data
        WHERE DATE_FORMAT(orderDate, '%%Y-%%m') = %(current_month)s;
    """
    sales_current_month_df = pd.read_sql(query_sales_current_month, engine, params={'current_month': CURRENT_MONTH})

    total_unique_invoices = sales_current_month_df['unique_invoices'].iloc[0]
    total_monthly_sales = sales_current_month_df['total_sales'].iloc[0]

    # Fetch total sales for the current month
    query_sales_february = """
        SELECT storeName, SUM(totalProductPrice) AS total_sales 
        FROM sales_data 
        WHERE DATE_FORMAT(orderDate, '%%Y-%%m') = %(current_month)s
        GROUP BY storeName;
    """
    sales_february_df = pd.read_sql(query_sales_february, engine, params={'current_month': CURRENT_MONTH})

    # Fetch total sales for the previous two months
    query_sales_previous_months = """
        SELECT storeName, SUM(totalProductPrice) AS total_sales 
        FROM sales_data 
        WHERE DATE_FORMAT(orderDate, '%%Y-%%m') IN %(previous_two_months)s
        GROUP BY storeName;
    """
    sales_previous_months_df = pd.read_sql(query_sales_previous_months, engine, params={'previous_two_months': tuple(PREVIOUS_TWO_MONTHS)})

    avg_sales_previous_months = sales_previous_months_df.groupby('storeName')['total_sales'].sum().reset_index()
    avg_sales_previous_months['avg_monthly_sales'] = avg_sales_previous_months['total_sales'] / 2
    avg_sales_previous_months.drop(columns=['total_sales'], inplace=True)


    engine.dispose()

    # Merge with previous sales data to avoid looping
    sales_february_df = sales_february_df.merge(avg_sales_previous_months, on="storeName", how="left").fillna(0)

    # Calculate growth trend
    sales_february_df["growth_arrow"] = sales_february_df.apply(
        lambda row: get_monthly_trend_arrow(row["total_sales"], row["avg_monthly_sales"]), axis=1
    )

    # Sort by total sales in descending order
    sales_february_df = sales_february_df.sort_values(by="total_sales", ascending=False)

    # Prepare final DataFrame
    sales_february_df.insert(0, "S.No", range(1, len(sales_february_df) + 1))
    sales_february_df["Sales & Trend"] = sales_february_df.apply(lambda row: f"{row['total_sales']:,.2f} {row['growth_arrow']}", axis=1)
    result_df = sales_february_df[["S.No", "storeName", "Sales & Trend"]].rename(columns={"storeName": "Store"})

    return result_df, total_unique_invoices, total_monthly_sales