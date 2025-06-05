import pandas as pd
from connector import get_db_connection
from queries.trend import get_monthly_trend_arrow
from monthly_query.date_utils import CURRENT_MONTH, PREVIOUS_TWO_MONTHS

def fetch_product_data_monthly():
    """Fetch product sales data for February 2025 and compare to the average of the last two months (December 2024 and January 2025)."""
    engine = get_db_connection()

    # Fetch total sales and quantity for the current month
    query_current_month = """
        SELECT productName, SUM(totalProductPrice) AS total_sales, SUM(quantity) AS total_quantity
        FROM sales_data
        WHERE DATE_FORMAT(orderDate, '%%Y-%%m') = %(current_month)s
        GROUP BY productName;
    """
    current_month_df = pd.read_sql(query_current_month, engine, params={'current_month': CURRENT_MONTH})

    # Fetch total sales and quantity for the previous two months
    query_previous_months = """
        SELECT productName, SUM(totalProductPrice) AS total_sales, SUM(quantity) AS total_quantity
        FROM sales_data
        WHERE DATE_FORMAT(orderDate, '%%Y-%%m') IN %(previous_two_months)s
        GROUP BY productName;
    """
    previous_months_df = pd.read_sql(query_previous_months, engine, params={'previous_two_months': tuple(PREVIOUS_TWO_MONTHS)})

    # Calculate average sales and quantity for the previous two months
    avg_previous_months = previous_months_df.groupby('productName').agg(
        avg_sales_previous_two_months=('total_sales', 'sum'),
        avg_quantity_previous_two_months=('total_quantity', 'sum')
    ).reset_index()
    avg_previous_months['avg_sales_previous_two_months'] /= 2

    engine.dispose()

    # Merge current and previous sales data
    df = current_month_df.merge(avg_previous_months, on="productName", how="left").fillna(0)

    # Calculate sales and quantity trends
    def calculate_growth_arrow(row):
        if row["avg_sales_previous_two_months"] == 0:
            return "→ (0%)"  # Display 0% when no growth calculation is possible
        change_percent = ((row["total_sales"] - row["avg_sales_previous_two_months"]) / row["avg_sales_previous_two_months"]) * 100
        arrow = "🡅" if change_percent > 0 else "🡇" if change_percent < 0 else "→"
        return f"{arrow} ({change_percent:.1f}%)"

    df["salesTrend"] = df.apply(calculate_growth_arrow, axis=1)

    # Format display values with trends - Round sales to 2 decimal places
    df["sales_display"] = df["total_sales"].round(2).astype(str) + " " + df["salesTrend"]
    df["quantity_display"] = df["total_quantity"].astype(str)  # Simplified quantity display

    # Sort by total sales in descending order and select top 100 products
    df = df.sort_values(by="total_sales", ascending=False).head(100)

    # Add Serial Number column (1 to N)
    df.insert(0, "S.No", range(1, len(df) + 1))

    # Select and rename columns for display
    df = df[["S.No", "productName", "sales_display", "quantity_display"]]
    df.columns = ["S.No", "Product Name", "Sales", "Quantity Sold"]

    return df