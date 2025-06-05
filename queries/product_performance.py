import pandas as pd
from connector import get_db_connection
from queries.trend import get_trend_arrow
import plotly.graph_objs as go

def fetch_product_data(default_start_date=None, default_end_date=None):
    """Fetch product sales data dynamically from product_sales table, limited to top 100 products by sales."""
    engine = get_db_connection()

    # Get the latest available orderDate dynamically
    last_date_query = "SELECT MAX(orderDate) FROM product_sales;"
    last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]

    if last_date is None:
        return pd.DataFrame(columns=["S.No", "Product Name", "Sales", "Quantity Sold"])

    # Set default_end_date to the latest available orderDate
    default_end_date = last_date if default_end_date is None else default_end_date
    default_start_date = pd.to_datetime(default_end_date) - pd.DateOffset(days=7) if default_start_date is None else default_start_date

    query = """
    WITH last_day_sales AS (
        SELECT 
            productName,
            Sales AS sales_today,
            QuantitySold AS quantity_today
        FROM product_sales
        WHERE orderDate = %(default_end_date)s
    ),
    previous_period AS (
        SELECT 
            productName,
            ROUND(AVG(Sales), 2) AS avg_sales_previous_days,
            ROUND(AVG(QuantitySold), 2) AS avg_quantity_previous_days
        FROM product_sales
        WHERE orderDate BETWEEN DATE_SUB(%(default_end_date)s, INTERVAL 7 DAY) AND DATE_SUB(%(default_end_date)s, INTERVAL 1 DAY)
        GROUP BY productName
    )
    SELECT 
        COALESCE(l.productName, p.productName) AS productName,
        COALESCE(l.sales_today, 0) AS sales_today,
        COALESCE(l.quantity_today, 0) AS quantity_today,
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

    df = pd.read_sql(query, engine, params={'default_end_date': default_end_date})
    engine.dispose()

    if df.empty:
        return pd.DataFrame(columns=["S.No", "Product Name", "Sales", "Quantity Sold"])

    # Calculate trend comparison
    df["salesTrend"] = df.apply(lambda row: get_trend_arrow(row["sales_today"], row["avg_sales_previous_days"]), axis=1)
    df["quantityTrend"] = df.apply(lambda row: get_trend_arrow(row["quantity_today"], row["avg_quantity_previous_days"]), axis=1)

    # Format display values with trends
    df["sales_display"] = df["sales_today"].astype(str) + " " + df["salesTrend"]
    df["quantity_display"] = df["quantity_today"].astype(str) + " " + df["quantityTrend"]

    # Add Serial Number column (1 to N)
    df.insert(0, "S.No", range(1, len(df) + 1))

    # Select and rename columns for display
    df = df[["S.No", "productName", "sales_display", "quantity_display"]]
    df.columns = ["S.No", "Product Name", "Sales", "Quantity Sold"]

    return df


def create_product_sales_bar_chart(df, top_n=50):
    """
    Create a bar chart of top N products by sales using Plotly.
    Expects a DataFrame with columns ['Product Name', 'Sales'] (with trend arrows in 'Sales').
    """
    if df.empty or 'Product Name' not in df.columns or 'Sales' not in df.columns:
        return go.Figure()

    # Extract numeric sales values for sorting and plotting
    def extract_numeric(s):
        # Handles values like "12345 🡅 (12.3%)"
        return float(str(s).split()[0].replace(',', ''))

    df_plot = df.copy()
    df_plot['SalesValue'] = df_plot['Sales'].apply(extract_numeric)

    # Sort and select top N
    df_plot = df_plot.sort_values(by='SalesValue', ascending=False).head(top_n)

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_plot['Product Name'],
                y=df_plot['SalesValue'],
                marker_color='#16a085',
                text=[f'₹{val:,.0f}' for val in df_plot['SalesValue']],
                textposition='outside'
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} Products by Sales",
        xaxis_title="Product Name",
        yaxis_title="Sales",
        xaxis_tickangle=-90,
        plot_bgcolor='white',
        margin=dict(l=40, r=20, t=60, b=120),
        height=700,
        width=1200
    )
    return fig