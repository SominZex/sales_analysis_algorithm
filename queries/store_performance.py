import pandas as pd
import plotly.graph_objs as go
from connector import get_db_connection
from queries.trend import get_trend_arrow

def fetch_total_sales():
    """Fetch total sales for the latest date and calculate average weekly growth percentage."""
    engine = get_db_connection()
    
    # Get the latest available date
    query_last_date = "SELECT MAX(orderDate) AS last_date FROM store_sales"
    last_date_df = pd.read_sql(query_last_date, engine)
    last_date = last_date_df['last_date'].iloc[0]

    # Fetch total sales for the last available day
    query_sales_today = f"""
        SELECT SUM(sales) AS total_sales 
        FROM store_sales 
        WHERE orderDate = '{last_date}';
    """
    sales_today_df = pd.read_sql(query_sales_today, engine)
    sales_today = sales_today_df['total_sales'].iloc[0] or 0

    # Fetch average total sales from last 7 days (excluding the latest day)
    query_sales_last_week = f"""
        SELECT AVG(total_sales) AS avg_weekly_sales FROM (
            SELECT SUM(sales) AS total_sales FROM store_sales 
            WHERE orderDate BETWEEN DATE('{last_date}') - INTERVAL 7 DAY AND DATE('{last_date}') - INTERVAL 1 DAY
            GROUP BY orderDate
        ) AS weekly_sales;
    """
    avg_sales_last_week_df = pd.read_sql(query_sales_last_week, engine)
    avg_sales_last_week = avg_sales_last_week_df['avg_weekly_sales'].iloc[0] or 0

    engine.dispose()

    # Calculate percentage growth
    if avg_sales_last_week > 0:
        weekly_growth = ((sales_today - avg_sales_last_week) / avg_sales_last_week) * 100
    else:
        weekly_growth = 0  # Prevent division by zero

    return sales_today, round(weekly_growth, 2)


def fetch_sales_data(default_start_date=None, default_end_date=None):
    """
    Fetch store sales data from store_sales table based on selected date range.
    Returns:
        formatted_df: DataFrame for table display
        chart_data: DataFrame with columns ['storeName', 'totalSales'] for charting
    """
    engine = get_db_connection()

    # Get the latest orderDate dynamically
    last_date_query = "SELECT MAX(orderDate) FROM store_sales;"
    last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]

    if last_date is None:
        empty_df = pd.DataFrame(columns=["S.No", "Store Name", "Number of Orders", "Sales", "Average Order Value"])
        empty_chart = pd.DataFrame(columns=["storeName", "totalSales"])
        return empty_df, empty_chart

    # Set default_end_date to last available date
    default_end_date = last_date if default_end_date is None else default_end_date
    default_start_date = pd.to_datetime(default_end_date) - pd.DateOffset(days=7) if default_start_date is None else default_start_date

    query = """
    WITH last_day_sales AS (
        SELECT 
            storeName,
            NoOfOrder AS orders_today,
            sales AS totalSales,
            AOV AS AOV_today
        FROM store_sales
        WHERE orderDate = %(default_end_date)s
    ),
    previous_7_days AS (
        SELECT 
            storeName,
            ROUND(AVG(NoOfOrder), 2) AS avg_orders_last_7_days,
            ROUND(AVG(sales), 2) AS avg_sales_last_7_days,
            ROUND(AVG(AOV), 2) AS avg_AOV_last_7_days
        FROM store_sales
        WHERE orderDate BETWEEN DATE_SUB(%(default_end_date)s, INTERVAL 7 DAY) AND DATE_SUB(%(default_end_date)s, INTERVAL 1 DAY)
        GROUP BY storeName
    )
    SELECT 
        COALESCE(l.storeName, p.storeName) AS storeName,
        COALESCE(l.orders_today, 0) AS orders_today,
        COALESCE(l.totalSales, 0) AS totalSales,
        COALESCE(l.AOV_today, 0) AS AOV_today,
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

    df = pd.read_sql(query, engine, params={'default_end_date': default_end_date})
    engine.dispose()

    if df.empty:
        empty_df = pd.DataFrame(columns=["S.No", "Store Name", "Number of Orders", "Sales", "Average Order Value"])
        empty_chart = pd.DataFrame(columns=["storeName", "totalSales"])
        return empty_df, empty_chart

    # Prepare chart data (numeric)
    chart_data = df[["storeName", "totalSales"]].copy()
    chart_data["totalSales"] = pd.to_numeric(chart_data["totalSales"], errors="coerce").fillna(0)

    # Add trend calculations for table
    df["ordersTrend"] = df.apply(lambda row: get_trend_arrow(row["orders_today"], row["avg_orders_last_7_days"]), axis=1)
    df["salesTrend"] = df.apply(lambda row: get_trend_arrow(row["totalSales"], row["avg_sales_last_7_days"]), axis=1)
    df["AOVTrend"] = df.apply(lambda row: get_trend_arrow(row["AOV_today"], row["avg_AOV_last_7_days"]), axis=1)

    # Format numbers for table
    df["orders_today"] = df["orders_today"].apply(lambda x: f"{int(x):,}")
    df["totalSales"] = df["totalSales"].apply(lambda x: f"{float(x):,.2f}")
    df["AOV_today"] = df["AOV_today"].apply(lambda x: f"{float(x):,.2f}")

    # Format display for table
    df["Number of Orders"] = df["orders_today"] + " " + df["ordersTrend"]
    df["Sales"] = df["totalSales"] + " " + df["salesTrend"]
    df["Average Order Value"] = df["AOV_today"] + " " + df["AOVTrend"]

    # Add serial number for table
    df.insert(0, "S.No", range(1, len(df) + 1))

    formatted_df = df[["S.No", "storeName", "Number of Orders", "Sales", "Average Order Value"]].rename(
        columns={"storeName": "Store Name"}
    )

    return formatted_df, chart_data

def create_store_sales_chart(chart_data, top_n=30):
    """
    Create a compact bar chart of top N stores by sales using Plotly.
    chart_data: DataFrame with columns ['storeName', 'totalSales']
    """
    import plotly.graph_objs as go

    if chart_data is None or chart_data.empty:
        return go.Figure()

    # Sort and select top N
    chart_data_sorted = chart_data.sort_values(by='totalSales', ascending=False).head(top_n)

    fig = go.Figure(
        data=[
            go.Bar(
                x=chart_data_sorted['storeName'],
                y=chart_data_sorted['totalSales'],
                marker_color='#3498db',
                text=[f'₹{val:,.0f}' for val in chart_data_sorted['totalSales']],
                textposition='outside'
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} Stores by Sales",
        xaxis_title="Store Name",
        yaxis_title="Sales",
        xaxis_tickangle=-90,
        plot_bgcolor='white',
        margin=dict(l=40, r=20, t=30, b=110),
        height=400, 
        width=1500
    )
    return fig