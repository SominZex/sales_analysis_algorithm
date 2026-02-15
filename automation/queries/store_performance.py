import pandas as pd
import plotly.graph_objs as go
from connector import get_db_connection
from queries.trend import get_trend_arrow
from datetime import timedelta

def fetch_total_sales():
    """Fetch total sales for the latest date and calculate average weekly growth percentage."""
    engine = get_db_connection()
    
    try:
        # Get the latest available date
        query_last_date = 'SELECT MAX("orderdate") AS last_date FROM store_sales;'
        last_date_df = pd.read_sql(query_last_date, engine)
        last_date = last_date_df['last_date'].iloc[0]

        # Fetch total sales for the last available day
        query_sales_today = f'''
            SELECT SUM("sales") AS total_sales 
            FROM store_sales 
            WHERE "orderdate" = '{last_date}';
        '''
        sales_today_df = pd.read_sql(query_sales_today, engine)
        sales_today = sales_today_df['total_sales'].iloc[0] or 0

        # Fetch average total sales from last 7 days (excluding the latest day)
        query_sales_last_week = '''
        SELECT AVG(total_sales) AS avg_weekly_sales 
        FROM (
            SELECT SUM("sales") AS total_sales 
            FROM store_sales
            WHERE "orderdate" BETWEEN %(last_date)s::date - INTERVAL '7 days'
                                AND %(last_date)s::date - INTERVAL '1 day'
            GROUP BY "orderdate"
        ) AS weekly_sales;
        '''

        avg_sales_last_week_df = pd.read_sql(query_sales_last_week, engine, params={'last_date': last_date})
        avg_sales_last_week = avg_sales_last_week_df['avg_weekly_sales'].iloc[0] or 0

        engine.dispose()

        # Calculate percentage growth
        weekly_growth = ((sales_today - avg_sales_last_week) / avg_sales_last_week * 100) if avg_sales_last_week > 0 else 0

        return sales_today, round(weekly_growth, 2)
    
    except Exception as e:
        print(f"Error in fetch_total_sales: {e}")
        engine.dispose()
        return 0, 0


def fetch_sales_data(default_start_date=None, default_end_date=None):
    """
    Fetch store sales data from store_sales table based on selected date range.
    Returns:
        formatted_df: DataFrame for table display
        chart_data: DataFrame with columns ['storename', 'totalSales'] for charting
    """
    engine = get_db_connection()

    try:
        # Get the latest orderDate dynamically
        last_date_query = 'SELECT MAX("orderdate") FROM store_sales;'
        last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]

        if last_date is None:
            empty_df = pd.DataFrame(columns=["S.No", "Store Name", "Number of Orders", "Sales", "Average Order Value"])
            empty_chart = pd.DataFrame(columns=["storename", "totalSales"])
            engine.dispose()
            return empty_df, empty_chart

        # Set default_end_date to last available date
        default_end_date = last_date if default_end_date is None else default_end_date
        default_start_date = pd.to_datetime(default_end_date) - pd.DateOffset(days=7) if default_start_date is None else default_start_date
        
        # Ensure default dates are proper date objects
        default_end_date = pd.to_datetime(default_end_date).date()
        default_start_date = pd.to_datetime(default_start_date).date()

        # NOW assign the values to end_date and start_date
        end_date = default_end_date
        start_date = default_start_date
        
        # MOVED this line after end_date is defined
        start_date_for_prev_7 = end_date - timedelta(days=7) 

        query = """
            WITH last_day_sales AS (
                SELECT 
                    "storename",
                    "nooforder" AS orders_today,
                    "sales" AS sales_today,
                    "aov" AS aov_today
                FROM store_sales
                WHERE "orderdate" = %(end_date)s
            ),
            previous_7_days AS (
                SELECT 
                    "storename",
                    ROUND(AVG("nooforder"), 2) AS avg_orders_last_7_days,
                    ROUND(AVG("sales"), 2) AS avg_sales_last_7_days,
                    ROUND(AVG("aov"), 2) AS avg_aov_last_7_days
                FROM store_sales
                WHERE "orderdate" BETWEEN %(start_date)s AND %(prev_end_date)s
                GROUP BY "storename"
            )
            SELECT 
                COALESCE(l."storename", p."storename") AS storename,
                COALESCE(l.orders_today, 0) AS orders_today,
                COALESCE(l.sales_today, 0) AS totalsales,
                COALESCE(l.aov_today, 0) AS aov_today,
                COALESCE(p.avg_orders_last_7_days, 0) AS avg_orders_last_7_days,
                COALESCE(p.avg_sales_last_7_days, 0) AS avg_sales_last_7_days,
                COALESCE(p.avg_aov_last_7_days, 0) AS avg_aov_last_7_days
            FROM last_day_sales l
            FULL OUTER JOIN previous_7_days p ON l."storename" = p."storename"
            ORDER BY COALESCE(l.sales_today, 0) DESC;
            """

        # Execute query with parameters
        df = pd.read_sql(query, engine, params={
            'start_date': start_date,
            'prev_end_date': end_date - timedelta(days=1),
            'end_date': end_date
        })

        engine.dispose()

        if df.empty:
            empty_df = pd.DataFrame(columns=["S.No", "Store Name", "Number of Orders", "Sales", "Average Order Value"])
            empty_chart = pd.DataFrame(columns=["storename", "totalSales"])
            return empty_df, empty_chart

        # Debug: Print column names to see what we actually have
        print("DataFrame columns:", df.columns.tolist())
        
        # Fix column name mapping for PostgreSQL lowercase returns
        column_mapping = {
            'totalsales': 'totalSales',
            'aov_today': 'AOV_today',
            'avg_orders_last_7_days': 'avg_orders_last_7_days',
            'avg_sales_last_7_days': 'avg_sales_last_7_days',
            'avg_aov_last_7_days': 'avg_AOV_last_7_days'
        }
        
        # Apply column mapping
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]
        
        # Ensure we have the required columns
        if 'totalSales' not in df.columns:
            if 'totalsales' in df.columns:
                df['totalSales'] = df['totalsales']
            else:
                df['totalSales'] = 0
                
        if 'AOV_today' not in df.columns:
            if 'aov_today' in df.columns:
                df['AOV_today'] = df['aov_today']
            else:
                df['AOV_today'] = 0

        # Prepare chart data (numeric) - with error handling
        if 'storename' in df.columns and 'totalSales' in df.columns:
            chart_data = df[["storename", "totalSales"]].copy()
            chart_data["totalSales"] = pd.to_numeric(chart_data["totalSales"], errors="coerce").fillna(0)
        else:
            print("Cannot create chart data - missing required columns")
            chart_data = pd.DataFrame(columns=["storename", "totalSales"])

        # Add trend calculations for table
        df["ordersTrend"] = df.apply(lambda row: get_trend_arrow(row["orders_today"], row["avg_orders_last_7_days"]), axis=1)
        df["salesTrend"] = df.apply(lambda row: get_trend_arrow(row["totalSales"], row["avg_sales_last_7_days"]), axis=1)
        df["AOVTrend"] = df.apply(lambda row: get_trend_arrow(row["AOV_today"], row["avg_AOV_last_7_days"]), axis=1)

        # Format numbers for table
        df["orders_today"] = df["orders_today"].apply(lambda x: f"{int(x):,}")
        df["totalSales_formatted"] = df["totalSales"].apply(lambda x: f"{float(x):,.2f}")
        df["AOV_today"] = df["AOV_today"].apply(lambda x: f"{float(x):,.2f}")

        # Format display for table
        df["Number of Orders"] = df["orders_today"] + " " + df["ordersTrend"]
        df["Sales"] = df["totalSales_formatted"] + " " + df["salesTrend"]
        df["Average Order Value"] = df["AOV_today"] + " " + df["AOVTrend"]

        # Add serial number for table
        df.insert(0, "S.No", range(1, len(df) + 1))

        formatted_df = df[["S.No", "storename", "Number of Orders", "Sales", "Average Order Value"]].rename(
            columns={"storename": "Store Name"}
        )

        return formatted_df, chart_data

    except Exception as e:
        print(f"Error in fetch_sales_data: {e}")
        engine.dispose()
        empty_df = pd.DataFrame(columns=["S.No", "Store Name", "Number of Orders", "Sales", "Average Order Value"])
        empty_chart = pd.DataFrame(columns=["storename", "totalSales"])
        return empty_df, empty_chart


def create_store_sales_chart(chart_data, top_n=30):
    """
    Create a compact bar chart of top N stores by sales using Plotly.
    chart_data: DataFrame with columns ['storename', 'totalSales']
    """
    if chart_data is None or chart_data.empty:
        return go.Figure().update_layout(title="No data available for chart")

    # Check if required columns exist
    if 'storename' not in chart_data.columns or 'totalSales' not in chart_data.columns:
        print(f"Chart data missing required columns. Available: {chart_data.columns.tolist()}")
        return go.Figure().update_layout(title="Chart data unavailable")

    try:
        # Sort and select top N
        chart_data_sorted = chart_data.sort_values(by='totalSales', ascending=False).head(top_n)

        fig = go.Figure(
            data=[
                go.Bar(
                    x=chart_data_sorted['storename'],
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
    
    except Exception as e:
        print(f"Error creating store chart: {e}")
        return go.Figure().update_layout(title="Error creating chart")