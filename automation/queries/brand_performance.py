import pandas as pd
from connector import get_db_connection
from queries.trend import get_trend_arrow
import plotly.graph_objs as go

def fetch_brand_data(default_start_date=None, default_end_date=None):
    """Fetch brand sales data dynamically from brand_sales table."""
    engine = get_db_connection()

    try:
        # Get the latest available orderDate dynamically
        last_date_query = 'SELECT MAX("orderdate") FROM brand_sales;'
        last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]

        if last_date is None:
            engine.dispose()
            return pd.DataFrame(columns=["S.No", "Brand Name", "Number of Orders", "Sales", "Average Order Value"])

        # Set default_end_date to the latest available orderDate
        default_end_date = last_date if default_end_date is None else default_end_date
        default_start_date = pd.to_datetime(default_end_date) - pd.Timedelta(days=7) if default_start_date is None else default_start_date

        # Convert to proper date format
        if isinstance(default_end_date, str):
            end_date = pd.to_datetime(default_end_date).date()
        elif hasattr(default_end_date, 'date'):
            end_date = default_end_date.date()
        else:
            end_date = default_end_date

        query = """
        WITH last_day_sales AS (
                SELECT 
                    "brandname",
                    "nooforders" AS orders_today,
                    "sales" AS sales_today,
                    "aov" AS AOV_today
                FROM brand_sales
                WHERE "orderdate" = %(end_date)s::date
            ),
            previous_period AS (
                SELECT 
                    "brandname",
                    ROUND(AVG("nooforders")::numeric, 2) AS avg_orders_previous_days,
                    ROUND(AVG("sales")::numeric, 2) AS avg_sales_previous_days,
                    ROUND(AVG("aov")::numeric, 2) AS avg_AOV_previous_days
                FROM brand_sales
                WHERE "orderdate" BETWEEN %(end_date)s::date - INTERVAL '7 days' 
                                    AND %(end_date)s::date - INTERVAL '1 day'
                GROUP BY "brandname"
            )
            SELECT 
                COALESCE(l.brandname, p.brandname) AS brandname,
                COALESCE(l.orders_today, 0) AS orders_today,
                COALESCE(l.sales_today, 0) AS sales_today,
                COALESCE(l.AOV_today, 0) AS AOV_today,
                COALESCE(p.avg_orders_previous_days, 0) AS avg_orders_previous_days,
                COALESCE(p.avg_sales_previous_days, 0) AS avg_sales_previous_days,
                COALESCE(p.avg_AOV_previous_days, 0) AS avg_AOV_previous_days
            FROM last_day_sales l
            LEFT JOIN previous_period p ON l.brandname = p.brandname

            UNION

            SELECT 
                p.brandname,
                0 AS orders_today,
                0 AS sales_today,
                0 AS AOV_today,
                p.avg_orders_previous_days,
                p.avg_sales_previous_days,
                p.avg_AOV_previous_days
            FROM previous_period p
            LEFT JOIN last_day_sales l ON p.brandname = l.brandname
            WHERE l.brandname IS NULL

            ORDER BY sales_today DESC
            LIMIT 50;
        """

        # Fixed: Use 'end_date' to match the parameter name in the query
        df = pd.read_sql(query, engine, params={'end_date': end_date})
        engine.dispose()

        if df.empty:
            return pd.DataFrame(columns=["S.No", "Brand Name", "Number of Orders", "Sales", "Average Order Value"])

        # Debug: Print column names to see what we actually have
        print("Brand DataFrame columns:", df.columns.tolist())
        
        # Handle column name mapping for PostgreSQL lowercase returns
        column_mapping = {
            'aov_today': 'AOV_today',
            'avg_aov_previous_days': 'avg_AOV_previous_days'
        }
        
        # Apply column mapping
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]
        
        # Ensure we have the required columns
        required_columns = ['orders_today', 'sales_today', 'AOV_today', 
                          'avg_orders_previous_days', 'avg_sales_previous_days', 'avg_AOV_previous_days']
        
        for col in required_columns:
            if col not in df.columns:
                print(f"Warning: Column {col} not found, setting to 0")
                df[col] = 0

        # Calculate trend comparison
        df["ordersTrend"] = df.apply(lambda row: get_trend_arrow(row["orders_today"], row["avg_orders_previous_days"]), axis=1)
        df["salesTrend"] = df.apply(lambda row: get_trend_arrow(row["sales_today"], row["avg_sales_previous_days"]), axis=1)
        df["AOVTrend"] = df.apply(lambda row: get_trend_arrow(row["AOV_today"], row["avg_AOV_previous_days"]), axis=1)

        # Format the display values with trend arrows
        df["orders_today"] = df["orders_today"].astype(str) + " " + df["ordersTrend"]
        df["sales_today"] = df["sales_today"].astype(str) + " " + df["salesTrend"]
        df["AOV_today"] = df["AOV_today"].astype(str) + " " + df["AOVTrend"]

        # Add Serial Number Column Before Brand Name
        df.insert(0, "S.No", range(1, len(df) + 1))

        df = df[["S.No", "brandname", "orders_today", "sales_today", "AOV_today"]]
        df.columns = ["S.No", "Brand Name", "Number of Orders", "Sales", "Average Order Value"]

        return df

    except Exception as e:
        print(f"Error in fetch_brand_data: {e}")
        engine.dispose()
        return pd.DataFrame(columns=["S.No", "Brand Name", "Number of Orders", "Sales", "Average Order Value"])


def create_brand_sales_bar_chart(df, top_n=15):
    """
    Create a bar chart of brandName vs Sales for the latest available day.
    Shows data labels on bars.
    Returns a Plotly Figure.
    """
    if df is None or df.empty:
        return go.Figure().update_layout(title="No brand data available")

    try:
        # Extract numeric sales values from the formatted strings
        def extract_numeric(s):
            try:
                return float(str(s).split()[0].replace(',', ''))
            except (ValueError, IndexError):
                return 0

        # Create a copy and extract numeric values
        df_plot = df.copy()
        
        # Check if we have the right columns
        if 'Brand Name' not in df_plot.columns or 'Sales' not in df_plot.columns:
            return go.Figure().update_layout(title="Missing required columns for chart")

        df_plot['SalesValue'] = df_plot['Sales'].apply(extract_numeric)
        
        # Sort by sales value and take top N
        df_plot = df_plot.sort_values(by='SalesValue', ascending=False).head(top_n)

        if df_plot.empty or df_plot['SalesValue'].sum() == 0:
            return go.Figure().update_layout(title="No sales data to display")

        fig = go.Figure(
            data=[
                go.Bar(
                    x=df_plot['Brand Name'],
                    y=df_plot['SalesValue'],
                    marker_color='#2980b9',
                    text=[f'₹{v:,.0f}' for v in df_plot['SalesValue']],
                    textposition='outside'
                )
            ]
        )
        fig.update_layout(
            title=f"Top {top_n} Brands by Sales",
            xaxis_title="Brand Name",
            yaxis_title="Sales",
            xaxis_tickangle=-90,
            plot_bgcolor='white',
            height=700,
            width=1200,
            margin=dict(l=40, r=20, t=60, b=120)
        )
        return fig

    except Exception as e:
        print(f"Error creating brand chart: {e}")
        return go.Figure().update_layout(title="Error creating chart")