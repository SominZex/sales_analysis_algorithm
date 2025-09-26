import pandas as pd
from connector import get_db_connection
from queries.trend import get_trend_arrow
import plotly.graph_objs as go

def fetch_subcategory_data(default_start_date=None, default_end_date=None):
    """Fetch subcategory sales data dynamically from category_sales table."""
    engine = get_db_connection()

    try:
        # Get the latest available orderDate dynamically
        last_date_query = 'SELECT MAX("orderdate") FROM category_sales;'
        last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]

        if last_date is None:
            engine.dispose()
            return pd.DataFrame(columns=["S.No", "Subcategory", "Sales"])

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
                    "subcategoryof",
                    SUM("sales") AS sales_today
                FROM category_sales
                WHERE "orderdate" = %(end_date)s::date
                GROUP BY "subcategoryof"
            ),
            previous_period AS (
                SELECT 
                    "subcategoryof",
                    ROUND(AVG("sales")::numeric, 2) AS avg_sales_previous_days
                FROM category_sales
                WHERE "orderdate" BETWEEN %(end_date)s::date - INTERVAL '7 days'
                                    AND %(end_date)s::date - INTERVAL '1 day'
                GROUP BY "subcategoryof"
            )
            SELECT 
                COALESCE(l."subcategoryof", p."subcategoryof") AS "subcategoryof",
                COALESCE(l.sales_today, 0) AS sales_today,
                COALESCE(p.avg_sales_previous_days, 0) AS avg_sales_previous_days
            FROM last_day_sales l
            LEFT JOIN previous_period p ON l."subcategoryof" = p."subcategoryof"

            UNION

            SELECT 
                p."subcategoryof",
                0 AS sales_today,
                p.avg_sales_previous_days
            FROM previous_period p
            LEFT JOIN last_day_sales l ON p."subcategoryof" = l."subcategoryof"
            WHERE l."subcategoryof" IS NULL

            ORDER BY sales_today DESC;
        """

        # Fixed: Use 'end_date' to match the parameter name in the query
        df = pd.read_sql(query, engine, params={'end_date': end_date})
        engine.dispose()

        if df.empty:
            return pd.DataFrame(columns=["S.No", "Subcategory", "Sales"])

        # Calculate trend comparison
        df["salesTrend"] = df.apply(lambda row: get_trend_arrow(row["sales_today"], row["avg_sales_previous_days"]), axis=1)
        df["sales_today"] = df["sales_today"].astype(str) + " " + df["salesTrend"]

        # Add Serial Number column
        df.insert(0, "S.No", df.index + 1)

        # Rename columns
        df = df[["S.No", "subcategoryof", "sales_today"]]
        df.columns = ["S.No", "Subcategory", "Sales"]

        return df

    except Exception as e:
        print(f"Error in fetch_subcategory_data: {e}")
        engine.dispose()
        return pd.DataFrame(columns=["S.No", "Subcategory", "Sales"])


def create_category_sales_chart(df, top_n=20):
    """
    Create a bar chart of top N subcategories by sales using Plotly.
    Expects a DataFrame with columns ['Subcategory', 'Sales'] (with trend arrows in 'Sales').
    """
    # Extract numeric sales values for sorting and plotting
    if df.empty or 'Subcategory' not in df.columns or 'Sales' not in df.columns:
        return go.Figure()

    # Remove trend arrows and convert to float for sorting/plotting
    def extract_numeric(s):
        try:
            return float(str(s).split()[0].replace(',', ''))
        except (ValueError, IndexError):
            return 0

    df_plot = df.copy()
    df_plot['SalesValue'] = df_plot['Sales'].apply(extract_numeric)

    # Sort and select top N
    df_plot = df_plot.sort_values(by='SalesValue', ascending=False).head(top_n)

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_plot['Subcategory'],
                y=df_plot['SalesValue'],
                marker_color='#8e44ad',
                text=[f'₹{val:,.0f}' for val in df_plot['SalesValue']],
                textposition='outside'
            )
        ]
    )
    fig.update_layout(
        title=f"Top {top_n} Subcategories by Sales",
        xaxis_title="Subcategory",
        yaxis_title="Sales",
        xaxis_tickangle=-90,
        plot_bgcolor='white',
        margin=dict(l=40, r=20, t=60, b=120),
        height=500,
        width=700
    )
    return fig