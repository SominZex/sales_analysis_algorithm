import pandas as pd
import pdfkit
import plotly.graph_objects as go
import base64
import os
import time
from io import BytesIO
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

DB_URI = "postgresql+psycopg2://<username>:<password>@<server_ip>/sales_data"
engine = create_engine(DB_URI, pool_pre_ping=True, pool_recycle=300)

PDFKIT_CONFIG = pdfkit.configuration(wkhtmltopdf="/usr/bin/wkhtmltopdf")


def safe_read_sql(query, params=None, retries=3, delay=3):
    """Executes SQL query with retries for transient DB disconnects"""
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                return pd.read_sql(query, conn, params=params)
        except OperationalError as e:
            print(f"⚠️ Database error (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
        except Exception as e:
            print(f"⚠️ Unexpected error (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("❌ Query failed after multiple retries.")

def get_unique_stores():
    """Fetch all unique store names"""
    query = 'SELECT DISTINCT "storeName" FROM "billing_data" ORDER BY "storeName";'
    df = safe_read_sql(query)
    return df["storeName"].dropna().tolist()


def plot_chart(df, x_col, y_col, title, top_n=10):
    """Generate a modern Plotly bar chart and return base64 image string"""
    if df.empty:
        return ""
    df_plot = df.head(top_n)

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_plot[x_col],
                y=df_plot[y_col],
                text=[f"₹{v:,.0f}" for v in df_plot[y_col]],
                textposition="outside",
                marker=dict(color="#0078d7"),
            )
        ]
    )
    fig.update_layout(
        title=dict(text=title, font=dict(size=18, color="#0078d7", family="Segoe UI")),
        xaxis=dict(title="", tickangle=-45, automargin=True),
        yaxis=dict(title="Sales (₹)", gridcolor="rgba(200,200,200,0.3)"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=40, r=40, t=50, b=80),
        height=500,
    )

    buffer = BytesIO()
    fig.write_image(buffer, format="png", scale=2)
    buffer.seek(0)
    img_base64 = base64.b64encode(buffer.read()).decode("utf-8")
    return f'<img src="data:image/png;base64,{img_base64}" style="display:block;margin:auto;width:90%;max-height:500px;">'

def generate_store_report(store_name):
    """Generate weekly PDF report for one store"""
    
    date_range_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        )
        SELECT 
            (max_date - INTERVAL '6 days')::date AS week_start,
            max_date AS week_end
        FROM latest_date;
    """
    
    date_info = safe_read_sql(date_range_query, params=(store_name,))
    
    if date_info.empty:
        week_start_str = "N/A"
        week_end_str = "N/A"
    else:
        week_start = date_info['week_start'].iloc[0]
        week_end = date_info['week_end'].iloc[0]
        week_start_str = week_start.strftime('%d %b %Y')
        week_end_str = week_end.strftime('%d %b %Y')
    
    # === Get Previous 2 Weeks Average and Current Week Sales ===
    comparison_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        ),
        sales_periods AS (
            SELECT 
                SUM(CASE 
                    WHEN "orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
                         AND "orderDate" <= (SELECT max_date FROM latest_date)
                    THEN "totalProductPrice" 
                    ELSE 0 
                END) AS current_week_sales,
                SUM(CASE 
                    WHEN "orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '14 days'
                         AND "orderDate" <= (SELECT max_date FROM latest_date) - INTERVAL '7 days'
                    THEN "totalProductPrice" 
                    ELSE 0 
                END) AS week_2_sales,
                SUM(CASE 
                    WHEN "orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '21 days'
                         AND "orderDate" <= (SELECT max_date FROM latest_date) - INTERVAL '14 days'
                    THEN "totalProductPrice" 
                    ELSE 0 
                END) AS week_3_sales
            FROM "billing_data"
            WHERE "storeName" = %s
        )
        SELECT 
            current_week_sales,
            (week_2_sales + week_3_sales) / 2.0 AS prev_2_weeks_avg
        FROM sales_periods;
    """
    
    comparison_df = safe_read_sql(comparison_query, params=(store_name, store_name))
    
    if not comparison_df.empty and comparison_df['current_week_sales'].iloc[0] is not None:
        current_week_sales = float(comparison_df['current_week_sales'].iloc[0])
        prev_2_weeks_avg = float(comparison_df['prev_2_weeks_avg'].iloc[0]) if comparison_df['prev_2_weeks_avg'].iloc[0] is not None else 0.0
        
        if prev_2_weeks_avg > 0:
            percentage_diff = ((current_week_sales - prev_2_weeks_avg) / prev_2_weeks_avg) * 100
            diff_sign = "+" if percentage_diff >= 0 else ""
            diff_color = "#28a745" if percentage_diff >= 0 else "#dc3545"
            comparison_text = f"""
                <div style="text-align: center; margin-top: 10px;">
                    <span style="font-size: 18px; color: #666;">Previous 2 Weeks Average: ₹{prev_2_weeks_avg:,.2f}</span><br>
                    <span style="font-size: 20px; font-weight: bold; color: {diff_color};">
                        {diff_sign}{percentage_diff:.2f}%
                    </span>
                </div>
            """
        else:
            comparison_text = '<div style="text-align: center; margin-top: 10px;"><span style="font-size: 18px; color: #666;">Insufficient data for comparison</span></div>'
    else:
        current_week_sales = 0.0
        comparison_text = '<div style="text-align: center; margin-top: 10px;"><span style="font-size: 18px; color: #666;">No sales data available</span></div>'
    
    # === Queries with Contribution % and Profit Margin %
    brand_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        ),
        total_sales AS (
            SELECT SUM(b."totalProductPrice") AS total
            FROM "billing_data" b
            WHERE b."storeName" = %s
              AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
              AND b."orderDate" <= (SELECT max_date FROM latest_date)
        )
        SELECT 
            b."brandName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2) AS total_sales,
            SUM(b."quantity") AS quantity_sold,
            ROUND((SUM(b."totalProductPrice") / NULLIF((SELECT total FROM total_sales), 0) * 100)::numeric, 2) AS contrib_percent,
            ROUND(
                CASE 
                    WHEN SUM(b."totalProductPrice") > 0 THEN
                        ((SUM(b."totalProductPrice") - SUM(COALESCE(b."costPrice", 0) * b."quantity")) / SUM(b."totalProductPrice") * 100)::numeric
                    ELSE 0
                END, 2
            ) AS PROFIT_MARGIN
        FROM "billing_data" b
        WHERE b."storeName" = %s
          AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
          AND b."orderDate" <= (SELECT max_date FROM latest_date)
        GROUP BY b."brandName"
        ORDER BY total_sales DESC
        LIMIT 50;
    """

    category_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        ),
        total_sales AS (
            SELECT SUM(b."totalProductPrice") AS total
            FROM "billing_data" b
            WHERE b."storeName" = %s
              AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
              AND b."orderDate" <= (SELECT max_date FROM latest_date)
        )
        SELECT 
            b."categoryName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2) AS total_sales,
            SUM(b."quantity") AS quantity_sold,
            ROUND((SUM(b."totalProductPrice") / NULLIF((SELECT total FROM total_sales), 0) * 100)::numeric, 2) AS contrib_percent,
            ROUND(
                CASE 
                    WHEN SUM(b."totalProductPrice") > 0 THEN
                        ((SUM(b."totalProductPrice") - SUM(COALESCE(b."costPrice", 0) * b."quantity")) / SUM(b."totalProductPrice") * 100)::numeric
                    ELSE 0
                END, 2
            ) AS PROFIT_MARGIN
        FROM "billing_data" b
        WHERE b."storeName" = %s
          AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
          AND b."orderDate" <= (SELECT max_date FROM latest_date)
        GROUP BY b."categoryName"
        ORDER BY total_sales DESC
        LIMIT 50;
    """

    product_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        ),
        total_sales AS (
            SELECT SUM(b."totalProductPrice") AS total
            FROM "billing_data" b
            WHERE b."storeName" = %s
              AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
              AND b."orderDate" <= (SELECT max_date FROM latest_date)
        )
        SELECT 
            b."productName",
            ROUND(SUM(b."totalProductPrice")::numeric, 2) AS total_sales,
            SUM(b."quantity") AS quantity_sold,
            ROUND((SUM(b."totalProductPrice") / NULLIF((SELECT total FROM total_sales), 0) * 100)::numeric, 2) AS contrib_percent,
            ROUND(
                CASE 
                    WHEN SUM(b."totalProductPrice") > 0 THEN
                        ((SUM(b."totalProductPrice") - SUM(COALESCE(b."costPrice", 0) * b."quantity")) / SUM(b."totalProductPrice") * 100)::numeric
                    ELSE 0
                END, 2
            ) AS PROFIT_MARGIN
        FROM "billing_data" b
        WHERE b."storeName" = %s
          AND b."orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
          AND b."orderDate" <= (SELECT max_date FROM latest_date)
        GROUP BY b."productName"
        ORDER BY total_sales DESC
        LIMIT 100;
    """

    # Fetch data
    brand_df = safe_read_sql(brand_query, params=(store_name, store_name, store_name))
    category_df = safe_read_sql(category_query, params=(store_name, store_name, store_name))
    product_df = safe_read_sql(product_query, params=(store_name, store_name, store_name))
    
    # Add percentage symbols
    for df in [brand_df, category_df, product_df]:
        if not df.empty:
            if 'contrib_percent' in df.columns:
                df['contrib_percent'] = df['contrib_percent'].astype(str) + '%'
            if 'profit_margin' in df.columns:
                df['profit_margin'] = df['profit_margin'].astype(str) + '%'

    # === Query for Total Sales, Total Cost, Total Profit, and Profit Margin% ===
    total_sales_profit_query = """
        WITH latest_date AS (
            SELECT MAX("orderDate")::date AS max_date
            FROM "billing_data"
            WHERE "storeName" = %s
        )
        SELECT 
            ROUND(SUM("totalProductPrice")::numeric, 2) AS total_weekly_sales,
            ROUND(SUM(COALESCE("costPrice", 0) * "quantity")::numeric, 2) AS total_weekly_cost,
            ROUND((SUM("totalProductPrice") - SUM(COALESCE("costPrice", 0) * "quantity"))::numeric, 2) AS total_weekly_profit,
            ROUND(
                CASE 
                    WHEN SUM("totalProductPrice") > 0 THEN
                        ((SUM("totalProductPrice") - SUM(COALESCE("costPrice", 0) * "quantity")) / SUM("totalProductPrice") * 100)::numeric
                    ELSE 0
                END, 2
            ) AS profit_margin_percent
        FROM "billing_data"
        WHERE "storeName" = %s
          AND "orderDate" > (SELECT max_date FROM latest_date) - INTERVAL '7 days'
          AND "orderDate" <= (SELECT max_date FROM latest_date);
    """
    
    total_sales_profit_df = safe_read_sql(total_sales_profit_query, params=(store_name, store_name))
    
    if not total_sales_profit_df.empty and total_sales_profit_df["total_weekly_sales"].iloc[0] is not None:
        total_weekly_sales = float(total_sales_profit_df["total_weekly_sales"].iloc[0])
        total_weekly_cost = float(total_sales_profit_df["total_weekly_cost"].iloc[0]) if total_sales_profit_df["total_weekly_cost"].iloc[0] is not None else 0.0
        total_weekly_profit = float(total_sales_profit_df["total_weekly_profit"].iloc[0]) if total_sales_profit_df["total_weekly_profit"].iloc[0] is not None else 0.0
        profit_margin_percent = float(total_sales_profit_df["profit_margin_percent"].iloc[0]) if total_sales_profit_df["profit_margin_percent"].iloc[0] is not None else 0.0
    else:
        total_weekly_sales = 0.0
        total_weekly_cost = 0.0
        total_weekly_profit = 0.0
        profit_margin_percent = 0.0

    # --- Charts ---
    brand_chart = plot_chart(brand_df, "brandName", "total_sales", "Top 10 Brands by Sales")
    category_chart = plot_chart(category_df, "categoryName", "total_sales", "Top 10 Categories by Sales")
    product_chart = plot_chart(product_df, "productName", "total_sales", "Top 10 Products by Sales")

    # --- HTML Template with Profit Display ---
    html_template = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{store_name} - Weekly Store Report</title>
        <style>
            body {{
                font-family: 'Segoe UI', sans-serif;
                margin: 20px;
                background-color: #f6f8fa;
                position: relative;
            }}
            .logo {{
                position: absolute;
                top: 40px;
                right: 20px;
                width: 100px;
                height: auto;
            }}
            h1 {{
                text-align: center;
                color: #333;
                margin-bottom: 10px;
                padding-top: 0px;
            }}
            h2 {{
                text-align: center;
                color: #0078d7;
                margin-bottom: 5px;
            }}
            .date-range {{
                text-align: center;
                color: #666;
                font-size: 16px;
                margin-bottom: 20px;
            }}
            .profit-section {{
                text-align: center;
                margin: 15px 0;
            }}
            .profit-label {{
                font-size: 18px;
                color: #666;
                display: inline-block;
                margin-right: 10px;
            }}
            .profit-value {{
                font-size: 20px;
                font-weight: bold;
                color: #28a745;
            }}
            .profit-margin {{
                font-size: 20px;
                font-weight: bold;
                color: #0078d7;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
                background-color: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
            }}
            th, td {{
                padding: 10px 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }}
            th {{
                background-color: #0078d7;
                color: white;
                text-transform: uppercase;
            }}
            tr:hover {{ background-color: #f1f1f1; }}
            .table-title {{
                color: #0078d7;
                font-size: 22px;
                font-weight: bold;
                text-align: center;
                margin: 20px 0 10px;
            }}
        </style>
    </head>
    <body>
        <img src="file:///home/azureuser/azure_analysis_algorithm/tns.png" class="logo" alt="Company Logo">
        <h1>📊 Weekly Store Report — {store_name}</h1>
        <div class="date-range">Week: {week_start_str} to {week_end_str}</div>
        <h2>Total Weekly Sales: ₹{total_weekly_sales:,.2f}</h2>
        
        <div class="profit-section">
            <span class="profit-label">Total Profit:</span>
            <span class="profit-value">₹{total_weekly_profit:,.2f}</span>
            <span style="margin: 0 15px;">|</span>
            <span class="profit-label">Profit Margin:</span>
            <span class="profit-margin">{profit_margin_percent:.2f}%</span>
        </div>
        
        {comparison_text}

        <div class="table-title">Top 50 Brands (by Sales)</div>
        {brand_df.to_html(index=False, classes="styled-table")}
        {brand_chart}

        <div class="table-title">Top 50 Categories (by Sales)</div>
        {category_df.to_html(index=False, classes="styled-table")}
        {category_chart}

        <div class="table-title">Top 100 Products (by Sales)</div>
        {product_df.to_html(index=False, classes="styled-table")}
        {product_chart}
    </body>
    </html>
    """

    # Save PDF
    os.makedirs("store_reports", exist_ok=True)
    pdf_path = os.path.join("store_reports", f"{store_name.replace(' ', '_')}_weekly_report.pdf")
    pdfkit.from_string(html_template, pdf_path, configuration=PDFKIT_CONFIG, options={"enable-local-file-access": ""})
    print(f"✅ Saved {store_name} report → {pdf_path}")


if __name__ == "__main__":
    store_names = get_unique_stores()
    print(f"Found {len(store_names)} stores.\nGenerating reports...\n")

    for store in store_names:
        try:
            generate_store_report(store)
            time.sleep(1)
        except Exception as e:
            print(f"❌ Error generating report for {store}: {e}")

    print("\n✅ All store reports generated successfully inside /store_reports/")