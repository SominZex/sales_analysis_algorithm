import dash
from dash import dcc, html, dash_table
import pandas as pd
from dash.dependencies import Input, Output, State
from queries.store_performance import fetch_sales_data
from queries.category_performance import fetch_subcategory_data
from queries.brand_performance import fetch_brand_data
from queries.product_performance import fetch_product_data
from queries.store_performance import fetch_total_sales
from queries.common import get_last_date
from datetime import datetime
from queries.store_performance import fetch_sales_data, create_store_sales_chart
from queries.category_performance import create_category_sales_chart
from queries.brand_performance import create_brand_sales_bar_chart
from queries.product_performance import create_product_sales_bar_chart

import os
from datetime import timedelta
import asyncio
import threading
import time
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from playwright.async_api import async_playwright
import psycopg2
import uuid
from werkzeug.serving import make_server
import os
from dotenv import load_dotenv

load_dotenv()


app = dash.Dash(__name__)
app.config.suppress_callback_exceptions = True



# Default date range (last 8 days)
default_end_date = get_last_date()
default_start_date = default_end_date - pd.Timedelta(days=8)

# Common styles
CARD_STYLE = {
    'backgroundColor': 'white',
    'borderRadius': '10px',
    'boxShadow': '0 4px 6px rgba(0, 0, 0, 0.1)',
    'padding': '20px',
    'margin': '20px 0',
}

TABLE_STYLE = {
    'width': '100%',
    'display': 'inline-block',
    'margin': 'auto',
    'backgroundColor': 'white',
    'borderRadius': '8px',
    'overflow': 'hidden'
}

HEADER_STYLE = {
    'backgroundColor': '#f8f9fa',
    'padding': '15px',
    'marginBottom': '20px',
    'borderBottom': '2px solid #eee',
    'textAlign': 'center'
}

EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'sender_email': 'email_id',
    'sender_password': 'app_pw',
    'to': 'to_mail',
    'cc_recipients': ['cc', 'mails'],
    'tracking_host': 'tracking_mail',
    'summary_recipient': 'mail'
}


DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 5432))


PG_CONFIG = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT
}



app.layout = html.Div([
    # Main container with background color
    html.Div([
        html.Div([
            html.H2("Daily Sales Report", 
                   style={
                       'color': '#2c3e50',
                       'fontSize': '32px',
                       'fontWeight': 'bold',
                       'marginBottom': '10px'
                   }),
            html.P(id="last-date-display", 
                  style={
                      'color': '#7f8c8d',
                      'fontSize': '16px'
                  }),
        ], style=HEADER_STYLE),

        # Hidden date picker
        html.Div([
            dcc.DatePickerRange(
                id='date-picker',
                start_date=default_start_date,
                end_date=default_end_date,
                display_format='YYYY-MM-DD'
            )
        ], style={'display': 'none'}),

        html.Div([
            html.H4(id="total-sales-display",
                style={
                    'color': '#27ae60',
                    'fontSize': '30px',
                    'fontWeight': 'bold',
                    'textAlign': 'center'
                }),
            html.P(id="weekly-growth-display",
                style={
                    'color': '#2980b9',
                    'fontSize': '20px',
                    'fontWeight': 'bold',
                    'textAlign': 'center'
                }),
        ], style={'marginBottom': '10px'}),


        # Store Performance Section
html.Div([
        html.H3("Store Performance", 
            style={
                'color': '#2c3e50',
                'fontSize': '24px',
                'marginBottom': '20px',
                'borderBottom': '4px solid #3498db',
                'textAlign': 'center',
                'paddingBottom': '10px'
            }),

        html.Div([
            html.Div([
                dash_table.DataTable(
                    id='store-table-left',
                    columns=[
                        {"name": "S.No", "id": "S.No"},
                        {"name": "Store Name", "id": "Store Name"},
                        {"name": "Number of Orders", "id": "Number of Orders"},
                        {"name": "Sales", "id": "Sales"},
                        {"name": "Average Order Value", "id": "Average Order Value"}
                    ],
                    style_table={'width': '100%', 'overflowX': 'auto'},
                    style_cell={
                        'textAlign': 'center',
                        'padding': '6px',
                        'fontSize': '14px'
                    },
                    style_header={
                        'backgroundColor': '#fff3cd',
                        'fontWeight': 'bold',
                        'borderBottom': '2px solid #dee2e6'
                    },
                    style_data={
                        'backgroundColor': 'white',
                        'border': '1px solid #f0f0f0'
                    },
                    style_data_conditional=[
                        {'if': {'row_index': 'odd'}, 'backgroundColor': '#fff9e6'},
                        {'if': {'column_id': "Store Name"}, 'textAlign': 'left'}
                    ] + [
                        {'if': {'column_id': col, 'filter_query': '{{{}}} contains "🡅"'.format(col)},
                        'color': '#006400', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Average Order Value"]
                    ] + [
                        {'if': {'column_id': col, 'filter_query': '{{{}}} contains "🡇"'.format(col)},
                        'color': '#e74c3c', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Average Order Value"]
                    ]
                )
            ], style={'flex': '1', 'textAlign': 'center'}),  

            html.Div([
                dash_table.DataTable(
                    id='store-table-right',
                    columns=[
                        {"name": "S.No", "id": "S.No"},
                        {"name": "Store Name", "id": "Store Name"},
                        {"name": "Number of Orders", "id": "Number of Orders"},
                        {"name": "Sales", "id": "Sales"},
                        {"name": "Average Order Value", "id": "Average Order Value"}
                    ],
                    style_table={'width': '100%', 'overflowX': 'auto'}, 
                    style_cell={
                        'textAlign': 'center',
                        'padding': '6px',
                        'fontSize': '14px'
                    },
                    style_header={
                        'backgroundColor': '#fff3cd',
                        'fontWeight': 'bold',
                        'borderBottom': '2px solid #dee2e6'
                    },
                    style_data={
                        'backgroundColor': 'white',
                        'border': '1px solid #f0f0f0'
                    },
                    style_data_conditional=[
                        {'if': {'row_index': 'odd'}, 'backgroundColor': '#fff9e6'},
                        {'if': {'column_id': "Store Name"}, 'textAlign': 'left'}
                    ] + [
                        {'if': {'column_id': col, 'filter_query': '{{{}}} contains "🡅"'.format(col)},
                        'color': '#006400', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Average Order Value"]
                    ] + [
                        {'if': {'column_id': col, 'filter_query': '{{{}}} contains "🡇"'.format(col)},
                        'color': '#e74c3c', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Average Order Value"]
                    ]
                )
            ], style={'flex': '1', 'textAlign': 'center'})  

        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '20px', 'width': '100%'}),
        # --- Add the chart right below the tables ---
        html.Div(style={'height': '80px'}),
        html.Div([
            dcc.Graph(
                id='store-sales-chart',
                config={'displayModeBar': False}
            )
        ], style={
            'display': 'flex',
            'justifyContent': 'center',
            'alignItems': 'center',
            'marginTop': '10px',
            'marginBottom': '5px',
            'width': '100%'
        }),
        ], style={'marginTop': '30px', 'marginBottom': '10px', 'width': '100%'})

    ], style=CARD_STYLE),
        # Category Performance Section
        html.Div(style={'height': '700px'}),

        html.Div([
            html.H3("Category Performance", 
                style={
                    'color': '#2c3e50',
                    'fontSize': '24px',
                    'marginBottom': '20px',
                    'paddingLeft': '10px',
                    'borderLeft': '4px solid #e67e22',
                    'borderBottom': '4px solid #3498db',
                    'textAlign': 'center'
                }),
            
            html.Div([
                # Left Table
                dash_table.DataTable(
                    id='category-table-left',
                    columns=[
                        {"name": "S.No", "id": "S.No"},
                        {"name": "Subcategory", "id": "Subcategory"},
                        {"name": "Sales", "id": "Sales"}
                    ],
                    style_table=TABLE_STYLE,
                    style_cell={
                        'textAlign': 'center',
                        'padding': '5px',
                        'fontSize': '12px'
                    },
                    style_header={
                        'backgroundColor': '#fff3cd',
                        'fontWeight': 'bold',
                        'borderBottom': '2px solid #dee2e6'
                    },
                    style_data={
                        'backgroundColor': 'white',
                        'border': '1px solid #f0f0f0'
                    },
                    style_data_conditional=[
                        {'if': {'row_index': 'odd'}, 'backgroundColor': '#fff9e6'},
                        {'if': {'column_id': "Subcategory"}, 'textAlign': 'left'}
                    ] + [
                        {'if': {'column_id': 'Sales', 'filter_query': '{Sales} contains "🡅"'}, 
                        'color': '#006400', 'fontWeight': 'bold'}
                    ] + [
                        {'if': {'column_id': 'Sales', 'filter_query': '{Sales} contains "🡇"'}, 
                        'color': '#e74c3c', 'fontWeight': 'bold'}
                    ]
                ),

                # Right Table
                dash_table.DataTable(
                    id='category-table-right',
                    columns=[
                        {"name": "S.No", "id": "S.No"},
                        {"name": "Subcategory", "id": "Subcategory"},
                        {"name": "Sales", "id": "Sales"}
                    ],
                    style_table=TABLE_STYLE,
                    style_cell={
                        'textAlign': 'center',
                        'padding': '5px',
                        'fontSize': '12px'
                    },
                    style_header={
                        'backgroundColor': '#fff3cd',
                        'fontWeight': 'bold',
                        'borderBottom': '2px solid #dee2e6'
                    },
                    style_data={
                        'backgroundColor': 'white',
                        'border': '1px solid #f0f0f0'
                    },
                    style_data_conditional=[
                        {'if': {'row_index': 'odd'}, 'backgroundColor': '#fff9e6'},
                        {'if': {'column_id': "Subcategory"}, 'textAlign': 'left'}
                    ] + [
                        {'if': {'column_id': 'Sales', 'filter_query': '{Sales} contains "🡅"'}, 
                        'color': '#006400', 'fontWeight': 'bold'}
                    ] + [
                        {'if': {'column_id': 'Sales', 'filter_query': '{Sales} contains "🡇"'}, 
                        'color': '#e74c3c', 'fontWeight': 'bold'}
                    ]
                ),

                # Chart to the right of the tables
                html.Div([
                    dcc.Graph(
                        id='category-sales-chart',
                        config={'displayModeBar': False},
                        style={'height': '380px', 'width': '300px'} 
                    )
                ], style={
                    'display': 'flex',
                    'alignItems': 'left',
                    'justifyContent': 'flex-start',
                    'marginLeft': '10px',
                    'minWidth': '340px',
                    'maxWidth': '340px'
                }),
            ], style={
                'display': 'flex',
                'justifyContent': 'flex-start',
                'gap': '10px',
                'alignItems': 'flex-start',
                'width': '100%'
            })
        ], style=CARD_STYLE),


        html.Div(style={'height': '300px'}),
# Brand Performance Section
        html.Div([  
            html.H3("Brand Performance", 
                style={
                    'color': '#2c3e50',
                    'fontSize': '24px',
                    'marginBottom': '20px',
                    'paddingLeft': '10px',
                    'borderLeft': '4px solid #9b59b6',
                    'borderBottom': '4px solid #3498db',
                    'textAlign': 'center'
                }),
            
            html.Div([
        # Brand tables row
        html.Div([
            dash_table.DataTable(
                id='brand-table-left',
                columns=[
                    {"name": "S.No", "id": "S.No"},
                    {"name": "Brand Name", "id": "Brand Name"},
                    {"name": "Number of Orders", "id": "Number of Orders"},
                    {"name": "Sales", "id": "Sales"},
                    {"name": "Average Order Value", "id": "Average Order Value"}
                ],
                style_table=TABLE_STYLE,
                style_cell={
                    'textAlign': 'center',
                    'padding': '6px',
                    'fontSize': '14px'
                },
                style_header={
                    'backgroundColor': '#fff3cd',
                    'fontWeight': 'bold',
                    'borderBottom': '2px solid #dee2e6'
                },
                style_data={
                    'backgroundColor': 'white',
                    'border': '1px solid #f0f0f0'
                },
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': '#fff9e6'},
                    {'if': {'column_id': "Brand Name"}, 'textAlign': 'left'}
                ] + [
                    {'if': {'column_id': col, 'filter_query': '{' + col + '} contains "🡅"'}, 
                    'color': '#006400', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Average Order Value"]
                ] + [
                    {'if': {'column_id': col, 'filter_query': '{' + col + '} contains "🡇"'}, 
                    'color': '#e74c3c', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Average Order Value"]
                ]
            ),
            dash_table.DataTable(
                id='brand-table-right',
                columns=[
                    {"name": "S.No", "id": "S.No"},
                    {"name": "Brand Name", "id": "Brand Name"},
                    {"name": "Number of Orders", "id": "Number of Orders"},
                    {"name": "Sales", "id": "Sales"},
                    {"name": "Average Order Value", "id": "Average Order Value"}
                ],
                style_table=TABLE_STYLE,
                style_cell={
                    'textAlign': 'center',
                    'padding': '6px',
                    'fontSize': '14px'
                },
                style_header={
                    'backgroundColor': '#fff3cd',
                    'fontWeight': 'bold',
                    'borderBottom': '2px solid #dee2e6'
                },
                style_data={
                    'backgroundColor': 'white',
                    'border': '1px solid #f0f0f0'
                },
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': '#fff9e6'},
                    {'if': {'column_id': "Brand Name"}, 'textAlign': 'left'}
                ] + [
                    {'if': {'column_id': col, 'filter_query': '{' + col + '} contains "🡅"'}, 
                    'color': '#006400', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Average Order Value"]
                ] + [
                    {'if': {'column_id': col, 'filter_query': '{' + col + '} contains "🡇"'}, 
                    'color': '#e74c3c', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Average Order Value"]
                ]
            )
        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '20px'}),

        # --- Add the chart right below the tables ---
        
        html.Div(style={'height': '230px'}),
        html.Div([
            dcc.Graph(
                id='brand-sales-chart',
                config={'displayModeBar': False},
                style={'height': '500px', 'width': '800px', 'margin': '0 auto'}
            )
        ], style={
            'display': 'flex',
            'justifyContent': 'flex-start',
            'alignItems': 'center',
            'marginTop': '30px',
            'marginBottom': '5px',
            'width': '80%'
        }),
    ], style=CARD_STYLE),


    html.Div(style={'height': '640px'}),

    # Product Performance Section
    html.Div([
        html.H3("Product Performance", 
            style={
                'color': '#2c3e50',
                'fontSize': '24px',
                'marginBottom': '20px',
                'paddingLeft': '10px',
                'borderLeft': '4px solid #9b59b6',
                'borderBottom': '4px solid #3498db',
                'textAlign': 'center'
            }),
        # Tables row
    html.Div([
        # Left Table
        html.Div([
            dash_table.DataTable(
                id='product-table-left',
                columns=[
                    {"name": "S.No", "id": "S.No"},
                    {"name": "Product Name", "id": "Product Name"},
                    {"name": "Sales", "id": "Sales"},
                    {"name": "Quantity Sold", "id": "Quantity Sold"}
                ],
                style_table={'width': '100%', 'overflowX': 'auto'},
                style_cell={
                    'textAlign': 'center',
                    'padding': '6px',
                    'fontSize': '14px'
                },
                style_header={
                    'backgroundColor': '#fff3cd',
                    'fontWeight': 'bold',
                    'borderBottom': '2px solid #dee2e6'
                },
                style_data={
                    'backgroundColor': 'white',
                    'border': '1px solid #f0f0f0'
                },
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': '#fff9e6'},
                    {'if': {'column_id': "Product Name"}, 'textAlign': 'left'}
                ] + [
                    {'if': {'column_id': col, 'filter_query': '{' + col + '} contains "🡅"'}, 
                     'color': '#006400', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Quantity Sold"]
                ] + [
                    {'if': {'column_id': col, 'filter_query': '{' + col + '} contains "🡇"'}, 
                     'color': '#e74c3c', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Quantity Sold"]
                ]
            )
        ], style={'width': '48%', 'max-width': '100%', 'overflowX': 'auto'}),

        # Right Table
        html.Div([
            dash_table.DataTable(
                id='product-table-right',
                columns=[
                    {"name": "S.No", "id": "S.No"},
                    {"name": "Product Name", "id": "Product Name"},
                    {"name": "Sales", "id": "Sales"},
                    {"name": "Quantity Sold", "id": "Quantity Sold"}
                ],
                style_table={'width': '100%', 'overflowX': 'auto'},
                style_cell={
                    'textAlign': 'center',
                    'padding': '6px',
                    'fontSize': '14px'
                },
                style_header={
                    'backgroundColor': '#fff3cd',
                    'fontWeight': 'bold',
                    'borderBottom': '2px solid #dee2e6' 
                },
                style_data={
                    'backgroundColor': 'white',
                    'border': '1px solid #f0f0f0'
                },
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': '#fff9e6'},
                    {'if': {'column_id': "Product Name"}, 'textAlign': 'left'}
                ] + [
                    {'if': {'column_id': col, 'filter_query': '{' + col + '} contains "🡅"'}, 
                     'color': '#006400', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Quantity Sold"]
                ] + [
                    {'if': {'column_id': col, 'filter_query': '{' + col + '} contains "🡇"'}, 
                     'color': '#e74c3c', 'fontWeight': 'bold'} for col in ["Number of Orders", "Sales", "Quantity Sold"]
                ]
            )
        ], style={'width': '48%', 'max-width': '100%', 'overflowX': 'auto'})
    ], style={
        'display': 'flex',
        'justifyContent': 'center',
        'gap': '10px',
        'margin': '0 auto',
        'max-width': '100%'
    }),


        html.Div([
            dcc.Graph(
                id='product-sales-chart',
                config={'displayModeBar': False},
                style={'height': '700px', 'width': '900px', 'marginLeft': '0'}
            )
        ], style={
            'display': 'flex',
            'justifyContent': 'flex-start', 
            'alignItems': 'center',
            'marginTop': '10px',
            'marginBottom': '5px',
            'width': '100%'
        }),
], style=CARD_STYLE),

])
])


def log_event(recipient, report_date, event):
    """Insert tracking event into PostgreSQL table"""
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tracking (recipient, report_date, event) VALUES (%s, %s, %s)",
        (recipient, report_date, event)
    )
    conn.commit()
    cur.close()
    conn.close()


async def save_pdf():
    os.makedirs("reports", exist_ok=True)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    file_path = os.path.join("reports", f"sales_report_{yesterday}.pdf")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Set longer timeout
        page.set_default_timeout(90000)
        
        print("Loading page...")
        await page.goto("http://127.0.0.1:8050", wait_until="networkidle", timeout=60000)
        
        print("Waiting for data to load...")
        
        # Wait for the specific content that indicates data has loaded
        # Wait for the total sales display to appear with actual data
        try:
            await page.wait_for_function(
                """() => {
                    const totalSales = document.querySelector('#total-sales-display');
                    return totalSales && totalSales.textContent.includes('Total Sales');
                }""",
                timeout=30000
            )
            print("Total sales data loaded")
        except:
            print("Warning: Total sales not found, continuing...")
        
        # Wait for tables to have data (not just loading state)
        try:
            await page.wait_for_function(
                """() => {
                    const tables = document.querySelectorAll('table tbody tr');
                    return tables.length > 5;
                }""",
                timeout=30000
            )
            print("Tables loaded with data")
        except:
            print("Warning: Tables may not be fully loaded")
        
        # Wait for charts to render (Plotly creates svg elements)
        try:
            await page.wait_for_selector('.js-plotly-plot .plotly', timeout=30000)
            print("Charts loaded")
        except:
            print("Warning: Charts may not be fully loaded")
        
        # Additional wait to ensure everything is rendered
        print("Waiting additional 20 seconds for complete rendering...")
        await asyncio.sleep(20)
        
        # Verify content before generating PDF
        content = await page.content()
        if "Loading..." in content and "Total Sales" not in content:
            print("WARNING: Page still showing loading state!")
        
        print("Generating PDF...")
        await page.pdf(
            path=file_path,
            format="A3",
            landscape=True,
            margin={"top": "0mm", "bottom": "0mm", "left": "0mm", "right": "0mm"},
            scale=1.0,
            print_background=True
        )
        await browser.close()
        print(f"PDF saved as {file_path}")
    return file_path, yesterday

def send_email_with_attachment():
    """Send email with PDF attachment and tracking pixel/link"""
    try:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        file_path = os.path.join("reports", f"sales_report_{yesterday}.pdf")
        
        if not os.path.exists(file_path):
            print(f"PDF file not found: {file_path}")
            return False

        # Create single email with TO and CC
        msg = MIMEMultipart('alternative')
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['to']  # data@newshop.in
        msg['Cc'] = ', '.join(EMAIL_CONFIG['cc_recipients'])  # All other recipients in CC
        msg['Subject'] = f"Daily Sales Report - {yesterday}"

        # Unique ID for this email for robust tracking
        unique_id = str(uuid.uuid4())

        # Tracking pixel URL for email opens
        tracking_pixel = f"{EMAIL_CONFIG['tracking_host']}/track_open/{unique_id}?recipient={EMAIL_CONFIG['to']}&report={yesterday}"

        # Optional PDF download link for click tracking
        download_link = f"{EMAIL_CONFIG['tracking_host']}/download/{unique_id}?recipient={EMAIL_CONFIG['to']}&report={yesterday}"

        # Email Body
        html_body = f"""
        <p>Dear Team,</p>
        <p>Please find attached the daily sales report for {yesterday}.</p>
        <img src="{tracking_pixel}" width="1" height="1" style="display:none;">
        <p>Best regards,<br>Automated Reporting System</p>
        """
        msg.attach(MIMEText(html_body, 'html'))

        # Attach PDF
        with open(file_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
        msg.attach(part)

        # Send to all recipients (TO + CC)
        all_recipients = [EMAIL_CONFIG['to']] + EMAIL_CONFIG['cc_recipients']
        
        # Server Config
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        server.sendmail(EMAIL_CONFIG['sender_email'], all_recipients, msg.as_string())
        server.quit()

        print(f"Email sent to {EMAIL_CONFIG['to']} with CC to {len(EMAIL_CONFIG['cc_recipients'])} recipients")

        # Log events for tracking
        log_event(EMAIL_CONFIG['to'], yesterday, "sent")
        for cc_recipient in EMAIL_CONFIG['cc_recipients']:
            log_event(cc_recipient, yesterday, "sent_cc")

        return True

    except Exception as e:
        print(f"Error sending email: {e}")
        return False

async def generate_and_send_report():
    await save_pdf()
    send_email_with_attachment()




@app.callback(
    [Output('store-table-left', 'data'),
     Output('store-table-right', 'data'),
     Output('store-sales-chart', 'figure'),
     Output('category-table-left', 'data'),
     Output('category-table-right', 'data'),
     Output('category-sales-chart', 'figure'),
     Output('brand-table-left', 'data'),
     Output('brand-table-right', 'data'),
     Output('brand-sales-chart', 'figure'),
     Output('product-table-left', 'data'),
     Output('product-table-right', 'data'),
     Output('product-sales-chart', 'figure'),
     Output('last-date-display', 'children'),
     Output('total-sales-display', 'children'),
     Output("weekly-growth-display", "children")],
    
    [Input('date-picker', 'start_date'), 
     Input('date-picker', 'end_date')]
)
def update_tables(start_date, end_date):
    try:
        # Convert string dates to datetime if necessary
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
            
        # Fetch sales data
        store_data, chart_data = fetch_sales_data(start_date, end_date)
        mid_index_store = len(store_data) // 2
        store_left = store_data.iloc[:mid_index_store]   
        store_right = store_data.iloc[mid_index_store:]

        fig = create_store_sales_chart(chart_data, top_n=30)

        category_data = fetch_subcategory_data(start_date, end_date)
        mid_index_category = len(category_data) // 2
        category_left = category_data.iloc[:mid_index_category]   
        category_right = category_data.iloc[mid_index_category:]
        category_fig = create_category_sales_chart(category_data, top_n=15)
        
        brand_data = fetch_brand_data(start_date, end_date)
        mid_index_brand = len(brand_data) // 2
        brand_left = brand_data.iloc[:mid_index_brand]
        brand_right = brand_data.iloc[mid_index_brand:]
        # Fixed: Pass brand_data to the chart function
        brand_fig = create_brand_sales_bar_chart(brand_data, top_n=30)
        
        product_data = fetch_product_data(start_date, end_date)
        mid_index_product = len(product_data) // 2
        product_left = product_data.iloc[:mid_index_product]
        product_right = product_data.iloc[mid_index_product:]
        product_fig = create_product_sales_bar_chart(product_data, top_n=30)
        
        last_date = get_last_date()
        
        formatted_date = last_date.strftime('%B %d, %Y')
        date_display = html.P([
            "📅 This report compares the latest available sales data with the average sales from the previous 7 days, Update: ",
            html.Span(formatted_date, style={'fontWeight': 'bold', 'fontSize': '18px', 'color': '#e74c3c'})
        ], style={'fontSize': '16px', 'color': '#2c3e50'})

        total_sales, weekly_growth = fetch_total_sales()
        total_sales_display = f"📊 Total Sales: ₹{total_sales:,.2f}" 

        growth_color = "#006400" if weekly_growth > 0 else "#e74c3c"
        weekly_growth_display = html.Span(
            f"📈 Avg Weekly Growth: {weekly_growth:.2f}%", 
            style={'fontWeight': 'bold', 'color': growth_color, 'fontSize': '20px'}
        )
        
        return (
            store_left.to_dict('records'),
            store_right.to_dict('records'),
            fig,
            category_left.to_dict('records'),
            category_right.to_dict('records'),
            category_fig,
            brand_left.to_dict('records'),
            brand_right.to_dict('records'),
            brand_fig,
            product_left.to_dict('records'),
            product_right.to_dict('records'),
            product_fig,
            date_display,
            total_sales_display,
            weekly_growth_display
        )
        
    except Exception as e: 
        print(f"Error: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return [], [], {}, [], [], {}, [], [], {}, [],[],{}, "Error fetching data", "N/A", "N/A"


def run_server():
    """Run the Dash server in a separate thread"""
    server = make_server('127.0.0.1', 8050, app.server)
    server.serve_forever()

async def generate_and_send_report():
    """Main function to orchestrate the entire process"""
    print("Starting Dash server...")
    
    # Start server in background thread
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # Wait for server to be ready
    print("Waiting for server to start...")
    max_retries = 60 
    for i in range(max_retries):
        try:
            response = requests.get("http://127.0.0.1:8050", timeout=5)
            if response.status_code == 200:
                print("Server is ready!")
                break
        except requests.exceptions.RequestException:
            if i % 10 == 0:
                print(f"Still waiting... ({i}s)")
            time.sleep(1)
    else:
        print("Server failed to start within timeout")
        return False
    
    # Give server more time to fully initialize
    print("Waiting for server to fully initialize...")
    time.sleep(10)
    
    try:
        # Generate PDF
        print("Generating PDF report...")
        await save_pdf()
        
        # Send email
        print("Sending email...")
        success = send_email_with_attachment()
        
        if success:
            print("Report generation and email sending completed successfully!")
        else:
            print("Error occurred during email sending")
            
    finally:
        # Terminate the process (this will kill the server)
        print("Shutting down server...")
        time.sleep(2)
        os._exit(0)
        
if __name__ == '__main__':
    asyncio.run(generate_and_send_report())
