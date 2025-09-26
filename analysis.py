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
        # html.Div(style={'height': '80px'}),
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
        # html.Div(style={'height': '300px'}),

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


        html.Div(style={'height': '80px'}),
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
        html.Div(style={'height': '100px'}),
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


    html.Div(style={'height': '600px'}),

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

    # White space between tables and chart
    html.Div(style={'height': '500px'}),

    # Product Sales Chart below the tables
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


if __name__ == '__main__':
    app.run(debug=False, dev_tools_ui=False, dev_tools_props_check=False)