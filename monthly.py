import dash
from dash import dcc, html, dash_table
import pandas as pd
from monthly_query.category_performance import fetch_subcategory_data_monthly
from monthly_query.brand_performance import brand_sales
from monthly_query.product_performance import fetch_product_data_monthly
from monthly_query.store_performance import fetch_monthly_sales

# Initialize Dash app
app = dash.Dash(__name__)
app.config.suppress_callback_exceptions = True

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
# Fetch and process data
store_data = fetch_monthly_sales()
subcategory_data = fetch_subcategory_data_monthly()
brand_data = brand_sales()
product_data = fetch_product_data_monthly()


# Create layout
app.layout = html.Div([

    html.Div([
        html.H2("Monthly Sales Analysis (Feb-2025)", 
            style={
                'color': 'green',
                'fontSize': '32px',
                'fontWeight': 'bold',
                'marginBottom': '20px',
                'textAlign': 'center',
                'borderBottom': '4px solid #3498db',
                'paddingBottom': '10px'
            })
    ], style=HEADER_STYLE),

    # Note on Calculation
    html.Div([
        html.P("Sales performance is measured against the average sales of last two months.",
            style={
                'color': 'red',
                'fontSize': '20px',
                'textAlign': 'center',
                'marginBottom': '40px'
            })
    ]),

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
            # Left Table
            html.Div([
                dash_table.DataTable(
                    id='store-table-left',
                    columns=[
                        {"name": "S.No", "id": "S.No"},
                        {"name": "Store Name", "id": "Store"},
                        {"name": "Sales", "id": "Sales & Trend"}
                    ],
                    data=store_data[:len(store_data)//2].to_dict('records'),
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
                        {'if': {'column_id': 'Sales & Trend', 'filter_query': '{Sales & Trend} contains "🡅"'}, 
                        'color': '#006400', 'fontWeight': 'bold'}
                    ] + [
                        {'if': {'column_id': 'Sales & Trend', 'filter_query': '{Sales & Trend} contains "🡇"'}, 
                        'color': '#e74c3c', 'fontWeight': 'bold'}
                    ]
                )
            ], style={'flex': '1', 'textAlign': 'center'}),  

            # Right Table
            html.Div([
                dash_table.DataTable(
                    id='store-table-right',
                    columns=[
                        {"name": "S.No", "id": "S.No"},
                        {"name": "Store Name", "id": "Store"},
                        {"name": "Sales", "id": "Sales & Trend"}
                    ],
                    data=store_data[len(store_data)//2:].to_dict('records'),
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
                        {'if': {'column_id': 'Sales & Trend', 'filter_query': '{Sales & Trend} contains "🡅"'}, 
                        'color': '#006400', 'fontWeight': 'bold'}
                    ] + [
                        {'if': {'column_id': 'Sales & Trend', 'filter_query': '{Sales & Trend} contains "🡇"'}, 
                        'color': '#e74c3c', 'fontWeight': 'bold'}
                    ]
                )
            ], style={'flex': '1', 'textAlign': 'center'})  

        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '20px', 'width': '100%'})
    ], style=CARD_STYLE),
    # Category Performance Section
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
                data=subcategory_data[:len(subcategory_data)//2].to_dict('records'),
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
                data=subcategory_data[len(subcategory_data)//2:].to_dict('records'),
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
                    {'if': {'column_id': "Subcategory"}, 'textAlign': 'left'}
                ] + [
                    {'if': {'column_id': 'Sales', 'filter_query': '{Sales} contains "🡅"'}, 
                    'color': '#006400', 'fontWeight': 'bold'}
                ] + [
                    {'if': {'column_id': 'Sales', 'filter_query': '{Sales} contains "🡇"'}, 
                    'color': '#e74c3c', 'fontWeight': 'bold'}
                ]
            )
        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '20px'})
    ], style=CARD_STYLE),

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
            # Left Table
            dash_table.DataTable(
                id='brand-table-left',
                columns=[
                    {"name": "S.No", "id": "S.No"},
                    {"name": "Brand Name", "id": "Brand"},
                    {"name": "Sales", "id": "Sales & Trend"}
                ],
                data=brand_data[:len(brand_data)//2].to_dict('records'),
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
                    {'if': {'column_id': 'Sales & Trend', 'filter_query': '{Sales & Trend} contains "🡅"'}, 
                    'color': '#006400', 'fontWeight': 'bold'}
                ] + [
                    {'if': {'column_id': 'Sales & Trend', 'filter_query': '{Sales & Trend} contains "🡇"'}, 
                    'color': '#e74c3c', 'fontWeight': 'bold'}
                ]
            ),

            # Right Table
            dash_table.DataTable(
                id='brand-table-right',
                columns=[
                    {"name": "S.No", "id": "S.No"},
                    {"name": "Brand Name", "id": "Brand"},
                    {"name": "Sales", "id": "Sales & Trend"}
                ],
                data=brand_data[len(brand_data)//2:].to_dict('records'),
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
                    {'if': {'column_id': 'Sales & Trend', 'filter_query': '{Sales & Trend} contains "🡅"'}, 
                    'color': '#006400', 'fontWeight': 'bold'}
                ] + [
                    {'if': {'column_id': 'Sales & Trend', 'filter_query': '{Sales & Trend} contains "🡇"'}, 
                    'color': '#e74c3c', 'fontWeight': 'bold'}
                ]
            )
        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '20px'})
    ], style=CARD_STYLE),


    html.Div(style={'height': '800px'}),

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
        
        html.Div([
            # Left Table
            html.Div([
                dash_table.DataTable(
                    id='product-table-left',
                    columns=[
                        {"name": "S.No", "id": "S.No"},
                        {"name": "Product ID", "id": "Product ID"},
                        {"name": "Sales", "id": "Sales"},
                        {"name": "Quantity Sold", "id": "Quantity Sold"}
                    ],
                    data=product_data[:len(product_data)//2].to_dict('records'),
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
                         'color': '#006400', 'fontWeight': 'bold'} for col in ["Sales", "Quantity Sold"]
                    ] + [
                        {'if': {'column_id': col, 'filter_query': '{' + col + '} contains "🡇"'}, 
                         'color': '#e74c3c', 'fontWeight': 'bold'} for col in ["Sales", "Quantity Sold"]
                    ]
                )
            ], style={'width': '48%', 'max-width': '100%', 'overflowX': 'auto'}),

            # Right Table
            html.Div([
                dash_table.DataTable(
                    id='product-table-right',
                    columns=[
                        {"name": "S.No", "id": "S.No"},
                        {"name": "Product ID", "id": "Product ID"},
                        {"name": "Sales", "id": "Sales"},
                        {"name": "Quantity Sold", "id": "Quantity Sold"}
                    ],
                    data=product_data[len(product_data)//2:].to_dict('records'),
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
                         'color': '#006400', 'fontWeight': 'bold'} for col in ["Sales", "Quantity Sold"]
                    ] + [
                        {'if': {'column_id': col, 'filter_query': '{' + col + '} contains "🡇"'}, 
                         'color': '#e74c3c', 'fontWeight': 'bold'} for col in ["Sales", "Quantity Sold"]
                    ]
                )
            ], style={'width': '48%', 'max-width': '100%', 'overflowX': 'auto'})
        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '10px', 'margin': '0 auto', 'max-width': '100%'})
    ], style=CARD_STYLE)
], style={'backgroundColor': '#f4f4f4', 'padding': '20px'})

# Run the server
if __name__ == '__main__':
    app.run_server(debug=False, dev_tools_ui=False, dev_tools_props_check=False)