import dash
from dash import dcc, html
from dash import dash_table
from dash.dependencies import Input, Output
import pandas as pd

# Import your functions from store_performance.py
from queries.store_performance import fetch_sales_data, create_store_sales_chart

# Initialize Dash app
app = dash.Dash(__name__)

# Layout
app.layout = html.Div([
    html.H2("Store Performance Table"),
    dash_table.DataTable(
        id='store-performance-table',
        columns=[
            {"name": "S.No", "id": "S.No"},
            {"name": "Store Name", "id": "Store Name"},
            {"name": "Number of Orders", "id": "Number of Orders"},
            {"name": "Sales", "id": "Sales"},
            {"name": "Average Order Value", "id": "Average Order Value"},
        ],
        data=[],  # Will be filled by callback
        style_table={'overflowX': 'auto', 'width': '100%'},
        style_cell={'textAlign': 'center', 'padding': '5px'},
        style_header={'backgroundColor': '#f2f2f2', 'fontWeight': 'bold'},
        page_size=30,
    ),
    html.Br(),
    html.H2("Top 30 Stores by Sales"),
    dcc.Graph(
        id='store-sales-chart'
    ),
    html.Br(),
    html.Button("Refresh Data", id="refresh-btn", n_clicks=0)
])

@app.callback(
    [Output('store-performance-table', 'data'),
     Output('store-sales-chart', 'figure')],
    [Input('refresh-btn', 'n_clicks')]
)
def update_store_performance(n_clicks):
    try:
        # Fetch data for the latest available date
        formatted_df, chart_data = fetch_sales_data()
        fig = create_store_sales_chart(chart_data, top_n=30)
        return formatted_df.to_dict('records'), fig
    except Exception as e:
        print("Error in update_store_performance:", e)
        # Return empty table and empty figure on error
        return [], {}

if __name__ == "__main__":
    app.run(debug=True)