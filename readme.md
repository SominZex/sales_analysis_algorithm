## Sales Performance Dashboard
This application is a sales performance dashboard built using Dash, a Python framework for building analytical web applications. The dashboard provides insights into store, category, brand, and product sales performance over a specified date range.

## Features
Sales Data Visualization: Displays sales data for stores, categories, brands, and products.
Date Range Selection: Users can select a start and end date to filter the sales data.
Data Comparison: Compares the latest available sales data with the average sales from the previous 7 days.
Performance Metrics: Shows total sales and average weekly growth percentage.
Conditional Formatting: Highlights positive and negative growth with different colors.

## Installation
Clone the Repository:

git clone https://github.com/SominZex/sales_analysis_algorithm.git

cd sales_analysis_algorithm

## Install Dependencies: Ensure you have Python installed, then install the required packages using:
pip install -r requirements.txt


## Run the Application: Start the Dash server by running:
### For daily analysis
python analysis.py

### For monthly analysis
python monthly.py

Navigate to /monthly_query/date_utils.py and change the date and month range you want to analyze

## Access the Dashboard: 
Open your web browser and navigate to http://127.0.0.1:8050 to view the dashboard.
Usage
Selecting Date Range
Use the date picker to select the start and end dates for the sales data you wish to analyze.
The dashboard will automatically update to reflect the selected date range.
Understanding the Dashboard
Store Performance: Displays sales data split into two tables for easier viewing.
Category Performance: Shows sales data for different categories, also split into two tables.
Brand Performance: Provides insights into brand sales, divided into two tables.
Product Performance: Lists product sales data, split into two tables for better readability.
Performance Metrics
Total Sales: Displays the total sales amount for the selected date range.
Average Weekly Growth: Shows the percentage growth compared to the previous week's average sales. Positive growth is highlighted in green, while negative growth is highlighted in red.
Error Handling
If there is an error fetching data, the dashboard will display an error message and default values.
Contributing
Contributions are welcome! Please fork the repository and submit a pull request with your changes.
