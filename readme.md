# Flowchart
<img width="1920" height="1080" alt="flowchrt" src="https://github.com/user-attachments/assets/454e4bf6-a3bc-4a28-b916-6d92e24f8a59" />


## Fully Automated Sales Performance Dashboard
This application is a sales performance dashboard. The dashboard provides Daily, Weekly and monthly insights into store, category, brand, and product sales performance over a specified date range.
No manual execution is required to run this system if you setup the cron accordingly.

## ETL Feature: 
#### Realtime sales data is updated through API call from the mainframe database to the sandboxDB.
#### Necessary tables are updated daily.

## Daily Sale report automation:
#### Sales Data Visualization: Displays sales data for stores, categories, brands, and products.
#### Data Comparison: Compares the latest available sales data with the average sales from the previous 7 days.
#### Performance Metrics: Shows total sales and average weekly growth percentage.
#### Conditional Formatting: Highlights positive and negative growth with different colors.
#### Email and WhatsApp automation: Daily report is sent Via mail and shared in WhatsApp group.

## Weekly Sale report automation:
#### Weekly sale report is run every monday to generate weekly sale report then sent Via Automated mail to the respective business partners.

## Monthly Sale report Automation:
#### Monthly sale report is automatically generated every 1st day of the month and mail automation sends the reports via mail to the respective business partners.

## Installation
##### Clone the Repository:

#### git clone https://github.com/SominZex/sales_analysis_algorithm.git

#### cd sales_analysis_algorithm

## Create environment
#### python3 -m venv env_name

## Activate env
#### source env_name/bin/activate (for MAC and Linux)
#### env_name\Scripts\activate.bat (for windows)

## Install Dependencies:
#### pip install -r requirements.txt


## Run the Application:

### Etl pipeline Manual Run:
#### python ./etl/etl_pip.py

### Table update:
#### python ./etl/product_update.py

### To run daily analysis Manually
#### python analysis.py

### To run Weekly anlaysis Manually
#### python weekly_reports.py

### To run monthly analysis Manually
#### python monthly_reports.py

#### NB: Navigate to /monthly_query/date_utils.py and change the date and month range if you want to analyze using "python monthly.py"

## Cron Setup for automation:
### Navigate to terminal and type "crontab -e" (linux only) then paste the folllwing cron jobs (make sure you have the necessary shell script created in the directory, .sh files are not included here):
#### 30 3 * * * /home/azureuser/etl/vmac/bin/python /home/azureuser/etl/etl_pip.py >> /home/azureuser/etl/etl_pip.log 2>&1
#### 38 3 * * * /home/azureuser/etl/vmac/bin/python /home/azureuser/etl/product_update.py >> /home/azureuser/etl/product_update.log 2>&1
#### 45 3 * * * /home/azureuser/azure_analysis_algorithm/run_analysis.sh
#### 50 4 * * * /home/azureuser/azure_analysis_algorithm/wa_sender.sh >> /home/azureuser/logs/wa_sender_cron.log 2>&1
#### 05 5 * * 1 /home/azureuser/azure_analysis_algorithm/run_weekly_reports.sh
#### 15 6 * * 1 /home/azureuser/azure_analysis_algorithm/weekly_mail.sh
#### 10 7 1 * * /home/azureuser/azure_analysis_algorithm/monthly_reports.sh
#### 02 8 1 * * /home/azureuser/azure_analysis_algorithm/monthly_mail.sh

Contributing
Contributions are welcome! Please fork the repository and submit a pull request with your changes.
