# Sales Analysis Automation System
# Flowchart
<img width="1920" height="1080" alt="flowchrt" src="https://github.com/user-attachments/assets/454e4bf6-a3bc-4a28-b916-6d92e24f8a59" />


## Overview
This project is a fully automated sales reporting system designed to generate daily, weekly, and monthly business performance reports in PDF format and automatically distribute them to stakeholders via Email and WhatsApp.
The system is designed to run without manual intervention once deployed and scheduled via cron jobs on an Azure Virtual Machine.
This project intentionally does not include any interactive dashboard or UI.
All insights are delivered automatically through scheduled reports.

## Key Capabilities 
#### -- Automated ETL pipeline for near real-time sales data
#### -- Scheduled report generation (Daily / Weekly / Monthly)
#### -- Business KPI computation and comparison logic
#### -- PDF report generation
#### -- Automated distribution via Email and WhatsApp
#### -- Cron-based orchestration on Azure VM
#### -- Logging for traceability and debugging

## ETL Pipeline
### Data Ingestion
#### -- Sales data is fetched via API calls from the mainframe database
#### -- Data is loaded into a sandbox / analytics database

### Data Update
#### -- Required tables are updated daily
#### -- Product master data updates are handled independently

## Daily Sales Report Automation
The daily report provides a short-term performance snapshot and is automatically generated and distributed every day.

### Included Metrics
#### Store-wise sales
#### Category-wise sales
#### Brand-wise sales
#### Product-wise sales

### Analysis Logic
#### -- Compares the latest available sales data against the average of the previous 7 day
#### -- Calculates: Total Sales, Avg weekly growth percentage
#### -- Applies Conditional Formating to highlight positive and negative growth

### Delivery
#### -- PDF Report generated automatically
#### -- Report sent Via Email and WhatsApp

## Weekly Sales Report Automation
#### -- Executed every Monday
#### -- Aggregates sales performance for the previous week
#### -- Designed for business partners and management
#### -- Distributed automatically via Email

## Monthly Sales Report Automation
#### -- Executed on the 1st day of every month
#### -- Provides a consolidated view of monthly performance
#### -- Distributed automatically via Email
#### -- No manual execution required


## Installation & Setup
##### Clone the Repository:

```bash
git clone https://github.com/SominZex/sales_analysis_algorithm.git
```

```bash
cd sales_analysis_algorithm
```

## Create environment
```bash
python3 -m venv env_name
```

## Activate env
```bash
env_name\Scripts\activate.bat
```

## Install Dependencies:
```bash
pip install -r requirements.txt
```


## Run the Application:

### Manual Execution
```bash
python ./etl/etl_pip.py
```

### Product Table update:
```bash
python ./etl/product_update.py
```

### To run daily analysis Manually
```bash
python analysis.py
```

### To run Weekly anlaysis Manually
```bash
python weekly_reports.py
```

### To run monthly analysis Manually
```bash
python monthly_reports.py
```

## Cron Configuration
### Navigate to terminal and type "crontab -e" (linux only) then paste the folllwing cron jobs (make sure you have the necessary shell script created in the directory, .sh files are not included here):
#### 30 3 * * * /home/azureuser/etl/vmac/bin/python /home/azureuser/etl/etl_pip.py >> /home/azureuser/etl/etl_pip.log 2>&1
#### 38 3 * * * /home/azureuser/etl/vmac/bin/python /home/azureuser/etl/product_update.py >> /home/azureuser/etl/product_update.log 2>&1
#### 45 3 * * * /home/azureuser/azure_analysis_algorithm/run_analysis.sh
#### 50 4 * * * /home/azureuser/azure_analysis_algorithm/wa_sender.sh >> /home/azureuser/logs/wa_sender_cron.log 2>&1
#### 05 5 * * 1 /home/azureuser/azure_analysis_algorithm/run_weekly_reports.sh
#### 15 6 * * 1 /home/azureuser/azure_analysis_algorithm/weekly_mail.sh
#### 10 7 1 * * /home/azureuser/azure_analysis_algorithm/monthly_reports.sh
#### 02 8 1 * * /home/azureuser/azure_analysis_algorithm/monthly_mail.sh

## Logging & Monitoring
### All executions generate logs for:
#### -- ETL jobs
#### -- Report generation
#### -- Notification delivery

### Logs are used for:
#### -- Failure diagnosis
#### -- Audit trails
#### -- Operational monitoring

## CI/Automation
### GitHub Actions are configured for:
#### -- Code validation
#### -- Basic test execution
### -- Runtime automation is handled exclusively by cron on Azure VM

## Final Note
#### This system is not a dashboard.
#### It is a production-oriented, report-driven automation engine built to deliver business insights reliably, automatically, and on schedule.


