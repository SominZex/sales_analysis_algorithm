#!/bin/bash

# Navigate to the project directory
cd /home/azureuser/azure_analysis_algorithm

# Activate virtual environment
source vmac/bin/activate

# Run the Python script
python weekly_reports.py >> /home/azureuser/azure_analysis_algorithm/cron.log 2>&1

# Deactivate virtual environment
deactivate
