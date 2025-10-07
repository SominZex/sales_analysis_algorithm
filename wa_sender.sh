#!/bin/bash

# Set environment
export DISPLAY=:0
export PATH=/usr/local/bin:/usr/bin:/bin:$PATH
export HOME=/home/azureuser

# Set working directory
cd /home/azureuser/azure_analysis_algorithm

# Run with correct Python path
echo "=== Execution started at $(date) UTC ===" >> /home/azureuser/cron_test.log
/home/azureuser/azure_analysis_algorithm/vmac/bin/python wa_sender.py >> /home/azureuser/cron_test.log 2>&1
echo "=== Execution ended at $(date) UTC ===" >> /home/azureuser/cron_test.log
echo "" >> /home/azureuser/cron_test.log
