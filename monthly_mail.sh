#!/bin/bash
# Monthly Reports Email Sender Wrapper Script

# Set paths
SCRIPT_DIR="/home/azureuser/azure_analysis_algorithm"
VENV_PATH="$SCRIPT_DIR/vmac"
LOG_DIR="$SCRIPT_DIR/logs"
LOG_FILE="$LOG_DIR/monthly_mail_cron.log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Log start time
echo "========================================" >> "$LOG_FILE"
echo "Starting Monthly Email sender at $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Change to script directory
cd "$SCRIPT_DIR" || exit 1

# Activate virtual environment
source "$VENV_PATH/bin/activate"

# Run the Python script and capture output
python monthly_mail.py >> "$LOG_FILE" 2>&1

# Log completion
EXIT_CODE=$?
echo "" >> "$LOG_FILE"
echo "Finished at $(date) with exit code: $EXIT_CODE" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Deactivate virtual environment
deactivate

exit $EXIT_CODE
