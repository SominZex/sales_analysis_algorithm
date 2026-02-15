#!/bin/bash

# --- Weekly Report Generator Script ---

SCRIPT_DIR="/home/azureuser/azure_analysis_algorithm"
VENV_PATH="$SCRIPT_DIR/vmac"
LOG_FILE="$SCRIPT_DIR/cron.log"
FLAG_FILE="$SCRIPT_DIR/weekly_ready.flag"

# Remove old flag if exists
rm -f "$FLAG_FILE"

# Navigate to project
cd "$SCRIPT_DIR" || exit 1

# Activate virtual environment
source "$VENV_PATH/bin/activate"

echo "========================================" >> "$LOG_FILE"
echo "Starting Weekly Report Generation at $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Run report generator
python weekly_reports.py >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "Weekly Report Generation Finished at $(date), exit code: $EXIT_CODE" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Only create flag if Python exited cleanly
if [ $EXIT_CODE -eq 0 ]; then
    touch "$FLAG_FILE"
else
    echo "ERROR: Report generation failed — NOT creating ready flag" >> "$LOG_FILE"
fi

# Deactivate
deactivate

exit $EXIT_CODE
