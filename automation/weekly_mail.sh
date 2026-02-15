#!/bin/bash

# --- Weekly Email Sender Script ---

SCRIPT_DIR="/home/azureuser/azure_analysis_algorithm"
VENV_PATH="$SCRIPT_DIR/vmac"
LOG_DIR="$SCRIPT_DIR/logs"
LOG_FILE="$LOG_DIR/weekly_mail_cron.log"
FLAG_FILE="$SCRIPT_DIR/weekly_ready.flag"

mkdir -p "$LOG_DIR"

echo "========================================" >> "$LOG_FILE"
echo "Starting Weekly Email Sender at $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# ---------- WAIT UNTIL REPORTS ARE READY ----------

MAX_WAIT=7200          # Max wait: 2 hours
WAITED=0
SLEEP_INTERVAL=30      # Check every 30 sec

echo "Waiting for weekly_ready.flag..." >> "$LOG_FILE"

while [ ! -f "$FLAG_FILE" ]; do
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "ERROR: Reports not ready after 2 hours. Aborting mail." >> "$LOG_FILE"
        exit 1
    fi

    sleep $SLEEP_INTERVAL
    WAITED=$((WAITED + SLEEP_INTERVAL))
done

echo "Reports are ready. Proceeding to send weekly emails." >> "$LOG_FILE"


# ---------- SEND EMAIL ----------

cd "$SCRIPT_DIR" || exit 1
source "$VENV_PATH/bin/activate"

python mail.py >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "" >> "$LOG_FILE"
echo "Finished sending emails at $(date), exit code: $EXIT_CODE" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

deactivate

# Remove flag after successful email
rm -f "$FLAG_FILE"

exit $EXIT_CODE
