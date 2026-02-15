#!/bin/bash
#
# WhatsApp PDF Sender - Simple & Complete
#

# Configuration
LOG_FILE="/home/azureuser/logs/wa_sender.log"
SUCCESS_FILE="/home/azureuser/logs/wa_sent_dates.txt"
WORK_DIR="/home/azureuser/azure_analysis_algorithm"
PYTHON_BIN="${WORK_DIR}/vmac/bin/python"
PDF_DATE=$(date -d "yesterday" '+%Y-%m-%d')
MAX_RETRIES=2
RETRY_DELAY=60

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Main execution
main() {
    # Create log directory
    mkdir -p "$(dirname "$LOG_FILE")"

    log "=========================================="
    log "WhatsApp PDF Sender"
    log "Date: $PDF_DATE"
    log "=========================================="

    # Check if already sent
    if [ -f "$SUCCESS_FILE" ] && grep -qx "$PDF_DATE" "$SUCCESS_FILE"; then
        log "✓ Already sent for $PDF_DATE - Skipping"
        exit 0
    fi

    # Change to work directory
    cd "$WORK_DIR" || {
        log "❌ Cannot change to directory: $WORK_DIR"
        exit 1
    }

    # Try sending with retries
    for attempt in $(seq 1 $MAX_RETRIES); do
        log "Attempt $attempt/$MAX_RETRIES..."

        if "$PYTHON_BIN" wa_sender.py; then
            # Success - record it
            mkdir -p "$(dirname "$SUCCESS_FILE")"
            echo "$PDF_DATE" >> "$SUCCESS_FILE"

            # Keep only last 90 days
            if [ -f "$SUCCESS_FILE" ]; then
                tail -90 "$SUCCESS_FILE" > "${SUCCESS_FILE}.tmp" && mv "${SUCCESS_FILE}.tmp" "$SUCCESS_FILE"
            fi

            log "✅ SUCCESS - PDF sent for $PDF_DATE"
            log "=========================================="
            exit 0
        fi

        log "❌ Attempt $attempt failed"

        # Wait before retry (except last attempt)
        if [ $attempt -lt $MAX_RETRIES ]; then
            log "Waiting ${RETRY_DELAY}s before retry..."
            sleep $RETRY_DELAY
        fi
    done

    # All attempts failed
    log "❌ FAILED after $MAX_RETRIES attempts"
    log "Check error files in: $WORK_DIR"
    log "=========================================="
    exit 1
}

# Run main
main
