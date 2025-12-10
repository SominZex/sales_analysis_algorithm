#!/bin/bash
# Robust WhatsApp Sender Script with Retry Logic
# Features: Auto-retry, exponential backoff, email notifications, comprehensive logging

# ============================================================================
# CONFIGURATION
# ============================================================================
LOG_FILE="/home/azureuser/logs/wa_sender.log"
WORK_DIR="/home/azureuser/azure_analysis_algorithm"
PYTHON_BIN="/home/azureuser/azure_analysis_algorithm/vmac/bin/python"
PDF_DATE=$(date -d "yesterday" '+%Y-%m-%d')
PDF_PATH="${WORK_DIR}/reports/sales_report_${PDF_DATE}.pdf"

# Retry configuration
MAX_RETRIES=5                    # Maximum number of retry attempts
INITIAL_WAIT=60                  # Initial wait time in seconds (1 minute)
MAX_WAIT=600                     # Maximum wait time in seconds (10 minutes)
TIMEOUT_PER_ATTEMPT=600          # Timeout for each attempt (10 minutes)

# Optional: Email notification settings (uncomment to enable)
# ENABLE_EMAIL_NOTIFICATIONS=true
# NOTIFICATION_EMAIL="your-email@example.com"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log_separator() {
    echo "=========================================="
}

send_notification() {
    local subject="$1"
    local message="$2"
    
    if [ "$ENABLE_EMAIL_NOTIFICATIONS" = "true" ] && [ -n "$NOTIFICATION_EMAIL" ]; then
        echo "$message" | mail -s "$subject" "$NOTIFICATION_EMAIL" 2>/dev/null || true
    fi
}

calculate_wait_time() {
    local attempt=$1
    local wait_time=$((INITIAL_WAIT * (2 ** (attempt - 1))))
    
    # Cap at MAX_WAIT
    if [ $wait_time -gt $MAX_WAIT ]; then
        wait_time=$MAX_WAIT
    fi
    
    echo $wait_time
}

check_prerequisites() {
    local errors=0
    
    # Check Python binary
    if [ ! -f "$PYTHON_BIN" ]; then
        log_message "❌ ERROR: Python binary not found: $PYTHON_BIN"
        errors=$((errors + 1))
    fi
    
    # Check PDF file
    if [ ! -f "$PDF_PATH" ]; then
        log_message "❌ ERROR: PDF not found: $PDF_PATH"
        log_message "   Expected file for date: $PDF_DATE"
        errors=$((errors + 1))
    else
        local pdf_size=$(stat -f%z "$PDF_PATH" 2>/dev/null || stat -c%s "$PDF_PATH" 2>/dev/null)
        log_message "✓ PDF found: $PDF_PATH ($(numfmt --to=iec-i --suffix=B $pdf_size 2>/dev/null || echo "${pdf_size} bytes"))"
    fi
    
    # Check work directory
    if [ ! -d "$WORK_DIR" ]; then
        log_message "❌ ERROR: Work directory not found: $WORK_DIR"
        errors=$((errors + 1))
    fi
    
    # Check if Python script exists
    if [ ! -f "${WORK_DIR}/wa_sender.py" ]; then
        log_message "❌ ERROR: wa_sender.py not found in $WORK_DIR"
        errors=$((errors + 1))
    fi
    
    # Check WhatsApp session directory
    if [ ! -d "${WORK_DIR}/whatsapp" ]; then
        log_message "⚠️  WARNING: WhatsApp session directory not found. QR code scan may be required."
    fi
    
    return $errors
}

cleanup_old_debug_files() {
    # Clean up debug files older than 7 days to save space
    if [ -d "$WORK_DIR" ]; then
        find "$WORK_DIR" -type f \( -name "debug_*.png" -o -name "*.html" -o -name "*_screenshot.png" \) -mtime +7 -delete 2>/dev/null || true
    fi
}

setup_environment() {
    # Create log directory
    mkdir -p "$(dirname "$LOG_FILE")"
    
    # Set runtime directory to avoid chromium permission errors
    export XDG_RUNTIME_DIR="/tmp/runtime-$(id -u)"
    mkdir -p "$XDG_RUNTIME_DIR"
    chmod 700 "$XDG_RUNTIME_DIR"
    
    # Change to work directory
    cd "$WORK_DIR" || {
        log_message "❌ FATAL: Failed to change to $WORK_DIR"
        return 1
    }
    
    # Set display for headless mode
    export DISPLAY=:0
    
    return 0
}

run_whatsapp_sender() {
    local attempt=$1
    
    log_message "📤 Starting WhatsApp sender (Attempt $attempt/$MAX_RETRIES)..."
    
    # Run with timeout
    timeout $TIMEOUT_PER_ATTEMPT "$PYTHON_BIN" wa_sender.py
    local exit_code=$?
    
    # Check exit code
    case $exit_code in
        0)
            log_message "✅ SUCCESS: WhatsApp message sent successfully!"
            return 0
            ;;
        124)
            log_message "⏱️  TIMEOUT: Script exceeded ${TIMEOUT_PER_ATTEMPT}s timeout"
            return 1
            ;;
        *)
            log_message "❌ FAILED: Script exited with code $exit_code"
            return 1
            ;;
    esac
}

retry_with_backoff() {
    local attempt=1
    local success=false
    
    while [ $attempt -le $MAX_RETRIES ]; do
        log_separator
        log_message "Attempt $attempt of $MAX_RETRIES"
        log_separator
        
        # Run the sender
        if run_whatsapp_sender $attempt; then
            success=true
            break
        fi
        
        # If this was the last attempt, don't wait
        if [ $attempt -eq $MAX_RETRIES ]; then
            log_message "❌ All $MAX_RETRIES attempts exhausted"
            break
        fi
        
        # Calculate wait time with exponential backoff
        local wait_time=$(calculate_wait_time $attempt)
        log_message "⏳ Waiting ${wait_time}s before retry (exponential backoff)..."
        
        # Save debug info about the failure
        if [ -f "final_error.png" ]; then
            mv final_error.png "error_attempt_${attempt}.png" 2>/dev/null || true
        fi
        
        if [ -f "timeout_page.html" ]; then
            mv timeout_page.html "timeout_attempt_${attempt}.html" 2>/dev/null || true
        fi
        
        sleep $wait_time
        attempt=$((attempt + 1))
    done
    
    if [ "$success" = true ]; then
        return 0
    else
        return 1
    fi
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    # Redirect all output to log file
    exec >> "$LOG_FILE" 2>&1
    
    log_separator
    log_message "🚀 WhatsApp Sender Starting"
    log_separator
    log_message "Date: $(date '+%Y-%m-%d %H:%M:%S')"
    log_message "PDF Date: $PDF_DATE"
    log_message "Max Retries: $MAX_RETRIES"
    log_message "Timeout per attempt: ${TIMEOUT_PER_ATTEMPT}s"
    log_separator
    
    # Cleanup old debug files
    cleanup_old_debug_files
    
    # Check prerequisites
    log_message "🔍 Checking prerequisites..."
    if ! check_prerequisites; then
        log_message "❌ FATAL: Prerequisites check failed"
        send_notification "WhatsApp Sender Failed - Prerequisites" "Prerequisites check failed. See log: $LOG_FILE"
        exit 1
    fi
    log_message "✓ All prerequisites passed"
    log_separator
    
    # Setup environment
    log_message "⚙️  Setting up environment..."
    if ! setup_environment; then
        log_message "❌ FATAL: Environment setup failed"
        send_notification "WhatsApp Sender Failed - Setup" "Environment setup failed. See log: $LOG_FILE"
        exit 1
    fi
    log_message "✓ Environment ready"
    log_separator
    
    # Record start time
    local start_time=$(date +%s)
    
    # Run with retry logic
    log_message "🔄 Starting retry loop..."
    if retry_with_backoff; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log_separator
        log_message "✅ SUCCESS: Message sent successfully!"
        log_message "⏱️  Total execution time: ${duration}s"
        log_separator
        
        send_notification "WhatsApp Sender Success" "PDF sent successfully for $PDF_DATE after some attempts. Duration: ${duration}s"
        exit 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log_separator
        log_message "❌ FINAL FAILURE: All retry attempts exhausted"
        log_message "⏱️  Total execution time: ${duration}s"
        log_message "📁 Check debug files in: $WORK_DIR"
        log_separator
        
        # List recent debug files
        log_message "Debug files available:"
        ls -lht "$WORK_DIR"/*.png "$WORK_DIR"/*.html 2>/dev/null | head -10 || log_message "  No debug files found"
        
        send_notification "WhatsApp Sender FAILED - All Retries Exhausted" \
            "Failed to send PDF for $PDF_DATE after $MAX_RETRIES attempts. Duration: ${duration}s. Check log: $LOG_FILE"
        exit 1
    fi
}

# Run main function
main
