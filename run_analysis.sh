#!/bin/bash
cd /home/azureuser/azure_analysis_algorithm
source vmac/bin/activate

pkill -f "Xvfb :99" 2>/dev/null || true

# Start virtual display
Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset &
XVFB_PID=$!
export DISPLAY=:99

# Wait for display to be ready
sleep 5

echo "Starting analysis at $(date)" >> analysis.log

# Run analysis with timeout (10 minutes max)
timeout 600 python analysis.py >> analysis.log 2>&1
EXIT_CODE=$?

# Clean up
kill $XVFB_PID 2>/dev/null || true
pkill -f "Xvfb :99" 2>/dev/null || true

echo "Analysis finished at $(date) with exit code $EXIT_CODE" >> analysis.log