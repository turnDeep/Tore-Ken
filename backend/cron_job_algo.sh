#!/bin/bash
LOG_DIR="/app/logs"
mkdir -p $LOG_DIR

# Using tee to write to log and stdout (for cron to capture if needed, though docker logs usually capture stdout)
# But standard practice in this repo seems to be appending to log file.
echo "$(date): Starting Algo scan..." >> $LOG_DIR/algo.log
cd /app
python -m backend.algo_scanner_cli >> $LOG_DIR/algo.log 2>&1
echo "$(date): Algo scan completed" >> $LOG_DIR/algo.log
