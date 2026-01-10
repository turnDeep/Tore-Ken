#!/bin/bash
LOG_DIR="/app/logs"
mkdir -p $LOG_DIR

echo "$(date): Starting HWB scan..." >> $LOG_DIR/hwb.log
cd /app
python -m backend.hwb_scanner_cli >> $LOG_DIR/hwb.log 2>&1
echo "$(date): HWB scan completed" >> $LOG_DIR/hwb.log