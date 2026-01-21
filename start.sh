#!/bin/bash
# This script is the entrypoint for the Docker container.
# It starts the uvicorn server.

# Exit immediately if a command exits with a non-zero status.
set -e

# Create logs directory if not exists
mkdir -p /app/logs

# Set timezone
echo "Setting timezone to Asia/Tokyo..."
export TZ=Asia/Tokyo

echo "Current system time and timezone:"
date

echo "Starting Uvicorn web server..."
# Start the uvicorn server in the foreground.
exec uvicorn backend.main:app --host 0.0.0.0 --port 8000
