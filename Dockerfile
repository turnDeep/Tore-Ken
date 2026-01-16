# Use the official Python image.
# Updated to Python 3.12 for pandas_ta compatibility
FROM python:3.12-slim-bookworm
WORKDIR /app

# Set the timezone to Japan Standard Time at the very beginning
ENV TZ=Asia/Tokyo

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies including tzdata for timezone support
RUN apt-get update && apt-get install -y \
    cron \
    curl \
    git \
    tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configure timezone properly for cron
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Fix potential PAM issue with cron in Docker
# This is a common issue where cron fails silently because of pam_loginuid.so
RUN if [ -f /etc/pam.d/cron ]; then \
    sed -i '/session    required     pam_loginuid.so/c\#session    required     pam_loginuid.so' /etc/pam.d/cron; \
    fi

# Copy backend requirements and install Python packages.
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files.
COPY backend /app/backend
COPY frontend /app/frontend

# Copy the startup script
COPY start.sh /app/start.sh

# Make scripts executable
RUN chmod +x /app/start.sh
RUN chmod +x /app/backend/run_job.sh

# Add cron job with explicit timezone
# Important: Include TZ in the crontab itself
# Updated schedule: 5,6 (JST) and 20,21 (UTC mapping to JST) for robustness.
# Runs every day (* * *), logic handled by python script.
RUN ( \
    echo "SHELL=/bin/bash" ; \
    echo "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" ; \
    echo "TZ=Asia/Tokyo" ; \
    echo "" ; \
    echo "15 5,6,20,21 * * * . /app/backend/cron-env.sh && python /app/backend/cron_scheduler.py >> /app/logs/cron_error.log 2>&1" \
) | crontab -

# Create logs directory
RUN mkdir -p /app/logs

# Start services using the startup script
CMD [ "/app/start.sh" ]
