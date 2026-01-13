import os
import sys
import subprocess
import logging
from datetime import datetime
import pytz

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.rvol_logic import MarketSchedule

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cron_scheduler")

def is_dst(dt_jst):
    """
    Determines if the corresponding US Eastern time is in DST.
    """
    # Convert JST time to US/Eastern
    dt_et = dt_jst.astimezone(pytz.timezone('US/Eastern'))
    return dt_et.dst() != list(filter(None, [dt_et.tzinfo.dst(dt_et)]))[0] if dt_et.tzinfo else False
    # Actually simpler: pytz timezone objects have .dst() method on datetime
    return dt_et.dst().total_seconds() != 0

def run_fetch_job():
    logger.info("Triggering fetch job...")
    try:
        # Assuming run_job.sh is executable and in the correct path
        script_path = "/app/backend/run_job.sh"
        if not os.path.exists(script_path):
             logger.error(f"Script not found: {script_path}")
             return

        result = subprocess.run([script_path, "fetch"], capture_output=True, text=True)
        logger.info(f"Job Output: {result.stdout}")
        if result.stderr:
            logger.error(f"Job Error: {result.stderr}")
    except Exception as e:
        logger.error(f"Failed to run job: {e}")

def main():
    # Current time in JST
    now_jst = datetime.now(pytz.timezone('Asia/Tokyo'))
    current_hour = now_jst.hour

    # Check DST status of US Eastern Time
    # We use US/Eastern to determine if the US market is observing DST.
    now_et = now_jst.astimezone(pytz.timezone('US/Eastern'))
    is_us_dst = now_et.dst().total_seconds() != 0

    logger.info(f"Scheduler running at {now_jst} (JST). Hour: {current_hour}. US DST: {is_us_dst}")

    should_run = False

    # Summer Time (DST): Market closes 05:00 JST -> Run at 05:15
    if current_hour == 5 and is_us_dst:
        should_run = True
        logger.info("Condition Met: Summer Time (05:xx JST)")

    # Winter Time (Standard): Market closes 06:00 JST -> Run at 06:15
    if current_hour == 6 and not is_us_dst:
        should_run = True
        logger.info("Condition Met: Winter Time (06:xx JST)")

    if should_run:
        run_fetch_job()
    else:
        logger.info("No job scheduled for this hour/season.")

if __name__ == "__main__":
    main()
