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

def should_run_at(now_jst):
    """
    Determines if the fetch job should run based on the given JST time.
    Logic:
    1. US Market closes Mon-Fri.
    2. Data fetch happens the next morning in JST.
       - Mon Close -> Tue Morning JST
       - Fri Close -> Sat Morning JST
       - So we run Tue-Sat JST. (Weekday 1-5)
    3. Time depends on US DST:
       - Summer (DST): Close 05:00 JST -> Run 05:xx
       - Winter (Std): Close 06:00 JST -> Run 06:xx
    """
    current_hour = now_jst.hour
    weekday = now_jst.weekday() # Mon=0, Tue=1, ... Sat=5, Sun=6

    # 1. Check Day of Week (Tue-Sat)
    # If it is Monday morning JST, it corresponds to Sunday US (Market Closed).
    # If it is Saturday morning JST, it corresponds to Friday US (Market Open).
    if weekday not in [1, 2, 3, 4, 5]:
        return False

    # 2. Check DST status of US Eastern Time
    now_et = now_jst.astimezone(pytz.timezone('US/Eastern'))
    is_us_dst = now_et.dst().total_seconds() != 0

    # 3. Check Hour vs Season
    if is_us_dst:
        # Summer: Market closes 05:00 JST -> Run at 05:xx
        if current_hour == 5:
            return True
    else:
        # Winter: Market closes 06:00 JST -> Run at 06:xx
        if current_hour == 6:
            return True

    return False

def main():
    # Current time in JST
    now_jst = datetime.now(pytz.timezone('Asia/Tokyo'))

    logger.info(f"Scheduler running at {now_jst} (JST). Weekday: {now_jst.weekday()}.")

    if should_run_at(now_jst):
        logger.info("Condition Met: Starting Job.")
        run_fetch_job()
    else:
        logger.info("No job scheduled for this hour/season/day.")

if __name__ == "__main__":
    main()
