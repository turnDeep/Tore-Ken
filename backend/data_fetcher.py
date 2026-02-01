import json
import os
import sys
import datetime
import logging
from pywebpush import webpush, WebPushException
from backend.screener_service import run_screener_process
from backend.security_manager import security_manager
# Market Analysis Imports
from backend.market_analysis_logic import get_market_analysis_data
from backend.market_chart_generator import generate_market_chart

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Determine paths
PROJECT_ROOT = os.getcwd()
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

def send_push_notifications(daily_data):
    """
    Sends push notifications to all subscribers with robust error handling and logging.
    References HanaView202601 implementation.
    """
    logger.info("Starting notification process...")

    # Initialize security manager to get keys
    security_manager.data_dir = DATA_DIR
    security_manager.initialize()

    subscriptions_file = os.path.join(DATA_DIR, 'push_subscriptions.json')
    if not os.path.exists(subscriptions_file):
        logger.info("No subscriptions file found. Skipping notifications.")
        return

    try:
        with open(subscriptions_file, 'r') as f:
            subscriptions = json.load(f)
    except Exception as e:
        logger.error(f"Error reading subscriptions file: {e}")
        return

    if not subscriptions:
        logger.info("No subscriptions found.")
        return

    logger.info(f"Found {len(subscriptions)} subscriptions. Sending notifications...")

    # Prepare notification payload
    # Note: The format must match what the service worker expects
    # status_text is "Screened: N"
    payload = {
        "title": "Market Data Updated",
        "body": f"Date: {daily_data.get('date')}\n{daily_data.get('status_text')}",
        "url": "/",
        "icon": "/icons/icon-192x192.png",
        "type": "data-update"
    }

    json_payload = json.dumps(payload)

    sent_count = 0
    failed_subscriptions = []

    for sub_id, subscription in list(subscriptions.items()):
        permission = subscription.get("permission", "standard")

        # Create a clean subscription object for webpush (removing 'permission' etc)
        clean_subscription = {
            "endpoint": subscription["endpoint"],
            "keys": subscription["keys"]
        }
        if "expirationTime" in subscription and subscription["expirationTime"] is not None:
            clean_subscription["expirationTime"] = subscription["expirationTime"]

        try:
            webpush(
                subscription_info=clean_subscription,
                data=json_payload,
                vapid_private_key=security_manager.vapid_private_key,
                vapid_claims={"sub": security_manager.vapid_subject}
            )
            sent_count += 1
            logger.debug(f"Notification sent to {sub_id} ({permission})")
        except WebPushException as ex:
            logger.warning(f"Push failed for {sub_id[:8]}...: {ex}")
            # If 410 Gone or 404 Not Found, the subscription is invalid
            if ex.response and ex.response.status_code in [404, 410]:
                failed_subscriptions.append(sub_id)
        except Exception as e:
            logger.error(f"Unexpected error sending to {sub_id[:8]}...: {e}")

    # Remove invalid subscriptions
    if failed_subscriptions:
        for sub_id in failed_subscriptions:
            if sub_id in subscriptions:
                del subscriptions[sub_id]
        try:
            with open(subscriptions_file, 'w') as f:
                json.dump(subscriptions, f)
            logger.info(f"Removed {len(failed_subscriptions)} invalid subscriptions")
        except Exception as e:
            logger.error(f"Error saving subscriptions after cleanup: {e}")

    # Log detailed stats matching HanaView style
    standard_count = sum(1 for s in subscriptions.values() if s.get('permission', 'standard') == 'standard')
    secret_count = sum(1 for s in subscriptions.values() if s.get('permission') == 'secret')
    ura_count = sum(1 for s in subscriptions.values() if s.get('permission') == 'ura')

    logger.info(f"Push notifications sent: {sent_count} | Standard: {standard_count}, Secret: {secret_count}, Ura: {ura_count}")

def fetch_and_notify():
    """
    Orchestrates the new screening process and sends notifications.
    This replaces the old logic.
    """
    logger.info("Executing fetch_and_notify (New MomentumX Logic)...")

    try:
        # 1. Market Analysis (SPY)
        logger.info("Generating Market Analysis Data (6 months)...")
        # get_market_analysis_data returns (market_data_list, spy_df)
        market_data, spy_df = get_market_analysis_data(period="6mo")

        if market_data:
            # Generate Chart Image
            chart_path = os.path.join(DATA_DIR, "market_chart.png")
            logger.info(f"Generating chart image at {chart_path}...")
            generate_market_chart(spy_df, chart_path)
        else:
            logger.error("Failed to generate market data.")

        # 2. MomentumX Screener
        daily_data = run_screener_process()

        # 3. Synchronize and Save Market Analysis
        # Ensure Market Analysis history does not exceed the Screener Date
        if market_data:
            if daily_data and 'date' in daily_data:
                screener_date_str = daily_data['date'].replace('-', '') # YYYY-MM-DD -> YYYYMMDD

                # Filter market_data to not exceed screener date
                original_len = len(market_data)
                market_data = [d for d in market_data if d['date_key'] <= screener_date_str]
                new_len = len(market_data)

                if new_len < original_len:
                    logger.warning(f"Truncated Market Analysis history from {original_len} to {new_len} entries to match Screener Date {screener_date_str}.")

            # Save market_analysis.json (Now synchronized)
            analysis_file = os.path.join(DATA_DIR, "market_analysis.json")
            with open(analysis_file, "w") as f:
                json.dump({
                    "history": market_data,
                    "last_updated": datetime.datetime.now().isoformat()
                }, f)
            logger.info(f"Saved {analysis_file}")

        # Merge Market Status into daily_data if available
        if daily_data and market_data and len(market_data) > 0:
            latest_market = market_data[-1]
            daily_data['market_status'] = latest_market['market_status']
            daily_data['status_text'] = latest_market['status_text'] # Overwrite "Screened: N" or append?
            # User wants "Market Analysis is formerly displayed". The frontend uses 'status_text' for the badge.
            # screener_service sets status_text to "Screened: N".
            # We should probably combine them or prioritize Market Status.
            # Frontend app.js uses status_text for badge color (Green/Red).
            # So we should use the Market Status text.

            # Update the JSON files saved by screener_service?
            # screener_service saves json inside run_screener_process.
            # We might need to update those files or modify run_screener_process to accept status.
            # For now, let's update daily_data here and re-save if we want consistency,
            # BUT screener_service already wrote to disk.

            # Re-writing the daily JSONs to include Market Status
            # Use date from daily_data to find the correct file
            today_str = daily_data.get('date', '').replace('-', '')
            if not today_str:
                today_str = datetime.datetime.now().strftime('%Y%m%d')

            json_path = os.path.join(DATA_DIR, f"{today_str}.json")
            latest_path = os.path.join(DATA_DIR, "latest.json")

            if os.path.exists(json_path):
                with open(json_path, 'r') as f:
                    saved_data = json.load(f)
                saved_data['market_status'] = latest_market['market_status']
                saved_data['status_text'] = latest_market['status_text']
                with open(json_path, 'w') as f:
                    json.dump(saved_data, f)

            if os.path.exists(latest_path):
                with open(latest_path, 'w') as f:
                    json.dump(saved_data, f) # Save updated data

            # Update local var for notification
            daily_data['market_status'] = latest_market['market_status']
            daily_data['status_text'] = latest_market['status_text']

        if daily_data:
            send_push_notifications(daily_data)
        else:
            logger.warning("No data generated, skipping notifications.")

    except Exception as e:
        logger.error(f"Error in fetch_and_notify: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_notify()
