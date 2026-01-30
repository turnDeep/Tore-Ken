import json
import os
import sys
import datetime
import logging
from pywebpush import webpush, WebPushException
from backend.screener_service import run_screener_process
from backend.security_manager import security_manager

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
        daily_data = run_screener_process()

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
