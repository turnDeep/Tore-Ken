import json
import os
import sys
import datetime
import logging
import pandas as pd
from pywebpush import webpush, WebPushException
from backend.rdt_logic import get_market_analysis_data, run_screener_for_tickers
from backend.chart_generator import generate_market_chart
from backend.security_manager import security_manager
from backend.get_tickers import update_stock_csv_from_fmp

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Determine paths
PROJECT_ROOT = os.getcwd()
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
STOCK_CSV_PATH = os.path.join(PROJECT_ROOT, 'backend', 'stock.csv')

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
    payload = {
        "title": "Market Data Updated",
        "body": f"Date: {daily_data.get('date')}\nStatus: {daily_data.get('status_text')} / Stocks: {len(daily_data.get('strong_stocks', []))}",
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

def load_tickers():
    # Attempt to update tickers from FMP before loading
    update_stock_csv_from_fmp(STOCK_CSV_PATH)

    if not os.path.exists(STOCK_CSV_PATH):
        logger.error(f"Stock CSV not found at {STOCK_CSV_PATH}")
        return []

    try:
        df = pd.read_csv(STOCK_CSV_PATH)
        if 'Ticker' in df.columns:
            tickers = df['Ticker'].astype(str).tolist()
        else:
            tickers = df.iloc[:, 0].astype(str).tolist()

        tickers = [t.strip().upper() for t in tickers if isinstance(t, str) and t.strip()]
        # Filter logic
        tickers = [t for t in tickers if not (len(t) == 5 and t[-1] in ['W', 'R', 'U'])]
        # Remove dots for yfinance (BRK.B -> BRK-B)
        tickers = [t.replace('.', '-') for t in tickers]

        logger.info(f"Loaded {len(tickers)} tickers from CSV.")
        return tickers
    except Exception as e:
        logger.error(f"Error loading tickers: {e}")
        return []

def fetch_and_notify():
    os.makedirs(DATA_DIR, exist_ok=True)

    logger.info("Generating Market Analysis Data (6 months)...")
    market_data, spy_df = get_market_analysis_data(period="6mo")

    if not market_data:
        logger.error("Failed to generate market data.")
        return

    # Generate Chart Image
    chart_path = os.path.join(DATA_DIR, "market_chart.png")
    logger.info(f"Generating chart image at {chart_path}...")
    generate_market_chart(spy_df, chart_path)

    # Save market analysis (Chart Data)
    analysis_file = os.path.join(DATA_DIR, "market_analysis.json")
    with open(analysis_file, "w") as f:
        json.dump({
            "history": market_data,
            "last_updated": datetime.datetime.now().isoformat()
        }, f)
    logger.info(f"Saved {analysis_file}")

    # --- Silver Analysis ---
    logger.info("Generating Silver Futures Analysis Data...")
    silver_data, silver_df = get_market_analysis_data(period="6mo", ticker="SI=F")

    if silver_data:
        # Generate Silver Chart
        silver_chart_path = os.path.join(DATA_DIR, "silver_chart.png")
        logger.info(f"Generating Silver chart image at {silver_chart_path}...")
        generate_market_chart(silver_df, silver_chart_path)

        # Save Silver Analysis
        silver_analysis_file = os.path.join(DATA_DIR, "silver_analysis.json")
        with open(silver_analysis_file, "w") as f:
            json.dump({
                "history": silver_data,
                "last_updated": datetime.datetime.now().isoformat()
            }, f)
        logger.info(f"Saved {silver_analysis_file}")
    else:
        logger.error("Failed to generate Silver data.")

    # Run Screener for TODAY (using the latest available data)
    logger.info("Running Screener...")
    tickers = load_tickers()

    if not tickers:
        logger.warning("No tickers found. Skipping screener.")
        strong_stocks = []
    else:
        # Determine latest date key from market data for file naming
        latest_item = market_data[-1]
        latest_date_key = latest_item["date_key"]

        # Pass DATA_DIR and date_key to enable chart generation
        strong_stocks = run_screener_for_tickers(tickers, spy_df, data_dir=DATA_DIR, date_key=latest_date_key)

    logger.info(f"Screener complete. Found {len(strong_stocks)} strong stocks.")

    # Save Daily JSON (Latest)
    latest_item = market_data[-1]
    latest_date_key = latest_item["date_key"]

    daily_data = {
        "date": latest_item["date"],
        "market_status": latest_item["market_status"],
        "status_text": latest_item["status_text"],
        "strong_stocks": strong_stocks,
        "last_updated": datetime.datetime.now().isoformat()
    }

    daily_filename = f"{latest_date_key}.json"
    daily_filepath = os.path.join(DATA_DIR, daily_filename)
    with open(daily_filepath, "w") as f:
        json.dump(daily_data, f)
    logger.info(f"Saved {daily_filename}")

    # Also save as 'latest.json'
    latest_filepath = os.path.join(DATA_DIR, "latest.json")
    with open(latest_filepath, "w") as f:
        json.dump(daily_data, f)

    # --- Backfill Logic: Check past 20 days ---
    logger.info("Checking for missing past data (20 days)...")

    # Get last 20 market dates from history (reverse order to prioritize recent)
    past_days = sorted(market_data, key=lambda x: x['date_key'], reverse=True)[:20]

    for item in past_days:
        d_key = item['date_key']
        d_str = item['date'] # YYYY/MM/DD or YYYY-MM-DD

        # Format target_date for yfinance slicing (YYYY-MM-DD)
        target_date_obj = datetime.datetime.strptime(d_key, "%Y%m%d")
        target_date_str = target_date_obj.strftime("%Y-%m-%d")

        f_path = os.path.join(DATA_DIR, f"{d_key}.json")

        if not os.path.exists(f_path):
            logger.info(f"[Backfill] Missing data for {d_str} ({d_key}). Generating...")

            # Run screener for this specific date
            # Note: This is expensive if many days are missing.
            # Ideally we would cache the bulk download, but run_screener_for_tickers handles chunks.
            # Passing target_date ensures correct historical state.
            backfill_stocks = run_screener_for_tickers(
                tickers,
                spy_df,
                data_dir=DATA_DIR,
                date_key=d_key,
                target_date=target_date_str
            )

            backfill_data = {
                "date": d_str,
                "market_status": item["market_status"],
                "status_text": item["status_text"],
                "strong_stocks": backfill_stocks,
                "last_updated": datetime.datetime.now().isoformat()
            }

            with open(f_path, "w") as f:
                json.dump(backfill_data, f)
            logger.info(f"[Backfill] Saved {d_key}.json with {len(backfill_stocks)} stocks.")
        else:
            # logger.debug(f"Data exists for {d_key}. Skipping.")
            pass

    # Send notifications (Only for the latest data)
    send_push_notifications(daily_data)

if __name__ == "__main__":
    # Handle command line arguments to support 'fetch' and 'generate' styles
    # In this simplified repo, we run the full process for either command to ensure data freshness and notification.
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command in ['fetch', 'generate']:
            logger.info(f"Executing command: {command}")
            fetch_and_notify()
        else:
            logger.warning(f"Unknown command: {command}. executing default fetch_and_notify.")
            fetch_and_notify()
    else:
        fetch_and_notify()
