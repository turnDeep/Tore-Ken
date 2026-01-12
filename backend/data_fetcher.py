import json
import os
import datetime
import logging
import pandas as pd
from pywebpush import webpush, WebPushException
from backend.rdt_logic import get_market_analysis_data, run_screener_for_tickers
from backend.chart_generator import generate_market_chart
from backend.security_manager import security_manager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Determine paths
# If running as python -m backend.data_fetcher from root
PROJECT_ROOT = os.getcwd()
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
STOCK_CSV_PATH = os.path.join(PROJECT_ROOT, 'backend', 'stock.csv')

def send_notifications(daily_data):
    """
    Sends push notifications to all subscribers.
    """
    logger.info("Starting notification process...")

    # Initialize security manager to get keys
    security_manager.data_dir = DATA_DIR  # Ensure data dir is correct
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
        "body": f"Date: {daily_data.get('date')}\nStatus: {daily_data.get('status_text')}\nStrong Stocks: {len(daily_data.get('strong_stocks', []))}",
        "url": "/",
        "icon": "/icons/icon-192x192.png"
    }

    json_payload = json.dumps(payload)

    success_count = 0
    fail_count = 0

    # Ideally we should remove invalid subscriptions here, but for simplicity
    # and to avoid race conditions with the running app, we'll just log errors.

    for sub_id, sub_info in subscriptions.items():
        try:
            webpush(
                subscription_info=sub_info,
                data=json_payload,
                vapid_private_key=security_manager.vapid_private_key,
                vapid_claims={"sub": security_manager.vapid_subject}
            )
            success_count += 1
        except WebPushException as ex:
            logger.warning(f"Push failed for {sub_id[:8]}...: {ex}")
            fail_count += 1
        except Exception as e:
            logger.error(f"Unexpected error sending to {sub_id[:8]}...: {e}")
            fail_count += 1

    logger.info(f"Notifications sent: {success_count} succeeded, {fail_count} failed.")

def load_tickers():
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

def main():
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

    # Run Screener for TODAY (using the latest available data)
    logger.info("Running Screener...")
    tickers = load_tickers()

    if not tickers:
        logger.warning("No tickers found. Skipping screener.")
        strong_stocks = []
    else:
        strong_stocks = run_screener_for_tickers(tickers, spy_df)

    logger.info(f"Screener complete. Found {len(strong_stocks)} strong stocks.")

    # Save Daily JSON
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

    # Also save as 'latest.json' for easier frontend access to "current" data
    latest_filepath = os.path.join(DATA_DIR, "latest.json")
    with open(latest_filepath, "w") as f:
        json.dump(daily_data, f)

    # Send notifications
    send_notifications(daily_data)

if __name__ == "__main__":
    main()
