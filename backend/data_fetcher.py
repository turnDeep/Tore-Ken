import json
import os
import datetime
import logging
import pandas as pd
from backend.rdt_logic import get_market_analysis_data, run_screener_for_tickers

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Determine paths
# If running as python -m backend.data_fetcher from root
PROJECT_ROOT = os.getcwd()
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
STOCK_CSV_PATH = os.path.join(PROJECT_ROOT, 'backend', 'stock.csv')

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

if __name__ == "__main__":
    main()
