
import sys
import os
import pandas as pd
import logging

# Append backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from backend.market_analysis_logic import get_market_analysis_data
from backend.market_chart_generator import generate_market_chart

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TICKERS = ["SLV", "^SOX"]

def main():
    for ticker in TICKERS:
        try:
            logger.info(f"Generating chart for {ticker}...")
            # 1. Get Data
            data, df = get_market_analysis_data(ticker=ticker, period="2y")

            if not data or df.empty:
                logger.error(f"No data found for {ticker}")
                continue

            # 2. Generate Chart
            # Clean ticker for filename (remove ^)
            safe_ticker = ticker.replace("^", "")
            output_file = f"data/market_analysis_{safe_ticker}.png"

            # The generator expects DataFrame, not the list of dicts.
            # generate_market_chart(df, output_path)

            generate_market_chart(df, output_file)
            logger.info(f"Chart saved to {output_file}")

        except Exception as e:
            logger.error(f"Failed to generate for {ticker}: {e}")

if __name__ == "__main__":
    main()
