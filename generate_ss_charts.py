
import os
import sys
import pandas as pd
import yfinance as yf
import logging
from datetime import datetime, timedelta

# Append backend
sys.path.append(os.path.join(os.getcwd(), 'backend'))

# Import Calculation Main Functions
from backend.calculate_atr_trailing_stop import main as run_atr
from backend.calculate_rti import main as run_rti
from backend.calculate_zone_rs import main as run_zone_rs
from backend.calculate_rs_percentile_histogram import main as run_rs_perc
from backend.calculate_rs_volatility_adjusted import main as run_rs_vol

# Import Chart Generator
from backend.chart_generator_mx import RDTChartGenerator

# Config
DATA_FOLDER = "data"
PRICE_DATA_PATH = os.path.join(DATA_FOLDER, "price_data_ohlcv.pkl")
TICKERS = ["GLD", "QQQ", "^NDX", "SLV", "^SOX", "^GSPC"] # GSPC for benchmark calc

logging.basicConfig(level=logging.INFO)

def update_price_data():
    """Fetches data and saves to price_data_ohlcv.pkl"""
    print(f"Fetching data for {TICKERS}...")

    # Use long period to ensure enough data for indicators (250 weeks warm-up for Volatility Adj RS)
    # 5 years is ample (~260 weeks)
    data = yf.download(TICKERS, period="5y", progress=False, group_by='ticker', auto_adjust=True)

    # Structure check
    # yfinance group_by='ticker' returns MultiIndex (Ticker, Price) usually.
    # But RDTDataFetcher expects (Price, Ticker) or handled correctly.
    # calculate_* scripts expect MultiIndex (Price, Ticker) if checking columns levels?
    # Let's check calculate_atr: it checks if isinstance(df.columns, pd.MultiIndex).

    # yf download with group_by='ticker' returns level 0 as Ticker.
    # We might need to swap levels to (Price, Ticker) for some scripts if they expect standard OHLCV columns at level 0.
    # Let's inspect calculate_atr.py:
    # "if isinstance(df.columns, pd.MultiIndex): ... optimized approach: weekly_open = df['Open'].resample..."
    # If level 0 is Ticker, df['Open'] will fail unless we swap.

    # Let's look at `data.columns`.
    # If grouped by ticker: (Ticker, Price).
    # We want to transform to (Price, Ticker) or just ensure scripts handle it.

    # Let's swap levels to match typical structure expected by vector backtesters
    # (Price, Ticker)

    if isinstance(data.columns, pd.MultiIndex):
        # Swap levels so Price is level 0
        data.columns = data.columns.swaplevel(0, 1)
        data.sort_index(axis=1, inplace=True)

    pd.to_pickle(data, PRICE_DATA_PATH)
    print(f"Saved price data to {PRICE_DATA_PATH}")

def run_calculations():
    """Runs all calculation scripts sequentially."""
    print("Running ATR Trailing Stop...")
    run_atr()

    print("Running RTI...")
    run_rti()

    print("Running Zone RS...")
    # Zone RS script uses argparse, but calling main() without args uses defaults.
    # We need to ensure sys.argv doesn't interfere if we were passing args to this script.
    # We are not passing args to this script, so clean sys.argv?
    # Actually calculate_zone_rs.py main() parses args.
    # It's safer to clear sys.argv or pass empty list to main if possible?
    # No, main() calls parser.parse_args().

    # Hack: temporarily clear sys.argv for the calls
    old_argv = sys.argv
    sys.argv = [sys.argv[0]] # Keep script name

    try:
        run_zone_rs()

        print("Running RS Percentile...")
        run_rs_perc()

        print("Running RS Volatility Adjusted...")
        run_rs_vol()

    finally:
        sys.argv = old_argv

def generate_charts():
    """Generates charts for target tickers."""
    generator = RDTChartGenerator()

    targets = ["SLV", "^SOX", "GLD", "QQQ"] # Generate for all discussed

    for ticker in targets:
        # Clean filename
        safe_ticker = ticker.replace("^", "")
        filename = f"data/{safe_ticker}_weekly_chart.png"

        try:
            generator.generate_chart(ticker, filename)
        except Exception as e:
            print(f"Failed to generate {ticker}: {e}")

def main():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    update_price_data()
    run_calculations()
    generate_charts()

if __name__ == "__main__":
    main()
