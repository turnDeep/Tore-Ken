import pandas as pd
import yfinance as yf
from backend.market_analysis_logic import get_market_analysis_data
from backend.market_chart_generator import generate_market_chart
from backend.chart_generator_mx import RDTChartGenerator
from backend.rdt_data_fetcher import download_price_data, save_price_data, load_existing_price_data, merge_price_data
import datetime
import os
import sys

# Constants
TICKERS = ["QQQ", "^NDX"] # US100 -> ^NDX
START_DATE = (datetime.datetime.now() - datetime.timedelta(days=365*10)).strftime('%Y-%m-%d')
END_DATE = datetime.datetime.now().strftime('%Y-%m-%d')

def update_data():
    print(f"Updating data for {TICKERS}...")
    existing_data, _ = load_existing_price_data()

    # Force download for these tickers to ensure we have them
    new_data = download_price_data(TICKERS, START_DATE, END_DATE)

    if new_data is not None:
        if existing_data is not None:
            # Merge with existing
            final_data = merge_price_data(existing_data, new_data)
        else:
            final_data = new_data
        save_price_data(final_data)
        print("Data updated successfully.")
    else:
        print("Failed to download data.")

def run_calculations():
    print("Running calculations...")
    # We need to run the calculation scripts.
    # Since they typically read from price_data_ohlcv.pkl and write to their own pkls,
    # running them as subprocesses or importing is needed.
    # Importing is cleaner but might have side effects if they are scripts.
    # Given they are independent scripts, running via os.system is safest to avoid state pollution.

    scripts = [
        "backend/calculate_atr_trailing_stop.py",
        "backend/calculate_rs_percentile_histogram.py",
        "backend/calculate_rs_volatility_adjusted.py",
        "backend/calculate_rti.py",
        "backend/calculate_zone_rs.py"
    ]

    for script in scripts:
        print(f"Running {script}...")
        ret = os.system(f"python {script}")
        if ret != 0:
            print(f"Error running {script}")

def generate_strong_stocks_charts():
    print("Generating Strong Stocks charts...")
    generator = RDTChartGenerator()
    for ticker in TICKERS:
        # Use QQQ for both just to be safe if ^NDX fails in chart generator due to symbol issues,
        # but let's try both.
        # Strong Stocks logic usually works on ETFs/Stocks. ^NDX might fail on some indicators if they require volume
        # and ^NDX volume is sometimes 0 or missing in some feeds.
        # However, chart_generator_mx checks for errors.

        output_name = f"data/strong_stocks_{ticker.replace('^', '')}.png"
        generator.generate_chart(ticker, output_name)

def generate_market_analysis_charts():
    print("Generating Market Analysis charts...")
    for ticker in TICKERS:
        # Market Analysis logic fetches its own data internally in get_market_analysis_data
        # so we don't need the pkl files for this part, but it's good we have them for strong stocks.

        # Note: get_market_analysis_data takes a ticker.
        # US100 -> ^NDX
        target = ticker

        print(f"Fetching Market Analysis data for {target}...")
        results, df = get_market_analysis_data(target, period="1y") # 1y for better view

        if not df.empty:
            output_name = f"data/market_analysis_{ticker.replace('^', '')}.png"
            print(f"Generating chart to {output_name}...")
            generate_market_chart(df, output_name)
        else:
            print(f"Failed to fetch market analysis data for {target}")

if __name__ == "__main__":
    # 1. Update Data (needed for Strong Stocks)
    update_data()

    # 2. Run Calculations (needed for Strong Stocks indicators)
    run_calculations()

    # 3. Generate Strong Stocks Charts
    generate_strong_stocks_charts()

    # 4. Generate Market Analysis Charts
    generate_market_analysis_charts()

    print("All tasks complete.")
