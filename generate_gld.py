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
TICKERS = ["GLD"]
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
        output_name = f"data/strong_stocks_{ticker}.png"
        generator.generate_chart(ticker, output_name)

def generate_market_analysis_charts():
    print("Generating Market Analysis charts...")
    for ticker in TICKERS:
        print(f"Fetching Market Analysis data for {ticker}...")
        results, df = get_market_analysis_data(ticker, period="1y")

        if not df.empty:
            output_name = f"data/market_analysis_{ticker}.png"
            print(f"Generating chart to {output_name}...")
            generate_market_chart(df, output_name)
        else:
            print(f"Failed to fetch market analysis data for {ticker}")

if __name__ == "__main__":
    update_data()
    run_calculations()
    generate_strong_stocks_charts()
    generate_market_analysis_charts()
    print("All tasks complete.")
