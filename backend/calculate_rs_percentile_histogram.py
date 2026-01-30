"""
Calculate RS Percentile Histogram (Historical Rank) based on Pine Script logic.
Timeframe: Weekly

Logic based on "RS Percentile Histogram (1M/3M)" by planetes0925.

Input:
- data/price_data_ohlcv.pkl (Daily data)
- Benchmark: ^GSPC (S&P 500)

Output:
- data/rs_percentile_histogram_weekly.pkl
"""
import os
import pandas as pd
import numpy as np
import yfinance as yf
import logging
import argparse
from datetime import datetime, timedelta

# Configuration
DATA_FOLDER = "data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

PRICE_DATA_PATH = os.path.join(DATA_FOLDER, "price_data_ohlcv.pkl")
OUTPUT_PATH = os.path.join(DATA_FOLDER, "rs_percentile_histogram_weekly.pkl")

BENCHMARK_SYMBOL = "^GSPC"

# Pine Script defaults (Applied to Weekly bars as requested)
# Note: "1M" mode uses 26 bars (26 weeks), "3M" mode uses 63 bars (63 weeks)
LOOKBACK_1M_W = 26
LOOKBACK_3M_W = 63
ALPHA_3M = 0.03
# If daily alpha is 0.03, weekly might be larger?
# Pine script: ewRet = alpha * ret + (1-alpha) * prev.
# For now, we stick to the provided logic structure but apply it to weekly bars.

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_price_data():
    if not os.path.exists(PRICE_DATA_PATH):
        logging.error(f"Price data not found at {PRICE_DATA_PATH}")
        return None
    return pd.read_pickle(PRICE_DATA_PATH)

def fetch_benchmark_data(start_date, end_date):
    logging.info(f"Fetching benchmark data ({BENCHMARK_SYMBOL})...")
    # Buffer for rolling calc
    buffer_days = 365
    start_dt = pd.to_datetime(start_date) - timedelta(days=buffer_days)
    try:
        bench = yf.download(BENCHMARK_SYMBOL, start=start_dt, end=end_date, progress=False, auto_adjust=True)
        return bench
    except Exception as e:
        logging.error(f"Error fetching benchmark: {e}")
        return None

def resample_to_weekly(df):
    # W-FRI: Weekly frequency ending on Friday
    weekly = df.resample('W-FRI').last()
    return weekly.dropna(how='all')

def calculate_historical_percentile(series, lookback):
    """
    Calculate percentile rank of current value against its own history (rolling window).
    """
    # rolling().rank(pct=True) returns 0.0 to 1.0. Multiply by 100.
    # Note: rank(pct=True) handles min_periods.
    return series.rolling(window=lookback).rank(pct=True) * 100

def calculate_rs_percentile(stocks_weekly, bench_weekly, mode="1M", lookback_1m=LOOKBACK_1M_W, length_3m=LOOKBACK_3M_W, alpha_3m=ALPHA_3M):
    """
    Calculate RS Series and its Historical Percentile.
    """
    logging.info(f"Calculating for Mode: {mode}")

    # Align
    common_index = stocks_weekly.index.intersection(bench_weekly.index)
    stocks = stocks_weekly.loc[common_index].copy()
    bench = bench_weekly.loc[common_index].copy()

    if isinstance(bench, pd.DataFrame):
        if 'Close' in bench.columns:
            bench_close = bench['Close']
        else:
            bench_close = bench.iloc[:, 0]
    else:
        bench_close = bench

    if isinstance(bench_close, pd.DataFrame):
        bench_close = bench_close.iloc[:, 0]

    rs_series = pd.DataFrame(index=stocks.index, columns=stocks.columns)
    lookback = 0

    if mode == "1M":
        # Price Ratio RS
        logging.info("  Logic: Price Ratio (Close / Benchmark)")
        rs_series = stocks.div(bench_close, axis=0)
        lookback = lookback_1m

    elif mode == "3M":
        # 3M EW Return RS
        # Logic:
        # retAsset = log(close / close[1])
        # ewRet = alpha * ret + (1-alpha) * prev
        # cumRet = ewRet * length
        # RS = (1 + cumRetAsset) / (1 + cumRetBench)

        logging.info("  Logic: 3M EW Log Returns")

        # Log returns
        ret_asset = np.log(stocks / stocks.shift(1))
        ret_bench = np.log(bench_close / bench_close.shift(1))

        # EMA (Exponential Moving Average) of returns
        # Pandas ewm: alpha specified directly. adjust=False matches `alpha * x + (1-alpha)*prev`
        ew_ret_asset = ret_asset.ewm(alpha=alpha_3m, adjust=False).mean()
        ew_ret_bench = ret_bench.ewm(alpha=alpha_3m, adjust=False).mean()

        # Cumulative
        cum_ret_asset = ew_ret_asset * length_3m
        cum_ret_bench = ew_ret_bench * length_3m # Broadcasting? Series vs DataFrame

        # RS
        # (1 + cumRetAsset) / (1 + cumRetBench)
        # Handle broadcasting carefully
        rs_series = (1 + cum_ret_asset).div(1 + cum_ret_bench, axis=0)

        lookback = length_3m

    logging.info(f"  Calculating Rolling Percentile (Lookback={lookback})...")

    # Calculate Historical Percentile for each column
    # Rolling rank vectorized across columns
    percentiles = rs_series.rolling(window=lookback, min_periods=lookback).rank(pct=True) * 100

    return percentiles, rs_series

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["1M", "3M", "BOTH"], default="BOTH", help="Calculation mode")
    parser.add_argument("--lookback_1m", type=int, default=LOOKBACK_1M_W, help=f"Lookback for 1M mode (default: {LOOKBACK_1M_W} weeks)")
    parser.add_argument("--length_3m", type=int, default=LOOKBACK_3M_W, help=f"Length for 3M mode (default: {LOOKBACK_3M_W} weeks)")
    args = parser.parse_args()

    logging.info("=== Starting RS Percentile Histogram Calculation (Weekly) ===")

    # 1. Load Data
    daily_data = load_price_data()
    if daily_data is None: return

    # Extract Close
    if isinstance(daily_data.columns, pd.MultiIndex):
        if 'Close' in daily_data.columns.get_level_values(0):
            daily_close = daily_data['Close']
        else:
            daily_close = daily_data.xs('Close', axis=1, level=0, drop_level=True)
    else:
        daily_close = daily_data

    # 2. Resample
    logging.info("Resampling to Weekly...")
    weekly_close = resample_to_weekly(daily_close)

    # 3. Benchmark
    start = daily_close.index.min()
    end = daily_close.index.max() + timedelta(days=5)
    bench_data = fetch_benchmark_data(start, end)
    if bench_data is None: return
    weekly_bench = resample_to_weekly(bench_data)

    results = {}

    # 4. Calculate
    modes_to_run = ["1M", "3M"] if args.mode == "BOTH" else [args.mode]

    for m in modes_to_run:
        perc, raw_rs = calculate_rs_percentile(
            weekly_close, weekly_bench,
            mode=m,
            lookback_1m=args.lookback_1m,
            length_3m=args.length_3m
        )
        results[f"Percentile_{m}"] = perc
        results[f"RS_Values_{m}"] = raw_rs

    results["Metadata"] = {
        "Benchmark": BENCHMARK_SYMBOL,
        "Timeframe": "Weekly",
        "Config": vars(args)
    }

    # 5. Save
    try:
        pd.to_pickle(results, OUTPUT_PATH)
        logging.info(f"✓ Saved to {OUTPUT_PATH}")
        logging.info("Keys: " + ", ".join(results.keys()))

        # Sample
        if "Percentile_1M" in results:
            last_date = results["Percentile_1M"].index[-1]
            sample = results["Percentile_1M"].iloc[-1].dropna().head(3)
            logging.info(f"\nSample 1M Percentiles ({last_date.date()}):")
            print(sample)

    except Exception as e:
        logging.error(f"Error saving: {e}")

if __name__ == "__main__":
    main()
