"""
Calculate Zone RS (RS Ratio) and Momentum based on RRG logic (Weekly timeframe).
Based on TradingView script "MomentumMap v1.1" logic.

Input:
- data/price_data_ohlcv.pkl (Daily data)
- Benchmark: ^GSPC (S&P 500) from Yahoo Finance

Output:
- data/zone_rs_weekly.pkl (Dictionary containing Ratio, Momentum, Zone DataFrames)
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
OUTPUT_PATH = os.path.join(DATA_FOLDER, "zone_rs_weekly.pkl")

# Default Parameters from the Pine Script
BENCHMARK_SYMBOL = "^GSPC" # S&P 500
RS_LENGTH_DEFAULT = 50             # rsLength
MOMENTUM_LENGTH_DEFAULT = 20       # momentumLength
# Note: Volume logic is omitted for the core calculation as requested "Zone RS and Momentum",
# but can be added if needed for the "Power Zone" filtering specifically.
# For this script, we calculate the raw metrics and the base Quadrant.

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_price_data():
    """Load daily price data from pickle."""
    if not os.path.exists(PRICE_DATA_PATH):
        logging.error(f"Price data not found at {PRICE_DATA_PATH}")
        return None

    try:
        df = pd.read_pickle(PRICE_DATA_PATH)
        logging.info(f"Loaded price data: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error loading price data: {e}")
        return None

def fetch_benchmark_data(start_date, end_date):
    """Fetch benchmark data from Yahoo Finance."""
    logging.info(f"Fetching benchmark data ({BENCHMARK_SYMBOL})...")
    try:
        # Buffer start date to ensure enough data for MA calculation
        # Estimation: RS_LENGTH + MOMENTUM_LENGTH weeks buffer
        buffer_days = (RS_LENGTH_DEFAULT + MOMENTUM_LENGTH_DEFAULT) * 7 * 2
        start_dt = pd.to_datetime(start_date) - timedelta(days=buffer_days)

        benchmark = yf.download(BENCHMARK_SYMBOL, start=start_dt, end=end_date, progress=False, auto_adjust=True)

        if benchmark.empty:
            logging.error("Benchmark data download failed (empty).")
            return None

        logging.info(f"Benchmark data fetched: {benchmark.shape}")
        return benchmark
    except Exception as e:
        logging.error(f"Error fetching benchmark: {e}")
        return None

def resample_to_weekly(df):
    """Resample daily data to weekly (Friday close)."""
    # W-FRI: Weekly frequency ending on Friday
    # Resample takes the last value of the week (Close price)
    weekly = df.resample('W-FRI').last()

    # Drop rows where all columns are NaN (e.g., market holidays shifting dates)
    weekly = weekly.dropna(how='all')
    return weekly

def calculate_zone_rs(stock_prices, benchmark_prices, rs_length, momentum_length):
    """
    Calculate RS Ratio, RS Momentum and determine Zones.

    Logic:
    rs = close / benchmarkClose
    rsSMA = ta.sma(rs, rsLength)
    rsRatio = rs / rsSMA
    rsMomentum = ta.roc(rsRatio, momentumLength)
    """
    logging.info("Aligning stock and benchmark data...")

    # Ensure indices are datetime
    stock_prices.index = pd.to_datetime(stock_prices.index)
    benchmark_prices.index = pd.to_datetime(benchmark_prices.index)

    # Align dates (intersection)
    common_index = stock_prices.index.intersection(benchmark_prices.index)
    stocks = stock_prices.loc[common_index].copy()
    bench = benchmark_prices.loc[common_index].copy()

    # If benchmark is a Series or single-column DataFrame, ensure it aligns for broadcasting
    if isinstance(bench, pd.DataFrame):
        # Use 'Close' if available, otherwise first column
        if 'Close' in bench.columns:
            bench_close = bench['Close']
        else:
            bench_close = bench.iloc[:, 0]
    else:
        bench_close = bench

    # Handle MultiIndex column for benchmark if necessary (unlikely for single ticker download but possible)
    if isinstance(bench_close, pd.DataFrame) and bench_close.shape[1] == 1:
        bench_close = bench_close.iloc[:, 0]

    logging.info("Calculating RS Ratio...")
    # 1. RS = Stock Close / Benchmark Close
    # Broadcasting: Divide each stock column by the benchmark series
    rs_raw = stocks.div(bench_close, axis=0)

    # 2. RS SMA
    rs_sma = rs_raw.rolling(window=rs_length).mean()

    # 3. RS Ratio = RS / RS SMA
    # Note: TradingView logic uses rs / rsSMA. Result fluctuates around 1.0.
    rs_ratio = rs_raw / rs_sma

    logging.info("Calculating RS Momentum...")
    # 4. RS Momentum = ROC(rsRatio, momentumLength)
    # ROC calculation: (Current - Prev) / Prev * 100
    # Or simpler: (Current / Prev - 1) * 100
    rs_momentum = rs_ratio.pct_change(periods=momentum_length) * 100

    logging.info("Determining Zones...")
    # 5. Determine Zones
    # Quadrant Logic:
    # Power: Ratio >= 1 and Momentum >= 0
    # Drift: Ratio >= 1 and Momentum < 0
    # Dead:  Ratio < 1  and Momentum < 0
    # Lift:  Ratio < 1  and Momentum >= 0

    # We will use integer codes for zones to save space/efficiency in DataFrame
    # 0: Dead (Red)
    # 1: Lift (Blue)
    # 2: Drift (Yellow)
    # 3: Power (Green)
    # -1: Unknown/NaN

    # Initialize with -1
    zones = pd.DataFrame(-1, index=rs_ratio.index, columns=rs_ratio.columns)

    # Masking for vectorized assignment
    # Condition: Valid Data (not NaN)
    valid_mask = rs_ratio.notna() & rs_momentum.notna()

    ratio_ge_1 = rs_ratio >= 1.0
    mom_ge_0   = rs_momentum >= 0.0

    # Power: Ratio >= 1 & Mom >= 0
    mask_power = valid_mask & ratio_ge_1 & mom_ge_0
    zones[mask_power] = 3 # Power

    # Drift: Ratio >= 1 & Mom < 0
    mask_drift = valid_mask & ratio_ge_1 & (~mom_ge_0)
    zones[mask_drift] = 2 # Drift

    # Lift: Ratio < 1 & Mom >= 0
    mask_lift = valid_mask & (~ratio_ge_1) & mom_ge_0
    zones[mask_lift] = 1 # Lift

    # Dead: Ratio < 1 & Mom < 0
    mask_dead = valid_mask & (~ratio_ge_1) & (~mom_ge_0)
    zones[mask_dead] = 0 # Dead

    # Drop initial NaN rows required for calculation
    valid_start_idx = max(rs_length, momentum_length)
    # Actually need RS_LENGTH for SMA, then + MOMENTUM_LENGTH for ROC
    # So valid data starts after RS_LENGTH + MOMENTUM_LENGTH

    rs_ratio = rs_ratio.iloc[valid_start_idx:]
    rs_momentum = rs_momentum.iloc[valid_start_idx:]
    zones = zones.iloc[valid_start_idx:]

    return rs_ratio, rs_momentum, zones

def main():
    parser = argparse.ArgumentParser(description='Calculate Weekly Zone RS and Momentum')
    parser.add_argument('--rs_length', type=int, default=RS_LENGTH_DEFAULT, help=f'RS Length (default: {RS_LENGTH_DEFAULT})')
    parser.add_argument('--momentum_length', type=int, default=MOMENTUM_LENGTH_DEFAULT, help=f'Momentum Length (default: {MOMENTUM_LENGTH_DEFAULT})')
    args = parser.parse_args()

    rs_len = args.rs_length
    mom_len = args.momentum_length

    logging.info("=== Starting Zone RS Calculation (Weekly) ===")
    logging.info(f"Parameters: RS_Length={rs_len}, Momentum_Length={mom_len}")

    # 1. Load Stock Data
    daily_data = load_price_data()
    if daily_data is None:
        return

    # Extract Close prices
    # Check if MultiIndex
    if isinstance(daily_data.columns, pd.MultiIndex):
        # Check if 'Close' is in level 0
        if 'Close' in daily_data.columns.get_level_values(0):
            daily_close = daily_data['Close']
        else:
            # Maybe 'Adj Close'
            logging.warning("'Close' not found in Level 0. Checking columns...")
            daily_close = daily_data.xs('Close', axis=1, level=0, drop_level=True)
    else:
        # Assuming simple DataFrame if not MultiIndex (unlikely given previous context)
        daily_close = daily_data

    logging.info(f"Daily Close Prices: {daily_close.shape}")

    # 2. Resample to Weekly
    logging.info("Resampling stocks to Weekly...")
    weekly_close = resample_to_weekly(daily_close)
    logging.info(f"Weekly Close Prices: {weekly_close.shape}")
    if not weekly_close.empty:
        logging.info(f"Date Range: {weekly_close.index.min()} to {weekly_close.index.max()}")

    # 3. Fetch Benchmark
    start_date = daily_close.index.min()
    end_date = daily_close.index.max() + timedelta(days=5) # Buffer for end

    benchmark_data = fetch_benchmark_data(start_date, end_date)
    if benchmark_data is None:
        return

    logging.info("Resampling benchmark to Weekly...")
    weekly_benchmark = resample_to_weekly(benchmark_data)

    # 4. Calculate Indicators
    rs_ratio, rs_momentum, zones = calculate_zone_rs(weekly_close, weekly_benchmark, rs_len, mom_len)

    # 5. Save Results
    result_dict = {
        "Ratio": rs_ratio,
        "Momentum": rs_momentum,
        "Zone": zones,
        "Zone_Map": {0: "Dead", 1: "Lift", 2: "Drift", 3: "Power"},
        "Metadata": {
            "Benchmark": BENCHMARK_SYMBOL,
            "RS_Length": rs_len,
            "Momentum_Length": mom_len,
            "Timeframe": "Weekly",
            "Last_Updated": datetime.now()
        }
    }

    try:
        pd.to_pickle(result_dict, OUTPUT_PATH)
        logging.info(f"✓ Results saved to {OUTPUT_PATH}")
        logging.info("Keys: " + ", ".join(result_dict.keys()))
        logging.info("=== Done ===")

        # Display sample
        if not rs_ratio.empty:
            last_date = rs_ratio.index[-1]
            logging.info(f"\nSample Data (Last Date: {last_date.date()}):")

            # Get top 3 Power Zone stocks
            latest_zones = zones.loc[last_date]
            power_stocks = latest_zones[latest_zones == 3].index.tolist()[:3]

            for symbol in power_stocks:
                r = rs_ratio.loc[last_date, symbol]
                m = rs_momentum.loc[last_date, symbol]
                logging.info(f"  {symbol}: Zone=Power, Ratio={r:.4f}, Momentum={m:.4f}")

    except Exception as e:
        logging.error(f"Error saving results: {e}")

if __name__ == "__main__":
    main()
