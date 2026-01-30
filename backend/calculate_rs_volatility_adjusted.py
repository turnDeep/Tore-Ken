"""
Calculate Volatility Adjusted Relative Strength (Weekly).
Based on Pine Script "Relative Strength (Volatility Adjusted)" by mattishenner.

Input:
- data/price_data_ohlcv.pkl (Daily data)
- Benchmark: SPY (from Yahoo Finance)

Output:
- data/rs_volatility_adjusted_weekly.pkl
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
OUTPUT_PATH = os.path.join(DATA_FOLDER, "rs_volatility_adjusted_weekly.pkl")

BENCHMARK_SYMBOL = "SPY"

# Default Parameters (Pine Script Defaults)
LOOKBACK_LENGTH = 100
MA_LENGTH = 100
MA_TYPE = "hma" # Options: hma, sma, ema
ATR_LENGTH = 50

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
    # Buffer for rolling calc (ATR + Lookback + MA)
    # Weekly estimation: (50 + 100 + 100) weeks approx
    buffer_days = (ATR_LENGTH + LOOKBACK_LENGTH + MA_LENGTH) * 7 * 2
    start_dt = pd.to_datetime(start_date) - timedelta(days=buffer_days)
    try:
        bench = yf.download(BENCHMARK_SYMBOL, start=start_dt, end=end_date, progress=False, auto_adjust=True)
        return bench
    except Exception as e:
        logging.error(f"Error fetching benchmark: {e}")
        return None

def resample_to_weekly(df):
    """Resample OHLCV to Weekly (Friday Close)."""
    # Logic:
    # Open: First day open
    # High: Max high
    # Low: Min low
    # Close: Last day close
    # Volume: Sum volume

    # Check column structure (MultiIndex or Single Level)
    if isinstance(df.columns, pd.MultiIndex):
        # We need to handle each ticker.
        # But resample().agg() works with dictionaries mapping columns to functions.
        # However, for MultiIndex (Level 0 = OHLCV, Level 1 = Ticker), simple resample rule is harder.

        # Strategy: Iterate over tickers? Or use stack/unstack?
        # Vectorized approach: resample on the whole DF?
        # If we just do df.resample('W-FRI').last(), we get weekly closes correct, but High/Low might be inaccurate (last day High vs Week High).
        # Pine Script logic needs Close for price, but ATR needs High/Low.
        # Accurate Week High/Low is needed for accurate Weekly ATR.

        # Optimized approach:
        # 1. Resample Close: .last()
        # 2. Resample High: .max()
        # 3. Resample Low: .min()
        # 4. Resample Open: .first() (though strictly not needed for ATR)

        # Since the structure is MultiIndex [Price, Ticker], we can do:
        # df['High'].resample('W-FRI').max()

        # Let's verify structure
        is_multi = True
        tickers = df.columns.get_level_values(1).unique()
    else:
        is_multi = False
        # Assuming single symbol structure like fetched from yfinance for SPY

    if is_multi:
        weekly_open = df['Open'].resample('W-FRI').first()
        weekly_high = df['High'].resample('W-FRI').max()
        weekly_low = df['Low'].resample('W-FRI').min()
        weekly_close = df['Close'].resample('W-FRI').last()

        # Reconstruct MultiIndex DataFrame?
        # Or just return dict of DataFrames?
        # The subsequent calculations need separate High/Low/Close matrices anyway.
        # Returning a wrapper/dict is cleaner.
        return {
            'Open': weekly_open,
            'High': weekly_high,
            'Low': weekly_low,
            'Close': weekly_close
        }
    else:
        # Single symbol (Benchmark)
        # Ensure columns exist
        w = df.resample('W-FRI').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        })
        return {
            'Open': w['Open'],
            'High': w['High'],
            'Low': w['Low'],
            'Close': w['Close']
        }

def calculate_atr(high, low, close, length):
    """
    Calculate ATR (Wilder's Smoothing).
    TR = max(H-L, |H-Cp|, |L-Cp|)
    ATR = RMA(TR, length)
    """
    logging.info("  Calculating True Range...")
    # Previous Close
    prev_close = close.shift(1)

    # TR Calculation (Vectorized)
    # tr1 = high - low
    # tr2 = abs(high - prev_close)
    # tr3 = abs(low - prev_close)
    # tr = max(tr1, tr2, tr3)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=0, keys=['1','2','3']).groupby(level=1).max()
    # Note: The concat method above might be memory intensive or tricky with MultiIndex alignment.
    # Better: Use numpy maximum

    # Align DataFrames
    # tr2 and tr3 might have NaNs at the start.

    # Ensuring values are floats
    h = high.values
    l = low.values
    pc = prev_close.values

    # Numpy vectorized max
    # Note: pc has NaN at index 0.
    with np.errstate(invalid='ignore'):
        t2 = np.abs(h - pc)
        t3 = np.abs(l - pc)
        # tr = np.maximum(h - l, np.maximum(t2, t3))
        # Careful with NaN propagation.
        # If prev_close is NaN, TR is High-Low (usually).
        tr_vals = np.nanmax(np.stack([h-l, t2, t3]), axis=0)

    tr = pd.DataFrame(tr_vals, index=close.index, columns=close.columns)

    logging.info(f"  Calculating ATR ({length})...")
    # RMA (Wilder's MA) is EWM with alpha = 1/length
    atr = tr.ewm(alpha=1/length, adjust=False, min_periods=length).mean()

    return atr

def calculate_wma(series, length):
    """Weighted Moving Average (needed for HMA)."""
    # WMA = sum(price * weight) / sum(weights)
    # Weights = 1, 2, ..., length
    # Pandas doesn't have built-in WMA.
    # Implementation using rolling apply (slow) or strided rolling (fast).
    # Since we need vectorized over columns, rolling apply is acceptable if efficient,
    # but strictly WMA can be computed as:
    # WMA = (Rolling Sum of (Sum of prices)) ? No.

    # Vectorized WMA:
    # We can iterate using rolling(window=length) and apply dot product.
    # For large datasets, this might be slow.
    # Optimization: Use convolution?

    weights = np.arange(1, length + 1)
    sum_weights = weights.sum()

    def wma_func(x):
        if np.isnan(x).any(): return np.nan
        return np.dot(x, weights) / sum_weights

    # This is slow for 5000 columns.
    # Alternative: construct linear combination manually?
    # Or assume for now that standard rolling.mean (SMA) is enough approximation if WMA is too heavy?
    # User requested HMA. HMA requires WMA.

    # Fast WMA using Numba or pure numpy rolling tricks?
    # Let's stick to a simple construct_wma helper that loops over columns if needed,
    # OR use the 'triangular' window type in rolling? No, WMA is linear weights.

    # Let's try pandas standard `.rolling().apply()` with raw=True and engine='numba' if installed.
    # Since numba is in requirements (from pandas-ta), we can try.

    return series.rolling(window=length).apply(wma_func, raw=True)

def calculate_hma(series, length):
    """Hull Moving Average."""
    # HMA = WMA(2*WMA(n/2) - WMA(n), sqrt(n))
    half_length = int(length / 2)
    sqrt_length = int(np.sqrt(length))

    logging.info(f"    Calculating HMA (Length {length})...")

    wma_half = calculate_wma(series, half_length)
    wma_full = calculate_wma(series, length)

    raw_hma = 2 * wma_half - wma_full
    hma = calculate_wma(raw_hma, sqrt_length)

    return hma

def calculate_sma(series, length):
    return series.rolling(window=length).mean()

def calculate_ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def calculate_ma(series, ma_type, length):
    if ma_type == "hma":
        return calculate_hma(series, length)
    elif ma_type == "sma":
        return calculate_sma(series, length)
    elif ma_type == "ema":
        return calculate_ema(series, length)
    else:
        logging.warning(f"Unknown MA type {ma_type}, using SMA")
        return calculate_sma(series, length)

def calculate_rs_volatility_adjusted(stock_data_dict, bench_data_dict, lookback, atr_length, ma_length, ma_type):
    """Core Calculation Logic."""

    # 1. Align Data
    s_close = stock_data_dict['Close']
    s_high = stock_data_dict['High']
    s_low = stock_data_dict['Low']

    b_close = bench_data_dict['Close']
    b_high = bench_data_dict['High']
    b_low = bench_data_dict['Low']

    # Align dates
    common_idx = s_close.index.intersection(b_close.index)
    s_close = s_close.loc[common_idx]
    s_high = s_high.loc[common_idx]
    s_low = s_low.loc[common_idx]

    b_close = b_close.loc[common_idx]
    b_high = b_high.loc[common_idx]
    b_low = b_low.loc[common_idx]

    # Benchmark is Series? Make it aligned DataFrame for broadcasting (though Series broadcasts fine)
    # Ensure Series
    if isinstance(b_close, pd.DataFrame): b_close = b_close.iloc[:, 0]
    if isinstance(b_high, pd.DataFrame): b_high = b_high.iloc[:, 0]
    if isinstance(b_low, pd.DataFrame): b_low = b_low.iloc[:, 0]

    # 2. Calculate ATRs
    logging.info("Calculating Benchmark ATR...")
    # Prepare DataFrame for bench to reuse calc function
    bench_df_c = b_close.to_frame()
    bench_df_h = b_high.to_frame()
    bench_df_l = b_low.to_frame()
    bench_atr = calculate_atr(bench_df_h, bench_df_l, bench_df_c, atr_length).iloc[:, 0]

    logging.info("Calculating Stock ATR (Vectorized)...")
    stock_atr = calculate_atr(s_high, s_low, s_close, atr_length)

    # 3. Normalized Changes
    # (inst - inst[1]) / inst_atr
    # (close - close[1]) / stock_atr

    logging.info("Calculating Normalized Changes...")
    bench_change = (b_close - b_close.shift(1)) / bench_atr
    stock_change = (s_close - s_close.shift(1)) / stock_atr

    # 4. Cumulative Sums (Rolling Lookback)
    logging.info(f"Calculating Rolling Sums (Lookback {lookback})...")
    # Note: math.sum(..., lookback) in Pine is a rolling sum

    cum_bench = bench_change.rolling(window=lookback).sum()
    cum_stock = stock_change.rolling(window=lookback).sum()

    # 5. RS
    # RS = Stock - Bench (Broadcasting)
    logging.info("Calculating Final RS...")
    rs = cum_stock.sub(cum_bench, axis=0)

    # 6. RS MA
    logging.info(f"Calculating RS MA ({ma_type.upper()} {ma_length})...")
    rs_ma = calculate_ma(rs, ma_type, ma_length)

    # 7. Trend State Classification
    logging.info("Classifying Trend States...")
    # 3: Strong (RS > 0 & RS > MA)
    # 2: Weakening (RS > 0 & RS < MA)
    # 1: Improving (RS < 0 & RS > MA)
    # 0: Weak (RS < 0 & RS < MA)

    trend_state = pd.DataFrame(0, index=rs.index, columns=rs.columns)

    # Masks
    rs_gt_0 = rs > 0
    rs_gt_ma = rs > rs_ma

    # Vectorized assignment
    # Init 0
    # Improving: ~rs_gt_0 & rs_gt_ma -> 1
    trend_state[ (~rs_gt_0) & rs_gt_ma ] = 1
    # Weakening: rs_gt_0 & ~rs_gt_ma -> 2
    trend_state[ rs_gt_0 & (~rs_gt_ma) ] = 2
    # Strong: rs_gt_0 & rs_gt_ma -> 3
    trend_state[ rs_gt_0 & rs_gt_ma ] = 3

    # Propagate NaNs where data is invalid
    invalid_mask = rs.isna() | rs_ma.isna()
    trend_state[invalid_mask] = -1 # Or NaN, but int is requested. Let's use -1 for invalid.

    # Remove initial buffer (NaNs)
    valid_start = max(ATR_LENGTH, LOOKBACK_LENGTH) + MA_LENGTH
    # A bit simplified, but safe to slice off calculation warm-up
    rs = rs.iloc[valid_start:]
    rs_ma = rs_ma.iloc[valid_start:]
    trend_state = trend_state.iloc[valid_start:]

    return rs, rs_ma, trend_state

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback", type=int, default=LOOKBACK_LENGTH)
    parser.add_argument("--ma_length", type=int, default=MA_LENGTH)
    parser.add_argument("--atr_length", type=int, default=ATR_LENGTH)
    parser.add_argument("--ma_type", type=str, default=MA_TYPE, choices=["hma", "sma", "ema"])
    args = parser.parse_args()

    logging.info("=== Starting RS Volatility Adjusted Calculation (Weekly) ===")

    # 1. Load Stocks
    daily_data = load_price_data()
    if daily_data is None: return

    # 2. Resample Stocks
    logging.info("Resampling Stocks to Weekly...")
    weekly_stocks = resample_to_weekly(daily_data)

    # 3. Load & Resample Benchmark
    start = daily_data.index.min()
    end = daily_data.index.max() + timedelta(days=5)
    bench_data = fetch_benchmark_data(start, end)
    if bench_data is None: return

    logging.info("Resampling Benchmark to Weekly...")
    weekly_bench = resample_to_weekly(bench_data)

    # 4. Calculate
    rs, rs_ma, trends = calculate_rs_volatility_adjusted(
        weekly_stocks, weekly_bench,
        args.lookback, args.atr_length, args.ma_length, args.ma_type
    )

    # 5. Save
    results = {
        "RS_Values": rs,
        "RS_MA": rs_ma,
        "Trend_State": trends,
        "Trend_Map": {0: "Weak", 1: "Improving", 2: "Weakening", 3: "Strong"},
        "Metadata": {
            "Benchmark": BENCHMARK_SYMBOL,
            "Timeframe": "Weekly",
            "Config": vars(args)
        }
    }

    try:
        pd.to_pickle(results, OUTPUT_PATH)
        logging.info(f"✓ Saved to {OUTPUT_PATH}")
        logging.info("Keys: " + ", ".join(results.keys()))

        # Sample
        if not rs.empty:
            last_date = rs.index[-1]
            # Find strongest
            strong = trends.loc[last_date]
            top = strong[strong == 3].index.tolist()[:3]
            logging.info(f"\nSample Strong Stocks ({last_date.date()}): {top}")

    except Exception as e:
        logging.error(f"Error saving: {e}")

if __name__ == "__main__":
    main()
