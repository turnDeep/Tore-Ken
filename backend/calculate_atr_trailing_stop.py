"""
Calculate ATR Trailing Stop Strategy (Weekly) using Numba.
Based on Pine Script by ceyhun.

Input:
- data/price_data_ohlcv.pkl (Daily data)

Output:
- data/atr_trailing_stop_weekly.pkl
"""
import os
import pandas as pd
import numpy as np
import logging
import argparse
from datetime import datetime
from numba import jit, prange

# Configuration
DATA_FOLDER = "data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

PRICE_DATA_PATH = os.path.join(DATA_FOLDER, "price_data_ohlcv.pkl")
OUTPUT_PATH = os.path.join(DATA_FOLDER, "atr_trailing_stop_weekly.pkl")

# Default Parameters
FAST_ATR_PERIOD = 5
FAST_ATR_MULTIPLIER = 0.5
SLOW_ATR_PERIOD = 10
SLOW_ATR_MULTIPLIER = 3.0

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

def resample_to_weekly(df):
    """Resample OHLCV to Weekly (Friday Close)."""
    # Check structure
    if isinstance(df.columns, pd.MultiIndex):
        # MultiIndex: (Price, Ticker)
        # Optimized approach:
        weekly_open = df['Open'].resample('W-FRI').first()
        weekly_high = df['High'].resample('W-FRI').max()
        weekly_low = df['Low'].resample('W-FRI').min()
        weekly_close = df['Close'].resample('W-FRI').last()

        # Align indices (drop mismatched rows usually at start/end or holidays)
        common_idx = weekly_close.index

        # Ensure alignment
        weekly_open = weekly_open.loc[common_idx]
        weekly_high = weekly_high.loc[common_idx]
        weekly_low = weekly_low.loc[common_idx]

        return weekly_open, weekly_high, weekly_low, weekly_close
    else:
        # Single ticker
        w = df.resample('W-FRI').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last'
        })
        return w['Open'], w['High'], w['Low'], w['Close']

def calculate_atr(high, low, close, length):
    """
    Calculate ATR (Wilder's Smoothing) using Pandas Vectorization.
    """
    logging.info(f"  Calculating ATR ({length})...")
    prev_close = close.shift(1)

    # Numpy for max calculation
    h = high.values
    l = low.values
    pc = prev_close.values

    with np.errstate(invalid='ignore'):
        tr1 = h - l
        tr2 = np.abs(h - pc)
        tr3 = np.abs(l - pc)
        tr_vals = np.nanmax(np.stack([tr1, tr2, tr3]), axis=0)

    tr = pd.DataFrame(tr_vals, index=close.index, columns=close.columns)

    # RMA (Wilder's MA) is EWM with alpha = 1/length
    atr = tr.ewm(alpha=1/length, adjust=False, min_periods=length).mean()
    return atr

@jit(nopython=True)
def calculate_trailing_stop_numba(close_arr, atr_arr, multiplier):
    """
    Recursive calculation of ATR Trailing Stop.

    Logic (Pine equivalent):
    SL = AF * atr
    Trail = iff(SC > nz(Trail[1]), max(nz(Trail[1]), SC - SL), iff(SC < nz(Trail[1]), min(nz(Trail[1]), SC + SL), ...))

    This essentially means:
    If Price > PrevTrail:
        NewTrail = max(PrevTrail, Price - SL)
        If Price crosses below NewTrail -> Flip to Bearish?
        Wait, Pine logic `iff(SC > nz(Trail[1]) ...)` checks the *previous* state against current price.

        Actually, the standard logic is:
        state: 1 (Bull), -1 (Bear)

        If state == 1:
            stop = max(stop, close - sl)
            if close < stop:
                state = -1
                stop = close + sl
        else:
            stop = min(stop, close + sl)
            if close > stop:
                state = 1
                stop = close - sl

    Pine Script `Trail1 := iff(...)` is a compact version of this.
    Let's implement the state-machine explicitly for clarity and speed.
    """
    n = len(close_arr)
    trail = np.zeros(n)
    # Initialize state (0: unknown/start, 1: bull, -1: bear)
    state = 0

    # First valid index
    # We need valid ATR. ATR usually NaN at start.
    # Numba handles NaNs in float arrays but comparisons might be tricky.

    for i in range(1, n):
        c = close_arr[i]
        c_prev = close_arr[i-1]
        a = atr_arr[i]

        if np.isnan(c) or np.isnan(a):
            trail[i] = np.nan
            continue

        sl = multiplier * a
        prev_trail = trail[i-1]

        # Initialization if prev is 0 or NaN
        if state == 0 or np.isnan(prev_trail) or prev_trail == 0:
            # Default start: Bearish or set to Price - SL?
            # Pine `nz(Trail[1], 0)`. If 0, `iff(SC > 0, max(0, SC-SL), ...)`
            # So if Price > 0, it sets Price - SL.
            state = 1
            trail[i] = c - sl
            continue

        # Logic Update
        # Pine:
        # iff(SC > prev, max(prev, SC-SL),
        #   iff(SC < prev, min(prev, SC+SL),
        #     iff(SC > prev, SC-SL, SC+SL))) -> Fallback?

        # This Pine logic implies:
        # If we are strictly above prev trail, we try to raise it (max).
        # If we are strictly below prev trail, we try to lower it (min).

        if state == 1:
            # Bullish mode
            if c > prev_trail:
                trail[i] = max(prev_trail, c - sl)
            else:
                # Crossed below
                state = -1
                trail[i] = min(prev_trail, c + sl)
                # Note: Pine might jump immediately to C+SL without `min` against prev.
                # Let's check: `iff(SC < nz(Trail[1]), min(nz(Trail[1]), SC + SL)`
                # If Price < PrevTrail, it sets min(PrevTrail, Price + SL).
                # Since Price < PrevTrail, Price + SL could be anything.
                # Usually standard TS flips to Price + SL immediately on reversal.
                # But the Pine script uses `min(prev, SC+SL)`.
                # If SC < prev, `min(prev, ...)` keeps it below prev? Yes.
                # So the line doesn't jump *up* when flipping to bear?

                # Let's trace strict Pine logic line by line.
                # T[i] depends on T[i-1]

                pass

        # Strict Pine Translation
        is_above = c > prev_trail
        is_below = c < prev_trail

        # Note: logic relies on `SC[1]` (prev close) too in Pine?
        # Pine: `SC > Trail[1] and SC[1] > Trail[1]` -> Continue Bull

        # Let's ignore state variable and compute exact formula
        # T_prev = trail[i-1]
        # SC = c
        # SC_1 = c_prev

        # term1: SC > T_prev and SC_1 > T_prev -> max(T_prev, SC - SL)
        # term2: SC < T_prev and SC_1 < T_prev -> min(T_prev, SC + SL)
        # term3: (else) SC > T_prev ? SC - SL : SC + SL

        if c > prev_trail and c_prev > prev_trail:
            trail[i] = max(prev_trail, c - sl)
        elif c < prev_trail and c_prev < prev_trail:
            trail[i] = min(prev_trail, c + sl)
        else:
            # Crossing or first touch
            if c > prev_trail:
                trail[i] = c - sl
            else:
                trail[i] = c + sl

    return trail

@jit(nopython=True, parallel=True)
def compute_all_trails(close_matrix, atr_matrix, multiplier):
    """
    Compute trails for all stocks in parallel.
    close_matrix: (n_days, n_stocks)
    atr_matrix: (n_days, n_stocks)
    """
    n_days, n_stocks = close_matrix.shape
    result = np.zeros((n_days, n_stocks))

    for j in prange(n_stocks):
        result[:, j] = calculate_trailing_stop_numba(close_matrix[:, j], atr_matrix[:, j], multiplier)

    return result

def calculate_strategies(close, high, low, atr_length_1, atr_mult_1, atr_length_2, atr_mult_2):
    """
    Main calculation wrapper.
    """
    # 1. Calculate ATRs (Pandas - Vectorized)
    logging.info("Calculating Fast ATR...")
    atr1 = calculate_atr(high, low, close, atr_length_1)

    logging.info("Calculating Slow ATR...")
    atr2 = calculate_atr(high, low, close, atr_length_2)

    # 2. Prepare Numpy Arrays
    logging.info("Preparing data for Numba...")
    # Fill NaNs? Numba handles them, but logic should be robust.
    close_vals = close.values.astype(np.float64)
    atr1_vals = atr1.values.astype(np.float64)
    atr2_vals = atr2.values.astype(np.float64)

    # 3. Calculate Trails (Numba - Parallel)
    logging.info("Computing Fast Trails (Numba Parallel)...")
    trail1_vals = compute_all_trails(close_vals, atr1_vals, float(atr_mult_1))

    logging.info("Computing Slow Trails (Numba Parallel)...")
    trail2_vals = compute_all_trails(close_vals, atr2_vals, float(atr_mult_2))

    # 4. Reconstruct DataFrames
    trail1 = pd.DataFrame(trail1_vals, index=close.index, columns=close.columns)
    trail2 = pd.DataFrame(trail2_vals, index=close.index, columns=close.columns)

    # 5. Determine State & Signals (Vectorized Pandas)
    logging.info("Determining States and Signals...")

    # Green: T1 > T2 and C > T2 and L > T2
    cond_green = (trail1 > trail2) & (close > trail2) & (low > trail2)

    # Blue: T1 > T2 and C > T2 and L < T2
    cond_blue = (trail1 > trail2) & (close > trail2) & (low < trail2)

    # Red: T2 > T1 and C < T2 and H < T2
    cond_red = (trail2 > trail1) & (close < trail2) & (high < trail2)

    # Yellow: T2 > T1 and C < T2 and H > T2
    cond_yellow = (trail2 > trail1) & (close < trail2) & (high > trail2)

    # State Encoding
    # 0: Red, 1: Yellow, 2: Blue, 3: Green
    trend_state = pd.DataFrame(0, index=close.index, columns=close.columns)
    trend_state[cond_yellow] = 1
    trend_state[cond_blue] = 2
    trend_state[cond_green] = 3

    # Signals
    # Buy: Crossover(T1, T2) -> T1 > T2 now AND T1 <= T2 prev
    # Sell: Crossunder(T1, T2) -> T1 < T2 now AND T1 >= T2 prev

    t1_gt_t2 = trail1 > trail2
    t1_gt_t2_prev = t1_gt_t2.shift(1).fillna(False).astype(bool)

    signals = pd.DataFrame(0, index=close.index, columns=close.columns)

    # Buy (1)
    signals[t1_gt_t2 & (~t1_gt_t2_prev)] = 1

    # Sell (-1)
    signals[(~t1_gt_t2) & t1_gt_t2_prev] = -1

    # Clean up initial data
    valid_start = max(atr_length_1, atr_length_2) + 1

    return trail1.iloc[valid_start:], trail2.iloc[valid_start:], trend_state.iloc[valid_start:], signals.iloc[valid_start:]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fast_period", type=int, default=FAST_ATR_PERIOD)
    parser.add_argument("--fast_mult", type=float, default=FAST_ATR_MULTIPLIER)
    parser.add_argument("--slow_period", type=int, default=SLOW_ATR_PERIOD)
    parser.add_argument("--slow_mult", type=float, default=SLOW_ATR_MULTIPLIER)
    args = parser.parse_args()

    logging.info("=== Starting ATR Trailing Stop Calculation (Weekly) ===")

    # 1. Load Data
    daily_data = load_price_data()
    if daily_data is None: return

    # 2. Resample
    logging.info("Resampling to Weekly...")
    op, hi, lo, cl = resample_to_weekly(daily_data)

    # 3. Calculate
    t1, t2, states, sigs = calculate_strategies(
        cl, hi, lo,
        args.fast_period, args.fast_mult,
        args.slow_period, args.slow_mult
    )

    # 4. Save
    results = {
        "Fast_Trail": t1,
        "Slow_Trail": t2,
        "Trend_State": states,
        "Signals": sigs,
        "Trend_Map": {0: "Red (Bear)", 1: "Yellow (Bear Rally)", 2: "Blue (Bull Dip)", 3: "Green (Bull)"},
        "Metadata": {
            "Timeframe": "Weekly",
            "Config": vars(args)
        }
    }

    try:
        pd.to_pickle(results, OUTPUT_PATH)
        logging.info(f"✓ Saved to {OUTPUT_PATH}")
        logging.info("Keys: " + ", ".join(results.keys()))

        # Sample
        if not sigs.empty:
            last_date = sigs.index[-1]
            buy_sigs = sigs.loc[last_date]
            buys = buy_sigs[buy_sigs == 1].index.tolist()[:5]

            strong_bulls = states.loc[last_date]
            bulls = strong_bulls[strong_bulls == 3].index.tolist()[:5]

            logging.info(f"\nSample ({last_date.date()}):")
            logging.info(f"  Buy Signals: {buys}")
            logging.info(f"  Strong Bull Stocks: {bulls}")

    except Exception as e:
        logging.error(f"Error saving: {e}")

if __name__ == "__main__":
    main()
