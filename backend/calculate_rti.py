"""
Calculate Range Tightening Indicator (RTI)+ (Weekly).
Based on Pine Script by unknown (provided in prompt).

Input:
- data/price_data_ohlcv.pkl (Daily data)

Output:
- data/rti_weekly.pkl
"""
import os
import pandas as pd
import numpy as np
import logging
import argparse
from datetime import datetime

# Configuration
DATA_FOLDER = "data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

PRICE_DATA_PATH = os.path.join(DATA_FOLDER, "price_data_ohlcv.pkl")
OUTPUT_PATH = os.path.join(DATA_FOLDER, "rti_weekly.pkl")

# Default Parameters
DEFAULT_LENGTH = 5

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
        # Assumes Level 0 = Price (Open, High, Low, Close), Level 1 = Ticker
        # We need High and Low for volatility calc.

        # Resample logic
        weekly_high = df['High'].resample('W-FRI').max()
        weekly_low = df['Low'].resample('W-FRI').min()

        # Drop rows where all are NaN (holidays)
        weekly_high = weekly_high.dropna(how='all')
        weekly_low = weekly_low.dropna(how='all')

        # Align indices
        common_idx = weekly_high.index.intersection(weekly_low.index)

        return weekly_high.loc[common_idx], weekly_low.loc[common_idx]
    else:
        # Single ticker structure (unlikely for main file but handled)
        w = df.resample('W-FRI').agg({'High': 'max', 'Low': 'min'})
        return w['High'], w['Low']

def calculate_rti(high, low, length):
    """
    Calculate RTI and Signals.
    """
    logging.info(f"Calculating RTI (Length {length})...")

    # 1. Volatility (Range)
    volatility = high - low

    # 2. Min/Max Volatility
    max_vol = volatility.rolling(window=length).max()
    min_vol = volatility.rolling(window=length).min()

    # 3. RTI Calculation
    # rti = 100 * (vol - min) / (max - min)
    denominator = max_vol - min_vol
    # Handle division by zero (replace 0 with NaN or inf, then fillna)
    denominator = denominator.replace(0, np.nan)

    rti = 100 * (volatility - min_vol) / denominator

    # 4. Signals
    logging.info("Generating Signals...")

    rti_prev = rti.shift(1)

    # Condition: Below 20
    below_20 = rti < 20

    # Condition: Consecutive Below 20 (Orange Dot)
    # Logic: Two or more consecutive bars below 20.
    # In vectorized form: Current < 20 AND Prev < 20.
    # (If Prev was < 20, count was >=1. If Current is < 20, count becomes >=2)
    # Wait, Pine Logic:
    # if below_20: count += 1 else count = 0
    # dot = count >= 2
    # This means if we have sequence [Below, Below, Below], dot is True on 2nd and 3rd.
    # Vectorized: (rti < 20) & (rti.shift(1) < 20) covers 2nd and subsequent.
    orange_dot = below_20 & (rti_prev < 20)

    # Condition: Expansion (Green Line)
    # rti_prev <= 20 and rti >= 2 * rti_prev
    expansion = (rti_prev <= 20) & (rti >= 2 * rti_prev)

    # 5. Signal Encoding
    # 0: Normal
    # 1: Tight (RTI < 20)
    # 2: Super Tight (Orange Dot)
    # 3: Expansion

    signals = pd.DataFrame(0, index=rti.index, columns=rti.columns)

    # Apply priority: Expansion > Dot > Tight > Normal
    # Wait, Pine plots dot as shape, and line color. Can happen same time?
    # Expansion relies on prev < 20, current >= 40 (double). Current is NOT < 20.
    # So Expansion and Tight/Dot are mutually exclusive on the *current* bar.
    # Tight/Dot requires current < 20. Expansion requires current >= 2*prev (>=0?).
    # If prev=5, double=10. 10 < 20. So expansion could be "Tight".
    # Pine: "range_expansion_condition = rti_prev <= 20 and rti >= 2 * rti_prev"
    # Pine Plot: color is green if expansion.
    # Pine Dot: plotted if dot_condition (count >= 2).

    # Let's map strict states:
    # 1. Check Tight (<20)
    signals[below_20] = 1
    # 2. Check Dot (Consecutive <20). Overwrites 1.
    signals[orange_dot] = 2
    # 3. Check Expansion. Overwrites others (Logic: it's a breakout event).
    signals[expansion] = 3

    # NaN propagation
    signals[rti.isna()] = -1

    # Slice off warm-up
    valid_start = length
    rti = rti.iloc[valid_start:]
    signals = signals.iloc[valid_start:]

    return rti, signals

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--length", type=int, default=DEFAULT_LENGTH, help=f"Lookback Length (default: {DEFAULT_LENGTH})")
    args = parser.parse_args()

    logging.info("=== Starting RTI Calculation (Weekly) ===")

    # 1. Load Data
    daily_data = load_price_data()
    if daily_data is None: return

    # 2. Resample
    logging.info("Resampling to Weekly...")
    high, low = resample_to_weekly(daily_data)

    # 3. Calculate
    rti_values, rti_signals = calculate_rti(high, low, args.length)

    # 4. Save
    results = {
        "RTI_Values": rti_values,
        "RTI_Signals": rti_signals,
        "Signals_Map": {0: "Normal", 1: "Tight (<20)", 2: "Orange Dot (Consecutive Tight)", 3: "Expansion"},
        "Metadata": {
            "Timeframe": "Weekly",
            "Length": args.length,
            "Last_Updated": datetime.now()
        }
    }

    try:
        pd.to_pickle(results, OUTPUT_PATH)
        logging.info(f"✓ Saved to {OUTPUT_PATH}")
        logging.info("Keys: " + ", ".join(results.keys()))

        # Sample
        if not rti_values.empty:
            last_date = rti_values.index[-1]

            # Find Expansion stocks
            sigs = rti_signals.loc[last_date]
            expansion_stocks = sigs[sigs == 3].index.tolist()[:5]

            # Find Orange Dots
            dot_stocks = sigs[sigs == 2].index.tolist()[:5]

            logging.info(f"\nSample ({last_date.date()}):")
            logging.info(f"  Expansion: {expansion_stocks}")
            logging.info(f"  Orange Dot: {dot_stocks}")

    except Exception as e:
        logging.error(f"Error saving: {e}")

if __name__ == "__main__":
    main()
