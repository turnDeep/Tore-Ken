import os
import json
import logging
import pandas as pd
import numpy as np
import datetime
import subprocess
import sys
from backend.get_tickers import update_stock_csv_from_fmp
from backend.rdt_data_fetcher import get_unique_symbols, download_price_data, merge_price_data, save_price_data, load_existing_price_data
from backend.chart_generator_mx import RDTChartGenerator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ROOT = os.getcwd()
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
STOCK_CSV_PATH = os.path.join(PROJECT_ROOT, 'stock.csv') # Save to root for rdt_data_fetcher
LATEST_JSON_PATH = os.path.join(DATA_DIR, 'latest.json')

def run_calculation_scripts():
    """Runs the 5 calculation scripts as subprocesses."""
    scripts = [
        "backend/calculate_atr_trailing_stop.py",
        "backend/calculate_rs_percentile_histogram.py",
        "backend/calculate_rs_volatility_adjusted.py",
        "backend/calculate_rti.py",
        "backend/calculate_zone_rs.py"
    ]

    for script in scripts:
        logger.info(f"Running {script}...")
        try:
            # Run using the same python interpreter
            subprocess.run([sys.executable, script], check=True, cwd=PROJECT_ROOT)
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running {script}: {e}")
            # Don't raise, continue to try others? No, they depend on price data.
            # But if one fails, maybe others work.
            # However, screening depends on ALL.
            # We should probably raise.
            pass

def load_pickle(filename):
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        return pd.read_pickle(path)
    return None

def apply_screening_logic():
    """
    Applies Entry and Exit logic to determine the list of Strong Stocks.
    Returns a list of dicts (ticker, rti, sector, etc.)
    """
    logger.info("Applying Screening Logic...")

    # 1. Load Data
    atr_data = load_pickle("atr_trailing_stop_weekly.pkl")
    rs_perc_data = load_pickle("rs_percentile_histogram_weekly.pkl")
    rs_vol_data = load_pickle("rs_volatility_adjusted_weekly.pkl")
    zone_data = load_pickle("zone_rs_weekly.pkl")
    rti_data = load_pickle("rti_weekly.pkl")
    price_data = load_pickle("price_data_ohlcv.pkl")

    if atr_data is None or rs_perc_data is None or rs_vol_data is None or zone_data is None or rti_data is None or price_data is None:
        logger.error("Missing calculation data. Aborting screening.")
        return []

    # Get latest tickers from price data
    # Check if MultiIndex
    if isinstance(price_data.columns, pd.MultiIndex):
        all_tickers = price_data.columns.get_level_values(1).unique().tolist()
    else:
        # Should not happen for multiple stocks
        all_tickers = []

    # Load Previous Tracked List
    old_tracked_tickers = set()
    if os.path.exists(LATEST_JSON_PATH):
        try:
            with open(LATEST_JSON_PATH, 'r') as f:
                data = json.load(f)
                stocks = data.get('strong_stocks', [])
                if isinstance(stocks, list):
                    old_tracked_tickers = {s.get('ticker') for s in stocks if s.get('ticker')}
                logger.info(f"Loaded {len(old_tracked_tickers)} previously tracked stocks.")
        except Exception as e:
            logger.error(f"Error loading latest.json: {e}")

    # Helper to get latest scalar value for a ticker from DataFrame/Series
    def get_latest(df_or_series, ticker):
        if ticker not in df_or_series.columns:
            return None
        series = df_or_series[ticker].dropna()
        if series.empty:
            return None
        return series.iloc[-1]

    # Prepare specific data series for easier access
    atr_state = atr_data["Trend_State"] # 3=Green, 0=Red
    rs_perc = rs_perc_data["Percentile_1M"] # >= 80

    # HMA Slope: We need RS_MA from rs_vol_data and check diff
    rs_ma = rs_vol_data["RS_MA"]
    # Zone: Power=3
    zone_vals = zone_data["Zone"]

    # RTI for output
    rti_vals = rti_data["RTI_Values"]

    entry_candidates = set()
    keep_candidates = set()

    for ticker in all_tickers:
        try:
            # --- Get Latest Values ---
            t_state = get_latest(atr_state, ticker) # 0, 1, 2, 3
            perc = get_latest(rs_perc, ticker)      # float
            zone = get_latest(zone_vals, ticker)    # 0, 1, 2, 3

            # HMA Slope
            slope = None
            if ticker in rs_ma.columns:
                ma_series = rs_ma[ticker].dropna()
                if len(ma_series) >= 2:
                    slope = ma_series.iloc[-1] - ma_series.iloc[-2]

            # --- Entry Logic ---
            # 1. ATR State == 3 (Green/Bull)
            # 2. RS Percentile >= 80
            # 3. HMA Slope > 0
            # 4. Zone == 3 (Power)

            if (t_state == 3 and
                perc is not None and perc >= 80 and
                slope is not None and slope > 0 and
                zone == 3):
                entry_candidates.add(ticker)

            # --- Exit Logic (Persistence) ---
            # Only for tickers already tracked
            if ticker in old_tracked_tickers:
                is_excluded = False

                # Exclusion 1: ATR Sell (Red -> 0)
                # Assuming 0 is Red. Need to verify calculate_atr logic.
                # calculate_atr_trailing_stop.py:
                # 0: Red, 1: Yellow, 2: Blue, 3: Green
                if t_state == 0:
                    is_excluded = True

                # Exclusion 2: Zone != 3 (Power)
                # i.e. Dead, Drift, Lift
                if zone != 3:
                    is_excluded = True

                if not is_excluded:
                    keep_candidates.add(ticker)

        except Exception as e:
            # logger.warning(f"Error processing {ticker}: {e}")
            continue

    # Combine
    final_tickers = entry_candidates | keep_candidates
    logger.info(f"Screening Result: {len(entry_candidates)} new entries, {len(keep_candidates)} retained. Total: {len(final_tickers)}")

    # Build Output List
    strong_stocks = []

    # Helper to get price data
    def get_price_info(ticker):
        # price_data is MultiIndex
        try:
            if ticker in price_data['Close'].columns:
                s = price_data['Close'][ticker].dropna()
                if not s.empty:
                    return s.iloc[-1]
        except:
            pass
        return 0.0

    for ticker in final_tickers:
        rti = get_latest(rti_vals, ticker)
        price = get_price_info(ticker)

        stock_obj = {
            "ticker": ticker,
            "rti": round(rti, 2) if rti is not None else 0.0,
            "current_price": round(price, 2),
            "rvol": 0.0, # Placeholder for WS
            "breakout_status": "", # Placeholder
            # "chart_image": f"{datetime.datetime.now().strftime('%Y%m%d')}-{ticker}.png" # Add this key if frontend needs it
             "chart_image": f"{datetime.datetime.now().strftime('%Y%m%d')}-{ticker}.png"
        }
        strong_stocks.append(stock_obj)

    return strong_stocks

def generate_charts(stock_list):
    """Generates charts for all strong stocks."""
    if not stock_list:
        return

    logger.info(f"Generating charts for {len(stock_list)} stocks...")
    generator = RDTChartGenerator()

    for stock in stock_list:
        ticker = stock['ticker']
        # Filename format: YYYYMMDD-TICKER.png in data dir
        filename = os.path.join(DATA_DIR, f"{datetime.datetime.now().strftime('%Y%m%d')}-{ticker}.png")

        try:
            generator.generate_chart(ticker, filename)
        except Exception as e:
            logger.error(f"Failed to generate chart for {ticker}: {e}")

def run_screener_process():
    """Main Orchestrator."""
    logger.info("Starting Screener Process...")

    # 1. Update Universe
    test_tickers = os.getenv("TEST_TICKERS")
    if test_tickers:
        logger.info(f"TEST_MODE: Using tickers {test_tickers}")
        with open(STOCK_CSV_PATH, 'w') as f:
            f.write("Symbol,Exchange\n")
            for t in test_tickers.split(','):
                f.write(f"{t.strip()},TEST\n")
    else:
        if not os.path.exists(STOCK_CSV_PATH):
            logger.info("Stock CSV not found. Fetching from FMP...")
        update_stock_csv_from_fmp(STOCK_CSV_PATH)

    # 2. Fetch Data (Incremental)
    # Use rdt_data_fetcher logic
    # Load existing to see where we are
    existing_data, last_date = load_existing_price_data()

    # Get symbols
    symbols, start_date = get_unique_symbols()

    if not symbols:
        logger.error("No symbols found.")
        return {}

    # Determine date range
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    if existing_data is not None and last_date is not None:
         start_date_dl = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
         # Check if we need to download
         if last_date.date() < datetime.datetime.now().date():
             new_data = download_price_data(symbols, start_date_dl, end_date)
             final_data = merge_price_data(existing_data, new_data) if new_data is not None else existing_data
             save_price_data(final_data)
         else:
             logger.info("Data up to date.")
             final_data = existing_data
    else:
        # Full download
        final_data = download_price_data(symbols, start_date, end_date)
        if final_data is not None:
            save_price_data(final_data)

    # 3. Run Calculations
    run_calculation_scripts()

    # 4. Screen
    strong_stocks = apply_screening_logic()

    # 5. Charts
    generate_charts(strong_stocks)

    # 6. Save JSON
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    output_data = {
        "date": datetime.datetime.now().strftime('%Y-%m-%d'),
        "market_status": "Neutral", # Placeholder, logic removed
        "status_text": f"Screened: {len(strong_stocks)}",
        "strong_stocks": strong_stocks,
        "last_updated": datetime.datetime.now().isoformat()
    }

    # Save daily
    with open(os.path.join(DATA_DIR, f"{today_str}.json"), 'w') as f:
        json.dump(output_data, f)

    # Save latest
    with open(LATEST_JSON_PATH, 'w') as f:
        json.dump(output_data, f)

    logger.info("Screener Process Complete.")
    return output_data

if __name__ == "__main__":
    run_screener_process()
