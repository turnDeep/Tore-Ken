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
from backend.fundamental_analysis import analyze_tickers_in_batch

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
            pass

def load_pickle(filename):
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        return pd.read_pickle(path)
    return None

def calculate_entry_date(ticker, atr_state_series, rs_perc_series, rs_ma_series, zone_series, lookback_weeks=52):
    """
    Backtracks to find the most recent continuous 'Entry' date for a ticker.
    Logic:
    1. Scan backward from today.
    2. Check state at each week:
       - IN State: (ATR != Sell/0) AND (Zone == Power/3)
       - ENTRY Trigger: (ATR==3) AND (Perc>=80) AND (Slope>0) AND (Zone==3)
    3. If currently NOT in 'IN State', return None.
    4. If in 'IN State', scan back until:
       - We hit an 'ENTRY Trigger' (Candidate Start)
       - AND precede it with an 'OUT State' (Confirmation of fresh entry)
       - OR we just find the earliest continuous 'IN State' point.

    Simplified Backtracking:
    - Iterate forward from (Today - lookback).
    - Maintain state: is_holding (Bool), current_entry_date (Date or None).
    - On each week:
        - Check Entry Conditions.
        - Check Exit Conditions.
        - Update state.
    - Return final current_entry_date if is_holding is True.
    """
    try:
        # Align all series to common index
        common_idx = atr_state_series.index.intersection(rs_perc_series.index).intersection(zone_series.index)
        if ticker in rs_ma_series.columns:
             ma_s = rs_ma_series[ticker].reindex(common_idx)
        else:
             return None # Missing data

        atr_s = atr_state_series[ticker].reindex(common_idx)
        perc_s = rs_perc_series[ticker].reindex(common_idx)
        zone_s = zone_series[ticker].reindex(common_idx)

        # Calculate Slope Series
        slope_s = ma_s.diff()

        # Sort index
        common_idx = common_idx.sort_values()

        # Slice for lookback
        start_date = datetime.datetime.now() - datetime.timedelta(weeks=lookback_weeks)
        valid_idx = common_idx[common_idx >= start_date]

        if valid_idx.empty:
            return None

        is_holding = False
        current_entry_date = None

        for date in valid_idx:
            # Values at this date
            t_state = atr_s.loc[date]
            perc = perc_s.loc[date]
            slope = slope_s.loc[date]
            zone = zone_s.loc[date]

            # Logic
            # Exit Check first? Or Entry?
            # If holding, check exit.
            if is_holding:
                # Exit Logic: ATR Sell (0) OR Zone != Power (3)
                if t_state == 0 or zone != 3:
                    is_holding = False
                    current_entry_date = None

            # If not holding (or just exited), check Entry
            if not is_holding:
                # Entry Logic
                if (t_state == 3 and
                    pd.notna(perc) and perc >= 80 and
                    pd.notna(slope) and slope > 0 and
                    zone == 3):
                    is_holding = True
                    current_entry_date = date.strftime('%Y-%m-%d')

        if is_holding:
            return current_entry_date
        return None

    except Exception as e:
        logger.error(f"Error calculating entry date for {ticker}: {e}")
        return None

def apply_screening_logic(is_weekend_screening=True, data_date=None):
    """
    Applies Entry and Exit logic to determine the list of Strong Stocks.
    Returns a list of dicts.

    is_weekend_screening: If True, calculates Entry/Exit/Persistence based on weekly criteria.
                          If False, only updates metrics (ADR%, Price) for existing list.
    data_date: The date of the data being used (datetime object or Timestamp). Used for versioning filenames.
    """
    logger.info(f"Applying Screening Logic (Weekend Mode: {is_weekend_screening})...")

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
    if isinstance(price_data.columns, pd.MultiIndex):
        all_tickers = price_data.columns.get_level_values(1).unique().tolist()
    else:
        all_tickers = []

    # Load Previous Tracked List
    old_tracked_map = {} # {ticker: entry_date}
    if os.path.exists(LATEST_JSON_PATH):
        try:
            with open(LATEST_JSON_PATH, 'r') as f:
                data = json.load(f)
                stocks = data.get('strong_stocks', [])
                if isinstance(stocks, list):
                    for s in stocks:
                        tk = s.get('ticker')
                        ed = s.get('entry_date')
                        if tk:
                            old_tracked_map[tk] = ed
                logger.info(f"Loaded {len(old_tracked_map)} previously tracked stocks.")
        except Exception as e:
            logger.error(f"Error loading latest.json: {e}")

    final_stocks = {} # {ticker: entry_date}

    if not is_weekend_screening:
        # Weekday: Just keep the old list
        final_stocks = old_tracked_map
        logger.info("Weekday mode: Retaining existing list, updating metrics only.")
    else:
        # Weekend: Run full screening
        # Prepare data series
        atr_state = atr_data["Trend_State"]
        rs_perc = rs_perc_data["Percentile_1M"]
        rs_ma = rs_vol_data["RS_MA"]
        zone_vals = zone_data["Zone"]

        entry_candidates = set() # (ticker, entry_date)
        keep_candidates = set() # (ticker, entry_date)

        # Helper to get latest scalar
        def get_latest(df_or_series, ticker):
            if ticker not in df_or_series.columns: return None
            series = df_or_series[ticker].dropna()
            if series.empty: return None
            return series.iloc[-1]

        for ticker in all_tickers:
            try:
                # --- Latest Values for Screening ---
                t_state = get_latest(atr_state, ticker)
                perc = get_latest(rs_perc, ticker)
                zone = get_latest(zone_vals, ticker)

                slope = None
                if ticker in rs_ma.columns:
                    ma_series = rs_ma[ticker].dropna()
                    if len(ma_series) >= 2:
                        slope = ma_series.iloc[-1] - ma_series.iloc[-2]

                # --- Entry Logic ---
                is_new_entry = False
                if (t_state == 3 and
                    perc is not None and perc >= 80 and
                    slope is not None and slope > 0 and
                    zone == 3):
                    is_new_entry = True

                    # Determine Entry Date
                    if ticker in old_tracked_map:
                        entry_date = old_tracked_map[ticker]
                    else:
                        entry_date = calculate_entry_date(ticker, atr_state, rs_perc, rs_ma, zone_vals)
                        if not entry_date:
                            entry_date = datetime.datetime.now().strftime('%Y-%m-%d') # Fallback

                    entry_candidates.add((ticker, entry_date))

                # --- Persistence Logic (Keep) ---
                if not is_new_entry and ticker in old_tracked_map:
                    is_excluded = False
                    if t_state == 0: is_excluded = True
                    if zone != 3: is_excluded = True

                    if not is_excluded:
                        # Keep
                        entry_date = old_tracked_map[ticker]
                        keep_candidates.add((ticker, entry_date))

            except Exception as e:
                continue

        # Combine
        for t, d in entry_candidates:
            final_stocks[t] = d
        for t, d in keep_candidates:
            final_stocks[t] = d

        logger.info(f"Screening Result: {len(entry_candidates)} entries, {len(keep_candidates)} kept. Total: {len(final_stocks)}")

    # --- Build Output with Metrics (Updated Daily) ---
    strong_stocks = []

    rti_vals = rti_data["RTI_Values"]
    rti_sigs = rti_data["RTI_Signals"]

    def get_latest_val(df_or_series, ticker):
        if ticker not in df_or_series.columns: return None
        series = df_or_series[ticker].dropna()
        if series.empty: return None
        return series.iloc[-1]

    def get_price_info(ticker):
        try:
            if ticker in price_data['Close'].columns:
                s = price_data['Close'][ticker].dropna()
                if not s.empty: return s.iloc[-1]
        except: pass
        return 0.0

    def calculate_adr_pct(ticker):
        """Calculates 20-day Average Daily Range %"""
        try:
            # Need High/Low/Close from price_data
            # Access MultiIndex
            high = price_data['High'][ticker]
            low = price_data['Low'][ticker]
            # close = price_data['Close'][ticker] # Not needed for formula (H/L) but usually normalized by something?
            # Standard ADR% formula: Mean((High/Low - 1) * 100) over 20 days?
            # Or (High - Low) / Close?
            # Memory says: "ADR% (20日間の平均日中変動率)"
            # Typically: ((High / Low) - 1) * 100

            # Use last 20 days
            high = high.dropna().tail(20)
            low = low.dropna().tail(20)

            if len(high) < 20: return 0.0

            daily_ranges = ((high / low) - 1) * 100
            return daily_ranges.mean()
        except:
            return 0.0

    for ticker, e_date in final_stocks.items():
        rti = get_latest_val(rti_vals, ticker)
        rti_signal = get_latest_val(rti_sigs, ticker)
        is_orange_dot = (rti_signal == 2)

        price = get_price_info(ticker)
        adr_pct = calculate_adr_pct(ticker)

        # Determine chart date string
        chart_date_str = data_date.strftime('%Y%m%d') if data_date else datetime.datetime.now().strftime('%Y%m%d')

        stock_obj = {
            "ticker": ticker,
            "rti": round(rti, 2) if rti is not None else 0.0,
            "is_orange_dot": bool(is_orange_dot),
            "current_price": round(price, 2),
            "adr_pct": round(adr_pct, 2),
            "rvol": 0.0,
            "breakout_status": "",
            "entry_date": e_date,
            "chart_image": f"{chart_date_str}-{ticker}.png"
        }
        strong_stocks.append(stock_obj)

    return strong_stocks

def generate_charts(stock_list, data_date=None):
    """Generates charts for all strong stocks."""
    if not stock_list:
        return

    logger.info(f"Generating charts for {len(stock_list)} stocks...")
    generator = RDTChartGenerator()

    # Determine date string for filenames
    chart_date_str = data_date.strftime('%Y%m%d') if data_date else datetime.datetime.now().strftime('%Y%m%d')

    for stock in stock_list:
        ticker = stock['ticker']
        filename = os.path.join(DATA_DIR, f"{chart_date_str}-{ticker}.png")
        try:
            generator.generate_chart(ticker, filename)
        except Exception as e:
            logger.error(f"Failed to generate chart for {ticker}: {e}")

def run_screener_process(force_weekend_mode=False):
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

    # 2. Fetch Data
    existing_data, last_date = load_existing_price_data()
    symbols, start_date = get_unique_symbols()

    if not symbols:
        logger.error("No symbols found.")
        return {}

    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    data_date = None

    if existing_data is not None and last_date is not None:
         start_date_dl = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
         if last_date.date() < datetime.datetime.now().date():
             new_data = download_price_data(symbols, start_date_dl, end_date)
             final_data = merge_price_data(existing_data, new_data) if new_data is not None else existing_data
             save_price_data(final_data)
         else:
             logger.info("Data up to date.")
             final_data = existing_data
    else:
        final_data = download_price_data(symbols, start_date, end_date)
        if final_data is not None:
            save_price_data(final_data)

    if final_data is not None and not final_data.empty:
        data_date = final_data.index[-1]

    if data_date is None:
        logger.error("No data available to process.")
        return {}

    # 3. Run Calculations
    run_calculation_scripts()

    # 4. Determine Screening Mode
    # Weekend Screening if:
    # - Explicitly forced
    # - Or today is Friday (weekday 4) AND we have today's data
    # - Or today is Saturday (5) or Sunday (6) (assuming Friday data is latest)

    # Note: data_date is a Timestamp.
    is_friday_data = (data_date.weekday() == 4)

    # Logic: Run full screener if the Latest Data is a Friday (Weekly Close).
    # Otherwise, just update metrics for existing list.
    is_weekend_screening = is_friday_data or force_weekend_mode or os.getenv("FORCE_WEEKEND_SCREENING") == "true"

    if not is_weekend_screening and not os.path.exists(LATEST_JSON_PATH):
        # Fallback: If no previous list exists, must run full screening even if not Friday
        logger.info("No previous list found. Forcing full screening.")
        is_weekend_screening = True

    # 5. Screen (with mode)
    strong_stocks = apply_screening_logic(is_weekend_screening=is_weekend_screening, data_date=data_date)

    # Fundamental Analysis
    if strong_stocks:
        tickers_to_analyze = [s['ticker'] for s in strong_stocks]
        logger.info("Running fundamental analysis...")
        fund_results = analyze_tickers_in_batch(tickers_to_analyze, delay=0.5)

        # Merge results
        for s in strong_stocks:
            ticker = s['ticker']
            if ticker in fund_results:
                res = fund_results[ticker]
                s['earnings_accel'] = res['earnings']['accelerating']
                s['revenue_accel'] = res['revenue']['accelerating']
                s['earnings_display'] = res['earnings']['display']
                s['revenue_display'] = res['revenue']['display']

    # 6. Charts
    generate_charts(strong_stocks, data_date=data_date)

    # 7. Notification Logic (Count stocks with ADR% >= 4.0)
    filtered_count = sum(1 for s in strong_stocks if s.get('adr_pct', 0) >= 4.0)

    # 8. Save JSON
    # Use data_date for filenames and content to ensure alignment with Market Analysis
    today_str = data_date.strftime('%Y%m%d')
    output_data = {
        "date": data_date.strftime('%Y-%m-%d'),
        "market_status": "Neutral",
        "status_text": f"Strong Stocks: {filtered_count}", # Updated text for notification
        "strong_stocks": strong_stocks,
        "last_updated": datetime.datetime.now().isoformat()
    }

    with open(os.path.join(DATA_DIR, f"{today_str}.json"), 'w') as f:
        json.dump(output_data, f)
    with open(LATEST_JSON_PATH, 'w') as f:
        json.dump(output_data, f)

    logger.info("Screener Process Complete.")
    return output_data

if __name__ == "__main__":
    run_screener_process()
