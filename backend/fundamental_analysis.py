import yfinance as yf
import pandas as pd
import numpy as np
import time
import logging

# Setup logging
logger = logging.getLogger(__name__)

def get_growth_rate(current, previous):
    if previous == 0 or pd.isna(previous) or pd.isna(current):
        return np.nan
    return (current - previous) / abs(previous)

def format_growth(val):
    if pd.isna(val):
        return "N/A"
    return f"{val:+.1%}"

def analyze_earnings_qoq(ticker):
    try:
        # Use deep history if possible
        ed = ticker.get_earnings_dates(limit=20)
        if ed is None or ed.empty or 'Reported EPS' not in ed.columns:
            return {'accelerating': False, 'status': 'No Data', 'display': ''}

        actuals = ed['Reported EPS'].dropna().sort_index()

        # Calculate QoQ Growth: pct_change(1)
        # Note: get_earnings_dates usually returns dates as index.
        # Make sure we are comparing same quarter last year?
        # The user's code uses `pct_change(1)`.
        # If the index is quarterly dates (e.g. 2023-03, 2023-06...), pct_change(1) is sequential QoQ.
        # "QoQ growth" usually means "vs same quarter last year" (YoY for quarterly data) OR "vs previous quarter".
        # In finance "QoQ" usually means sequential, "YoY" means same q last year.
        # The user's code uses `pct_change(1)` on sorted index.
        # If `actuals` contains strictly quarterly data, this is Sequential Growth.
        # However, for seasonality, often YoY is used.
        # But I must follow the user's provided code logic: `actuals.pct_change(1)`.

        qoq_growth = actuals.pct_change(1)
        valid_growth = qoq_growth.dropna()

        if len(valid_growth) < 2:
            return {'accelerating': False, 'status': 'Insufficient History', 'display': ''}

        # 1. Historical Acceleration: Latest QoQ > Previous QoQ
        g_latest_actual_qoq = valid_growth.iloc[-1]
        g_prev_actual_qoq = valid_growth.iloc[-2]
        latest_val = actuals.iloc[-1] # Needed for next step estimate QoQ calc

        hist_accel = bool(g_latest_actual_qoq > g_prev_actual_qoq)

        # 2. Estimates Acceleration
        est_accel = True # Assume true until proven false
        g_0q_qoq = None
        g_1q_qoq = None

        try:
            est_df = ticker.get_earnings_estimate()
            if est_df is not None and not est_df.empty:
                val_0q = None
                val_1q = None

                if '0q' in est_df.index and 'avg' in est_df.columns:
                    val_0q = est_df.loc['0q', 'avg']

                if '+1q' in est_df.index and 'avg' in est_df.columns:
                    val_1q = est_df.loc['+1q', 'avg']

                if val_0q is not None:
                    g_0q_qoq = get_growth_rate(val_0q, latest_val)

                if val_1q is not None and val_0q is not None:
                    g_1q_qoq = get_growth_rate(val_1q, val_0q)

                # Check 0q vs Latest (Non-Decreasing)
                if g_0q_qoq is not None:
                    if g_0q_qoq < g_latest_actual_qoq:
                        est_accel = False

                # Check 1q vs 0q (Non-Decreasing)
                if g_1q_qoq is not None and g_0q_qoq is not None:
                    if g_1q_qoq < g_0q_qoq:
                        est_accel = False

                # If no estimates available, est_accel stays True (relying on hist)?
                # Or require at least one estimate?
                # User request implies "Accelerating in order".
                # If data is missing, we can't confirm "in order acceleration".
                if g_0q_qoq is None and g_1q_qoq is None:
                     # No future data, so cannot confirm future acceleration.
                     # However, if only hist is present, is it "accelerating"?
                     # Usually "growth acceleration" requires looking ahead if available.
                     # Let's say if NO estimates, default to True (neutral) or False?
                     # Given the prompt emphasizes the sequence displayed, if only 2 items are displayed (Prev->Latest),
                     # and they accelerate, then it is accelerating.
                     pass


        except Exception as e:
            # logger.warning(f"Error fetching earnings estimates: {e}")
            pass

        # Construct Display String
        # User requested: "EPS(QoQ): +17.2% → -26.5% → +16.7%"
        # We will show: Prev -> Latest -> Future
        # Which future? The one that triggered acceleration, or just 0q?
        # Let's show 0q if available.

        display_str = f"EPS(QoQ): {format_growth(g_prev_actual_qoq)} → {format_growth(g_latest_actual_qoq)}"

        if g_0q_qoq is not None:
             display_str += f" → {format_growth(g_0q_qoq)}"

        if g_1q_qoq is not None:
             display_str += f" → {format_growth(g_1q_qoq)}"

        return {
            'accelerating': bool(hist_accel and est_accel),
            'hist_accel': hist_accel,
            'est_accel': est_accel,
            'display': display_str
        }

    except Exception as e:
        logger.error(f"Error in analyze_earnings_qoq: {e}")
        return {'accelerating': False, 'status': 'Error', 'display': ''}

def analyze_revenue_qoq(ticker):
    try:
        # Note: quarterly_income_stmt might be expensive or limited
        fin = ticker.quarterly_income_stmt
        if fin is None or fin.empty:
            return {'accelerating': False, 'status': 'No Data', 'display': ''}

        fin = fin.T.sort_index()
        if 'Total Revenue' not in fin.columns:
             return {'accelerating': False, 'status': 'No Column', 'display': ''}

        rev = fin['Total Revenue'].dropna()

        # QoQ
        qoq_growth = rev.pct_change(1)
        valid_growth = qoq_growth.dropna()

        if len(valid_growth) < 2:
             return {'accelerating': False, 'status': 'Insufficient History', 'display': ''}

        g_latest_actual_qoq = valid_growth.iloc[-1]
        g_prev_actual_qoq = valid_growth.iloc[-2]
        latest_val = rev.iloc[-1]

        hist_accel = bool(g_latest_actual_qoq > g_prev_actual_qoq)

        # Estimates
        est_accel = True
        g_0q_qoq = None
        g_1q_qoq = None

        try:
            est_df = ticker.get_revenue_estimate()
            if est_df is not None and not est_df.empty:
                val_0q = None
                val_1q = None

                if '0q' in est_df.index and 'avg' in est_df.columns:
                    val_0q = est_df.loc['0q', 'avg']

                if '+1q' in est_df.index and 'avg' in est_df.columns:
                    val_1q = est_df.loc['+1q', 'avg']

                if val_0q is not None:
                    g_0q_qoq = get_growth_rate(val_0q, latest_val)

                if val_1q is not None and val_0q is not None:
                    g_1q_qoq = get_growth_rate(val_1q, val_0q)

                # Check 0q vs Latest (Non-Decreasing)
                if g_0q_qoq is not None:
                    if g_0q_qoq < g_latest_actual_qoq:
                        est_accel = False

                # Check 1q vs 0q (Non-Decreasing)
                if g_1q_qoq is not None and g_0q_qoq is not None:
                    if g_1q_qoq < g_0q_qoq:
                        est_accel = False

        except Exception as e:
            # logger.warning(f"Error fetching revenue estimates: {e}")
            pass

        display_str = f"Rev(QoQ): {format_growth(g_prev_actual_qoq)} → {format_growth(g_latest_actual_qoq)}"

        if g_0q_qoq is not None:
             display_str += f" → {format_growth(g_0q_qoq)}"

        if g_1q_qoq is not None:
             display_str += f" → {format_growth(g_1q_qoq)}"

        return {
            'accelerating': bool(hist_accel and est_accel),
            'hist_accel': hist_accel,
            'est_accel': est_accel,
            'display': display_str
        }

    except Exception as e:
        logger.error(f"Error in analyze_revenue_qoq: {e}")
        return {'accelerating': False, 'status': 'Error', 'display': ''}

def analyze_ticker(symbol):
    # logger.info(f"Analyzing fundamentals for {symbol}...")
    try:
        ticker = yf.Ticker(symbol)

        earnings_res = analyze_earnings_qoq(ticker)
        revenue_res = analyze_revenue_qoq(ticker)

        return {
            'symbol': symbol,
            'earnings': earnings_res,
            'revenue': revenue_res
        }
    except Exception as e:
        logger.error(f"Error analyzing ticker {symbol}: {e}")
        return {
            'symbol': symbol,
            'earnings': {'accelerating': False, 'display': ''},
            'revenue': {'accelerating': False, 'display': ''}
        }

def analyze_tickers_in_batch(tickers, delay=1.0):
    """
    Analyzes a list of tickers with a delay to respect API limits.
    Returns a dictionary: { symbol: result_dict }
    """
    results = {}
    total = len(tickers)
    logger.info(f"Starting fundamental analysis for {total} tickers...")

    for i, symbol in enumerate(tickers):
        logger.info(f"Analyzing {symbol} ({i+1}/{total})")
        res = analyze_ticker(symbol)
        results[symbol] = res
        time.sleep(delay)

    return results
