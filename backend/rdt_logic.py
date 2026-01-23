import pandas as pd
import pandas_ta as ta
import numpy as np
import yfinance as yf
import datetime
import os
from backend.screener_logic import RDTIndicators
from backend.chart_generator import generate_stock_chart
from scipy.signal import argrelextrema

def calculate_wma(series, length):
    """Calculates Weighted Moving Average (WMA)."""
    weights = np.arange(1, length + 1)
    sum_weights = weights.sum()
    return series.rolling(window=length).apply(lambda x: np.dot(x, weights) / sum_weights, raw=True)

def calculate_tsv_approximation(df, length=13, ma_length=7, ma_type='EMA'):
    """
    Calculates Time Segmented Volume (TSV) approximation.
    """
    price_change = df['Close'].diff()
    signed_volume = df['Volume'] * price_change
    tsv_raw = signed_volume.rolling(window=length).sum()

    if ma_type == 'EMA':
        tsv_smoothed = tsv_raw.ewm(span=ma_length, adjust=False).mean()
    elif ma_type == 'SMA':
        tsv_smoothed = tsv_raw.rolling(window=ma_length).mean()
    else:
        tsv_smoothed = tsv_raw.rolling(window=ma_length).mean()

    return tsv_smoothed

def calculate_stochrsi_1op(df, rsi_length=14, stoch_length=14, k_smooth=5, d_smooth=5):
    """
    Calculates StochRSI with HEAVIER WMA smoothing (5, 5) to mimic 1OP cycles.
    """
    rsi = ta.rsi(df['Close'], length=rsi_length)
    rsi_low = rsi.rolling(window=stoch_length).min()
    rsi_high = rsi.rolling(window=stoch_length).max()

    denominator = rsi_high - rsi_low
    denominator = denominator.replace(0, np.nan)

    stoch_raw = ((rsi - rsi_low) / denominator) * 100
    stoch_raw = stoch_raw.fillna(50)

    k_line = calculate_wma(stoch_raw, k_smooth)
    d_line = calculate_wma(k_line, d_smooth)

    return k_line, d_line

def detect_cycle_phases(df):
    """
    Detects Bullish and Bearish Cycle Phases based on StochRSI Crosses.
    """
    if 'Fast_K' not in df.columns or 'Slow_D' not in df.columns:
        return None, None

    k = df['Fast_K'].values
    d = df['Slow_D'].values

    bullish_phase = np.zeros(len(df), dtype=bool)
    bearish_phase = np.zeros(len(df), dtype=bool)

    # State: 0 = Neutral/Unknown, 1 = Bullish, -1 = Bearish
    state = 0

    for i in range(1, len(df)):
        # Check Crosses
        cross_up = (k[i-1] <= d[i-1]) and (k[i] > d[i])
        cross_down = (k[i-1] >= d[i-1]) and (k[i] < d[i])

        if cross_up:
            state = 1
        elif cross_down:
            state = -1

        if state == 1:
            bullish_phase[i] = True
        elif state == -1:
            bearish_phase[i] = True

    return bullish_phase, bearish_phase

def get_market_analysis_data(period="6mo"):
    """
    Fetches SPY data, calculates indicators, and returns a list of dictionaries.
    Returns: (list_of_dicts, spy_dataframe)
    """
    ticker = "SPY"
    try:
        # Use simple download.
        df = yf.download(ticker, period=period, interval="1d", progress=False)
        if df.empty:
            return None, None

        if isinstance(df.columns, pd.MultiIndex):
            try:
                df.columns = df.columns.droplevel(1)
            except:
                pass
        df.columns = [c.capitalize() for c in df.columns]

        # Indicators
        df['TSV'] = calculate_tsv_approximation(df, length=12, ma_length=7, ma_type='EMA')
        df['Fast_K'], df['Slow_D'] = calculate_stochrsi_1op(df, rsi_length=14, stoch_length=14, k_smooth=5, d_smooth=5)

        # Phases
        bull_mask, bear_mask = detect_cycle_phases(df)
        df['Bullish_Phase'] = bull_mask
        df['Bearish_Phase'] = bear_mask

        results = []
        for i in range(len(df)):
            date = df.index[i]
            date_key = date.strftime('%Y%m%d')
            date_str = date.strftime('%Y/%-m/%-d')

            is_bull = bool(df['Bullish_Phase'].iloc[i])
            is_bear = bool(df['Bearish_Phase'].iloc[i])

            current_status = "Green" if is_bull else ("Red" if is_bear else "Neutral")

            if i > 0:
                prev_bull = bool(df['Bullish_Phase'].iloc[i-1])
                prev_bear = bool(df['Bearish_Phase'].iloc[i-1])
                prev_status = "Green" if prev_bull else ("Red" if prev_bear else "Neutral")
            else:
                prev_status = "Neutral"

            status_text = ""
            status_color = "Green"

            if current_status == "Green":
                status_color = "Green"
                if prev_status == "Red":
                    status_text = "Red to Green"
                elif prev_status == "Green":
                    status_text = "still Green"
                else:
                    status_text = "Start Green"
            elif current_status == "Red":
                status_color = "Red"
                if prev_status == "Green":
                    status_text = "Green to Red"
                elif prev_status == "Red":
                    status_text = "still Red"
                else:
                    status_text = "Start Red"
            else:
                status_text = "Neutral"
                status_color = "Gray"

            results.append({
                "date_key": date_key,
                "date": date_str,
                "open": float(df['Open'].iloc[i]),
                "high": float(df['High'].iloc[i]),
                "low": float(df['Low'].iloc[i]),
                "close": float(df['Close'].iloc[i]),
                "tsv": float(df['TSV'].iloc[i]) if not pd.isna(df['TSV'].iloc[i]) else None,
                "fast_k": float(df['Fast_K'].iloc[i]) if not pd.isna(df['Fast_K'].iloc[i]) else None,
                "slow_d": float(df['Slow_D'].iloc[i]) if not pd.isna(df['Slow_D'].iloc[i]) else None,
                "market_status": status_color,
                "status_text": status_text
            })

        return results, df

    except Exception as e:
        print(f"Error in get_market_analysis_data: {e}")
        return None, None

def calculate_zigzag_scipy(df, order=5):
    """
    Calculates ZigZag pivots using scipy.signal.argrelextrema.
    Returns a list of dictionaries: [{'idx': idx, 'date': date, 'price': price, 'type': 'high'/'low'}, ...]
    """
    if df is None or df.empty:
        return []

    # 1. Find local maxs and mins
    highs = df['High'].values
    lows = df['Low'].values
    dates = df.index

    # Find indexes of local extrema
    high_idxs = argrelextrema(highs, np.greater, order=order)[0]
    low_idxs = argrelextrema(lows, np.less, order=order)[0]

    candidates = []
    for idx in high_idxs:
        candidates.append({'idx': idx, 'date': dates[idx], 'price': highs[idx], 'type': 'high'})
    for idx in low_idxs:
        candidates.append({'idx': idx, 'date': dates[idx], 'price': lows[idx], 'type': 'low'})

    candidates.sort(key=lambda x: x['idx'])

    if not candidates:
        return []

    # 2. Filter for Alternating High/Low (ZigZag Logic)
    stack = [candidates[0]]

    for current in candidates[1:]:
        last = stack[-1]

        if last['type'] == current['type']:
            if last['type'] == 'high':
                if current['price'] > last['price']:
                    stack.pop()
                    stack.append(current)
            else:
                if current['price'] < last['price']:
                    stack.pop()
                    stack.append(current)
        else:
            stack.append(current)

    return stack

def calculate_anchored_vwap_252_high(df):
    """
    Calculates AVWAP anchored to the highest high of the last 252 days.
    Uses OHLC4 as input price.
    Returns a pandas Series (avwap).
    """
    if df is None or len(df) < 1:
        return None

    # 1. OHLC4 Calculation
    average_price = (df['Open'] + df['High'] + df['Low'] + df['Close']) / 4
    pv = average_price * df['Volume']

    # 2. Identify Anchor (Highest High in last 252 days relative to end of data)
    # We find the max high in the last 252 rows (or less if not enough data).
    lookback = min(252, len(df))
    last_window = df.tail(lookback)
    anchor_idx = last_window['High'].idxmax()

    # 3. Calculate AVWAP from anchor
    # Create a mask for data from anchor onwards
    mask = df.index >= anchor_idx

    cum_pv = pv[mask].cumsum()
    cum_vol = df['Volume'][mask].cumsum()

    avwap = cum_pv / cum_vol

    # Reindex to match original df, filling pre-anchor with NaN
    avwap_series = avwap.reindex(df.index)

    return avwap_series

def analyze_vcp_logic(df, pivots, lookback=5):
    """
    Analyzes VCP characteristics: Contraction, Tightness, Dry Up.
    Checks the last `lookback` days for qualification to allow for breakout volume.
    Returns a dictionary of metrics.
    """
    vcp_metrics = {
        'contractions': [],
        'is_contracting': False,
        'tightness_val': 0.0,
        'is_tight': False,
        'vol_vs_sma': 0.0,
        'is_dry_up': False,
        'vcp_qualified': False,
        'qualified_date': None
    }

    if df is None or df.empty:
        return vcp_metrics

    # 1. Contraction Analysis (Wave Depths - Structural Check)
    contractions = []
    pivots_rev = list(reversed(pivots))

    for i in range(len(pivots_rev) - 1):
        p2 = pivots_rev[i]   # More recent
        p1 = pivots_rev[i+1] # Older

        if p1['type'] == 'high' and p2['type'] == 'low':
            depth = (p2['price'] - p1['price']) / p1['price']
            contractions.append(abs(depth))

        if len(contractions) >= 4:
            break

    contractions = list(reversed(contractions))
    vcp_metrics['contractions'] = [round(c * 100, 2) for c in contractions]

    is_contracting = False
    if len(contractions) >= 2:
        if contractions[-1] < max(contractions[:-1]) and contractions[-1] < 0.10:
            is_contracting = True
    elif len(contractions) == 1:
            if contractions[0] < 0.15:
                is_contracting = True

    vcp_metrics['is_contracting'] = bool(is_contracting)

    # 2. Iterate Backwards through Lookback Window for Tightness/DryUp
    # We want to see if it WAS valid VCP within the last few days (before the volume came in)

    # Pre-calculate indicators needed for the window
    # Tightness (10d volatility)
    hl_range = (df['High'] - df['Low']) / df['Close']
    rolling_tightness = hl_range.rolling(window=10).mean()

    # Dry Up (50d Vol SMA)
    vol_sma50 = df['Volume'].rolling(window=50).mean()

    window_end = len(df)
    window_start = max(0, window_end - lookback)

    best_date = None
    passed_any = False

    # Last values for display (current state)
    current_tightness = rolling_tightness.iloc[-1]
    current_vol = df['Volume'].iloc[-1]
    current_vol_sma = vol_sma50.iloc[-1]

    vcp_metrics['tightness_val'] = float(round(current_tightness * 100, 2)) if pd.notna(current_tightness) else 0.0
    vcp_metrics['vol_vs_sma'] = float(round(current_vol / current_vol_sma, 2)) if (pd.notna(current_vol_sma) and current_vol_sma > 0) else 0.0

    # These booleans represent CURRENT state (which might be False if breakout occurring)
    vcp_metrics['is_tight'] = bool(vcp_metrics['tightness_val'] < 4.0)
    vcp_metrics['is_dry_up'] = bool(vcp_metrics['vol_vs_sma'] < 0.7)

    # Loop back to find a qualifying day
    for i in range(window_end - 1, window_start - 1, -1):
        idx = df.index[i]

        t_val = rolling_tightness.iloc[i]
        v_val = df['Volume'].iloc[i]
        sma_val = vol_sma50.iloc[i]

        if pd.isna(t_val) or pd.isna(sma_val) or sma_val == 0:
            continue

        is_t = t_val < 0.04
        is_d = v_val < (sma_val * 0.7)

        if is_t and is_d:
            passed_any = True
            best_date = idx.strftime('%Y-%m-%d')
            # If we find a match closest to today, we can stop or keep looking for 'best'.
            # Stopping at most recent match is logical.
            break

    vcp_metrics['vcp_qualified'] = bool(passed_any)
    vcp_metrics['qualified_date'] = best_date

    return vcp_metrics

def run_screener_for_tickers(tickers, spy_df, data_dir=None, date_key=None, target_date=None):
    """
    Runs the RDT screener for a list of tickers against the SPY dataframe.
    If data_dir and date_key are provided, generates and saves charts for passing stocks.
    If target_date (str 'YYYY-MM-DD' or datetime) is provided, screens based on that specific date.
    """
    strong_stocks = []

    # Prepare target date filter
    target_dt = None
    if target_date:
        if isinstance(target_date, str):
             target_dt = pd.to_datetime(target_date)
        else:
             target_dt = target_date

    # Determine reference date for data freshness check
    # If target_date is set, we check freshness against that.
    # Otherwise, we check against the latest available market date (from spy_df).
    reference_dt = target_dt
    if reference_dt is None and spy_df is not None and not spy_df.empty:
        # Assuming spy_df index is DatetimeIndex and sorted
        reference_dt = spy_df.index[-1]
        # Ensure it is timezone-naive to match stock data (which we set ignore_tz=True)
        if reference_dt.tz is not None:
             reference_dt = reference_dt.tz_localize(None)

    try:
        # Fetch 1y data to ensure we have enough for 200 SMA
        # Note: If target_date is far in the past, "1y" from now might not be enough context for that date.
        # But for "past month backfill", 1y is fine.
        CHUNK_SIZE = 100
        for i in range(0, len(tickers), CHUNK_SIZE):
            chunk = tickers[i:i+CHUNK_SIZE]

            try:
                # We fetch standard 1y. If we needed deeper history for old backfills, we'd adjust start/end.
                data = yf.download(chunk, period="1y", group_by='ticker', threads=True, progress=False, ignore_tz=True)
            except Exception as e:
                print(f"Error fetching chunk {i}: {e}")
                continue

            for ticker in chunk:
                try:
                    if len(chunk) == 1:
                        if isinstance(data.columns, pd.MultiIndex):
                             df = data[ticker]
                        else:
                             df = data
                    else:
                        if ticker not in data.columns.levels[0]:
                            continue
                        df = data[ticker]

                    if df.empty:
                        continue

                    df = df.dropna(how='all')

                    # If target date is set, slice data up to that date
                    if target_dt:
                        # Ensure index is datetime
                        if not isinstance(df.index, pd.DatetimeIndex):
                             df.index = pd.to_datetime(df.index)

                        # Slice
                        df = df[df.index <= target_dt]
                        if df.empty:
                            continue

                    if len(df) < 200:
                        continue

                    # Also need to slice SPY to match context if strict,
                    # but calculate_all handles alignment via intersection.
                    # Ideally we pass the full SPY so indicators can be calc'd properly,
                    # then we look at the last row.

                    df_calc = RDTIndicators.calculate_all(df, spy_df)

                    if df_calc.empty:
                        continue

                    last_row = df_calc.iloc[-1]

                    # Verify the last row date matches target_date (or is the closest trading day)
                    # If backfilling, we want the state AT that date.
                    # Also applies to current date: check against market reference to filter delisted stocks.
                    if reference_dt:
                        row_date = last_row.name
                        # Ensure row_date is tz-naive
                        if hasattr(row_date, 'tz') and row_date.tz is not None:
                             row_date = row_date.tz_localize(None)

                        # If the last available data is older than reference date by too much (e.g. > 5 days), skip
                        # This filters out delisted stocks (like HOUS) or stale data.
                        if (reference_dt - row_date).days > 5:
                            continue

                    check_res = RDTIndicators.check_filters(last_row)

                    if check_res['All_Pass']:
                        # --- Calculate VCP Metrics and Plot Data ---
                        pivots = calculate_zigzag_scipy(df_calc, order=5)
                        avwap = calculate_anchored_vwap_252_high(df_calc)
                        vcp_metrics = analyze_vcp_logic(df_calc, pivots)

                        vcp_data = {
                            'pivots': pivots,
                            'avwap': avwap
                        }

                        stock_info = {
                            "ticker": ticker,
                            "rrs": round(last_row['RRS'], 2),
                            "rvol": round(last_row['RVol'], 2),
                            "adr_pct": round(last_row['ADR_Percent'], 2),
                            "atr_multiple": round(last_row['ATR_Multiple_50MA'], 2),
                            "prev_high": round(last_row['High'], 2),
                            "vcp_metrics": vcp_metrics  # Save metrics to JSON
                        }

                        # Generate Chart
                        if data_dir and date_key:
                            chart_filename = f"{date_key}-{ticker}.png"
                            chart_path = os.path.join(data_dir, chart_filename)
                            # Pass vcp_data to chart generator
                            if generate_stock_chart(df_calc, chart_path, ticker, vcp_data=vcp_data):
                                stock_info["chart_image"] = chart_filename

                        strong_stocks.append(stock_info)

                except Exception as e:
                    # print(f"Error screening {ticker}: {e}")
                    continue

    except Exception as e:
        print(f"Error in run_screener_for_tickers: {e}")
        return []

    return strong_stocks
