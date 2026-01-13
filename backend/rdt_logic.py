import pandas as pd
import pandas_ta as ta
import numpy as np
import yfinance as yf
import datetime
import os
from backend.screener_logic import RDTIndicators
from backend.chart_generator import generate_stock_chart

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
                    status_text = "Red to ●Green"
                elif prev_status == "Green":
                    status_text = "still ●Green"
                else:
                    status_text = "Start ●Green"
            elif current_status == "Red":
                status_color = "Red"
                if prev_status == "Green":
                    status_text = "Green to ●Red"
                elif prev_status == "Red":
                    status_text = "still ●Red"
                else:
                    status_text = "Start ●Red"
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

def run_screener_for_tickers(tickers, spy_df, data_dir=None, date_key=None):
    """
    Runs the RDT screener for a list of tickers against the SPY dataframe.
    If data_dir and date_key are provided, generates and saves charts for passing stocks.
    """
    strong_stocks = []

    try:
        # Fetch 1y data to ensure we have enough for 200 SMA
        CHUNK_SIZE = 100
        for i in range(0, len(tickers), CHUNK_SIZE):
            chunk = tickers[i:i+CHUNK_SIZE]

            try:
                data = yf.download(chunk, period="1y", group_by='ticker', threads=True, progress=False, ignore_tz=True)
            except Exception as e:
                print(f"Error fetching chunk {i}: {e}")
                continue

            for ticker in chunk:
                try:
                    if len(chunk) == 1:
                        df = data
                    else:
                        if ticker not in data.columns.levels[0]:
                            continue
                        df = data[ticker]

                    if df.empty:
                        continue

                    df = df.dropna(how='all')

                    if len(df) < 200:
                        continue

                    df_calc = RDTIndicators.calculate_all(df, spy_df)

                    if df_calc.empty:
                        continue

                    last_row = df_calc.iloc[-1]
                    check_res = RDTIndicators.check_filters(last_row)

                    if check_res['All_Pass']:
                        stock_info = {
                            "ticker": ticker,
                            "rrs": round(last_row['RRS'], 2),
                            "rvol": round(last_row['RVol'], 2),
                            "adr_pct": round(last_row['ADR_Percent'], 2),
                            "atr_multiple": round(last_row['ATR_Multiple_50MA'], 2)
                        }

                        # Generate Chart
                        if data_dir and date_key:
                            chart_filename = f"{date_key}-{ticker}.png"
                            chart_path = os.path.join(data_dir, chart_filename)
                            if generate_stock_chart(df_calc, chart_path, ticker):
                                stock_info["chart_image"] = chart_filename

                        strong_stocks.append(stock_info)

                except Exception as e:
                    # print(f"Error screening {ticker}: {e}")
                    continue

    except Exception as e:
        print(f"Error in run_screener_for_tickers: {e}")
        return []

    return strong_stocks
