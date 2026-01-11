import pandas as pd
import pandas_ta as ta
import numpy as np
import yfinance as yf
import os
from curl_cffi import requests as curl_requests
import datetime

# --- RDTDataFetcher ---
class RDTDataFetcher:
    def __init__(self):
        self.session = curl_requests.Session(impersonate="chrome")

    def fetch_spy(self, period="2y"):
        try:
            # Using yf.download to respect existing logic
            obj = yf.Ticker("SPY", session=self.session)
            hist = obj.history(period=period)
            if hist.empty:
                return None
            return hist
        except Exception as e:
            print(f"Error fetching SPY: {e}")
            return None

    def fetch_batch(self, tickers, period="2y"):
        if not tickers:
            return {}
        try:
            # group_by='ticker' ensures we get a MultiIndex (Ticker, OHLC)
            # auto_adjust=True to get 'Close' as adjusted close usually, but let's stick to default
            data = yf.download(tickers, period=period, group_by='ticker', threads=True, progress=False, ignore_tz=True)
            result = {}
            if len(tickers) == 1:
                # If single ticker, data might not be multiindex if group_by is ignored by yf sometimes
                # But with group_by='ticker' it should be consistent.
                # However, for single ticker, yf.download might return just the OHLC columns.
                # Let's check columns.
                if isinstance(data.columns, pd.MultiIndex):
                    # Should not happen for single ticker usually with simple call, but group_by forces it?
                    # Actually yf 0.2.x changed behavior.
                    # Let's assume if columns are simple, it's the DF.
                    pass
                if not data.empty:
                    result[tickers[0]] = data
            else:
                for ticker in tickers:
                    try:
                        df = data[ticker]
                        if not df.empty and not df.isnull().all().all():
                            result[ticker] = df
                    except KeyError:
                        pass
            return result
        except Exception as e:
            print(f"Batch fetch error: {e}")
            return {}

# --- RDTIndicators ---
class RDTIndicators:
    @staticmethod
    def calculate_all(df, spy_df):
        df = df.sort_index()
        spy_df = spy_df.sort_index()
        if df.index.tz is not None: df.index = df.index.tz_localize(None)
        if spy_df.index.tz is not None: spy_df.index = spy_df.index.tz_localize(None)

        common_index = df.index.intersection(spy_df.index)
        df = df.loc[common_index].copy()
        spy_df = spy_df.loc[common_index].copy()

        if df.empty: return df

        # SMAs
        df['SMA_10'] = ta.sma(df['Close'], length=10)
        df['SMA_50'] = ta.sma(df['Close'], length=50)
        df['SMA_100'] = ta.sma(df['Close'], length=100)
        df['SMA_200'] = ta.sma(df['Close'], length=200)

        # ATR
        df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)

        # ADR%
        daily_range_pct = ((df['High'] / df['Low']) - 1) * 100
        df['ADR_Percent'] = daily_range_pct.rolling(window=20).mean()

        # RVol
        df['Vol_SMA_20'] = ta.sma(df['Volume'], length=20)
        df['RVol'] = df['Volume'] / df['Vol_SMA_20']
        df['Vol_SMA_10'] = ta.sma(df['Volume'], length=10)

        # RRS
        spy_atr = ta.atr(spy_df['High'], spy_df['Low'], spy_df['Close'], length=14)
        delta_stock = df['Close'].diff()
        delta_spy = spy_df['Close'].diff()
        expected_move = delta_spy * (df['ATR'] / spy_atr)
        df['RRS_Daily'] = (delta_stock - expected_move) / df['ATR']
        df['RRS'] = df['RRS_Daily'].rolling(window=12).sum().fillna(0)

        return df

    @staticmethod
    def check_filters(row):
        rrs_pass = row['RRS'] > 1.0
        rvol_pass = row['RVol'] > 1.5 if pd.notna(row['RVol']) else False
        adr_pass = row['ADR_Percent'] > 4.0 if pd.notna(row['ADR_Percent']) else False
        liq_pass = row['Vol_SMA_10'] > 1_000_000 if pd.notna(row['Vol_SMA_10']) else False
        price_pass = row['Close'] > 5.0

        if pd.isna(row['SMA_50']) or pd.isna(row['SMA_100']) or pd.isna(row['SMA_200']):
            trend_pass = False
        else:
            trend_pass = (row['Close'] > row['SMA_50']) and \
                         (row['SMA_50'] > row['SMA_100']) and \
                         (row['SMA_100'] > row['SMA_200'])

        return {
            'RRS_Pass': rrs_pass,
            'RVol_Pass': rvol_pass,
            'ADR_Pass': adr_pass,
            'Liquidity_Pass': liq_pass,
            'Price_Pass': price_pass,
            'Trend_Pass': trend_pass,
            'All_Pass': rrs_pass and rvol_pass and adr_pass and liq_pass and price_pass and trend_pass
        }

# --- Market Analysis Helpers (from one_op_viz.py) ---
def calculate_wma(series, length):
    weights = np.arange(1, length + 1)
    sum_weights = weights.sum()
    return series.rolling(window=length).apply(lambda x: np.dot(x, weights) / sum_weights, raw=True)

def calculate_tsv_approximation(df, length=12, ma_length=7, ma_type='EMA'):
    price_change = df['Close'].diff()
    signed_volume = df['Volume'] * price_change
    tsv_raw = signed_volume.rolling(window=length).sum()
    if ma_type == 'EMA':
        tsv_smoothed = tsv_raw.ewm(span=ma_length, adjust=False).mean()
    else:
        tsv_smoothed = tsv_raw.rolling(window=ma_length).mean()
    return tsv_smoothed

def calculate_stochrsi_1op(df, rsi_length=14, stoch_length=14, k_smooth=5, d_smooth=5):
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
    k = df['Fast_K'].values
    d = df['Slow_D'].values
    bullish_phase = np.zeros(len(df), dtype=bool)
    bearish_phase = np.zeros(len(df), dtype=bool)
    state = 0 # 0 neutral, 1 bull, -1 bear

    for i in range(1, len(df)):
        cross_up = (k[i-1] <= d[i-1]) and (k[i] > d[i])
        cross_down = (k[i-1] >= d[i-1]) and (k[i] < d[i])
        if cross_up: state = 1
        elif cross_down: state = -1

        if state == 1: bullish_phase[i] = True
        elif state == -1: bearish_phase[i] = True
    return bullish_phase, bearish_phase

def get_market_analysis_data(period="6mo"):
    # Fetch SPY
    fetcher = RDTDataFetcher()
    df = fetcher.fetch_spy(period=period)
    if df is None or df.empty:
        return None

    # Flatten MultiIndex if exists
    if isinstance(df.columns, pd.MultiIndex):
        try:
            df.columns = df.columns.droplevel(1)
        except:
            pass

    # Calculate Indicators
    df['TSV'] = calculate_tsv_approximation(df, length=12, ma_length=7, ma_type='EMA')
    df['Fast_K'], df['Slow_D'] = calculate_stochrsi_1op(df)

    # Detect Cycles
    bull_mask, bear_mask = detect_cycle_phases(df)
    df['Bullish_Phase'] = bull_mask
    df['Bearish_Phase'] = bear_mask

    # Prepare Output
    output = []

    for i in range(len(df)):
        date_str = df.index[i].strftime('%Y-%m-%d')
        date_key = df.index[i].strftime('%Y%m%d')

        is_bull = df['Bullish_Phase'].iloc[i]
        is_bear = df['Bearish_Phase'].iloc[i]

        current_state = "Neutral"
        if is_bull: current_state = "Green"
        elif is_bear: current_state = "Red"

        # Determine status text
        if i == 0:
            status_text = f"still {current_state}"
        else:
            prev_bull = df['Bullish_Phase'].iloc[i-1]
            prev_bear = df['Bearish_Phase'].iloc[i-1]
            prev_state = "Neutral"
            if prev_bull: prev_state = "Green"
            elif prev_bear: prev_state = "Red"

            if prev_state == current_state:
                status_text = f"still {current_state}"
            else:
                status_text = f"{prev_state} to {current_state}"

        output.append({
            "date": date_str,
            "date_key": date_key,
            "open": float(df['Open'].iloc[i]),
            "high": float(df['High'].iloc[i]),
            "low": float(df['Low'].iloc[i]),
            "close": float(df['Close'].iloc[i]),
            "tsv": float(df['TSV'].iloc[i]) if not pd.isna(df['TSV'].iloc[i]) else None,
            "fast_k": float(df['Fast_K'].iloc[i]) if not pd.isna(df['Fast_K'].iloc[i]) else None,
            "slow_d": float(df['Slow_D'].iloc[i]) if not pd.isna(df['Slow_D'].iloc[i]) else None,
            "market_status": current_state,
            "status_text": status_text
        })

    return output, df # Return df to be used by screener if needed (for SPY comparison)

def run_screener_for_tickers(tickers, spy_df):
    fetcher = RDTDataFetcher()

    # Batch processing to avoid memory/rate limit issues
    BATCH_SIZE = 50
    results = []

    for i in range(0, len(tickers), BATCH_SIZE):
        batch_tickers = tickers[i : i + BATCH_SIZE]
        batch_data = fetcher.fetch_batch(batch_tickers, period="1y")

        for ticker, df in batch_data.items():
            if df.empty or len(df) < 200: continue

            try:
                df_calc = RDTIndicators.calculate_all(df, spy_df)
                if df_calc.empty: continue

                last_row = df_calc.iloc[-1]
                check_res = RDTIndicators.check_filters(last_row)

                if check_res['All_Pass']:
                    results.append({
                        'Ticker': ticker,
                        'Close': round(last_row['Close'], 2),
                        'RRS': round(last_row['RRS'], 2),
                        'RVol': round(last_row['RVol'], 2),
                        'ADR': round(last_row['ADR_Percent'], 2)
                    })
            except Exception as e:
                continue

    return results
