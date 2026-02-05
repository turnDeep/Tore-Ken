import pandas as pd
import pandas_ta as ta
import numpy as np
import yfinance as yf
import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def get_market_analysis_data(ticker="SPY", period="6mo"):
    """
    Fetches SPY data, calculates indicators, and returns a list of dictionaries.
    Returns: (list_of_dicts, spy_dataframe)
    """
    try:
        # Use simple download.
        df = yf.download(ticker, period=period, interval="1d", progress=False)
        if df.empty:
            logger.error("Market data download failed")
            return [], pd.DataFrame()

        # Handle MultiIndex columns (Price, Ticker) -> Flatten to just Price
        if isinstance(df.columns, pd.MultiIndex):
            try:
                df.columns = df.columns.droplevel('Ticker')
            except KeyError:
                if df.columns.nlevels > 1:
                     df.columns = df.columns.droplevel(1)

        # Ensure Index is tz-naive
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        df.columns = [c.capitalize() for c in df.columns]

        # Indicators
        df['TSV'] = calculate_tsv_approximation(df, length=12, ma_length=7, ma_type='EMA')
        df['Fast_K'], df['Slow_D'] = calculate_stochrsi_1op(df, rsi_length=14, stoch_length=14, k_smooth=5, d_smooth=5)

        # Bollinger Bands (20, 2)
        bb = ta.bbands(df['Close'], length=20, std=2)
        if bb is not None:
            # Handle pandas_ta column naming variations
            cols = bb.columns
            # Dynamically find columns ending with specific patterns if strict names fail
            bbl = [c for c in cols if c.startswith('BBL')][0]
            bbm = [c for c in cols if c.startswith('BBM')][0]
            bbu = [c for c in cols if c.startswith('BBU')][0]

            df['BB_Lower'] = bb[bbl]
            df['BB_Mid'] = bb[bbm]
            df['BB_Upper'] = bb[bbu]
        else:
            # Fallback if ta.bbands fails
            df['BB_Mid'] = df['Close'].rolling(20).mean()
            std = df['Close'].rolling(20).std()
            df['BB_Upper'] = df['BB_Mid'] + 2 * std
            df['BB_Lower'] = df['BB_Mid'] - 2 * std

        # RSI (14)
        df['RSI'] = ta.rsi(df['Close'], length=14)

        # BB + RSI + StochRSI Logic Signals
        # Buy: Touched Lower BB + StochRSI Cross Up + RSI < 45
        # Sell: Touched Upper BB + StochRSI Cross Down + RSI > 55

        # Helper for StochRSI Cross
        k = df['Fast_K']
        d = df['Slow_D']
        cross_up = (k.shift(1) <= d.shift(1)) & (k > d)
        cross_down = (k.shift(1) >= d.shift(1)) & (k < d)

        # Helper for Price vs BB (Relaxed: Price <= Lower BB * 1.01 to catch touches)
        touched_lower = df['Low'] <= df['BB_Lower'] * 1.01
        touched_upper = df['High'] >= df['BB_Upper'] * 0.99

        # Context
        rsi_oversold = df['RSI'] < 50 # Relaxed from 45 to 50 for broader detection in this example
        rsi_overbought = df['RSI'] > 50

        # Signals
        df['BB_RSI_Buy'] = touched_lower & cross_up & rsi_oversold
        df['BB_RSI_Sell'] = touched_upper & cross_down & rsi_overbought

        # Phases
        bull_mask, bear_mask = detect_cycle_phases(df)
        df['Bullish_Phase'] = bull_mask
        df['Bearish_Phase'] = bear_mask

        # Generate Trend Signal for Chart Generator (1: Bull, -1: Bear, 0: Neutral)
        # The legacy logic uses statuses like "Green", "Red", "Neutral".
        # Chart generator expects 'Trend_Signal' column with 1, -1, 0.
        conditions = [df['Bullish_Phase'] == True, df['Bearish_Phase'] == True]
        choices = [1, -1]
        df['Trend_Signal'] = np.select(conditions, choices, default=0)

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
            status_color = "Green" # Default?

            # Replicate legacy text logic
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

            # Map legacy status color to frontend expectations if needed
            # Frontend uses status_text for badge color logic:
            # includes "Red to" -> Green badge (bullish reversal)
            # includes "Green to" -> Red badge (bearish reversal)
            # includes "Green" -> Green
            # includes "Red" -> Red

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
                "rsi": float(df['RSI'].iloc[i]) if not pd.isna(df['RSI'].iloc[i]) else None,
                "bb_upper": float(df['BB_Upper'].iloc[i]) if not pd.isna(df['BB_Upper'].iloc[i]) else None,
                "bb_lower": float(df['BB_Lower'].iloc[i]) if not pd.isna(df['BB_Lower'].iloc[i]) else None,
                "bb_rsi_buy": bool(df['BB_RSI_Buy'].iloc[i]),
                "bb_rsi_sell": bool(df['BB_RSI_Sell'].iloc[i]),
                "market_status": status_color, # Legacy field
                "status_text": status_text
            })

        return results, df

    except Exception as e:
        logger.error(f"Error in get_market_analysis_data: {e}")
        return [], pd.DataFrame()
