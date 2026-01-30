import pandas as pd
import pandas_ta as ta
import yfinance as yf
import numpy as np
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_trend_signals(df):
    """
    Calculates the Market Trend Signal (Green/Red/Neutral) based on RDT logic.
    Green: Price > 5d MA > 20d MA > 50d MA (Perfect Order)
    Red: Price < 5d MA < 20d MA < 50d MA
    Neutral: Otherwise
    """
    # Simple Moving Averages
    df['SMA_5'] = ta.sma(df['Close'], length=5)
    df['SMA_20'] = ta.sma(df['Close'], length=20)
    df['SMA_50'] = ta.sma(df['Close'], length=50)

    conditions = [
        (df['Close'] > df['SMA_5']) & (df['SMA_5'] > df['SMA_20']) & (df['SMA_20'] > df['SMA_50']),
        (df['Close'] < df['SMA_5']) & (df['SMA_5'] < df['SMA_20']) & (df['SMA_20'] < df['SMA_50'])
    ]
    choices = [1, -1] # 1: Green, -1: Red
    df['Trend_Signal'] = np.select(conditions, choices, default=0) # 0: Neutral

    return df

def get_market_analysis_data(ticker="SPY", period="1y"):
    """
    Fetches market data (SPY), calculates trend signals, and prepares data for the frontend/chart.
    Returns: (list_of_dicts_for_json, dataframe_for_chart)
    """
    try:
        # Fetch Data
        df = yf.download(ticker, period=period, progress=False, ignore_tz=True)
        if df.empty:
            logger.error("Market data download failed")
            return [], pd.DataFrame()

        # Handle MultiIndex columns (Price, Ticker) -> Flatten to just Price
        if isinstance(df.columns, pd.MultiIndex):
            # If the second level is the ticker, we can just drop it
            try:
                df.columns = df.columns.droplevel('Ticker')
            except KeyError:
                # If 'Ticker' level doesn't exist by name, try level 1
                if df.columns.nlevels > 1:
                     df.columns = df.columns.droplevel(1)

        # Calculate Signals
        df = calculate_trend_signals(df)

        # Additional Indicators for Chart (TSV approx, StochRSI)
        # TSV Approximation: (Close - Close[1]) * Volume
        df['TSV'] = df['Close'].diff() * df['Volume']
        df['TSV_MA'] = ta.sma(df['TSV'], length=13)

        # StochRSI
        stochrsi = ta.stochrsi(df['Close'], length=14, rsi_length=14, k=3, d=3)
        if stochrsi is not None:
            df = pd.concat([df, stochrsi], axis=1)
            # Find columns that start with STOCHRSIk and STOCHRSId and rename them to exact names
            k_col = next((c for c in df.columns if 'STOCHRSIk' in c), None)
            d_col = next((c for c in df.columns if 'STOCHRSId' in c), None)

            rename_dict = {}
            if k_col: rename_dict[k_col] = 'StochRSI_K'
            if d_col: rename_dict[d_col] = 'StochRSI_D'

            if rename_dict:
                df.rename(columns=rename_dict, inplace=True)

        # Format for JSON history (Last 6 months only to keep JSON small? Or full period?)
        # Frontend slider needs history. 6 months (approx 126 days) is good default.
        # "Market Analysis 6ヶ月チャート"

        start_date = df.index[-1] - pd.DateOffset(months=6)
        history_df = df.loc[start_date:].copy()

        market_data_list = []

        # Helper to generate status text
        def get_status_text(row, prev_row):
            sig = row['Trend_Signal']
            if prev_row is None:
                prev_sig = 0
            else:
                prev_sig = prev_row['Trend_Signal']

            if sig == 1:
                return "Green Zone" if prev_sig == 1 else "to Green Zone"
            elif sig == -1:
                return "Red Zone" if prev_sig == -1 else "to Red Zone"
            else:
                return "Neutral"

        for i in range(len(history_df)):
            row = history_df.iloc[i]
            prev_row = history_df.iloc[i-1] if i > 0 else None

            date_str = row.name.strftime('%Y-%m-%d')
            date_key = row.name.strftime('%Y%m%d')

            status_text = get_status_text(row, prev_row)

            market_data_list.append({
                "date": date_str,
                "date_key": date_key,
                "market_status": "Bull" if row['Trend_Signal'] == 1 else "Bear" if row['Trend_Signal'] == -1 else "Neutral",
                "status_text": status_text,
                "close": round(row['Close'], 2)
            })

        return market_data_list, history_df

    except Exception as e:
        logger.error(f"Error in get_market_analysis_data: {e}")
        return [], pd.DataFrame()
