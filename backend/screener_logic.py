import pandas as pd
import pandas_ta as ta

class RDTIndicators:
    @staticmethod
    def calculate_all(df, spy_df):
        """
        Calculates all RDT system indicators for a given stock DataFrame.
        """
        # Ensure df is sorted
        df = df.sort_index()

        # Normalize Timezones for df
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        # --- Calculate Stock-Intrinsic Indicators (Before Alignment) ---
        # Calculate these on the full available history (1y) to avoid NaN in SMAs

        # Basic Price & MA
        df['SMA_10'] = ta.sma(df['Close'], length=10)
        df['SMA_20'] = ta.sma(df['Close'], length=20)
        df['SMA_50'] = ta.sma(df['Close'], length=50)
        df['SMA_100'] = ta.sma(df['Close'], length=100)
        df['SMA_200'] = ta.sma(df['Close'], length=200)

        # ATR (14)
        df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)

        # ADR% (20-day Average Daily Range %)
        daily_range_pct = ((df['High'] / df['Low']) - 1) * 100
        df['ADR_Percent'] = daily_range_pct.rolling(window=20).mean()

        # RVol (Fuel)
        df['Vol_SMA_20'] = ta.sma(df['Volume'], length=20)
        df['RVol'] = df['Volume'] / df['Vol_SMA_20']

        # Volume Moving Average Check (Dry Up)
        df['Vol_SMA_5'] = ta.sma(df['Volume'], length=5)
        df['Vol_SMA_50'] = ta.sma(df['Volume'], length=50)

        # Up/Down Volume Ratio (Accumulation Check)
        # Ratio of 'Total volume of rising days' to 'Total volume of falling days' in the past 50 days.
        close_change = df['Close'].diff()
        up_volume = df['Volume'].where(close_change > 0, 0)
        down_volume = df['Volume'].where(close_change < 0, 0)

        up_vol_sum_50 = up_volume.rolling(window=50).sum()
        down_vol_sum_50 = down_volume.rolling(window=50).sum()

        # Avoid division by zero
        df['Up_Down_Volume_Ratio_50'] = up_vol_sum_50 / down_vol_sum_50.replace(0, 1)

        # Liquidity Check (10-day Avg Volume)
        df['Vol_SMA_10'] = ta.sma(df['Volume'], length=10)

        # --- Alignment with Market Data (SPY) ---
        # Ensure spy_df is sorted and normalized
        spy_df = spy_df.sort_index()
        if spy_df.index.tz is not None:
            spy_df.index = spy_df.index.tz_localize(None)

        # Align dates (intersection)
        common_index = df.index.intersection(spy_df.index)

        # If no overlap, return empty or handle gracefully
        if common_index.empty:
            return pd.DataFrame() # Return empty DF

        df = df.loc[common_index].copy()
        spy_df = spy_df.loc[common_index].copy()

        if df.empty:
            return df

        # --- RRS (Real Relative Strength) - Requires Aligned Data ---
        spy_atr = ta.atr(spy_df['High'], spy_df['Low'], spy_df['Close'], length=14)

        # Calculate Deltas (Price Change)
        delta_stock = df['Close'].diff()
        delta_spy = spy_df['Close'].diff()

        # Expected Move = Delta_SPY * (Stock_ATR / SPY_ATR)
        expected_move = delta_spy * (df['ATR'] / spy_atr)

        # RRS
        df['RRS_Daily'] = (delta_stock - expected_move) / df['ATR']

        # RRS (Smoothed)
        df['RRS'] = df['RRS_Daily'].rolling(window=12).sum().fillna(0)

        # --- ATR% Multiple from 50-MA ---
        # 1. ATR % (ATR / Price)
        atr_pct = df['ATR'] / df['Close']

        # 2. % Gain From 50-MA
        pct_from_50ma = (df['Close'] - df['SMA_50']) / df['SMA_50']

        # 3. ATR Multiple (Avoid division by zero)
        df['ATR_Multiple_50MA'] = pct_from_50ma / atr_pct
        # Handle cases where ATR is 0 or NaN
        df['ATR_Multiple_50MA'] = df['ATR_Multiple_50MA'].fillna(0.0)
        # Handle Infinite values (division by zero results)
        df['ATR_Multiple_50MA'] = df['ATR_Multiple_50MA'].replace([float('inf'), float('-inf')], 0.0)

        return df

    @staticmethod
    def check_filters(row):
        """
        Checks if the latest row meets the RDT criteria.
        Returns a dictionary of results.
        """
        # 1. RRS > 1.0 (Most Important)
        rrs_pass = row['RRS'] > 1.0

        # 2. Volume Check: Dry Up and Accumulation
        # Dry Up: Vol_SMA_5 < 0.7 * Vol_SMA_50
        # Accumulation: Up/Down Volume Ratio >= 1.0

        dry_up_pass = False
        if pd.notna(row['Vol_SMA_5']) and pd.notna(row['Vol_SMA_50']):
            dry_up_pass = row['Vol_SMA_5'] < (0.7 * row['Vol_SMA_50'])

        accumulation_pass = False
        if pd.notna(row['Up_Down_Volume_Ratio_50']):
            accumulation_pass = row['Up_Down_Volume_Ratio_50'] >= 1.0

        # Combine into one volume pass condition for the screener
        vol_pass = dry_up_pass and accumulation_pass

        # 3. ADR% > 4% (Potential)
        adr_pass = row['ADR_Percent'] > 4.0 if pd.notna(row['ADR_Percent']) else False

        # 4. Liquidity: Avg Vol (10) > 1,000,000
        liq_pass = row['Vol_SMA_10'] > 1_000_000 if pd.notna(row['Vol_SMA_10']) else False

        # 5. Price > $5
        price_pass = row['Close'] > 5.0

        # 6. Trend Structure (Blue Sky / Strong Trend)
        # Price > SMA10 > SMA20 > SMA50
        if pd.isna(row['SMA_10']) or pd.isna(row['SMA_20']) or pd.isna(row['SMA_50']):
            trend_pass = False
        else:
            trend_pass = (row['Close'] > row['SMA_10']) and \
                         (row['SMA_10'] > row['SMA_20']) and \
                         (row['SMA_20'] > row['SMA_50'])

        return {
            'RRS_Pass': rrs_pass,
            'Volume_Pass': vol_pass,  # Replaces RVol_Pass in filter logic
            'RVol_Pass': vol_pass,    # Legacy key compatibility if needed (value reflects new logic)
            'ADR_Pass': adr_pass,
            'Liquidity_Pass': liq_pass,
            'Price_Pass': price_pass,
            'Trend_Pass': trend_pass,
            'All_Pass': rrs_pass and vol_pass and adr_pass and liq_pass and price_pass and trend_pass
        }
