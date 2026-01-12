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
        # SPY ATR (Calculated on spy_df - ensure it's not NaN for the period)
        # Note: spy_df passed in is likely the analysis window (6mo).
        # ta.atr needs 14 days. If spy_df starts exactly at start of 6mo, first 14 days of ATR are NaN.
        # Ideally spy_df should have some buffer, but for now we proceed.
        # If spy_df is processed from get_market_analysis_data, it might already have indicators?
        # No, get_market_analysis_data returns a DF with indicators, but we are passed the raw(ish) spy_df?
        # Actually in data_fetcher.py, 'spy_df' returned by get_market_analysis_data DOES have indicators like 'TSV', 'Fast_K' etc.
        # But does it have 'ATR'? Let's check get_market_analysis_data. It doesn't calculate ATR explicitly.
        # So we calculate it here.
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

        return df

    @staticmethod
    def check_filters(row):
        """
        Checks if the latest row meets the RDT criteria.
        Returns a dictionary of results.
        Refined for Tore-Ken specifications:
        - RVol > 1.0
        - ADR% > 2.5%
        - Vol_SMA_10 > 500,000
        - Trend: Price > SMA50 > SMA200
        """
        # 1. RRS > 1.0 (Most Important - kept strict as per standard RDT? Or relaxed?)
        # Memory doesn't explicitly say RRS is relaxed, so we keep > 1.0 or imply standard.
        # Standard RDT often wants RRS > 1.0 or positive. Let's keep 1.0.
        rrs_pass = row['RRS'] > 1.0

        # 2. RVol > 1.0 (Relaxed from 1.5)
        rvol_pass = row['RVol'] > 1.0 if pd.notna(row['RVol']) else False

        # 3. ADR% > 2.5% (Relaxed from 4.0)
        adr_pass = row['ADR_Percent'] > 2.5 if pd.notna(row['ADR_Percent']) else False

        # 4. Liquidity: Avg Vol (10) > 500,000 (Relaxed from 1,000,000)
        liq_pass = row['Vol_SMA_10'] > 500_000 if pd.notna(row['Vol_SMA_10']) else False

        # 5. Price > $5 (Standard)
        price_pass = row['Close'] > 5.0

        # 6. Trend Structure (Relaxed)
        # Price > SMA50 > SMA200 (removed SMA100 check)
        if pd.isna(row['SMA_50']) or pd.isna(row['SMA_200']):
            trend_pass = False
        else:
            trend_pass = (row['Close'] > row['SMA_50']) and \
                         (row['SMA_50'] > row['SMA_200'])

        return {
            'RRS_Pass': rrs_pass,
            'RVol_Pass': rvol_pass,
            'ADR_Pass': adr_pass,
            'Liquidity_Pass': liq_pass,
            'Price_Pass': price_pass,
            'Trend_Pass': trend_pass,
            'All_Pass': rrs_pass and rvol_pass and adr_pass and liq_pass and price_pass and trend_pass
        }
