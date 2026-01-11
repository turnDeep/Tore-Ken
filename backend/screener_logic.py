import pandas as pd
import pandas_ta as ta

class RDTIndicators:
    @staticmethod
    def calculate_all(df, spy_df):
        """
        Calculates all RDT system indicators for a given stock DataFrame.
        """
        # Ensure df and spy_df are sorted by date
        df = df.sort_index()
        spy_df = spy_df.sort_index()

        # Normalize Timezones (remove timezone info for alignment)
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        if spy_df.index.tz is not None:
            spy_df.index = spy_df.index.tz_localize(None)

        # Align dates
        common_index = df.index.intersection(spy_df.index)
        df = df.loc[common_index].copy()
        spy_df = spy_df.loc[common_index].copy()

        if df.empty:
            return df

        # --- Basic Price & MA ---
        df['SMA_10'] = ta.sma(df['Close'], length=10)
        df['SMA_50'] = ta.sma(df['Close'], length=50)
        df['SMA_100'] = ta.sma(df['Close'], length=100)
        df['SMA_200'] = ta.sma(df['Close'], length=200)

        # --- ATR ---
        # ATR 14
        df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)

        # --- ADR% ---
        # 20-day Average Daily Range %
        # Method 1: ((High / Low) - 1) * 100
        daily_range_pct = ((df['High'] / df['Low']) - 1) * 100
        df['ADR_Percent'] = daily_range_pct.rolling(window=20).mean()

        # --- RVol (Fuel) ---
        # Relative Volume = Volume / 20-day SMA Volume
        # Note: If running intraday, this needs scaling. Assuming EOD for now.
        df['Vol_SMA_20'] = ta.sma(df['Volume'], length=20)
        df['RVol'] = df['Volume'] / df['Vol_SMA_20']

        # Liquidity Check (10-day Avg Volume)
        df['Vol_SMA_10'] = ta.sma(df['Volume'], length=10)

        # --- RRS (Real Relative Strength) ---
        # SPY ATR
        spy_atr = ta.atr(spy_df['High'], spy_df['Low'], spy_df['Close'], length=14)

        # Calculate Deltas (Price Change)
        # Using 1-day change
        delta_stock = df['Close'].diff()
        delta_spy = spy_df['Close'].diff()

        # Expected Move = Delta_SPY * (Stock_ATR / SPY_ATR)
        # Note: Align spy_atr to df index (already done via common_index)
        expected_move = delta_spy * (df['ATR'] / spy_atr)

        # RRS = (Delta_Stock - Expected_Move) / Stock_ATR
        # This normalizes the excess return by the stock's own volatility
        # We calculate the daily value first
        df['RRS_Daily'] = (delta_stock - expected_move) / df['ATR']

        # RRS (Smoothed) for Trend Stability
        # Use a rolling sum (e.g., 12 days) to match 1OSI-style trend indicators
        # This prevents daily flickering in the screener
        df['RRS'] = df['RRS_Daily'].rolling(window=12).sum().fillna(0)

        return df

    @staticmethod
    def check_filters(row):
        """
        Checks if the latest row meets the RDT criteria.
        Returns a dictionary of results.
        """
        # 1. RRS > 1.0 (Most Important)
        rrs_pass = row['RRS'] > 1.0

        # 2. RVol > 1.5 (Fuel)
        # Handle division by zero or NaN
        rvol_pass = row['RVol'] > 1.5 if pd.notna(row['RVol']) else False

        # 3. ADR% > 4% (Potential)
        adr_pass = row['ADR_Percent'] > 4.0 if pd.notna(row['ADR_Percent']) else False

        # 4. Liquidity: Avg Vol (10) > 1,000,000
        liq_pass = row['Vol_SMA_10'] > 1_000_000 if pd.notna(row['Vol_SMA_10']) else False

        # 5. Price > $5
        price_pass = row['Close'] > 5.0

        # 6. Trend Structure (Blue Sky / Strong Trend)
        # Price > SMA50 > SMA100 > SMA200
        # Check if SMAs are not NaN
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
