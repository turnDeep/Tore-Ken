import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time, timezone
import pytz
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketSchedule:
    """
    Handles US Market hours and conversion to JST.
    """
    JST = pytz.timezone('Asia/Tokyo')
    ET = pytz.timezone('US/Eastern')

    @staticmethod
    def is_market_open():
        """
        Checks if the US market is currently open (regular trading hours 9:30 - 16:00 ET).
        """
        now_et = datetime.now(MarketSchedule.ET)

        # Check if it's a weekday (0=Monday, 4=Friday)
        if now_et.weekday() > 4:
            return False

        # Market Hours in ET
        market_open = time(9, 30)
        market_close = time(16, 0)

        current_time = now_et.time()

        return market_open <= current_time <= market_close

    @staticmethod
    def get_market_start_jst():
        """
        Returns the market start time in JST for the current/next session.
        Useful for logging or display.
        """
        now_et = datetime.now(MarketSchedule.ET)
        today_open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        return today_open_et.astimezone(MarketSchedule.JST)

def generate_volume_profile(ticker: str, lookback_days: int = 20) -> pd.DataFrame:
    """
    Generates the volume profile (baseline) for a ticker based on historical 5-minute bars.
    Returns a DataFrame with 'AvgVolume' and 'CumVolume' indexed by time (9:30, 9:35, ...).
    """
    logger.info(f"[{ticker}] Fetching past {lookback_days} days of 5m data...")

    # yfinance allows up to 60 days of 5m data.
    start_date = datetime.now() - timedelta(days=lookback_days * 2 + 10)

    try:
        df = yf.download(ticker, start=start_date, interval="5m", progress=False)

        if df.empty:
            logger.warning(f"[{ticker}] No data found.")
            return pd.DataFrame()

        # Handle MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Ensure index is datetime
        if not isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            time_col = 'Datetime' if 'Datetime' in df.columns else 'Date'
            if time_col in df.columns:
                df = df.set_index(time_col)

        # Convert to Eastern Time
        if df.index.tz is None:
             df.index = df.index.tz_localize('UTC').tz_convert('US/Eastern')
        else:
             df.index = df.index.tz_convert('US/Eastern')

        # Filter out "today"
        today_et = datetime.now(pytz.timezone('US/Eastern')).date()
        df_history = df[df.index.date < today_et].copy()

        # Fetch Daily Data for Scaling (Same period)
        # We need the exact dates present in 5m history to ensure Apple-to-Apple comparison.
        try:
            # Re-use start_date, but fetch daily
            df_daily = yf.download(ticker, start=start_date, interval="1d", progress=False)
            if isinstance(df_daily.columns, pd.MultiIndex):
                df_daily.columns = df_daily.columns.get_level_values(0)

            # Filter for the same historical window (strictly < today)
            df_daily = df_daily[df_daily.index.date < today_et]

            # Align start date to the actual 5m data start to match the window
            if not df_history.empty:
                min_date = df_history.index.date.min()
                df_daily = df_daily[df_daily.index.date >= min_date]

        except Exception as e:
            logger.warning(f"[{ticker}] Error fetching daily data for scaling: {e}")
            df_daily = pd.DataFrame()

        # Group by Time and calculate MEAN (instead of Median)
        df_history['Time'] = df_history.index.time
        profile = df_history.groupby('Time')['Volume'].mean()
        profile_df = profile.to_frame(name='AvgVolume')

        # Reindex to ensure full market day coverage (9:30 to 15:55 for 5m bars)
        market_times = []
        curr = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
        end = curr.replace(hour=16, minute=0)
        while curr < end:
            market_times.append(curr.time())
            curr += timedelta(minutes=5)

        profile_df = profile_df.reindex(market_times, fill_value=0)

        # Calculate Cumulative Volume
        profile_df['CumVolume'] = profile_df['AvgVolume'].cumsum()

        # Apply Scaling if Daily Data is available
        if not df_daily.empty:
            daily_mean = df_daily['Volume'].mean()
            profile_sum = profile_df['AvgVolume'].sum()

            if profile_sum > 0:
                scale_factor = daily_mean / profile_sum
                logger.info(f"[{ticker}] Applying scale factor: {scale_factor:.4f} (Daily: {daily_mean:.0f} / Profile: {profile_sum:.0f})")
                profile_df['AvgVolume'] = profile_df['AvgVolume'] * scale_factor
                profile_df['CumVolume'] = profile_df['AvgVolume'].cumsum()

        logger.info(f"[{ticker}] Baseline generated with {len(profile_df)} slots.")
        return profile_df

    except Exception as e:
        logger.error(f"[{ticker}] Error generating profile: {e}")
        return pd.DataFrame()


class RealTimeRvolAnalyzer:
    def __init__(self, ticker: str, profile: pd.DataFrame, prev_high: float = None):
        self.ticker = ticker
        self.profile = profile
        self.prev_high = prev_high
        self.current_rvol = 0.0

        # State
        self.current_day_volume = 0
        self.current_price = 0.0

        # Breakout State
        self.orb_5m_high = None
        self.breakout_status = None  # "DAILY_BREAKOUT", "ORB_5M", or None

    def process_message(self, msg: dict):
        """
        Process a WebSocket message (dict) from yfinance.
        """
        try:
            # Timestamp (ms)
            ts_ms = msg.get('time')
            if not ts_ms:
                return
            ts_ms = int(ts_ms)
            current_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=pytz.timezone('US/Eastern'))

            # Extract Price
            price_str = msg.get('price') or msg.get('regularMarketPrice') or msg.get('last_price')
            if price_str is not None:
                self.current_price = float(price_str)
                self._check_breakouts(current_dt)

            # Extract Volume
            day_volume_str = msg.get('day_volume') or msg.get('dayVolume')

            day_volume = int(day_volume_str) if day_volume_str is not None else None

            # Logic to maintain reliable current_day_volume
            if day_volume is not None:
                self.current_day_volume = day_volume

            # Update RVOL
            self._update_rvol(current_dt)

        except Exception as e:
            logger.error(f"[{self.ticker}] Error processing message: {e}")

    def _check_breakouts(self, current_dt: datetime):
        """
        Check for Daily Breakout and 5m ORB.
        """
        # 1. ORB 5M Logic
        # Time window: 9:30 - 9:35 ET
        # During this window, track the High.
        # After 9:35, if Price > High, trigger ORB.
        market_open = current_dt.replace(hour=9, minute=30, second=0, microsecond=0)
        orb_end = current_dt.replace(hour=9, minute=35, second=0, microsecond=0)

        if current_dt < market_open:
            return

        if current_dt <= orb_end:
            # We are in the ORB formation period
            if self.orb_5m_high is None or self.current_price > self.orb_5m_high:
                self.orb_5m_high = self.current_price
        else:
            # We are after the formation period
            # Check for Breakout if we have a valid ORB high
            if self.orb_5m_high and self.current_price > self.orb_5m_high:
                # If we were already in DAILY_BREAKOUT, keep it (higher priority typically?)
                # Actually, user wants to know if "entered on opening range highs".
                # We can flag ORB if it happens.
                if self.breakout_status != "DAILY_BREAKOUT":
                     self.breakout_status = "ORB_5M"

        # 2. Daily Breakout Logic
        if self.prev_high and self.current_price > self.prev_high:
            self.breakout_status = "DAILY_BREAKOUT"

        # If price falls back?
        # Typically breakout status is sticky for the day if we want to show "it happened".
        # But if currently trading below, maybe we should clear it?
        # User says "Enter when...", so signal is "NOW".
        # If price drops back, it's a failed breakout or pullback.
        # I'll make it state-based on *current* price.
        if self.prev_high and self.current_price <= self.prev_high:
             if self.breakout_status == "DAILY_BREAKOUT":
                 self.breakout_status = None # Reset if falls back
                 # Check if ORB is still valid?
                 if self.orb_5m_high and self.current_price > self.orb_5m_high:
                     self.breakout_status = "ORB_5M"

        if self.orb_5m_high and self.current_price <= self.orb_5m_high:
             if self.breakout_status == "ORB_5M":
                 self.breakout_status = None

    def _update_rvol(self, current_dt: datetime):
        """Calculate Cumulative RVol"""
        if self.profile.empty or self.current_day_volume == 0:
            return

        # Current time details
        # We need to find the "Bucket" we are currently in.
        # E.g. 9:32 is in the 9:30 bucket.
        # Determine the start of the current 5m bar
        minute_floor = (current_dt.minute // 5) * 5
        bar_start_time = time(current_dt.hour, minute_floor)

        # 1. Get Baseline Volume accumulated up to the START of this bar
        #    This is CumVolume of the *previous* bucket (bar_start_time - 5min).
        #    Alternatively, Sum(AvgVolume) where Time < bar_start_time.

        # Ensure we can look up in profile
        if bar_start_time not in self.profile.index:
            # logger.warning(f"Bar start {bar_start_time} not in profile index")
            return

        # Get CumVolume of the *previous* bucket
        # We can find the location of bar_start_time
        try:
            loc = self.profile.index.get_loc(bar_start_time)

            if loc > 0:
                # Cumulative volume of all completed bars before this one
                # Note: profile.iloc[loc-1]['CumVolume'] gives sum up to end of previous bar.
                base_accumulated_vol = self.profile.iloc[loc-1]['CumVolume']
            else:
                # First bar of the day (9:30)
                base_accumulated_vol = 0

            # 2. Add interpolated volume for the current bar
            current_bar_avg = self.profile.loc[bar_start_time, 'AvgVolume']

            # Elapsed seconds in current bar (0 to 300)
            elapsed_seconds = (current_dt.minute % 5) * 60 + current_dt.second
            # Clamp to 300 (end of bar)
            elapsed_seconds = min(elapsed_seconds, 300)

            # Interpolate: assume volume comes in linearly over the 5 mins
            # (Or we could use a more complex curve, but linear is standard for TSV interpolation)
            current_bar_expected = current_bar_avg * (elapsed_seconds / 300.0)

            expected_total_vol = base_accumulated_vol + current_bar_expected

            # 3. Calculate Ratio
            # To match Screener RVol (Volume / SMA20), we need to include the current day's volume in the average.
            # Screener Denominator = (Sum_Past_19_Days + Current_Day) / 20.
            # expected_total_vol represents the Average of Past N Days (where N=20).
            # We approximate Sum_Past_19_Days as 19 * expected_total_vol.

            if expected_total_vol > 0:
                adjusted_baseline = (19 * expected_total_vol + self.current_day_volume) / 20.0
                self.current_rvol = self.current_day_volume / adjusted_baseline
            else:
                self.current_rvol = 0.0

        except Exception as e:
            logger.error(f"Error calc rvol for {self.ticker}: {e}")

    def get_status(self):
        """Returns the current status dictionary."""
        return {
            "rvol": self.current_rvol,
            "current_price": self.current_price,
            "breakout": self.breakout_status,
            "prev_high": self.prev_high,
            "orb_high": self.orb_5m_high
        }
