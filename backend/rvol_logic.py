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

        # Filter for regular market hours (9:30 - 16:00 ET) if needed,
        # but yfinance 5m usually returns market hours.
        # We need to extract the TIME component in ET.

        # Convert to Eastern Time just to be safe if it's not
        if df.index.tz is None:
            # Assume UTC if no TZ, but yfinance usually returns localized or UTC
             df.index = df.index.tz_localize('UTC').tz_convert('US/Eastern')
        else:
             df.index = df.index.tz_convert('US/Eastern')

        # Filter out "today" to ensure baseline is historical
        today_et = datetime.now(pytz.timezone('US/Eastern')).date()
        df_history = df[df.index.date < today_et].copy()

        # Group by Time and calculate Median (or Mean)
        # Using Median is more robust to outliers
        df_history['Time'] = df_history.index.time
        profile = df_history.groupby('Time')['Volume'].median()

        # Create DataFrame
        profile_df = profile.to_frame(name='AvgVolume')

        logger.info(f"[{ticker}] Baseline generated with {len(profile_df)} slots.")
        return profile_df

    except Exception as e:
        logger.error(f"[{ticker}] Error generating profile: {e}")
        return pd.DataFrame()


class RealTimeRvolAnalyzer:
    def __init__(self, ticker: str, profile: pd.DataFrame):
        self.ticker = ticker
        self.profile = profile
        self.current_rvol = 0.0

        # State for current 5-min bar
        self.current_bar = {
            'start_time': None,
            'volume': 0,
            'last_day_volume': None  # To calculate delta
        }

    def _get_bar_start_time(self, dt_obj: datetime):
        """Rounds datetime to the nearest 5-minute floor."""
        minute = (dt_obj.minute // 5) * 5
        return dt_obj.replace(minute=minute, second=0, microsecond=0)

    def process_message(self, msg: dict):
        """
        Process a WebSocket message (dict) from yfinance.
        Expected keys: 'id', 'price', 'time', 'dayVolume', 'lastSize'
        """
        try:
            # Timestamp (ms)
            ts_ms = msg.get('time')
            if not ts_ms:
                return

            ts_ms = int(ts_ms) # Ensure int

            current_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=pytz.timezone('US/Eastern'))

            # Volume Calculation
            tick_volume = 0
            # day_volume and last_size come as strings from yfinance WebSocket
            # Support both snake_case (new yfinance) and camelCase (old/some messages)
            day_volume_str = msg.get('day_volume') or msg.get('dayVolume')
            last_size_str = msg.get('last_size') or msg.get('lastSize')

            day_volume = int(day_volume_str) if day_volume_str is not None else None
            last_size = int(last_size_str) if last_size_str is not None else 0

            if day_volume is None and last_size == 0:
                # Log only once per minute to avoid spamming if a ticker is consistently missing data
                if current_dt.second == 0:
                    logger.debug(f"[{self.ticker}] Warning: No volume data in message: {msg.keys()}")

            if day_volume is not None:
                if self.current_bar['last_day_volume'] is not None:
                    diff = day_volume - self.current_bar['last_day_volume']
                    if diff >= 0:
                        tick_volume = diff
                else:
                    # First packet, can't calc diff yet, fallback to last_size
                    tick_volume = last_size

                self.current_bar['last_day_volume'] = day_volume
            else:
                tick_volume = last_size

            # 5-min Bar Logic
            bar_start = self._get_bar_start_time(current_dt)

            if self.current_bar['start_time'] is None:
                self.current_bar['start_time'] = bar_start
                self.current_bar['volume'] = tick_volume
            elif bar_start > self.current_bar['start_time']:
                # New bar started, finalize old one (optional) and reset
                self.current_bar['start_time'] = bar_start
                self.current_bar['volume'] = tick_volume
            else:
                # Same bar, accumulate
                self.current_bar['volume'] += tick_volume

            # Calculate RVOL
            self._update_rvol(current_dt)

        except Exception as e:
            logger.error(f"[{self.ticker}] Error processing message: {e}")

    def _update_rvol(self, current_dt: datetime):
        """Calculate and update self.current_rvol"""
        if self.profile.empty:
            return

        slot_time = self.current_bar['start_time'].time()

        if slot_time in self.profile.index:
            avg_vol = self.profile.loc[slot_time, 'AvgVolume']
            current_vol = self.current_bar['volume']

            if avg_vol > 0:
                # Simple RVOL based on accumulated volume vs full bucket average
                # Note: For strict "in-progress" comparison, we might want to project or use ratio.
                # The user prompt report suggests: "Realtime accumulated vs Completed Past Average" is simplest.
                # But it also mentions projection.
                # "Time-Segmented Relative Volume" usually means Volume_Now / Avg_Volume_At_Same_Time_In_Past.
                # If we are 1 minute into a 5 minute bar, our volume is naturally 1/5th of the full bar.
                # Comparing it to a full 5-min average will show low RVOL.
                # We should probably project it, or compare to "Avg Volume up to this minute".
                # But our profile is 5-min resolution.

                # Let's use Linear Projection as described in the report for better UX.
                elapsed_seconds = (current_dt - self.current_bar['start_time']).total_seconds()

                # Avoid noise in first few seconds
                if elapsed_seconds > 10:
                    projected_vol = current_vol * (300 / elapsed_seconds)
                    self.current_rvol = projected_vol / avg_vol
                else:
                     # Fallback to simple ratio (will be small)
                    self.current_rvol = current_vol / avg_vol
