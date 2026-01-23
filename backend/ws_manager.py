import asyncio
import json
import os
import logging
from typing import Dict, List
import yfinance as yf
from datetime import datetime
import pytz
from backend.rvol_logic import MarketSchedule, generate_volume_profile, RealTimeRvolAnalyzer
from backend.data_fetcher import fetch_and_notify

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.getcwd(), 'data')

def fetch_ticker_volume(ticker):
    """
    Helper to fetch volume for a single ticker (to be run in executor).
    """
    try:
        t = yf.Ticker(ticker)
        return t.fast_info.last_volume
    except Exception as e:
        logger.error(f"Error fetching volume for {ticker}: {e}")
        return None

class WebSocketManager:
    _instance = None

    def __init__(self):
        self.analyzers: Dict[str, RealTimeRvolAnalyzer] = {}
        self.running = False
        self.task = None
        self.monitor_task = None
        self.scheduler_task = None
        self.polling_task = None
        self.tickers: List[str] = []
        self.last_fetch_date = None  # Tracks the date of the last successful data fetch

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def load_tickers(self):
        """Loads tickers from data/latest.json"""
        try:
            latest_path = os.path.join(DATA_DIR, 'latest.json')
            if not os.path.exists(latest_path):
                logger.warning("latest.json not found. No tickers to monitor.")
                return

            with open(latest_path, 'r') as f:
                data = json.load(f)

            strong_stocks = data.get('strong_stocks', [])
            self.tickers = [s['ticker'] for s in strong_stocks]

            # Filter valid tickers
            self.tickers = [t for t in self.tickers if isinstance(t, str)]
            logger.info(f"Loaded {len(self.tickers)} tickers for monitoring: {self.tickers}")

        except Exception as e:
            logger.error(f"Error loading tickers: {e}")

    async def initialize_analyzers(self):
        """Initializes RVol analyzers (fetches history). This can be slow."""
        logger.info("Initializing RVol analyzers...")
        loop = asyncio.get_running_loop()

        for ticker in self.tickers:
            try:
                # Run profile generation in a thread to avoid blocking the event loop
                profile = await loop.run_in_executor(None, generate_volume_profile, ticker)
                if not profile.empty:
                    self.analyzers[ticker] = RealTimeRvolAnalyzer(ticker, profile)
                else:
                    logger.warning(f"Could not generate profile for {ticker}")
            except Exception as e:
                logger.error(f"Error initializing analyzer for {ticker}: {e}")

        logger.info(f"Initialized {len(self.analyzers)} analyzers.")

    async def retry_missing_analyzers(self):
        """Retries initialization for tickers that failed."""
        missing_tickers = [t for t in self.tickers if t not in self.analyzers]
        if not missing_tickers:
            return

        logger.info(f"Retrying initialization for missing tickers: {missing_tickers}")
        loop = asyncio.get_running_loop()

        for ticker in missing_tickers:
            try:
                profile = await loop.run_in_executor(None, generate_volume_profile, ticker)
                if not profile.empty:
                    self.analyzers[ticker] = RealTimeRvolAnalyzer(ticker, profile)
                    logger.info(f"Successfully initialized analyzer for {ticker}")
                else:
                    logger.warning(f"Still could not generate profile for {ticker}")
            except Exception as e:
                logger.error(f"Error initializing analyzer for {ticker}: {e}")

    async def start(self):
        """Starts the background task."""
        if self.running:
            return

        # Don't block startup with initialization. Let _run handle it.
        self.running = True
        self.task = asyncio.create_task(self._run())
        self.monitor_task = asyncio.create_task(self._monitor_analyzers())
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        self.polling_task = asyncio.create_task(self._poll_volumes_loop())
        logger.info("WebSocketManager started.")

    async def stop(self):
        """Stops the background task."""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass

        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass

        if self.polling_task:
            self.polling_task.cancel()
            try:
                await self.polling_task
            except asyncio.CancelledError:
                pass

        logger.info("WebSocketManager stopped.")

    def handle_message(self, msg):
        """Callback for WebSocket messages."""
        try:
            # msg is a dict (decoded from protobuf)
            ticker_id = msg.get('id')
            if ticker_id and ticker_id in self.analyzers:
                self.analyzers[ticker_id].process_message(msg)
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def _monitor_analyzers(self):
        """Periodically checks for missing analyzers and retries them."""
        while self.running:
            await asyncio.sleep(60)
            if MarketSchedule.is_market_open():
                 await self.retry_missing_analyzers()

    async def _poll_volumes_loop(self):
        """Periodically polls volume for all tickers to ensure RVol is up to date."""
        logger.info("Volume polling loop started.")
        while self.running:
            try:
                # Poll every 15 seconds
                await asyncio.sleep(15)

                if not self.analyzers:
                    continue

                if MarketSchedule.is_market_open() or os.getenv("DEBUG_WS", "false").lower() == "true":
                    loop = asyncio.get_running_loop()
                    # Iterate over a copy of keys to avoid runtime error if dictionary changes
                    current_tickers = list(self.analyzers.keys())

                    for ticker in current_tickers:
                         if not self.running: break
                         if ticker in self.analyzers:
                             vol = await loop.run_in_executor(None, fetch_ticker_volume, ticker)
                             if vol:
                                 self.analyzers[ticker].update_volume_from_polling(vol)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(15)

    async def _scheduler_loop(self):
        """Independent loop for post-market scheduled tasks."""
        logger.info("Scheduler loop started.")
        while self.running:
            try:
                # Run every minute
                await asyncio.sleep(60)

                if MarketSchedule.is_market_open():
                    continue

                # --- Automatic Data Fetching Logic (16:15 ET) ---
                now_et = datetime.now(pytz.timezone('US/Eastern'))
                # Check if today is Monday-Friday (0-4)
                if now_et.weekday() <= 4:
                    # Target time: 16:15 ET
                    target_time = now_et.replace(hour=16, minute=15, second=0, microsecond=0)

                    # Trigger if:
                    # 1. Current time >= 16:15
                    # 2. We haven't successfully fetched for this date yet
                    if now_et >= target_time and self.last_fetch_date != now_et.date():
                        logger.info(f"Detected post-market time ({now_et}). Triggering automatic data fetch...")

                        # Run fetch_and_notify in a separate thread because it's blocking/heavy
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, fetch_and_notify)

                        # Update last fetch date to prevent re-execution today
                        self.last_fetch_date = now_et.date()
                        logger.info(f"Automatic data fetch completed for {self.last_fetch_date}.")

                        # Reload tickers after fetch to ensure we have the latest strong stocks
                        self.load_tickers()
                        # Clear old analyzers so we re-initialize with new stocks next open
                        self.analyzers = {}

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(60) # Wait before retry

    async def _run(self):
        """Main loop."""
        # Initial load attempt
        self.load_tickers()
        await self.initialize_analyzers()

        while self.running:
            # Check market hours
            force_run = os.getenv("DEBUG_WS", "false").lower() == "true"
            is_open = MarketSchedule.is_market_open()

            if is_open or force_run:
                logger.info(f"Market Open: {is_open} (Force: {force_run}). Connecting to WebSocket...")
                try:
                    # Re-load tickers if needed (e.g., if we were waiting)
                    if not self.analyzers:
                         self.load_tickers()
                         await self.initialize_analyzers()
                    else:
                         # Retry any missing ones (e.g. TTMI failed on first try)
                         await self.retry_missing_analyzers()

                    # yfinance WebSocket wrapper
                    async with yf.AsyncWebSocket() as ws:
                        if not self.tickers:
                             logger.warning("No tickers to subscribe.")
                             await asyncio.sleep(60)
                             self.load_tickers()
                             await self.initialize_analyzers()
                             continue

                        await ws.subscribe(self.tickers)
                        logger.info(f"Subscribed to {self.tickers}")

                        await ws.listen(message_handler=self.handle_message)

                except Exception as e:
                    logger.error(f"WebSocket error: {e}")
                    await asyncio.sleep(10) # Backoff
            else:
                logger.info("Market Closed. Waiting...")

                if not self.analyzers:
                    # Try to reload if we are waiting, in preparation for next open
                    self.load_tickers()
                    await self.initialize_analyzers()

                # Wait 5 minutes before next check
                await asyncio.sleep(300)

    def get_all_rvols(self) -> Dict[str, float]:
        """Returns current RVol for all monitored tickers."""
        return {
            ticker: analyzer.current_rvol
            for ticker, analyzer in self.analyzers.items()
        }

# Global instance accessor
ws_manager = WebSocketManager.get_instance()
