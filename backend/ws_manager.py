import asyncio
import json
import os
import logging
from typing import Dict, List
import yfinance as yf
from backend.rvol_logic import MarketSchedule, generate_volume_profile, RealTimeRvolAnalyzer

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.getcwd(), 'data')

class WebSocketManager:
    _instance = None

    def __init__(self):
        self.analyzers: Dict[str, RealTimeRvolAnalyzer] = {}
        self.running = False
        self.task = None
        self.monitor_task = None
        self.poll_task = None
        self.tickers: List[str] = []

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
        self.poll_task = asyncio.create_task(self._poll_volumes_loop())
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

        if self.poll_task:
            self.poll_task.cancel()
            try:
                await self.poll_task
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

    def _fetch_volumes_sync(self, tickers: List[str]) -> Dict[str, int]:
        """Fetches volume for a list of tickers synchronously (for use in executor)."""
        results = {}
        for t in tickers:
            try:
                 # fast_info access triggers API call
                 vol = yf.Ticker(t).fast_info.last_volume
                 if vol is not None:
                     results[t] = vol
            except Exception as e:
                # Log only debug or warning to avoid spam
                # logger.debug(f"Error polling volume for {t}: {e}")
                pass
        return results

    async def _poll_volumes_loop(self):
        """Periodically polls for volume data as a fallback/primary source."""
        logger.info("Starting volume polling loop...")
        loop = asyncio.get_running_loop()

        while self.running:
            try:
                # Check market open or debug flag
                force_run = os.getenv("DEBUG_WS", "false").lower() == "true"
                is_open = MarketSchedule.is_market_open()

                if is_open or force_run:
                    tickers = list(self.analyzers.keys())
                    if tickers:
                        # Run blocking fetch in thread executor
                        volumes = await loop.run_in_executor(None, self._fetch_volumes_sync, tickers)

                        for t, vol in volumes.items():
                            if t in self.analyzers:
                                self.analyzers[t].update_volume(vol)

            except Exception as e:
                logger.error(f"Error in volume polling loop: {e}")

            await asyncio.sleep(15)  # Poll every 15 seconds

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
                logger.info("Market Closed. Waiting and refreshing data...")
                # When market is closed, we should refresh data periodically
                await asyncio.sleep(300) # Wait 5 mins

                self.load_tickers()
                await self.initialize_analyzers()

    def get_all_rvols(self) -> Dict[str, float]:
        """Returns current RVol for all monitored tickers."""
        return {
            ticker: analyzer.current_rvol
            for ticker, analyzer in self.analyzers.items()
        }

# Global instance accessor
ws_manager = WebSocketManager.get_instance()
