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

    async def start(self):
        """Starts the background task."""
        if self.running:
            return

        self.load_tickers()
        await self.initialize_analyzers()

        self.running = True
        self.task = asyncio.create_task(self._run())
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

    async def _run(self):
        """Main loop."""
        while self.running:
            # Check market hours
            # For testing/demo, we might want to bypass this check or have a "force" mode.
            # But per requirements: "When venue starts..."
            # However, during development, I need to verify it works even if market is closed (maybe using replay or just connecting).
            # yfinance WebSocket usually allows connection 24/7 but only sends data when there is data (e.g. after hours or crypto).
            # The tickers are stocks.

            # Force run if env var DEBUG_WS is set, otherwise check schedule
            force_run = os.getenv("DEBUG_WS", "false").lower() == "true"
            is_open = MarketSchedule.is_market_open()

            if is_open or force_run:
                logger.info(f"Market Open: {is_open} (Force: {force_run}). Connecting to WebSocket...")
                try:
                    # yfinance WebSocket wrapper
                    # Note: AsyncWebSocket is available in newer versions
                    async with yf.AsyncWebSocket() as ws:
                        if not self.tickers:
                             logger.warning("No tickers to subscribe.")
                             await asyncio.sleep(60)
                             continue

                        await ws.subscribe(self.tickers)
                        logger.info(f"Subscribed to {self.tickers}")

                        await ws.listen(message_handler=self.handle_message)

                except Exception as e:
                    logger.error(f"WebSocket error: {e}")
                    await asyncio.sleep(10) # Backoff
            else:
                logger.info("Market Closed. Waiting...")
                await asyncio.sleep(300) # Check every 5 mins

    def get_all_rvols(self) -> Dict[str, float]:
        """Returns current RVol for all monitored tickers."""
        return {
            ticker: analyzer.current_rvol
            for ticker, analyzer in self.analyzers.items()
        }

# Global instance accessor
ws_manager = WebSocketManager.get_instance()
