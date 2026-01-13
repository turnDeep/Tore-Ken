import os
import pandas as pd
from typing import List, Dict, Optional
import time
from curl_cffi.requests import Session
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class FMPTickerFetcher:
    """FMP Stock List API Wrapper"""

    # Changed from stock-screener to stock/list (Free Tier Compatible)
    BASE_URL = "https://financialmodelingprep.com/api/v3/stock/list"

    def __init__(self, api_key: str = None, rate_limit: int = None):
        """
        Initialize FMP Ticker Fetcher

        Args:
            api_key: FMP API Key (default: from env FMP_API_KEY)
            rate_limit: API rate limit per minute (default: from env FMP_RATE_LIMIT or 750)
        """
        self.api_key = api_key or os.getenv('FMP_API_KEY')
        if not self.api_key:
            raise ValueError(
                "FMP API Key is required. Set FMP_API_KEY environment variable "
                "or pass api_key parameter."
            )

        # Rate limit settings
        self.rate_limit = rate_limit or int(os.getenv('FMP_RATE_LIMIT', '300')) # Default to lower limit for safety
        self.session = Session(impersonate="chrome110")
        self.request_timestamps = []

    def _enforce_rate_limit(self):
        """Enforce the configured API rate limit per minute."""
        current_time = time.time()
        # Remove timestamps older than 60 seconds
        self.request_timestamps = [t for t in self.request_timestamps if current_time - t < 60]

        if len(self.request_timestamps) >= self.rate_limit:
            # Sleep until the oldest request is older than 60 seconds
            sleep_time = 60 - (current_time - self.request_timestamps[0]) + 0.1
            logger.info(f"Rate limit reached. Sleeping for {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)
            # Trim the list again after sleeping
            current_time = time.time()
            self.request_timestamps = [t for t in self.request_timestamps if current_time - t < 60]

        self.request_timestamps.append(current_time)

    def _make_request(self, params: Dict) -> List[Dict]:
        """
        Make API request with error handling and rate limiting.
        """
        self._enforce_rate_limit()

        params['apikey'] = self.api_key

        try:
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")
            else:
                raise ValueError(f"Unexpected response format: {data}")

        except Exception as e:
            logger.error(f"API request failed: {e}")
            return []

    def get_all_stocks(self) -> pd.DataFrame:
        """
        Get all stocks from FMP and filter locally for NASDAQ/NYSE and Stock type.
        This uses the free /stock/list endpoint.
        """
        logger.info("Fetching full stock list from FMP...")

        # /stock/list generally returns the full list without pagination params
        stocks = self._make_request({})

        if not stocks:
            return pd.DataFrame()

        logger.info(f"Retrieved {len(stocks)} total items. Filtering...")

        filtered_stocks = []

        # Valid exchanges (FMP uses 'NASDAQ', 'NYSE', 'AMEX' etc.)
        valid_exchanges = ['NASDAQ', 'NYSE']

        for stock in stocks:
            # 1. Filter by Type: Must be 'stock' (not 'etf', 'fund', 'trust')
            stock_type = str(stock.get('type', '')).lower()
            if stock_type != 'stock':
                continue

            # 2. Filter by Exchange
            exchange = str(stock.get('exchangeShortName', '')).upper()
            if exchange not in valid_exchanges:
                # Fallback: check 'exchange' field if exchangeShortName is missing or different
                exchange_long = str(stock.get('exchange', '')).upper()
                if 'NASDAQ' in exchange_long:
                    exchange = 'NASDAQ'
                elif 'NYSE' in exchange_long:
                    exchange = 'NYSE'
                else:
                    continue

            # 3. Exclude specific patterns (like test tickers, etc.) if needed
            symbol = stock.get('symbol', '')
            if not symbol or '^' in symbol or '.' in symbol: # Optionally exclude dot tickers if preferred
                 # keeping dots is sometimes necessary (BRK.B), but yfinance needs conversion.
                 # The user code handles dot conversion later in load_tickers
                 pass

            filtered_stocks.append({
                'Ticker': symbol,
                'Exchange': exchange,
                'Name': stock.get('name', ''),
                'Price': stock.get('price', 0),
                'ExchangeShort': stock.get('exchangeShortName', '')
            })

        df = pd.DataFrame(filtered_stocks)

        # Remove duplicates
        if not df.empty:
            df.drop_duplicates(subset=['Ticker'], keep='first', inplace=True)

        return df

def update_stock_csv_from_fmp(filepath: str = 'stock.csv') -> bool:
    """
    Fetches tickers from FMP (Free Plan Compatible) and updates the stock.csv file.
    """
    try:
        logger.info("Starting FMP ticker update (Stock List Mode)...")
        fetcher = FMPTickerFetcher()

        df = fetcher.get_all_stocks()

        if df.empty:
            logger.warning("FMP returned no stocks after filtering. Update aborted.")
            return False

        logger.info(f"Total stocks after filtering: {len(df)}")

        # Save to CSV (Ticker and Exchange only)
        output_df = df[['Ticker', 'Exchange']].copy()
        output_df.to_csv(filepath, index=False)
        logger.info(f"Successfully updated {filepath}")
        return True

    except Exception as e:
        logger.error(f"Failed to update stock CSV from FMP: {e}")
        return False

if __name__ == '__main__':
    # When run as script, update backend/stock.csv
    target_path = 'stock.csv'
    if os.path.exists('backend/stock.csv'):
        target_path = 'backend/stock.csv'

    update_stock_csv_from_fmp(target_path)
