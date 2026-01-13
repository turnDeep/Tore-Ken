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
    """FMP Stock Screener API Wrapper"""

    BASE_URL = "https://financialmodelingprep.com/api/v3/stock-screener"

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

        # Rate limit settings (default 750 req/min for Premium Plan)
        self.rate_limit = rate_limit or int(os.getenv('FMP_RATE_LIMIT', '750'))
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

    def get_stocks_by_exchange(self, exchange: str) -> List[Dict]:
        """Get pure individual stocks from a specific exchange"""
        params = {
            'isEtf': 'false',              # Exclude ETFs
            'isFund': 'false',             # Exclude Mutual Funds
            'isActivelyTrading': 'true',   # Exclude inactive stocks
            'exchange': exchange.lower(),
            'limit': 10000                 # Max retrieval
        }

        logger.info(f"Fetching stocks for {exchange.upper()}...")
        stocks = self._make_request(params)
        logger.info(f"  Retrieved {len(stocks)} stocks for {exchange.upper()}")

        return stocks

    def get_all_stocks(self, exchanges: List[str] = None) -> pd.DataFrame:
        """Get all individual stocks from specified exchanges"""
        if exchanges is None:
            exchanges = ['nasdaq', 'nyse', 'amex']

        all_stocks = []

        for exchange in exchanges:
            stocks = self.get_stocks_by_exchange(exchange)

            for stock in stocks:
                all_stocks.append({
                    'Ticker': stock.get('symbol'),
                    'Exchange': exchange.upper(),
                    'CompanyName': stock.get('companyName', ''),
                    'MarketCap': stock.get('marketCap', 0),
                    'Sector': stock.get('sector', ''),
                    'Industry': stock.get('industry', ''),
                    'Country': stock.get('country', '')
                })

        df = pd.DataFrame(all_stocks)

        # Remove duplicates (if ticker listed on multiple exchanges)
        if not df.empty:
            df.drop_duplicates(subset=['Ticker'], keep='first', inplace=True)

        return df

def update_stock_csv_from_fmp(filepath: str = 'stock.csv') -> bool:
    """
    Fetches tickers from FMP and updates the stock.csv file.
    Returns True if successful, False otherwise.
    """
    try:
        logger.info("Starting FMP ticker update...")
        fetcher = FMPTickerFetcher()
        exchanges = ['nasdaq', 'nyse']

        df = fetcher.get_all_stocks(exchanges)

        if df.empty:
            logger.warning("FMP returned no stocks. Update aborted.")
            return False

        logger.info(f"Total stocks retrieved: {len(df)}")

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
    # Assuming script is run from repo root or backend/
    target_path = 'stock.csv'
    if os.path.exists('backend/stock.csv'):
        target_path = 'backend/stock.csv'

    update_stock_csv_from_fmp(target_path)
