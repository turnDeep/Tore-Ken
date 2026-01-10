import json
import logging
import logging.handlers
import os
import re
import sys
from datetime import datetime, timedelta, timezone
import pytz
import time
import math
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from curl_cffi.requests import Session
import httpx
from io import StringIO
from urllib.parse import urlparse
from .image_generator import generate_fear_greed_chart
from dotenv import load_dotenv
from .gemini_client import gemini_client

# Load environment variables from .env file
load_dotenv()

# --- Constants ---
# 絶対パスで定義（cronからの実行でも正しく動作するように）
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
RAW_DATA_PATH = os.path.join(DATA_DIR, 'data_raw.json')
FINAL_DATA_PATH_PREFIX = os.path.join(DATA_DIR, 'data_')

# URLs
CNN_FEAR_GREED_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/"
YAHOO_FINANCE_NEWS_URL = "https://finance.yahoo.com/topic/stock-market-news/"
YAHOO_EARNINGS_CALENDAR_URL = "https://finance.yahoo.com/calendar/earnings"
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ100_WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"

# Monex URLs
MONEX_ECONOMIC_CALENDAR_URL = "https://mst.monex.co.jp/pc/servlet/ITS/report/EconomyIndexCalendar"
MONEX_US_EARNINGS_URL = "https://mst.monex.co.jp/mst/servlet/ITS/fi/FIClosingCalendarUSGuest"
MONEX_JP_EARNINGS_URL = "https://mst.monex.co.jp/mst/servlet/ITS/fi/FIClosingCalendarJPGuest"

# Tickers
VIX_TICKER = "^VIX"
T_NOTE_TICKER = "^TNX"

# Country to Emoji Mapping
COUNTRY_EMOJI_MAP = {
    "jpn": "🇯🇵",
    "usa": "🇺🇸",
    "eur": "🇪🇺",
    "gbr": "🇬🇧",
    "deu": "🇩🇪",
    "fra": "🇫🇷",
    "aus": "🇦🇺",
    "nzl": "🇳🇿",
    "can": "🇨🇦",
    "che": "🇨🇭",
    "chn": "🇨🇳",
    "hkg": "🇭🇰",
    "ind": "🇮🇳",
    "bra": "🇧🇷",
    "zaf": "🇿🇦",
    "tur": "🇹🇷",
    "kor": "🇰🇷",
    "sgp": "🇸🇬",
}

# Important tickers from originalcalendar.py
US_TICKER_LIST = ["AAPL", "NVDA", "MSFT", "GOOG", "META", "AMZN", "NFLX", "BRK-B", "TSLA", "AVGO", 
                  "LLY", "WMT", "JPM", "V", "UNH", "XOM", "ORCL", "MA", "HD", "PG", "COST", "JNJ", 
                  "ABBV", "TMUS", "BAC", "CRM", "KO", "CVX", "VZ", "MRK", "AMD", "PEP", "CSCO", 
                  "LIN", "ACN", "WFC", "TMO", "ADBE", "MCD", "ABT", "BX", "PM", "NOW", "IBM", "AXP", 
                  "MS", "TXN", "GE", "QCOM", "CAT", "ISRG", "DHR", "INTU", "DIS", "CMCSA", "AMGN", 
                  "T", "GS", "PFE", "NEE", "CHTR", "RTX", "BKNG", "UBER", "AMAT", "SPGI", "LOW", 
                  "BLK", "PGR", "UNP", "SYK", "HON", "ETN", "SCHW", "LMT", "TJX", "COP", "ANET", 
                  "BSX", "KKR", "VRTX", "C", "PANW", "ADP", "NKE", "BA", "MDT", "FI", "UPS", "SBUX", 
                  "ADI", "CB", "GILD", "MU", "BMY", "DE", "PLD", "MMC", "INTC", "AMT", "SO", "LRCX", 
                  "ELV", "DELL", "PLTR", "REGN", "MDLZ", "MO", "HCA", "SHW", "KLAC", "ICE", "CI", "ABNB"]

JP_TICKER_LIST = ["7203", "8306", "6501", "6861", "6758", "9983", "6098", "9984", "8316", "9432", 
                  "4519", "4063", "8058", "8001", "8766", "8035", "9433", "8031", "7974", "4568", 
                  "9434", "8411", "2914", "7267", "7741", "7011", "4502", "6857", "6902", "4661", 
                  "6503", "3382", "6367", "8725", "4578", "6702", "6981", "6146", "7751", "6178", 
                  "4543", "4901", "6273", "8053", "8002", "6954", "5108", "8591", "6301", "8801", 
                  "6723", "8750", "6762", "6594", "9020", "6701", "9613", "4503", "8267", "8630", 
                  "6752", "6201", "9022", "7733", "4452", "4689", "2802", "5401", "1925", "7269", 
                  "8802", "8113", "2502", "8015", "4612", "4307", "1605", "8309", "8308", "1928", 
                  "8604", "9101", "6326", "4684", "7532", "9735", "8830", "9503", "5020", "3659", 
                  "9843", "6971", "7832", "4091", "7309", "4755", "9104", "4716", "7936", "9766", 
                  "4507", "8697", "5802", "2503", "7270", "6920", "6869", "6988", "2801", "2587", 
                  "3407", "5803", "7201", "8593", "9531", "4523", "9107", "7202", "3092", "8601", 
                  "5019", "9202", "9435", "1802", "4768", "7911", "4151", "9502", "6586", "7701", 
                  "3402", "7272", "9532", "9697", "4911", "9021", "8795", "3064", "7259", "1812", 
                  "2897", "7912", "4324", "6504", "7013", "7550", "6645", "5713", "5411", "4188"]

# --- Error Handling ---
class MarketDataError(Exception):
    """Custom exception for data fetching and processing errors."""
    def __init__(self, code, message=None):
        self.code = code
        self.message = message or ERROR_CODES.get(code, "An unknown error occurred.")
        super().__init__(f"[{self.code}] {self.message}")

ERROR_CODES = {
    "E001": "Gemini API key is not configured.",
    "E002": "Data file could not be read.",
    "E003": "Failed to connect to an external API.",
    "E004": "Failed to fetch Fear & Greed Index data.",
    "E005": "AI content generation failed.",
    "E006": "Failed to fetch heatmap data.",
    "E007": "Failed to fetch calendar data via Selenium.",
}

# --- Logging Configuration ---
LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'app.log')

# Create a stream handler for console output
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

# Create a formatter and set it for both handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

# Get the root logger and add handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Avoid adding handlers multiple times if this module is reloaded
if not logger.handlers:
    logger.addHandler(stream_handler)


# --- Main Data Fetching Class ---
class MarketDataFetcher:
    def __init__(self):
        # curl_cffiのSessionを使用してブラウザを偽装
        self.http_session = Session(impersonate="chrome110", headers={'Accept-Language': 'en-US,en;q=0.9'})
        # yfinance用のセッションも別途作成
        self.yf_session = Session(impersonate="safari15_5")
        self.data = {"market": {}, "news": [], "indicators": {"economic": [], "us_earnings": [], "jp_earnings": []}}

    def _clean_non_compliant_floats(self, obj):
        if isinstance(obj, dict):
            return {k: self._clean_non_compliant_floats(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._clean_non_compliant_floats(elem) for elem in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    def _get_favicon_url(self, url):
        """Extracts the base URL and returns a potential favicon URL."""
        try:
            parsed_url = urlparse(url)
            # Use Google's S2 converter which is good at finding icons
            return f"https://www.google.com/s2/favicons?domain={parsed_url.netloc}&sz=64"
        except Exception as e:
            logger.warning(f"Could not parse URL for favicon: {url} - {e}")
            return None

    # --- Ticker List Fetching ---
    def _get_sp500_tickers(self):
        logger.info("Fetching S&P 500 ticker list from Wikipedia...")
        try:
            response = self.http_session.get(SP500_WIKI_URL, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'id': 'constituents'})
            tickers = [row.find_all('td')[0].text.strip() for row in table.find_all('tr')[1:]]
            return [t.replace('.', '-') for t in tickers]
        except Exception as e:
            logger.error(f"Failed to get S&P 500 tickers: {e}")
            return []

    def _get_nasdaq100_tickers(self):
        logger.info("Fetching NASDAQ 100 ticker list from Wikipedia...")
        try:
            response = self.http_session.get(NASDAQ100_WIKI_URL, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'id': 'constituents'})
            tickers = [row.find_all('td')[0].text.strip() for row in table.find_all('tr')[1:] if len(row.find_all('td')) > 0]
            return [t.replace('.', '-') for t in tickers]
        except Exception as e:
            logger.error(f"Failed to get NASDAQ 100 tickers: {e}")
            return []

    # --- Data Fetching Methods ---
    def _fetch_yfinance_data(self, ticker_symbol, period="5d", interval="1h", resample_period='4h'):
        """Yahoo Finance API対策を含むデータ取得"""
        try:
            ticker = yf.Ticker(ticker_symbol, session=self.yf_session)
            hist = ticker.history(period=period, interval=interval)

            if hist.empty:
                raise ValueError("No data returned")

            hist.index = hist.index.tz_convert('Asia/Tokyo')
            resampled_hist = hist['Close'].resample(resample_period).ohlc().dropna()
            current_price = hist['Close'].iloc[-1]
            history_list = [
                {
                    "time": index.strftime('%Y-%m-%dT%H:%M:%S'),
                    "open": round(row['open'], 2),
                    "high": round(row['high'], 2),
                    "low": round(row['low'], 2),
                    "close": round(row['close'], 2)
                } for index, row in resampled_hist.iterrows()
            ]
            return {"current": round(current_price, 2), "history": history_list}
        except Exception as e:
            logger.error(f"Error fetching {ticker_symbol}: {e}")
            raise MarketDataError("E003", f"yfinance failed for {ticker_symbol}: {e}") from e

    def fetch_vix(self):
        logger.info("Fetching VIX data...")
        try:
            self.data['market']['vix'] = self._fetch_yfinance_data(VIX_TICKER, period="60d")
        except MarketDataError as e:
            self.data['market']['vix'] = {"current": None, "history": [], "error": str(e)}
            logger.error(f"VIX fetch failed: {e}")

    def fetch_t_note_future(self):
        logger.info("Fetching T-note future data...")
        try:
            self.data['market']['t_note_future'] = self._fetch_yfinance_data(T_NOTE_TICKER, period="60d")
        except MarketDataError as e:
            self.data['market']['t_note_future'] = {"current": None, "history": [], "error": str(e)}
            logger.error(f"T-Note fetch failed: {e}")

    def _get_historical_value(self, data, days_ago):
        target_date = datetime.now() - timedelta(days=days_ago)
        closest_item = min(data, key=lambda x: abs(datetime.fromtimestamp(x['x'] / 1000) - target_date))
        return closest_item['y'] if closest_item else None

    def _get_fear_greed_category(self, value):
        if value is None: return "Unknown"
        if value <= 25: return "Extreme Fear";
        if value <= 45: return "Fear";
        if value <= 55: return "Neutral";
        if value <= 75: return "Greed";
        return "Extreme Greed"

    def fetch_fear_greed_index(self):
        logger.info("Fetching Fear & Greed Index...")
        try:
            start_date = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
            url = f"{CNN_FEAR_GREED_URL}{start_date}"
            response = self.http_session.get(url, timeout=30)
            response.raise_for_status()
            api_data = response.json()
            fg_data = api_data.get('fear_and_greed_historical', {}).get('data', [])
            if not fg_data: raise ValueError("No historical data found")

            current_value = fg_data[-1]['y']
            previous_close_val = self._get_historical_value(fg_data, 1)
            week_ago_val = self._get_historical_value(fg_data, 7)
            month_ago_val = self._get_historical_value(fg_data, 30)
            year_ago_val = self._get_historical_value(fg_data, 365)

            # Store the original data structure for other parts of the app
            self.data['market']['fear_and_greed'] = {
                'now': round(current_value),
                'previous_close': round(previous_close_val) if previous_close_val is not None else None,
                'prev_week': round(week_ago_val) if week_ago_val is not None else None,
                'prev_month': round(month_ago_val) if month_ago_val is not None else None,
                'prev_year': round(year_ago_val) if year_ago_val is not None else None,
                'category': self._get_fear_greed_category(current_value)
            }

            # Prepare data for image generation
            chart_data = {
                "center_value": round(current_value),
                "history": {
                    "previous_close": {"label": "Previous close", "status": self._get_fear_greed_category(previous_close_val), "value": round(previous_close_val) if previous_close_val is not None else 'N/A'},
                    "week_ago": {"label": "1 week ago", "status": self._get_fear_greed_category(week_ago_val), "value": round(week_ago_val) if week_ago_val is not None else 'N/A'},
                    "month_ago": {"label": "1 month ago", "status": self._get_fear_greed_category(month_ago_val), "value": round(month_ago_val) if month_ago_val is not None else 'N/A'},
                    "year_ago": {"label": "1 year ago", "status": self._get_fear_greed_category(year_ago_val), "value": round(year_ago_val) if year_ago_val is not None else 'N/A'}
                }
            }

            # Generate the chart
            logger.info("Generating Fear & Greed gauge chart...")
            generate_fear_greed_chart(chart_data)

        except Exception as e:
            logger.error(f"Error fetching or generating Fear & Greed Index: {e}")
            self.data['market']['fear_and_greed'] = {'now': None, 'error': f"[E004] {ERROR_CODES['E004']}: {e}"}

    def fetch_calendar_data(self):
        """Fetch economic indicators and earnings calendar."""
        dt_now = datetime.now()
        
        # Fetch economic indicators
        self._fetch_economic_indicators(dt_now)

        # Fetch earnings
        logger.info("Fetching earnings calendar data...")
        try:
            # Fetch US earnings
            self._fetch_us_earnings(dt_now)
            
            # Fetch JP earnings
            self._fetch_jp_earnings(dt_now)
            
        except Exception as e:
            logger.error(f"Error during earnings data fetching: {e}")
            if 'error' not in self.data['indicators']:
                 self.data['indicators']['error'] = f"[E007] {ERROR_CODES['E007']}: {e}"

    def _fetch_economic_indicators(self, dt_now):
        """Fetch economic indicators from Monex using curl_cffi and BeautifulSoup. Timezone-aware."""
        logger.info("Fetching economic indicators from Monex...")
        try:
            response = self.http_session.get(MONEX_ECONOMIC_CALENDAR_URL, timeout=30)
            response.raise_for_status()
            html_content = response.content.decode('shift_jis', errors='replace')
            soup = BeautifulSoup(html_content, 'lxml')

            table = soup.find('table', class_='eindicator-list')
            if not table:
                logger.warning("Could not find the expected economic calendar table.")
                self.data['indicators']['economic'] = []
                return

            indicators = []
            jst = timezone(timedelta(hours=9))
            dt_now_jst = datetime.now(jst)

            # On Monday (weekday() == 0), fetch for the whole week. Otherwise, for the next 26 hours.
            if dt_now_jst.weekday() == 0:
                end_date = dt_now_jst + timedelta(days=6)
            else:
                end_date = dt_now_jst + timedelta(hours=26)
            logger.info(f"Fetching economic indicators until {end_date.strftime('%Y-%m-%d %H:%M')}")

            current_date_str = ""

            for row in table.find('tbody').find_all('tr'):
                cells = row.find_all('td')

                try:
                    # Handle date cells with rowspan
                    if 'rowspan' in cells[0].attrs:
                        current_date_str = cells[0].text.strip()
                        cell_offset = 0
                    else:
                        cell_offset = -1

                    time_str = cells[1 + cell_offset].text.strip()
                    if not time_str or time_str == '-':
                        continue

                    # Handle "24:00" as next day's "00:00"
                    date_offset = timedelta(days=0)
                    if time_str == "24:00":
                        time_str = "00:00"
                        date_offset = timedelta(days=1)

                    full_date_str = f"{dt_now_jst.year}/{current_date_str.split('(')[0]} {time_str}"
                    tdatetime = datetime.strptime(full_date_str, '%Y/%m/%d %H:%M') + date_offset
                    tdatetime_aware = tdatetime.replace(tzinfo=jst)

                    if not (dt_now_jst - timedelta(hours=2) < tdatetime_aware < end_date):
                        continue

                    importance_str = cells[2 + cell_offset].text.strip()
                    if "★" not in importance_str:
                        continue

                    # Extract country emoji
                    country_cell = cells[3 + cell_offset]
                    img_tag = country_cell.find('img')
                    emoji = ''
                    if img_tag and img_tag.get('src'):
                        match = re.search(r'inner_flag_(\w+)\.(?:gif|png)', img_tag['src'])
                        if match:
                            country_code = match.group(1)
                            emoji = COUNTRY_EMOJI_MAP.get(country_code, '')

                    def get_value(cell_index, default='--'):
                        val = cells[cell_index].text.strip()
                        return val if val else default

                    name = get_value(4 + cell_offset)

                    indicator = {
                        "datetime": tdatetime_aware.strftime('%m/%d %H:%M'),
                        "name": f"{emoji} {name}".strip(),
                        "importance": importance_str,
                        "previous": get_value(5 + cell_offset),
                        "forecast": get_value(6 + cell_offset),
                        "type": "economic"
                    }
                    indicators.append(indicator)

                except (ValueError, IndexError) as e:
                    logger.debug(f"Skipping row in economic indicators: {row.text.strip()} due to {e}")
                    continue
            
            self.data['indicators']['economic'] = indicators
            logger.info(f"Fetched {len(indicators)} economic indicators successfully.")

        except Exception as e:
            logger.error(f"Error fetching economic indicators: {e}")
            self.data['indicators']['economic'] = []

    def _fetch_us_earnings(self, dt_now):
        """Fetch US earnings calendar from Monex using curl_cffi."""
        logger.info("Fetching US earnings calendar from Monex...")
        try:
            response = self.http_session.get(MONEX_US_EARNINGS_URL, timeout=30)
            response.raise_for_status()
            html_content = response.content.decode('shift_jis', errors='replace')
            tables = pd.read_html(StringIO(html_content), flavor='lxml')
            
            jst = timezone(timedelta(hours=9))
            dt_now_jst = dt_now.astimezone(jst)

            # On Monday (weekday() == 0), fetch for the whole week. Otherwise, for the next 26 hours.
            if dt_now_jst.weekday() == 0:
                end_date = dt_now_jst + timedelta(days=6)
            else:
                end_date = dt_now_jst + timedelta(hours=26)
            logger.info(f"Fetching US earnings until {end_date.strftime('%Y-%m-%d')}")

            earnings = []
            for df in tables:
                if df.empty: continue
                for i in range(len(df)):
                    try:
                        ticker, company_name, date_str, time_str = None, None, None, None
                        for col_idx in range(len(df.columns)):
                            val = str(df.iloc[i, col_idx]) if pd.notna(df.iloc[i, col_idx]) else ""
                            if val in US_TICKER_LIST: ticker = val
                            elif "/" in val and len(val) >= 8: date_str = val
                            elif ":" in val and len(val) >= 5: time_str = val
                            elif len(val) > 3 and val != "nan" and not company_name: company_name = val[:20]

                        if ticker and date_str and time_str:
                            text0 = date_str[:10] + " " + time_str[:5]
                            tdatetime_naive = datetime.strptime(text0, '%Y/%m/%d %H:%M')
                            # The source provides US time. A simple +13h is used as an approximation for JST.
                            tdatetime_jst = tdatetime_naive + timedelta(hours=13)
                            # Make it aware for comparison
                            tdatetime_aware_jst = jst.localize(tdatetime_jst)

                            if dt_now_jst - timedelta(hours=2) < tdatetime_aware_jst < end_date:
                                earnings.append({"datetime": tdatetime_aware_jst.strftime('%m/%d %H:%M'), "ticker": ticker, "company": f"({company_name})" if company_name else "", "type": "us_earnings"})
                    except Exception as e:
                        logger.debug(f"Skipping row {i} in US earnings: {e}")
            
            self.data['indicators']['us_earnings'] = earnings
            logger.info(f"Fetched {len(earnings)} US earnings")
        except Exception as e:
            logger.error(f"Error fetching US earnings: {e}")
            self.data['indicators']['us_earnings'] = []

    def _parse_jp_earnings_date(self, date_str, current_datetime, tz):
        """Helper to parse Japanese date strings and handle year-end rollover."""
        match = re.search(r'(\d{1,2})月(\d{1,2})日.*?(\d{1,2}):(\d{1,2})', date_str)
        if match:
            month, day, hour, minute = map(int, match.groups())
            year = current_datetime.year
            # Handle year rollover: if the parsed month is less than the current month,
            # it's likely for the next year (e.g., parsing Jan data in Dec).
            if month < current_datetime.month:
                year += 1

            naive_dt = datetime(year, month, day, hour, minute)
            return tz.localize(naive_dt)
        return None

    def _fetch_jp_earnings(self, dt_now):
        """Fetch Japanese earnings calendar from Monex using curl_cffi."""
        logger.info("Fetching Japanese earnings calendar from Monex...")
        try:
            response = self.http_session.get(MONEX_JP_EARNINGS_URL, timeout=30)
            response.raise_for_status()
            html_content = response.content.decode('shift_jis', errors='replace')
            tables = pd.read_html(StringIO(html_content), flavor='lxml')

            jst = timezone(timedelta(hours=9))
            dt_now_jst = dt_now.astimezone(jst)

            # On Monday (weekday() == 0), fetch for the whole week. Otherwise, for the next 26 hours.
            if dt_now_jst.weekday() == 0:
                end_date = dt_now_jst + timedelta(days=6)
            else:
                end_date = dt_now_jst + timedelta(hours=26)
            logger.info(f"Fetching JP earnings until {end_date.strftime('%Y-%m-%d')}")

            earnings = []
            for df in tables:
                if df.empty: continue
                for i in range(len(df)):
                    try:
                        ticker, company_name, date_time_str = None, None, None
                        for col_idx in range(len(df.columns)):
                            val = str(df.iloc[i, col_idx]) if pd.notna(df.iloc[i, col_idx]) else ""
                            match = re.search(r'(\d{4})', val)
                            if not ticker and match and match.group(1) in JP_TICKER_LIST:
                                ticker = match.group(1)
                                if not val.strip().isdigit():
                                    name_match = re.search(r'^([^（\(]+)', val)
                                    if name_match: company_name = name_match.group(1).strip()[:20]
                            elif not date_time_str and "/" in val and "日" in val: date_time_str = val.strip()
                            elif not company_name and len(val) > 2 and val != 'nan' and not val.strip().isdigit() and "/" not in val: company_name = val.strip()[:20]

                        if ticker and date_time_str:
                            # Parse the Japanese date string into an aware datetime object, handling year-end
                            parsed_date_jst = self._parse_jp_earnings_date(date_time_str, dt_now_jst, jst)
                            if parsed_date_jst and (dt_now_jst - timedelta(hours=2) < parsed_date_jst < end_date):
                                earnings.append({"datetime": parsed_date_jst.strftime('%m/%d %H:%M'), "ticker": ticker, "company": f"({company_name})" if company_name else "", "type": "jp_earnings"})
                    except Exception as e:
                        logger.debug(f"Skipping row {i} in JP earnings: {e}")

            self.data['indicators']['jp_earnings'] = earnings
            logger.info(f"Fetched {len(earnings)} Japanese earnings")
        except Exception as e:
            logger.error(f"Error fetching Japanese earnings: {e}")
            self.data['indicators']['jp_earnings'] = []

    def fetch_yahoo_finance_news(self):
        """Fetches recent news from Yahoo Finance using the yfinance library and filters them."""
        logger.info("Fetching and filtering news from Yahoo Finance using yfinance...")
        try:
            # Define tickers for major US indices
            indices = {"NASDAQ Composite (^IXIC)": "^IXIC", "S&P 500 (^GSPC)": "^GSPC", "Dow 30 (^DJI)": "^DJI"}
            all_raw_news = []

            for name, ticker_symbol in indices.items():
                logger.info(f"Fetching news for {name}...")
                try:
                    ticker = yf.Ticker(ticker_symbol, session=self.yf_session)
                    news = ticker.news
                    if news:
                        all_raw_news.extend(news)
                    else:
                        logger.warning(f"No news returned from yfinance for {ticker_symbol}.")
                except Exception as e:
                    logger.error(f"Failed to fetch news for {ticker_symbol}: {e}")
                    continue # Continue to the next ticker

            # Deduplicate news based on the article link to avoid redundancy
            unique_news = []
            seen_links = set()
            for article in all_raw_news:
                try:
                    # The unique identifier for a news article is its URL.
                    link = article['content']['canonicalUrl']['url']
                    if link not in seen_links:
                        unique_news.append(article)
                        seen_links.add(link)
                except KeyError:
                    # Log if a link is not found, but continue processing other articles.
                    logger.warning(f"Could not find link for article, skipping: {article.get('content', {}).get('title', 'No Title')}")
                    continue

            raw_news = unique_news

            if not raw_news:
                logger.warning("No news returned from yfinance for any of the specified indices.")
                self.data['news_raw'] = []
                return

            now_utc = datetime.now(timezone.utc)

            # On Monday (weekday() == 0), fetch news from the last 7 days (168 hours)
            # Otherwise, fetch from the last 24 hours.
            hours_to_fetch = 168 if now_utc.weekday() == 0 else 24
            fetch_since_date = now_utc - timedelta(hours=hours_to_fetch)

            logger.info(f"Fetching news from the last {hours_to_fetch} hours (since {fetch_since_date.strftime('%Y-%m-%d %H:%M:%S UTC')})...")

            # 1. Filter news within the specified time frame
            filtered_news = []
            for article in raw_news:
                try:
                    # pubDate is a string like '2025-09-08T17:42:03Z'
                    pub_date_str = article['content']['pubDate']
                    # fromisoformat doesn't like the 'Z' suffix
                    publish_time = datetime.fromisoformat(pub_date_str.replace('Z', '+00:00'))

                    if publish_time >= fetch_since_date:
                        article['publish_time_dt'] = publish_time # Store for sorting
                        filtered_news.append(article)
                except (KeyError, TypeError) as e:
                    logger.warning(f"Could not process article, skipping: {e} - {article}")
                    continue

            # 2. Sort by publish time descending (latest first)
            filtered_news.sort(key=lambda x: x['publish_time_dt'], reverse=True)

            # 3. Format all filtered news
            formatted_news = []
            for item in filtered_news:
                try:
                    link = item['content']['canonicalUrl']['url']
                    favicon_url = self._get_favicon_url(link)
                    formatted_news.append({
                        "title": item['content']['title'],
                        "link": link,
                        "publisher": item['content']['provider']['displayName'],
                        "summary": item['content'].get('summary', ''),
                        "source_icon_url": favicon_url
                    })
                except KeyError as e:
                    logger.warning(f"Skipping article due to missing key {e}: {item.get('content', {}).get('title', 'No Title')}")
                    continue

            self.data['news_raw'] = formatted_news
            logger.info(f"Fetched {len(all_raw_news)} raw news items, found {len(unique_news)} unique articles, {len(filtered_news)} within the last {hours_to_fetch} hours, storing the top {len(formatted_news)}.")

        except Exception as e:
            logger.error(f"Error fetching or processing yfinance news: {e}")
            self.data['news_raw'] = []

    def fetch_heatmap_data(self):
        """ヒートマップデータ取得（API対策強化版）"""
        logger.info("Fetching heatmap data...")
        try:
            sp500_tickers = self._get_sp500_tickers()
            nasdaq100_tickers = self._get_nasdaq100_tickers()
            logger.info(f"Found {len(sp500_tickers)} S&P 500 tickers and {len(nasdaq100_tickers)} NASDAQ 100 tickers.")

            # Fetch S&P 500 data
            sp500_heatmaps = self._fetch_stock_performance_for_heatmap(sp500_tickers, batch_size=30)
            self.data['sp500_heatmap_1d'] = sp500_heatmaps.get('1d', {"stocks": []})
            self.data['sp500_heatmap_1w'] = sp500_heatmaps.get('1w', {"stocks": []})
            self.data['sp500_heatmap_1m'] = sp500_heatmaps.get('1m', {"stocks": []})
            # For backward compatibility with AI commentary
            self.data['sp500_heatmap'] = self.data.get('sp500_heatmap_1d', {"stocks": []})

            # Fetch NASDAQ 100 data
            nasdaq_heatmaps = self._fetch_stock_performance_for_heatmap(nasdaq100_tickers, batch_size=30)
            self.data['nasdaq_heatmap_1d'] = nasdaq_heatmaps.get('1d', {"stocks": []})
            self.data['nasdaq_heatmap_1w'] = nasdaq_heatmaps.get('1w', {"stocks": []})
            self.data['nasdaq_heatmap_1m'] = nasdaq_heatmaps.get('1m', {"stocks": []})
            # For backward compatibility with AI commentary
            self.data['nasdaq_heatmap'] = self.data.get('nasdaq_heatmap_1d', {"stocks": []})

            # Fetch Sector ETF data
            sector_etf_tickers = ["XLK", "XLY", "XLV", "XLP", "XLB", "XLU", "XLI", "XLC", "XLRE", "XLF", "XLE"]
            logger.info(f"Fetching data for {len(sector_etf_tickers)} sector ETFs.")
            sector_etf_heatmaps = self._fetch_etf_performance_for_heatmap(sector_etf_tickers)
            self.data['sector_etf_heatmap_1d'] = sector_etf_heatmaps.get('1d', {"etfs": []})
            self.data['sector_etf_heatmap_1w'] = sector_etf_heatmaps.get('1w', {"etfs": []})
            self.data['sector_etf_heatmap_1m'] = sector_etf_heatmaps.get('1m', {"etfs": []})

            # Create combined S&P 500 and ETF heatmaps
            logger.info("Creating combined S&P 500 and Sector ETF heatmap data...")
            for period in ['1d', '1w', '1m']:
                sp500_stocks = self.data.get(f'sp500_heatmap_{period}', {}).get('stocks', [])
                etfs = self.data.get(f'sector_etf_heatmap_{period}', {}).get('etfs', [])

                # The frontend only needs ticker and performance.
                # No need to add a 'type' field as they will be rendered identically.
                combined_items = sp500_stocks + etfs
                self.data[f'sp500_combined_heatmap_{period}'] = {"items": combined_items}

        except Exception as e:
            logger.error(f"Error during heatmap data fetching: {e}")
            error_payload = {"stocks": [], "error": f"[E006] {ERROR_CODES['E006']}: {e}"}
            self.data['sp500_heatmap_1d'] = error_payload
            self.data['sp500_heatmap_1w'] = error_payload
            self.data['sp500_heatmap_1m'] = error_payload
            self.data['nasdaq_heatmap_1d'] = error_payload
            self.data['nasdaq_heatmap_1w'] = error_payload
            self.data['nasdaq_heatmap_1m'] = error_payload
            self.data['sp500_heatmap'] = error_payload
            self.data['nasdaq_heatmap'] = error_payload
            etf_error_payload = {"etfs": [], "error": f"[E006] {ERROR_CODES['E006']}: {e}"}
            self.data['sector_etf_heatmap_1d'] = etf_error_payload
            self.data['sector_etf_heatmap_1w'] = etf_error_payload
            self.data['sector_etf_heatmap_1m'] = etf_error_payload
            self.data['sp500_combined_heatmap_1d'] = {"items": []}
            self.data['sp500_combined_heatmap_1w'] = {"items": []}
            self.data['sp500_combined_heatmap_1m'] = {"items": []}

    def _fetch_stock_performance_for_heatmap(self, tickers, batch_size=30):
        """改善版：レート制限対策を含むヒートマップ用データ取得（業種・フラット構造対応）。1日、1週間、1ヶ月のパフォーマンスを計算する。"""
        if not tickers:
            return {"1d": {"stocks": []}, "1w": {"stocks": []}, "1m": {"stocks": []}}

        heatmaps = {
            "1d": {"stocks": []},
            "1w": {"stocks": []},
            "1m": {"stocks": []}
        }

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]

            for ticker_symbol in batch:
                try:
                    ticker_obj = yf.Ticker(ticker_symbol, session=self.yf_session)
                    info = ticker_obj.info
                    # 1ヶ月分のデータを取得（約22営業日 + 余裕）
                    hist = ticker_obj.history(period="35d")

                    if hist.empty:
                        logger.warning(f"No history for {ticker_symbol}, skipping.")
                        continue

                    sector = info.get('sector', 'N/A')
                    industry = info.get('industry', 'N/A')
                    market_cap = info.get('marketCap', 0)

                    if sector == 'N/A' or industry == 'N/A' or market_cap == 0:
                        logger.warning(f"Skipping {ticker_symbol} due to missing sector, industry, or market cap.")
                        continue

                    base_stock_data = {
                        "ticker": ticker_symbol,
                        "sector": sector,
                        "industry": industry,
                        "market_cap": market_cap
                    }

                    latest_close = hist['Close'].iloc[-1]

                    # 1-Day Performance
                    if len(hist) >= 2 and hist['Close'].iloc[-2] != 0:
                        perf_1d = ((latest_close - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
                        stock_1d = base_stock_data.copy()
                        stock_1d["performance"] = round(perf_1d, 2)
                        heatmaps["1d"]["stocks"].append(stock_1d)

                    # 1-Week Performance (5 trading days)
                    if len(hist) >= 6 and hist['Close'].iloc[-6] != 0:
                        perf_1w = ((latest_close - hist['Close'].iloc[-6]) / hist['Close'].iloc[-6]) * 100
                        stock_1w = base_stock_data.copy()
                        stock_1w["performance"] = round(perf_1w, 2)
                        heatmaps["1w"]["stocks"].append(stock_1w)

                    # 1-Month Performance (20 trading days)
                    if len(hist) >= 21 and hist['Close'].iloc[-21] != 0:
                        perf_1m = ((latest_close - hist['Close'].iloc[-21]) / hist['Close'].iloc[-21]) * 100
                        stock_1m = base_stock_data.copy()
                        stock_1m["performance"] = round(perf_1m, 2)
                        heatmaps["1m"]["stocks"].append(stock_1m)

                except Exception as e:
                    logger.error(f"Could not fetch data for {ticker_symbol}: {e}")
                    time.sleep(0.5)
                    continue

            if i + batch_size < len(tickers):
                logger.info(f"Processed {min(i + batch_size, len(tickers))}/{len(tickers)} tickers, waiting...")
                time.sleep(3)

        return heatmaps

    def _fetch_etf_performance_for_heatmap(self, tickers):
        """Fetches 1-day, 1-week, and 1-month performance for a list of ETFs."""
        if not tickers:
            return {"1d": {"etfs": []}, "1w": {"etfs": []}, "1m": {"etfs": []}}

        heatmaps = {
            "1d": {"etfs": []},
            "1w": {"etfs": []},
            "1m": {"etfs": []}
        }

        for ticker_symbol in tickers:
            try:
                ticker_obj = yf.Ticker(ticker_symbol, session=self.yf_session)
                # 1ヶ月分のデータを取得（約22営業日 + 余裕）
                hist = ticker_obj.history(period="35d")

                if hist.empty:
                    logger.warning(f"No history for ETF {ticker_symbol}, skipping.")
                    continue

                base_etf_data = {
                    "ticker": ticker_symbol,
                }

                latest_close = hist['Close'].iloc[-1]

                # 1-Day Performance
                if len(hist) >= 2 and hist['Close'].iloc[-2] != 0:
                    perf_1d = ((latest_close - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
                    etf_1d = base_etf_data.copy()
                    etf_1d["performance"] = round(perf_1d, 2)
                    heatmaps["1d"]["etfs"].append(etf_1d)

                # 1-Week Performance (5 trading days)
                if len(hist) >= 6 and hist['Close'].iloc[-6] != 0:
                    perf_1w = ((latest_close - hist['Close'].iloc[-6]) / hist['Close'].iloc[-6]) * 100
                    etf_1w = base_etf_data.copy()
                    etf_1w["performance"] = round(perf_1w, 2)
                    heatmaps["1w"]["etfs"].append(etf_1w)

                # 1-Month Performance (20 trading days)
                if len(hist) >= 21 and hist['Close'].iloc[-21] != 0:
                    perf_1m = ((latest_close - hist['Close'].iloc[-21]) / hist['Close'].iloc[-21]) * 100
                    etf_1m = base_etf_data.copy()
                    etf_1m["performance"] = round(perf_1m, 2)
                    heatmaps["1m"]["etfs"].append(etf_1m)

            except Exception as e:
                logger.error(f"Could not fetch data for ETF {ticker_symbol}: {e}")
                continue

        # Sort by ticker name
        for period in heatmaps:
            if 'etfs' in heatmaps[period]:
                heatmaps[period]['etfs'].sort(key=lambda x: x['ticker'])

        return heatmaps

    # --- AI Generation ---
    def _call_gemini_api(self, prompt, max_tokens=None):
        """A generalized method to call the Gemini API."""
        try:
            logger.info(f"Calling Gemini API...")
            content = gemini_client.generate_content(prompt)

            if not content:
                logger.error("Empty content in Gemini API response")
                raise MarketDataError("E005", "Empty content in Gemini API response")

            content = content.strip()
            logger.debug(f"Received response (first 200 chars): {content[:200]}")

            # Clean markdown code blocks if present
            if content.startswith("```json"):
                content = content.replace("```json", "").replace("```", "").strip()
            elif content.startswith("```"):
                content = content.replace("```", "").strip()

            try:
                return json.loads(content)
            except json.JSONDecodeError as je:
                logger.error(f"Failed to parse JSON response: {content[:500]}")
                raise MarketDataError("E005", f"Invalid JSON response: {je}") from je

        except Exception as e:
            logger.error(f"Error calling Gemini API: {e}")
            raise MarketDataError("E005", str(e)) from e

    def generate_unified_report(self):
        logger.info("Generating unified AI report...")

        # --- 1. Prepare Data ---

        # Market Data
        fear_greed_data = self.data.get('market', {}).get('fear_and_greed', {})
        fg_now_val = fear_greed_data.get('now', 'N/A')
        fg_now_cat = self._get_fear_greed_category(fg_now_val)
        fg_week_val = fear_greed_data.get('prev_week', 'N/A')
        fg_week_cat = self._get_fear_greed_category(fg_week_val)
        fg_month_val = fear_greed_data.get('prev_month', 'N/A')
        fg_month_cat = self._get_fear_greed_category(fg_month_val)

        vix_history = self.data.get('market', {}).get('vix', {}).get('history', [])
        t_note_history = self.data.get('market', {}).get('t_note_future', {}).get('history', [])

        def format_history(history, days=30):
            if not history: return "N/A"
            recent_history = history[- (days * 6) :]
            return ", ".join([str(item['close']) for item in recent_history])

        vix_history_str = format_history(vix_history)
        t_note_history_str = format_history(t_note_history)

        # News Data
        raw_news = self.data.get('news_raw', [])
        news_content = ""
        for i, item in enumerate(raw_news): # Use all raw news
            news_content += f"記事{i+1}: タイトル: {item['title']}, 概要: {item.get('summary', 'N/A')}, URL: {item['link']}\n"
        if not news_content: news_content = "ニュースなし"

        # Heatmap Data
        def get_stock_performance_str(stocks, count=5):
            if not stocks: return "N/A", "N/A"
            valid_stocks = [s for s in stocks if isinstance(s.get('performance'), (int, float))]
            sorted_stocks = sorted(valid_stocks, key=lambda x: x.get('performance', 0), reverse=True)
            top = ', '.join([f"{s['ticker']} ({s['performance']:.2f}%)" for s in sorted_stocks[:count]])
            bottom = ', '.join([f"{s['ticker']} ({s['performance']:.2f}%)" for s in sorted_stocks[-count:]])
            return top, bottom

        # SP500
        sp500_stocks = self.data.get('sp500_heatmap_1d', {}).get('stocks', [])
        sp500_top, sp500_bottom = get_stock_performance_str(sp500_stocks)

        # Sector ETFs
        etf_heatmap_1d = self.data.get('sector_etf_heatmap_1d', {}).get('etfs', [])
        etf_heatmap_1w = self.data.get('sector_etf_heatmap_1w', {}).get('etfs', [])
        etf_heatmap_1m = self.data.get('sector_etf_heatmap_1m', {}).get('etfs', [])

        def get_etf_str(etfs):
            if not etfs: return "N/A", "N/A"
            sorted_etfs = sorted(etfs, key=lambda x: x.get('performance', 0), reverse=True)
            top = ', '.join([f"{s['ticker']} ({s['performance']:.2f}%)" for s in sorted_etfs[:3]])
            bottom = ', '.join([f"{s['ticker']} ({s['performance']:.2f}%)" for s in sorted_etfs[-3:]])
            return top, bottom

        etf_1d_top, etf_1d_bottom = get_etf_str(etf_heatmap_1d)
        etf_1w_top, _ = get_etf_str(etf_heatmap_1w)
        etf_1m_top, _ = get_etf_str(etf_heatmap_1m)

        # Nasdaq
        nasdaq_stocks = self.data.get('nasdaq_heatmap_1d', {}).get('stocks', [])
        nasdaq_top, nasdaq_bottom = get_stock_performance_str(nasdaq_stocks)

        # Indicators Data
        jst = timezone(timedelta(hours=9))
        today = datetime.now(jst)
        is_monday = today.weekday() == 0

        # Economic
        economic_indicators = self.data.get("indicators", {}).get("economic", [])
        us_indicators = [ind for ind in economic_indicators if "🇺🇸" in ind.get("name", "")]
        def sort_key_ind(indicator):
            importance = indicator.get("importance", "")
            if "★★★" in importance: return 0
            if "★★" in importance: return 1
            if "★" in importance: return 2
            return 3
        us_indicators.sort(key=sort_key_ind)

        if is_monday:
            target_indicators = us_indicators[:25]
        else:
            target_indicators = us_indicators # All for the day

        indicators_str = "\n".join([f"- {ind['name']} (重要度: {ind['importance']}): 前回: {ind['previous']}, 市場予測: {ind['forecast']}" for ind in target_indicators])
        if not indicators_str: indicators_str = "なし"

        # Earnings
        us_earnings = self.data.get("indicators", {}).get("us_earnings", [])
        def earnings_sort_key(earning):
            return 0 if earning.get("ticker") in US_TICKER_LIST else 1
        us_earnings.sort(key=earnings_sort_key)

        if is_monday:
            target_earnings = us_earnings[:30]
        else:
            target_earnings = us_earnings[:15]

        earnings_str = "\n".join([f"- {earning.get('company', '')} ({earning.get('ticker')})" for earning in target_earnings])
        if not earnings_str: earnings_str = "なし"

        # Column Data
        try:
            memo_file_path = os.getenv('HANA_MEMO_FILE', 'backend/hana-memo-202509.txt')
            with open(memo_file_path, 'r', encoding='utf-8') as f:
                memo_content = f.read()
        except FileNotFoundError:
            memo_content = "メモファイルが見つかりません。"

        vix_val = self.data.get('market', {}).get('vix', {}).get('current', 'N/A')
        tnote_val = self.data.get('market', {}).get('t_note_future', {}).get('current', 'N/A')
        market_structure_str = f"Fear & Greed Index: {fg_now_val}, VIX指数: {vix_val}, 米国10年債金利: {tnote_val}%"

        # Specific instructions based on Monday vs other days
        if is_monday:
            column_instructions = """
            1.  **⭐今週の注目ポイント**
                - 「経済指標」と「ニュース」を参考に、今週の相場で最も重要となるイベントやテーマを特定。
            2.  **📌いまの市場の構図**
                - 「市場の構図データ」を基に、現在の市場センチメントを要約。
            3.  **🌸今週の戦略アドバイス**
                - 上記を総合的に判断し、心構えや注目点を提案（投資判断ワード禁止）。
            """
            indicator_task = "今週発表される米国の主要な経済指標から特に重要なものを5つ程度選び、週間の見通しを解説 (400字程度)"
            earnings_task = "今週決算発表を予定している米国の主要企業リストから特に重要なものを5社程度選び、週間の見通しを解説 (400字程度)"
        else:
            column_instructions = """
            1.  **⭐本日の注目ポイント**
                - 「経済指標」と「ニュース」を参考に、本日の相場で最も重要となるイベントやテーマを特定。
            2.  **📌いまの市場の構図**
                - 「市場の構図データ」を基に、現在の市場センチメントを要約。
            3.  **🌸今日の戦略アドバイス**
                - 上記を総合的に判断し、心構えや注目点を提案（投資判断ワード禁止）。
            """
            indicator_task = "本日発表される米国の経済指標の中から最も重要なものを3つ程度選び、市場への影響を解説 (300字程度)"
            earnings_task = "本日決算発表を予定している米国企業リストの中から注目すべきものを3〜5社選び、解説 (300字程度)"


        # --- 2. Construct Prompt ---
        prompt = f"""
        あなたはプロの金融アナリストです。以下の提供データを基に、日本の個人投資家向けに複数のレポートを作成してください。

        # 提供データ

        ## 1. 市場データ
        - Fear & Greed Index: 1ヶ月前 {fg_month_val}({fg_month_cat}), 1週間前 {fg_week_val}({fg_week_cat}), 現在 {fg_now_val}({fg_now_cat})
        - VIX指数推移(直近): {vix_history_str}
        - 米国10年債金利推移(直近): {t_note_history_str}
        - 市場の構図データ: {market_structure_str}

        ## 2. ニュース
        {news_content}

        ## 3. ヒートマップデータ (S&P 500)
        - セクターETF (1日) 上位: {etf_1d_top}, 下位: {etf_1d_bottom}
        - セクターETF (1週間) 上位: {etf_1w_top}
        - セクターETF (1ヶ月) 上位: {etf_1m_top}
        - 個別株 (1日) 上昇: {sp500_top}, 下落: {sp500_bottom}

        ## 4. ヒートマップデータ (NASDAQ 100)
        - 個別株 (1日) 上昇: {nasdaq_top}, 下落: {nasdaq_bottom}

        ## 5. 経済指標
        {indicators_str}

        ## 6. 決算発表
        {earnings_str}

        ## 7. メモ (コラム用参考情報)
        {memo_content}

        # 作成指示

        以下の各セクションのレポートを作成し、JSON形式で出力してください。

        ## A. market_commentary (市場センチメント解説)
        - 1ヶ月間の「推移」から読み取れるセンチメント変化を300字程度で解説。
        - Fear & Greed, VIX, 金利の動向と相互関連性を分析。
        - 現在の市場状況（リスクオン/オフなど）を結論付ける。

        ## B. news_analysis (ニュース分析)
        - `summary`: 今朝の市場ムードを表す3行サマリー。
        - `topics`: 重要トピック3つ。各トピックは `title` (20字以内), `analysis` (事実・解釈・影響をまとめた改行なしの文章), `url` を含む。巨大テックやマクロ経済を優先。

        ## C. sp500_commentary (S&P 500 ヒートマップ解説)
        - 250~300字程度。
        - セクターETFの動きから短期・中期トレンドとセクターローテーションの兆候を分析。
        - 個別株の動きとセクターの関連性を説明。

        ## D. nasdaq_commentary (NASDAQ 100 ヒートマップ解説)
        - 200~250字程度。
        - 上昇・下落銘柄から市場のテーマを要約。
        - 注目銘柄の背景要因を推測。

        ## E. economic_commentary (経済指標解説)
        - {indicator_task}
        - 予測との乖離による影響を考慮。

        ## F. earnings_commentary (決算解説)
        - {earnings_task}
        - 市場期待と株価反応の可能性を解説。

        ## G. column (ワンポイント市況解説)
        - `title`: "AI解説"
        - `content`: 以下の構成で作成。
            {column_instructions}
            - 見出し以外に記号や絵文字は使用しない。

        # 出力形式
        必ず以下のJSON形式で出力してください。Markdownのコードブロックは含めないでください。

        {{
          "market_commentary": "...",
          "news_analysis": {{
            "summary": "...",
            "topics": [ {{ "title": "...", "analysis": "...", "url": "..." }}, ... ]
          }},
          "sp500_commentary": "...",
          "nasdaq_commentary": "...",
          "economic_commentary": "...",
          "earnings_commentary": "...",
          "column": {{
            "title": "AI解説",
            "content": "..."
          }}
        }}
        """

        try:
            # Call Gemini
            response_json = self._call_gemini_api(prompt)

            # Distribute results
            self.data['market']['ai_commentary'] = response_json.get('market_commentary', '生成失敗')
            self.data['news'] = response_json.get('news_analysis', {'summary': '生成失敗', 'topics': []})

            # Heatmaps
            if 'sp500_heatmap' not in self.data: self.data['sp500_heatmap'] = {}
            self.data['sp500_heatmap']['ai_commentary'] = response_json.get('sp500_commentary', '生成失敗')

            if 'nasdaq_heatmap' not in self.data: self.data['nasdaq_heatmap'] = {}
            self.data['nasdaq_heatmap']['ai_commentary'] = response_json.get('nasdaq_commentary', '生成失敗')

            # Indicators
            self.data['indicators']['economic_commentary'] = response_json.get('economic_commentary', '生成失敗')
            self.data['indicators']['earnings_commentary'] = response_json.get('earnings_commentary', '生成失敗')

            # Column
            col_data = response_json.get('column', {})
            report_type = "weekly_report" if is_monday else "daily_report"
            if col_data:
                self.data['column'] = {
                    report_type: {
                        "title": col_data.get('title', 'AI解説'),
                        "date": today.isoformat(),
                        "content": col_data.get('content', '生成失敗')
                    }
                }
            else:
                 self.data['column'] = { report_type: { "error": "生成失敗" } }

        except Exception as e:
            logger.error(f"Unified generation failed: {e}")
            # Set error messages for all
            err_msg = "AI生成中にエラーが発生しました。"
            self.data['market']['ai_commentary'] = err_msg
            self.data['news'] = {'summary': err_msg, 'topics': []}
            if 'sp500_heatmap' not in self.data: self.data['sp500_heatmap'] = {}
            self.data['sp500_heatmap']['ai_commentary'] = err_msg
            if 'nasdaq_heatmap' not in self.data: self.data['nasdaq_heatmap'] = {}
            self.data['nasdaq_heatmap']['ai_commentary'] = err_msg
            self.data['indicators']['economic_commentary'] = err_msg
            self.data['indicators']['earnings_commentary'] = err_msg
            report_type = "weekly_report" if is_monday else "daily_report"
            self.data['column'] = { report_type: { "error": err_msg } }

    def cleanup_old_data(self):
        """Deletes data files older than 7 days."""
        logger.info("Cleaning up old data files...")
        try:
            today = datetime.now()
            seven_days_ago = today - timedelta(days=7)

            for filename in os.listdir(DATA_DIR):
                match = re.match(r'data_(\d{4}-\d{2}-\d{2})\.json', filename)
                if match:
                    file_date_str = match.group(1)
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                    if file_date < seven_days_ago:
                        file_path = os.path.join(DATA_DIR, filename)
                        os.remove(file_path)
                        logger.info(f"Deleted old data file: {filename}")
        except Exception as e:
            logger.error(f"Error during data cleanup: {e}")

    # --- Main Execution Methods ---
    def fetch_all_data(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        logger.info("--- Starting Raw Data Fetch ---")

        fetch_tasks = [
            self.fetch_vix,
            self.fetch_t_note_future,
            self.fetch_fear_greed_index,
            self.fetch_calendar_data,  # Changed from fetch_economic_indicators
            self.fetch_yahoo_finance_news,
            self.fetch_heatmap_data
        ]

        for task in fetch_tasks:
            try:
                task()
            except MarketDataError as e:
                logger.error(f"Failed to execute fetch task '{task.__name__}': {e}")

        # Clean the data before writing to file
        self.data = self._clean_non_compliant_floats(self.data)

        with open(RAW_DATA_PATH, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"--- Raw Data Fetch Completed. Saved to {RAW_DATA_PATH} ---")
        return self.data

    def generate_report(self):
        logger.info("--- Starting Report Generation ---")
        if not os.path.exists(RAW_DATA_PATH):
            logger.error(f"{RAW_DATA_PATH} not found. Run fetch first.")
            return
        with open(RAW_DATA_PATH, 'r', encoding='utf-8') as f:
            self.data = json.load(f)

        # Unified AI Generation
        self.generate_unified_report()

        jst = timezone(timedelta(hours=9))
        self.data['date'] = datetime.now(jst).strftime('%Y-%m-%d')
        self.data['last_updated'] = datetime.now(jst).isoformat()

        # Clean the data before writing to file
        self.data = self._clean_non_compliant_floats(self.data)

        final_path = f"{FINAL_DATA_PATH_PREFIX}{self.data['date']}.json"
        with open(final_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        with open(os.path.join(DATA_DIR, 'data.json'), 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"--- Report Generation Completed. Saved to {final_path} ---")

        self.cleanup_old_data()

        return self.data


    def send_push_notifications(self, custom_notification_data=None):
        """Push通知を権限に基づいてフィルタリングして送信"""
        logger.info("Sending push notifications...")

        try:
            from .security_manager import security_manager
            security_manager.data_dir = DATA_DIR
            security_manager.initialize()

            from pywebpush import webpush, WebPushException

            subscriptions_file = os.path.join(DATA_DIR, 'push_subscriptions.json')
            if not os.path.exists(subscriptions_file):
                logger.warning(f"❌ Push subscriptions file not found: {subscriptions_file}")
                logger.info("💡 Users need to re-login and grant notification permission")
                logger.info("No push subscriptions found")
                return 0

            # ファイルサイズ確認（追加）
            file_size = os.path.getsize(subscriptions_file)
            if file_size == 0:
                logger.warning(f"❌ Push subscriptions file is empty")
                logger.info("No push subscriptions found")
                return 0

            with open(subscriptions_file, 'r') as f:
                subscriptions = json.load(f)
            logger.info(f"📁 Reading {len(subscriptions)} subscriptions")

            if not subscriptions:
                logger.info("No active push subscriptions")
                return 0

            if custom_notification_data:
                notification_data = custom_notification_data
            else:
                jst = timezone(timedelta(hours=9))
                current_time = datetime.now(jst)
                notification_data = {
                    "title": "朝の市況データ更新完了",
                    "body": f"{current_time.strftime('%H:%M')}の最新データが準備できました",
                    "type": "data-update"
                }

            notification_type = notification_data.get("type")

            sent_count = 0
            failed_subscriptions = []

            for sub_id, subscription in list(subscriptions.items()):
                permission = subscription.get("permission", "standard")

                # Determine whether to send the notification based on its type and user permission
                should_send = False

                if notification_type == "hwb-scan":
                    # For HWB scans, only send to 'secret' or 'ura' users
                    if permission in ["secret", "ura"]:
                        should_send = True
                elif notification_type == "algo-scan":
                    # For Algo scans, only send to 'ura' users
                    if permission == "ura":
                        should_send = True
                else:
                    # For all other notifications (e.g., data updates), send to everyone
                    should_send = True

                if not should_send:
                    logger.info(f"Skipping HWB notification for {sub_id} due to insufficient '{permission}' permission.")
                    continue

                # ✅ webpush用にクリーンなサブスクリプションオブジェクトを作成（permissionフィールドを除外）
                clean_subscription = {
                    "endpoint": subscription["endpoint"],
                    "keys": subscription["keys"]
                }
                if "expirationTime" in subscription and subscription["expirationTime"] is not None:
                    clean_subscription["expirationTime"] = subscription["expirationTime"]

                try:
                    webpush(
                        subscription_info=clean_subscription,  # ✅ クリーンなオブジェクトを使用
                        data=json.dumps(notification_data),
                        vapid_private_key=security_manager.vapid_private_key,
                        vapid_claims={"sub": security_manager.vapid_subject}
                    )
                    sent_count += 1
                    logger.debug(f"Notification sent to subscription {sub_id} with permission '{permission}'")
                except WebPushException as ex:
                    logger.error(f"Failed to send notification to {sub_id}: {ex}")
                    if ex.response and ex.response.status_code == 410:
                        failed_subscriptions.append(sub_id)
                except Exception as e:
                    logger.error(f"Unexpected error sending notification to {sub_id}: {e}")

            if failed_subscriptions:
                for sub_id in failed_subscriptions:
                    if sub_id in subscriptions:
                        del subscriptions[sub_id]
                with open(subscriptions_file, 'w') as f:
                    json.dump(subscriptions, f)
                logger.info(f"Removed {len(failed_subscriptions)} invalid subscriptions")

            # 権限別の内訳をロギング
            standard_count = sum(1 for s in subscriptions.values() if s.get('permission', 'standard') == 'standard')
            secret_count = sum(1 for s in subscriptions.values() if s.get('permission') == 'secret')
            ura_count = sum(1 for s in subscriptions.values() if s.get('permission') == 'ura')
            logger.info(f"Push notifications sent: {sent_count} | "
                        f"Standard: {standard_count}, Secret: {secret_count}, Ura: {ura_count}")

            return sent_count

        except Exception as e:
            logger.error(f"Error sending push notifications: {e}")
            return 0

    def generate_report_with_notification(self):
        """レポート生成とPush通知を一体化"""
        # 既存のレポート生成
        self.generate_report()

        # 成功したら通知を送信（失敗してもレポート生成は成功とする）
        try:
            if self.data.get('date'):
                sent_count = self.send_push_notifications()
                logger.info(f"Report generation complete. Notifications sent: {sent_count}")
            else:
                logger.warning("Report generated but no date found, skipping notifications")
        except Exception as e:
            logger.error(f"Failed to send notifications after report generation: {e}")
            # 通知失敗してもレポート生成は成功とする


if __name__ == '__main__':
    # For running the script directly, load .env file.
    from dotenv import load_dotenv
    load_dotenv()

    if os.path.basename(os.getcwd()) == 'backend':
        os.chdir('..')
    if len(sys.argv) > 1:
        fetcher = MarketDataFetcher()
        if sys.argv[1] == 'fetch':
            fetcher.fetch_all_data()
        elif sys.argv[1] == 'generate':
            # generateコマンドの場合は通知も送信
            fetcher.generate_report_with_notification()
        else:
            print("Usage: python backend/data_fetcher.py [fetch|generate]")
    else:
        print("Usage: python backend/data_fetcher.py [fetch|generate]")