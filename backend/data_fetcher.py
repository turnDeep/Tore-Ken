import json
import logging
import logging.handlers
import os
import re
import sys
from datetime import datetime, timedelta, timezone
import math
import yfinance as yf
from curl_cffi.requests import Session
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

# Tickers
VIX_TICKER = "^VIX"
T_NOTE_TICKER = "^TNX"

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
        self.data = {"market": {}}

    def _clean_non_compliant_floats(self, obj):
        if isinstance(obj, dict):
            return {k: self._clean_non_compliant_floats(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._clean_non_compliant_floats(elem) for elem in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

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

        vix_val = self.data.get('market', {}).get('vix', {}).get('current', 'N/A')
        tnote_val = self.data.get('market', {}).get('t_note_future', {}).get('current', 'N/A')
        market_structure_str = f"Fear & Greed Index: {fg_now_val}, VIX指数: {vix_val}, 米国10年債金利: {tnote_val}%"

        # --- 2. Construct Prompt ---
        prompt = f"""
        あなたはプロの金融アナリストです。以下の提供データを基に、日本の個人投資家向けに市況レポートを作成してください。

        # 提供データ

        ## 1. 市場データ
        - Fear & Greed Index: 1ヶ月前 {fg_month_val}({fg_month_cat}), 1週間前 {fg_week_val}({fg_week_cat}), 現在 {fg_now_val}({fg_now_cat})
        - VIX指数推移(直近): {vix_history_str}
        - 米国10年債金利推移(直近): {t_note_history_str}
        - 市場の構図データ: {market_structure_str}

        # 作成指示

        以下のセクションのレポートを作成し、JSON形式で出力してください。

        ## A. market_commentary (市場センチメント解説)
        - 1ヶ月間の「推移」から読み取れるセンチメント変化を300字程度で解説。
        - Fear & Greed, VIX, 金利の動向と相互関連性を分析。
        - 現在の市場状況（リスクオン/オフなど）を結論付ける。

        # 出力形式
        必ず以下のJSON形式で出力してください。Markdownのコードブロックは含めないでください。

        {{
          "market_commentary": "..."
        }}
        """

        try:
            # Call Gemini
            response_json = self._call_gemini_api(prompt)

            # Distribute results
            self.data['market']['ai_commentary'] = response_json.get('market_commentary', '生成失敗')

        except Exception as e:
            logger.error(f"Unified generation failed: {e}")
            # Set error messages
            err_msg = "AI生成中にエラーが発生しました。"
            self.data['market']['ai_commentary'] = err_msg

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
            self.fetch_fear_greed_index
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
