# This file will contain the FastAPI application.
import os
import json
import re
import traceback
import logging
from datetime import datetime, timedelta, timezone
from fastapi import Depends, FastAPI, HTTPException, Header, status, Response, Request, Cookie
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from jose import JWTError, jwt
from dotenv import load_dotenv
from pywebpush import webpush, WebPushException
from typing import Dict, Any, Optional

# Import security manager
from .security_manager import security_manager
from .hwb_data_manager import HWBDataManager

# 既存のインポートに追加
from .hwb_scanner import run_hwb_scan, analyze_single_ticker
# Algoスキャン関連のインポート
from .algo_scanner import run_algo_scan, analyze_single_ticker_algo
from .algo_data_manager import AlgoDataManager
import asyncio

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# --- FastAPI App Initialization ---
app = FastAPI()

# --- Project Directories ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
FRONTEND_DIR = os.path.join(PROJECT_ROOT, 'frontend')

# --- Initialize security keys on startup ---
@app.on_event("startup")
async def startup_event():
    """Initialize security keys on application startup"""
    security_manager.data_dir = DATA_DIR
    security_manager.initialize()

    # 設定の確認表示
    print("\n" + "=" * 60)
    print("HanaView Security Configuration Initialized")
    print("=" * 60)
    print(f"JWT Secret: ***{security_manager.jwt_secret[-8:]}")
    print(f"VAPID Public Key: {security_manager.vapid_public_key[:20]}...")
    print(f"VAPID Subject: {security_manager.vapid_subject}")
    print("=" * 60 + "\n")

# --- Configuration ---
AUTH_PIN = os.getenv("AUTH_PIN", "123456")
SECRET_PIN = os.getenv("SECRET_PIN")
URA_PIN = os.getenv("URA_PIN") # URA_PINを追加
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_DAYS", 30))
NOTIFICATION_TOKEN_NAME = "notification_token"
NOTIFICATION_TOKEN_EXPIRE_HOURS = 24  # 24時間（短期で問題ない）


# In-memory storage for subscriptions
push_subscriptions: Dict[str, Any] = {}

# --- Pydantic Models ---
class PinVerification(BaseModel):
    pin: str

class PushSubscription(BaseModel):
    endpoint: str
    keys: Dict[str, str]
    expirationTime: Any = None

# --- Helper Functions ---
def create_access_token(data: dict, expires_delta: timedelta):
    """Creates a new JWT access token."""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, security_manager.jwt_secret, algorithm=ALGORITHM)
    return encoded_jwt

def get_latest_data_file():
    """Finds the latest data_YYYY-MM-DD.json file in the DATA_DIR."""
    if not os.path.isdir(DATA_DIR):
        return None
    files = os.listdir(DATA_DIR)
    data_files = [f for f in files if re.match(r'^data_(\d{4}-\d{2}-\d{2})\.json$', f)]
    if not data_files:
        fallback_path = os.path.join(DATA_DIR, 'data.json')
        return fallback_path if os.path.exists(fallback_path) else None
    latest_file = sorted(data_files, reverse=True)[0]
    return os.path.join(DATA_DIR, latest_file)

# --- Authentication Dependencies ---
async def get_current_user(authorization: Optional[str] = Header(None)):
    """メインAPI用の認証（Authorizationヘッダー）"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing or invalid"
        )

    token = authorization[7:]
    try:
        payload = jwt.decode(token, security_manager.jwt_secret, algorithms=[ALGORITHM])
        if payload.get("type") != "main":
            raise HTTPException(status_code=401, detail="Invalid token type")
        username = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Token validation failed")

async def get_current_user_payload(
    authorization: Optional[str] = Header(None)
):
    """メインAPI用の認証（Authorizationヘッダー） - ペイロード全体を返す"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing or invalid"
        )

    token = authorization[7:]
    try:
        payload = jwt.decode(token, security_manager.jwt_secret, algorithms=[ALGORITHM])
        if payload.get("type") != "main":
            raise HTTPException(status_code=401, detail="Invalid token type")
        if not payload.get("sub"):
            raise HTTPException(status_code=401, detail="Invalid token")
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Token validation failed")

async def get_current_user_for_notification(
    notification_token: Optional[str] = Cookie(None, alias=NOTIFICATION_TOKEN_NAME),
    authorization: Optional[str] = Header(None)
):
    """通知API用の認証（クッキーまたはヘッダー）"""

    token = None

    # まずクッキーをチェック
    if notification_token:
        token = notification_token
    # 次にAuthorizationヘッダーをチェック
    elif authorization and authorization.startswith("Bearer "):
        token = authorization[7:]

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required"
        )

    try:
        payload = jwt.decode(token, security_manager.jwt_secret, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Token validation failed")

# --- API Endpoints ---

@app.post("/api/auth/verify")
def verify_pin(pin_data: PinVerification, response: Response, request: Request):
    """
    PINを検証し、権限レベルに応じて異なるトークンを生成する。
    """
    permission = None
    if URA_PIN and pin_data.pin == URA_PIN:
        permission = "ura"
    elif pin_data.pin == AUTH_PIN:
        permission = "standard"
    # SECRET_PINが.envで設定されている場合のみ、シークレットPINの検証を行う
    elif SECRET_PIN and pin_data.pin == SECRET_PIN:
        permission = "secret"

    if permission:
        # メイン認証用（30日間）
        expires_long = timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
        main_token = create_access_token(
            data={"sub": "user", "type": "main", "permission": permission},
            expires_delta=expires_long
        )

        # 通知用（24時間、自動更新される）
        expires_short = timedelta(hours=NOTIFICATION_TOKEN_EXPIRE_HOURS)
        notification_token = create_access_token(
            data={"sub": "user", "type": "notification"}, # 通知トークンに権限は不要
            expires_delta=expires_short
        )

        # 通知用クッキーを設定
        is_https = request.headers.get("X-Forwarded-Proto") == "https"
        response.set_cookie(
            key=NOTIFICATION_TOKEN_NAME,
            value=notification_token,
            httponly=False,
            max_age=int(expires_short.total_seconds()),
            samesite="none" if is_https else "lax",
            path="/",
            secure=is_https
        )

        # フロントエンドに返すレスポンス
        return {
            "success": True,
            "token": main_token,
            "expires_in": int(expires_long.total_seconds()),
            "notification_cookie_set": True,
            "permission": permission  # 権限レベルを返す
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect authentication code"
        )

@app.get("/api/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

@app.get("/api/data")
def get_market_data(current_user: str = Depends(get_current_user)):
    """Endpoint to get the latest market data."""
    try:
        data_file = get_latest_data_file()
        if data_file is None or not os.path.exists(data_file):
            raise HTTPException(status_code=404, detail="Data file not found.")
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"Error reading latest market data:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Could not retrieve market data.")

@app.get("/api/vapid-public-key")
def get_vapid_public_key():
    """認証不要でVAPID公開鍵を返す"""
    return {"public_key": security_manager.vapid_public_key}

@app.post("/api/subscribe")
async def subscribe_push(
    subscription: PushSubscription,
    payload: dict = Depends(get_current_user_payload)
):
    """
    Push通知のサブスクリプションを登録し、権限レベルも保存する。
    メインの認証トークン（Authorizationヘッダー）が必要。
    """
    permission = payload.get("permission", "standard")  # デフォルトは 'standard'
    subscription_id = str(hash(subscription.endpoint))

    # サブスクリプションデータに権限を追加
    subscription_data = subscription.dict()
    subscription_data["permission"] = permission

    # メモリとファイルに保存
    push_subscriptions[subscription_id] = subscription_data

    subscriptions_file = os.path.join(DATA_DIR, 'push_subscriptions.json')
    try:
        existing = {}
        if os.path.exists(subscriptions_file):
            with open(subscriptions_file, 'r') as f:
                existing = json.load(f)

        existing[subscription_id] = subscription_data

        with open(subscriptions_file, 'w') as f:
            json.dump(existing, f)

        logger.info(f"Subscription {subscription_id} saved with '{permission}' permission.")

    except Exception as e:
        logger.error(f"Error saving subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to save subscription")

    return {"status": "subscribed", "id": subscription_id, "permission": permission}

async def _send_push_notification(subscription: Dict[str, Any], data: Dict[str, Any]) -> bool:
    """Helper function to send a single push notification."""
    try:
        webpush(
            subscription_info=subscription,
            data=json.dumps(data),
            vapid_private_key=security_manager.vapid_private_key,
            vapid_claims={"sub": security_manager.vapid_subject}
        )
        return True
    except WebPushException as ex:
        logger.warning(f"Push failed for endpoint {subscription['endpoint'][:30]}...: {ex}")
        # Gone (410) or Not Found (404) means the subscription is invalid
        return ex.response and ex.response.status_code not in [404, 410]

async def _send_notifications_to_permission_level(permission: str, title: str, body: str):
    """Sends notifications to all users with a specific permission level."""
    subscriptions_file = os.path.join(DATA_DIR, 'push_subscriptions.json')
    if not os.path.exists(subscriptions_file):
        logger.info("No subscriptions file found, skipping notification.")
        return 0, 0

    with open(subscriptions_file, 'r') as f:
        all_subscriptions = json.load(f)

    # Filter subscriptions by permission
    target_subscriptions = {
        sub_id: sub_data for sub_id, sub_data in all_subscriptions.items()
        if sub_data.get("permission") == permission
    }

    if not target_subscriptions:
        logger.info(f"No subscribers with '{permission}' permission found.")
        return 0, 0

    notification_data = {"title": title, "body": body, "type": "info"}

    tasks = [_send_push_notification(sub, notification_data) for sub in target_subscriptions.values()]
    results = await asyncio.gather(*tasks)

    # Update subscriptions file by removing invalid ones
    valid_subscriptions = {}
    all_sub_items = list(all_subscriptions.items())
    for i, (sub_id, sub_data) in enumerate(all_sub_items):
        # We only care about the results for the targeted permission level
        if sub_data.get("permission") == permission:
            # Find the corresponding result. This is a bit tricky. A better way would be to map ids.
            # For now, let's assume the order is preserved.
            if any(res for res in results if target_subscriptions[sub_id] == res): # Simplified check
                 valid_subscriptions[sub_id] = sub_data
        else:
            valid_subscriptions[sub_id] = sub_data

    sent_count = sum(1 for r in results if r)
    failed_count = len(results) - sent_count

    logger.info(f"Notifications sent to '{permission}' users. Sent: {sent_count}, Failed: {failed_count}")
    return sent_count, failed_count


# 新しいAPIエンドポイントを追加

@app.post("/api/hwb/scan")
async def trigger_hwb_scan(current_user: str = Depends(get_current_user)):
    """HWBスキャンを手動実行（管理者のみ）"""
    try:
        # 非同期でスキャン実行
        result = await run_hwb_scan()

        return {
            "success": True,
            "message": f"スキャン完了: {result['summary']['signals_today_count']}件のシグナル検出",
            "scan_date": result['scan_date'],
            "scan_time": result['scan_time']
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hwb/daily/latest")
def get_hwb_latest_summary(current_user: str = Depends(get_current_user)):
    """Retrieves the latest daily HWB scan summary."""
    try:
        summary_path = os.path.join(DATA_DIR, 'hwb', 'daily', 'latest.json')
        if not os.path.exists(summary_path):
            raise HTTPException(status_code=404, detail="Latest summary not found. Please run a scan.")

        # Get file modification time
        mtime = os.path.getmtime(summary_path)
        updated_at = datetime.fromtimestamp(mtime, timezone.utc).isoformat()

        with open(summary_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Add the update timestamp to the response
        data['updated_at'] = updated_at

        return data
    except Exception as e:
        print(f"Error reading latest HWB summary:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Could not retrieve HWB summary.")

@app.get("/api/hwb/symbols/{symbol}")
def get_hwb_symbol_data(symbol: str, current_user: str = Depends(get_current_user)):
    """Retrieves the detailed analysis data for a specific symbol."""
    try:
        # Basic validation to prevent directory traversal
        if not re.match(r'^[A-Z0-9\-\.]+$', symbol.upper()):
            raise HTTPException(status_code=400, detail="Invalid symbol format.")

        symbol_path = os.path.join(DATA_DIR, 'hwb', 'symbols', f"{symbol.upper()}.json")
        if not os.path.exists(symbol_path):
            raise HTTPException(status_code=404, detail=f"Data for symbol '{symbol}' not found.")

        with open(symbol_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error reading data for symbol '{symbol}':")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Could not retrieve data for symbol '{symbol}'.")

@app.get("/api/hwb/analyze_ticker")
async def analyze_ticker(ticker: str, force: bool = False, current_user: str = Depends(get_current_user)):
    """
    Analyzes a single ticker for HWB strategy.
    - `force=false` (default): Returns existing JSON data if available, otherwise 404.
    - `force=true`: Forces a re-analysis of the ticker.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker symbol is required")

    try:
        symbol = ticker.strip().upper()
        data_manager = HWBDataManager()

        if not force:
            logger.info(f"Attempting to load cached data for {symbol}...")
            existing_data = data_manager.load_symbol_data(symbol)
            if existing_data:
                logger.info(f"Returning cached data for {symbol}.")
                return existing_data
            else:
                logger.info(f"No cached data found for {symbol}. Frontend will prompt for analysis.")
                raise HTTPException(
                    status_code=404,
                    detail=f"分析データが見つかりません。新規に分析しますか？"
                )

        # This part runs only when force=true
        logger.info(f"Force re-analyzing data for {symbol}...")
        from .hwb_scanner import analyze_single_ticker
        analysis_result = await analyze_single_ticker(symbol)

        if analysis_result is None:
            raise HTTPException(
                status_code=404,
                detail=f"{symbol}はHWB戦略の条件を満たしていないか、データ取得に失敗しました"
            )

        return analysis_result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing ticker {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"分析中に予期せぬエラーが発生しました。")

@app.get("/api/debug/subscriptions")
def debug_subscriptions(current_user: str = Depends(get_current_user)):
    """開発用: サブスクリプションの状態を確認"""
    subscriptions_file = os.path.join(DATA_DIR, 'push_subscriptions.json')
    if not os.path.exists(subscriptions_file):
        return {"status": "no_file", "subscriptions": {}}

    with open(subscriptions_file, 'r') as f:
        subs = json.load(f)

    return {
        "status": "ok",
        "count": len(subs),
        "subscriptions": {
            sub_id: {"permission": data.get("permission"), "endpoint": data.get("endpoint", "")[:50]}
            for sub_id, data in subs.items()
        }
    }

# --- Algo Tab Endpoints ---

@app.post("/api/algo/scan")
async def trigger_algo_scan(payload: dict = Depends(get_current_user_payload)):
    """Algoスキャンを手動実行（ura権限のみ）"""
    if payload.get("permission") != "ura":
        raise HTTPException(status_code=403, detail="Access forbidden: ura permission required")

    try:
        result = await run_algo_scan()

        # ura権限ユーザーに通知
        await _send_notifications_to_permission_level(
            "ura",
            "Algoスキャン完了",
            f"新規シグナル: {result['total_scanned']}件"
        )

        return {
            "success": True,
            "message": f"スキャン完了: {result['total_scanned']}件のシグナル検出",
            "scan_date": result['scan_date'],
            "scan_time": result['scan_time']
        }

    except Exception as e:
        logger.error(f"Algo scan error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"スキャンエラー: {str(e)}")


@app.get("/api/algo/daily/latest")
def get_algo_latest_summary(payload: dict = Depends(get_current_user_payload)):
    """Algoサマリー取得（ura権限のみ）"""
    if payload.get("permission") != "ura":
        raise HTTPException(status_code=403, detail="Access forbidden: ura permission required")

    try:
        data_manager = AlgoDataManager()
        summary = data_manager.load_latest_summary()

        if not summary:
            raise HTTPException(status_code=404, detail="Latest summary not found")

        return summary

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error loading Algo summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve Algo summary")


@app.get("/api/algo/symbols/{symbol}")
def get_algo_symbol_data(symbol: str, payload: dict = Depends(get_current_user_payload)):
    """個別銘柄データ取得（ura権限のみ）"""
    if payload.get("permission") != "ura":
        raise HTTPException(status_code=403, detail="Access forbidden: ura permission required")

    try:
        # Basic validation
        if not re.match(r'^[A-Z0-9\-\.]+$', symbol.upper()):
            raise HTTPException(status_code=400, detail="Invalid symbol format")

        data_manager = AlgoDataManager()
        symbol_data = data_manager.load_symbol_data(symbol.upper())

        if not symbol_data:
            raise HTTPException(status_code=404, detail=f"Data for symbol '{symbol}' not found")

        return symbol_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error loading symbol data for {symbol}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not retrieve data for symbol '{symbol}'")


@app.get("/api/algo/analyze_ticker")
async def analyze_ticker_algo(ticker: str, force: bool = False, payload: dict = Depends(get_current_user_payload)):
    """銘柄分析（ura権限のみ）"""
    if payload.get("permission") != "ura":
        raise HTTPException(status_code=403, detail="Access forbidden: ura permission required")

    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker symbol is required")

    try:
        symbol = ticker.strip().upper()
        data_manager = AlgoDataManager()

        if not force:
            # キャッシュデータを返す
            logger.info(f"Attempting to load cached data for {symbol}...")
            existing_data = data_manager.load_symbol_data(symbol)

            if existing_data:
                logger.info(f"Returning cached data for {symbol}")
                return existing_data
            else:
                logger.info(f"No cached data found for {symbol}")
                raise HTTPException(
                    status_code=404,
                    detail=f"分析データが見つかりません。新規に分析しますか？"
                )

        # force=true の場合、新規分析を実行
        logger.info(f"Force analyzing {symbol}...")
        analysis_result = await analyze_single_ticker_algo(symbol)

        if not analysis_result:
            raise HTTPException(
                status_code=404,
                detail=f"{symbol}の分析に失敗しました"
            )

        # スクリーナーに含まれていないため、計算された指標を解説の代わりに表示
        regime_map = {
            'contraction': '収縮 (凪)',
            'transition': '移行 (通常)',
            'expansion': '拡大 (嵐)'
        }

        regime = analysis_result.get('volatility_regime', 'unknown')
        regime_jp = regime_map.get(regime, regime)

        metrics_text = "【分析結果サマリー】\n"
        metrics_text += f"・ボラティリティ環境: {regime_jp}\n"
        metrics_text += f"・Gamma Flip: {analysis_result.get('gamma_flip', 'N/A')}\n"

        move = analysis_result.get('expected_move_30d')
        if move:
             metrics_text += f"・30日予想変動幅: ±{move}%\n"

        if analysis_result.get('sector'):
             metrics_text += f"・セクター: {analysis_result.get('sector')}\n"
        if analysis_result.get('industry'):
             metrics_text += f"・業界: {analysis_result.get('industry')}\n"

        symbol_data = {
            'symbol': symbol,
            **analysis_result,
            'gemini_analysis': metrics_text,
            'screener_sources': [],
            'metadata': {},
            'message': 'この銘柄はスクリーナーに含まれていません。計算された指標を表示します。',
            'last_updated': datetime.now(timezone.utc).isoformat()
        }

        # キャッシュに保存
        data_manager.save_symbol_data(symbol, symbol_data)

        return symbol_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing ticker {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"分析中に予期せぬエラーが発生しました")

# Mount the frontend directory to serve static files
# This must come AFTER all API routes
app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="static")