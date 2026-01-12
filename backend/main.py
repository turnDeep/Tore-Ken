# This file will contain the FastAPI application.
import os
import json
import re
import traceback
import logging
from datetime import datetime, timedelta, timezone
from fastapi import Depends, FastAPI, HTTPException, Header, status, Response, Request, Cookie
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from jose import JWTError, jwt
from dotenv import load_dotenv
from pywebpush import webpush, WebPushException
from typing import Dict, Any, Optional

# Import security manager
from .security_manager import security_manager
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
    print("Tore-ken Security Configuration Initialized")
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
    print(f"DEBUG: Received PIN verification request. PIN: '{pin_data.pin}', Expected: '{AUTH_PIN}'")
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

@app.get("/api/market-analysis")
def get_market_analysis(current_user: str = Depends(get_current_user)):
    """Returns the market analysis chart data."""
    path = os.path.join(DATA_DIR, "market_analysis.json")
    if not os.path.exists(path):
         raise HTTPException(status_code=404, detail="Market analysis data not found.")
    with open(path, "r", encoding='utf-8') as f:
        return json.load(f)

@app.get("/api/market-chart.png")
def get_market_chart(current_user: str = Depends(get_current_user)):
    """Returns the market analysis chart image."""
    path = os.path.join(DATA_DIR, "market_chart.png")
    if not os.path.exists(path):
         raise HTTPException(status_code=404, detail="Chart image not found.")
    return FileResponse(path)

@app.get("/api/daily/{date_key}")
def get_daily_data(date_key: str, current_user: str = Depends(get_current_user)):
    """Returns the daily data (Strong Stocks, Status) for a specific date (YYYYMMDD)."""
    # Validation
    if not re.match(r'^\d{8}$', date_key):
         raise HTTPException(status_code=400, detail="Invalid date format.")

    path = os.path.join(DATA_DIR, f"{date_key}.json")
    if not os.path.exists(path):
         # If exact date not found, maybe return a default or just 404.
         # For the slider, we might hit days without data (weekends etc if not handled).
         # But the slider should be built from the chart history which has valid dates.
         raise HTTPException(status_code=404, detail="Data for this date not found.")
    with open(path, "r", encoding='utf-8') as f:
        return json.load(f)

@app.get("/api/data")
def get_latest_data(current_user: str = Depends(get_current_user)):
    """Endpoint to get the latest market data (latest.json)."""
    try:
        # Try latest.json first
        path = os.path.join(DATA_DIR, "latest.json")
        if os.path.exists(path):
            with open(path, "r", encoding='utf-8') as f:
                return json.load(f)

        # Fallback to old logic
        data_file = get_latest_data_file()
        if data_file is None or not os.path.exists(data_file):
            raise HTTPException(status_code=404, detail="Data file not found.")
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error reading latest market data: {e}")
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

# Mount the frontend directory to serve static files
# This must come AFTER all API routes
app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="static")