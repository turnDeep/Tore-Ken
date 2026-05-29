from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from backend.summary_style import compose_seven_layer_summary

try:
    from numba import njit
except Exception:  # pragma: no cover - optional acceleration
    njit = None


load_dotenv()

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
PRICE_DATA_PATH = DATA_DIR / "price_data_ohlcv.pkl"
PROFILE_CACHE_PATH = DATA_DIR / "recognition_gap_profiles.json"
FUNDAMENTAL_CACHE_PATH = DATA_DIR / "recognition_gap_fundamentals.json"
NEWS_CACHE_PATH = DATA_DIR / "recognition_gap_news.json"
ESTIMATE_CACHE_PATH = DATA_DIR / "recognition_gap_estimates.json"
RANKING_JSON_PATH = DATA_DIR / "recognition_gap_ranking.json"
RANKING_CSV_PATH = DATA_DIR / "recognition_gap_ranking.csv"

def parse_top_n(value: Any = None) -> int | None:
    """Return None for all rows; positive int for an explicit cap."""
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"", "0", "all", "none", "unlimited"}:
        return None
    number = int(text)
    return number if number > 0 else None


DEFAULT_TOP_N = parse_top_n(os.getenv("RECOGNITION_GAP_TOP_N", "0"))
MIN_CLOSE = float(os.getenv("RECOGNITION_GAP_MIN_CLOSE", "1.5"))
MIN_DOLLAR_VOLUME20 = float(os.getenv("RECOGNITION_GAP_MIN_DOLLAR_VOLUME20", "1000000"))
MAX_PROFILE_FETCH = int(os.getenv("RECOGNITION_GAP_PROFILE_FETCH_LIMIT", "350"))
MAX_FUNDAMENTAL_FETCH = int(os.getenv("RECOGNITION_GAP_FUNDAMENTAL_FETCH_LIMIT", "150"))
MAX_NEWS_FETCH = int(os.getenv("RECOGNITION_GAP_NEWS_FETCH_LIMIT", "150"))
MAX_ESTIMATE_FETCH = int(os.getenv("RECOGNITION_GAP_ESTIMATE_FETCH_LIMIT", "150"))
NEWS_PRE_SIGNAL_DAYS = int(os.getenv("RECOGNITION_GAP_NEWS_PRE_SIGNAL_DAYS", "60"))
NEWS_RECENT_DAYS = int(os.getenv("RECOGNITION_GAP_NEWS_RECENT_DAYS", "14"))
NEWS_LIMIT_PER_SYMBOL = int(os.getenv("RECOGNITION_GAP_NEWS_LIMIT_PER_SYMBOL", "100"))

BIOTECH_TERMS = (
    "biotechnology",
    "biotech",
    "drug manufacturers",
    "pharmaceutical",
    "pharmaceuticals",
    "clinical",
    "therapeutics",
)

STRUCTURAL_INDUSTRY_TERMS = (
    "semiconductor",
    "electronic",
    "communications",
    "telecom",
    "aerospace",
    "defense",
    "satellite",
    "software",
    "computer hardware",
    "data center",
    "electrical equipment",
    "specialty industrial",
    "metal",
    "uranium",
    "solar",
    "oil",
    "gas",
    "energy",
)


if njit:

    @njit(cache=True)
    def _true_range_numba(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
        out = np.empty(high.shape[0], dtype=np.float64)
        prev_close = close[0]
        for i in range(high.shape[0]):
            a = high[i] - low[i]
            b = abs(high[i] - prev_close)
            c = abs(low[i] - prev_close)
            out[i] = max(a, b, c)
            prev_close = close[i]
        return out

else:

    def _true_range_numba(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
        prev_close = np.roll(close, 1)
        prev_close[0] = close[0]
        return np.maximum.reduce([high - low, np.abs(high - prev_close), np.abs(low - prev_close)])


@dataclass
class RecognitionGapRow:
    rank: int
    symbol: str
    company: str
    entry_date: str
    signal_date: str
    asof_date: str
    asof_close: float
    entry_price: float
    return_since_entry: float
    sector: str
    industry: str
    country: str
    market_cap: float | None
    adr_or_non_us: bool
    revenue_yoy: float | None
    revenue_qoq: float | None
    revenue_yoy_prev: float | None
    ttm_revenue: float | None
    ttm_eps: float | None
    latest_eps: float | None
    eps_yoy: float | None
    eps_qoq: float | None
    eps_yoy_prev: float | None
    eps_qoq_prev: float | None
    news_pre_60d_titles: str
    news_ep_to_asof_titles: str
    news_recent_titles: str
    news_evidence_ja: str
    backlog_signal: bool
    contract_signal: bool
    order_signal: bool
    institutional_signal: bool
    warning_signal: bool
    guidance_signal: bool
    guidance_positive_signal: bool
    next_quarter_revenue_est: float | None
    next_quarter_eps_est: float | None
    current_year_revenue_est: float | None
    current_year_eps_est: float | None
    next_year_revenue_est: float | None
    next_year_eps_est: float | None
    next_quarter_revenue_growth_est: float | None
    next_quarter_eps_growth_est: float | None
    current_year_revenue_growth_est: float | None
    current_year_eps_growth_est: float | None
    next_year_revenue_growth_est: float | None
    next_year_eps_growth_est: float | None
    estimate_snapshot_date: str
    data_integrity: str
    price_trend: str
    volume_demand_durability: str
    institutional_and_supply: str
    supply_risk_severity: str
    catalyst_quality: str
    fundamental_confirmation: str
    thesis_state: str
    thesis_substate: str
    daily_flags: list[str]
    risk_notes_ja: str
    seven_layer_summary_ja: str
    recommendation_priority: int
    ret20: float
    ret60: float
    ret126: float
    ret252: float
    ret60_resid_spy: float
    volume_ratio20: float
    avg_dollar_volume20: float
    post_signal_dv_persistence: float
    up_down_volume_ratio_20d: float
    atr14_pct: float


def _safe_float(value: Any, default: float = math.nan) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _pct(value: float | None) -> str:
    if value is None or not np.isfinite(value):
        return "-"
    return f"{value * 100:+.1f}%"


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _trim(text: str, max_chars: int = 300) -> str:
    text = " ".join(_clean_text(text).split())
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip("、。,. ") + "…"


def _load_price_data(path: Path = PRICE_DATA_PATH) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"price data not found: {path}")
    data = pd.read_pickle(path)
    if not isinstance(data.columns, pd.MultiIndex):
        raise ValueError("price_data_ohlcv.pkl must use MultiIndex columns like ('Close', 'AAPL').")
    return data.sort_index()


def _field(data: pd.DataFrame, field: str) -> pd.DataFrame:
    if field not in data.columns.get_level_values(0):
        return pd.DataFrame(index=data.index)
    return data[field].copy()


def _load_stock_csv_profiles() -> dict[str, dict[str, Any]]:
    path = PROJECT_ROOT / "stock.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    if "Ticker" in df.columns and "Symbol" not in df.columns:
        df = df.rename(columns={"Ticker": "Symbol"})
    profiles: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        symbol = _clean_text(row.get("Symbol")).upper()
        if not symbol:
            continue
        profiles[symbol] = {
            "symbol": symbol,
            "companyName": _clean_text(row.get("CompanyName") or row.get("companyName")),
            "sector": _clean_text(row.get("Sector") or row.get("sector")),
            "industry": _clean_text(row.get("Industry") or row.get("industry")),
            "country": _clean_text(row.get("Country") or row.get("country")),
            "mktCap": _safe_float(row.get("MarketCap") or row.get("marketCap"), math.nan),
            "exchange": _clean_text(row.get("Exchange") or row.get("exchange")),
        }
    return profiles


def _load_profile_cache() -> dict[str, dict[str, Any]]:
    profiles = _load_stock_csv_profiles()
    if PROFILE_CACHE_PATH.exists():
        try:
            cached = json.loads(PROFILE_CACHE_PATH.read_text(encoding="utf-8"))
            for key, value in cached.items():
                if isinstance(value, dict):
                    profiles[key.upper()] = {**profiles.get(key.upper(), {}), **value}
        except Exception as exc:
            logger.warning("failed to read profile cache: %s", exc)
    return profiles


def _save_profile_cache(profiles: dict[str, dict[str, Any]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROFILE_CACHE_PATH.write_text(json.dumps(profiles, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_fundamental_cache() -> dict[str, dict[str, Any]]:
    if not FUNDAMENTAL_CACHE_PATH.exists():
        return {}
    try:
        cached = json.loads(FUNDAMENTAL_CACHE_PATH.read_text(encoding="utf-8"))
        return {key.upper(): value for key, value in cached.items() if isinstance(value, dict)}
    except Exception as exc:
        logger.warning("failed to read fundamental cache: %s", exc)
        return {}


def _save_fundamental_cache(cache: dict[str, dict[str, Any]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FUNDAMENTAL_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_news_cache() -> dict[str, dict[str, Any]]:
    if not NEWS_CACHE_PATH.exists():
        return {}
    try:
        cached = json.loads(NEWS_CACHE_PATH.read_text(encoding="utf-8"))
        return {key.upper(): value for key, value in cached.items() if isinstance(value, dict)}
    except Exception as exc:
        logger.warning("failed to read news cache: %s", exc)
        return {}


def _save_news_cache(cache: dict[str, dict[str, Any]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    NEWS_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_estimate_cache() -> dict[str, dict[str, Any]]:
    if not ESTIMATE_CACHE_PATH.exists():
        return {}
    try:
        cached = json.loads(ESTIMATE_CACHE_PATH.read_text(encoding="utf-8"))
        return {key.upper(): value for key, value in cached.items() if isinstance(value, dict)}
    except Exception as exc:
        logger.warning("failed to read estimate cache: %s", exc)
        return {}


def _save_estimate_cache(cache: dict[str, dict[str, Any]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ESTIMATE_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_date(value: Any) -> pd.Timestamp | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        ts = pd.Timestamp(text)
        if ts.tzinfo is not None:
            ts = ts.tz_convert(None)
        return ts.normalize()
    except Exception:
        return None


def _statement_available(item: dict[str, Any], asof_ts: pd.Timestamp | None) -> bool:
    if asof_ts is None:
        return True
    asof_ts = asof_ts.normalize()
    filing = _parse_date(item.get("acceptedDate") or item.get("fillingDate") or item.get("filingDate"))
    if filing is not None:
        return filing <= asof_ts
    period_end = _parse_date(item.get("date"))
    return bool(period_end is not None and period_end <= asof_ts)


def _growth(current: Any, base: Any) -> float:
    current_value = _safe_float(current, math.nan)
    base_value = _safe_float(base, math.nan)
    if not np.isfinite(current_value) or not np.isfinite(base_value) or base_value == 0:
        return math.nan
    return current_value / abs(base_value) - 1


def _compute_fundamentals(items: list[dict[str, Any]], asof_ts: pd.Timestamp | None = None) -> dict[str, float | None]:
    usable = [item for item in items if isinstance(item, dict) and _statement_available(item, asof_ts)]
    usable.sort(key=lambda item: _parse_date(item.get("date")) or pd.Timestamp.min, reverse=True)
    if not usable:
        return {}
    latest = usable[0]
    prev_q = usable[1] if len(usable) > 1 else {}
    prev_prev_q = usable[2] if len(usable) > 2 else {}
    year_ago = usable[4] if len(usable) > 4 else {}
    prev_year_ago = usable[5] if len(usable) > 5 else {}
    latest_eps = _safe_float(latest.get("eps") or latest.get("epsdiluted"), math.nan)
    prev_eps = _safe_float(prev_q.get("eps") or prev_q.get("epsdiluted"), math.nan)
    prev_prev_eps = _safe_float(prev_prev_q.get("eps") or prev_prev_q.get("epsdiluted"), math.nan)
    year_ago_eps = _safe_float(year_ago.get("eps") or year_ago.get("epsdiluted"), math.nan)
    prev_year_ago_eps = _safe_float(prev_year_ago.get("eps") or prev_year_ago.get("epsdiluted"), math.nan)
    ttm_revenue = sum(_safe_float(item.get("revenue"), math.nan) for item in usable[:4])
    ttm_eps_values = [_safe_float(item.get("eps") or item.get("epsdiluted"), math.nan) for item in usable[:4]]
    ttm_eps = sum(ttm_eps_values) if all(np.isfinite(value) for value in ttm_eps_values) else math.nan

    return {
        "latest_revenue": _safe_float(latest.get("revenue"), math.nan),
        "revenue_yoy": _growth(latest.get("revenue"), year_ago.get("revenue")),
        "revenue_qoq": _growth(latest.get("revenue"), prev_q.get("revenue")),
        "revenue_yoy_prev": _growth(prev_q.get("revenue"), prev_year_ago.get("revenue")),
        "ttm_revenue": ttm_revenue if np.isfinite(ttm_revenue) and ttm_revenue > 0 else None,
        "ttm_eps": ttm_eps if np.isfinite(ttm_eps) else None,
        "latest_eps": latest_eps if np.isfinite(latest_eps) else None,
        "eps_yoy": _growth(latest_eps, year_ago_eps),
        "eps_qoq": _growth(latest_eps, prev_eps),
        "eps_yoy_prev": _growth(prev_eps, prev_year_ago_eps),
        "eps_qoq_prev": _growth(prev_eps, prev_prev_eps),
    }


def _news_item_date(item: dict[str, Any]) -> pd.Timestamp | None:
    return _parse_date(item.get("publishedDate") or item.get("date"))


def _normalize_news_item(item: dict[str, Any], source_type: str) -> dict[str, str]:
    return {
        "symbol": _clean_text(item.get("symbol")).upper(),
        "date": _clean_text(item.get("publishedDate") or item.get("date")),
        "title": _clean_text(item.get("title")),
        "text": _clean_text(item.get("text") or item.get("content") or item.get("site")),
        "source_type": source_type,
        "url": _clean_text(item.get("url") or item.get("link")),
    }


def _load_symbol_news(
    symbol: str,
    cache: dict[str, dict[str, Any]],
    fetch_state: dict[str, int],
    from_date: pd.Timestamp,
    to_date: pd.Timestamp,
) -> list[dict[str, str]]:
    symbol = symbol.upper()
    from_text = from_date.strftime("%Y-%m-%d")
    to_text = to_date.strftime("%Y-%m-%d")
    cache_key = f"{symbol}|{from_text}|{to_text}"
    cached = cache.get(cache_key)
    if cached and isinstance(cached.get("items"), list):
        return cached["items"]

    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key or api_key.lower().startswith("your_"):
        return []
    if fetch_state.get("count", 0) >= MAX_NEWS_FETCH:
        return []

    try:
        import requests
    except Exception:
        logger.warning("requests is not installed; skipping FMP news enrichment")
        return []

    items: list[dict[str, str]] = []
    endpoints = [
        (
            "https://financialmodelingprep.com/api/v3/stock_news",
            {"tickers": symbol, "from": from_text, "to": to_text, "limit": NEWS_LIMIT_PER_SYMBOL, "apikey": api_key},
            "news",
        ),
        (
            f"https://financialmodelingprep.com/api/v3/press-releases/{symbol}",
            {"limit": NEWS_LIMIT_PER_SYMBOL, "apikey": api_key},
            "press_release",
        ),
    ]
    for url, params, source_type in endpoints:
        if fetch_state.get("count", 0) >= MAX_NEWS_FETCH:
            break
        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            fetch_state["count"] = fetch_state.get("count", 0) + 1
            payload = response.json()
            if not isinstance(payload, list):
                continue
            for raw in payload:
                if not isinstance(raw, dict):
                    continue
                event = _normalize_news_item(raw, source_type)
                event_date = _news_item_date(event)
                if event_date is None or event_date < from_date.normalize() or event_date > to_date.normalize():
                    continue
                if event["symbol"] and event["symbol"] != symbol:
                    continue
                if event["title"]:
                    items.append(event)
        except Exception as exc:
            logger.warning("FMP news fetch failed for %s via %s: %s", symbol, source_type, exc)

    # Deduplicate by title/date/source while preserving recency order.
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, str]] = []
    for item in sorted(items, key=lambda x: _news_item_date(x) or pd.Timestamp.min, reverse=True):
        key = (item.get("date", "")[:10], item.get("title", "").lower(), item.get("source_type", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    cache[cache_key] = {"fetched_at": datetime.now().isoformat(), "items": deduped}
    return deduped


def _join_titles(events: list[dict[str, str]], limit: int = 5) -> str:
    return " | ".join(event["title"] for event in events[:limit] if event.get("title"))


def _extract_amount(text: str, keyword: str = "") -> str:
    pattern = r"\$([0-9]+(?:\.[0-9]+)?)\s*(million|billion)"
    match = None
    if keyword:
        for candidate in re.finditer(pattern, text, flags=re.IGNORECASE):
            start = max(0, candidate.start() - 100)
            end = min(len(text), candidate.end() + 120)
            if keyword.lower() in text[start:end].lower():
                match = candidate
                break
        if match is None:
            return ""
    if match is None:
        match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return ""
    unit = "億ドル" if match.group(2).lower() == "billion" else "百万ドル"
    return f"{match.group(1)}{unit}"


def _classify_news(events: list[dict[str, str]], signal_date: pd.Timestamp, asof_ts: pd.Timestamp) -> dict[str, Any]:
    recent_start = asof_ts.normalize() - pd.Timedelta(days=NEWS_RECENT_DAYS)
    pre_start = signal_date.normalize() - pd.Timedelta(days=NEWS_PRE_SIGNAL_DAYS)
    pre_events: list[dict[str, str]] = []
    post_events: list[dict[str, str]] = []
    recent_events: list[dict[str, str]] = []
    for event in events:
        event_date = _news_item_date(event)
        if event_date is None:
            continue
        event_date = event_date.normalize()
        if pre_start <= event_date < signal_date.normalize():
            pre_events.append(event)
        if signal_date.normalize() <= event_date <= asof_ts.normalize():
            post_events.append(event)
        if recent_start <= event_date <= asof_ts.normalize():
            recent_events.append(event)

    text = " | ".join(f"{event.get('title', '')} {event.get('text', '')}" for event in post_events + recent_events).lower()
    backlog = "backlog" in text or "受注残" in text
    contract = any(term in text for term in ("contract", "award", "task order", "agreement", "契約"))
    order = any(term in text for term in ("order", "orders", "booking", "bookings", "book-to-bill", "受注"))
    institution = any(term in text for term in ("fund disclosed", "institutional", "13f", "stake", "shares worth", "大量保有"))
    warning = any(term in text for term in ("red flag", "class action", "lawsuit", "investigation", "overvalued"))
    guidance = any(term in text for term in ("guidance", "outlook", "forecast", "expects", "full-year", "next quarter", "見通し", "予想"))
    guidance_positive = any(
        term in text
        for term in (
            "raises guidance",
            "raised guidance",
            "increase guidance",
            "increased guidance",
            "above consensus",
            "boosts outlook",
            "raises outlook",
            "strong outlook",
            "upbeat outlook",
            "上方修正",
        )
    )

    evidence: list[str] = []
    if backlog:
        amount = _extract_amount(text, "backlog")
        evidence.append(f"受注残{amount}" if amount else "受注残")
    if contract:
        amount = _extract_amount(text, "contract") or _extract_amount(text, "agreement") or _extract_amount(text)
        evidence.append(f"契約{amount}" if amount else "契約・受注")
    if order:
        evidence.append("受注/ブッキング")
    if institution:
        evidence.append("機関投資家買い")
    if warning:
        evidence.append("警戒見出し")
    if guidance_positive:
        evidence.append("会社見通し上振れ")
    elif guidance:
        evidence.append("会社見通し")

    return {
        "pre_events": pre_events,
        "post_events": post_events,
        "recent_events": recent_events,
        "news_evidence_ja": "ニュース・開示: " + "、".join(dict.fromkeys(evidence)) if evidence else "",
        "backlog_signal": backlog,
        "contract_signal": contract,
        "order_signal": order,
        "institutional_signal": institution,
        "warning_signal": warning,
        "guidance_signal": guidance,
        "guidance_positive_signal": guidance_positive,
    }


def _fetch_fundamentals(
    symbol: str,
    cache: dict[str, dict[str, Any]],
    fetch_state: dict[str, int],
    asof_ts: pd.Timestamp | None = None,
) -> dict[str, float | None]:
    cached = cache.get(symbol.upper())
    if cached and isinstance(cached.get("income_statement_quarterly"), list):
        return _compute_fundamentals(cached["income_statement_quarterly"], asof_ts)

    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key or api_key.lower().startswith("your_"):
        return {}
    if fetch_state.get("count", 0) >= MAX_FUNDAMENTAL_FETCH:
        return {}

    try:
        import requests
    except Exception:
        logger.warning("requests is not installed; skipping FMP fundamental enrichment")
        return {}

    url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}"
    try:
        response = requests.get(url, params={"period": "quarter", "limit": 12, "apikey": api_key}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        fetch_state["count"] = fetch_state.get("count", 0) + 1
        if not isinstance(payload, list):
            return {}
        cache[symbol.upper()] = {
            "fetched_at": datetime.now().isoformat(),
            "income_statement_quarterly": payload,
        }
        return _compute_fundamentals(payload, asof_ts)
    except Exception as exc:
        logger.warning("FMP fundamental fetch failed for %s: %s", symbol, exc)
        return {}


def _growth_from_estimate(estimate: Any, base: Any) -> float:
    est = _safe_float(estimate, math.nan)
    base_value = _safe_float(base, math.nan)
    if not np.isfinite(est) or not np.isfinite(base_value) or base_value <= 0:
        return math.nan
    return est / base_value - 1


def _eps_growth_from_estimate(estimate: Any, base: Any) -> float:
    est = _safe_float(estimate, math.nan)
    base_value = _safe_float(base, math.nan)
    if not np.isfinite(est) or not np.isfinite(base_value) or base_value <= 0:
        return math.nan
    return est / base_value - 1


def _select_estimate(items: list[dict[str, Any]], asof_ts: pd.Timestamp | None, *, year_offset: int | None = None) -> dict[str, Any]:
    if not items:
        return {}
    asof_norm = (asof_ts or pd.Timestamp.today()).normalize()
    future = []
    for item in items:
        item_date = _parse_date(item.get("date"))
        if item_date is None:
            continue
        if item_date >= asof_norm:
            future.append((item_date, item))
    future.sort(key=lambda pair: pair[0])
    if year_offset is None:
        return future[0][1] if future else {}
    target_year = asof_norm.year + year_offset
    for item_date, item in future:
        if item_date.year == target_year:
            return item
    return future[0][1] if future else {}


def _compute_estimates(raw: dict[str, Any], fundamentals: dict[str, Any], asof_ts: pd.Timestamp | None = None) -> dict[str, Any]:
    fetched_at = _parse_date(raw.get("fetched_at"))
    if asof_ts is not None and fetched_at is not None:
        # Analyst estimates are point-in-time evidence. A cache fetched long after
        # a historical as-of date would leak future consensus revisions.
        if fetched_at.normalize() > asof_ts.normalize() + pd.Timedelta(days=3):
            return {}
    elif asof_ts is not None and fetched_at is None:
        return {}

    quarter_items = raw.get("quarterly") if isinstance(raw.get("quarterly"), list) else []
    annual_items = raw.get("annual") if isinstance(raw.get("annual"), list) else []
    next_quarter = _select_estimate(quarter_items, asof_ts)
    current_year = _select_estimate(annual_items, asof_ts, year_offset=0)
    next_year = _select_estimate(annual_items, asof_ts, year_offset=1)

    latest_revenue = _safe_float(fundamentals.get("latest_revenue"), math.nan)
    ttm_revenue = _safe_float(fundamentals.get("ttm_revenue"), math.nan)
    ttm_eps = _safe_float(fundamentals.get("ttm_eps"), math.nan)
    latest_eps = _safe_float(fundamentals.get("latest_eps"), math.nan)

    next_quarter_revenue = _safe_float(next_quarter.get("estimatedRevenueAvg"), math.nan)
    next_quarter_eps = _safe_float(next_quarter.get("estimatedEpsAvg"), math.nan)
    current_year_revenue = _safe_float(current_year.get("estimatedRevenueAvg"), math.nan)
    current_year_eps = _safe_float(current_year.get("estimatedEpsAvg"), math.nan)
    next_year_revenue = _safe_float(next_year.get("estimatedRevenueAvg"), math.nan)
    next_year_eps = _safe_float(next_year.get("estimatedEpsAvg"), math.nan)

    return {
        "next_quarter_revenue_est": next_quarter_revenue if np.isfinite(next_quarter_revenue) else None,
        "next_quarter_eps_est": next_quarter_eps if np.isfinite(next_quarter_eps) else None,
        "current_year_revenue_est": current_year_revenue if np.isfinite(current_year_revenue) else None,
        "current_year_eps_est": current_year_eps if np.isfinite(current_year_eps) else None,
        "next_year_revenue_est": next_year_revenue if np.isfinite(next_year_revenue) else None,
        "next_year_eps_est": next_year_eps if np.isfinite(next_year_eps) else None,
        "next_quarter_revenue_growth_est": _growth_from_estimate(next_quarter_revenue, latest_revenue),
        "next_quarter_eps_growth_est": _eps_growth_from_estimate(next_quarter_eps, latest_eps),
        "current_year_revenue_growth_est": _growth_from_estimate(current_year_revenue, ttm_revenue),
        "current_year_eps_growth_est": _eps_growth_from_estimate(current_year_eps, ttm_eps),
        "next_year_revenue_growth_est": _growth_from_estimate(next_year_revenue, current_year_revenue),
        "next_year_eps_growth_est": _eps_growth_from_estimate(next_year_eps, current_year_eps),
        "estimate_snapshot_date": _clean_text(raw.get("fetched_at")),
    }


def _fetch_estimates(
    symbol: str,
    cache: dict[str, dict[str, Any]],
    fetch_state: dict[str, int],
    fundamentals: dict[str, Any],
    asof_ts: pd.Timestamp | None = None,
) -> dict[str, Any]:
    symbol = symbol.upper()
    cached = cache.get(symbol)
    if cached and isinstance(cached.get("quarterly"), list) and isinstance(cached.get("annual"), list):
        return _compute_estimates(cached, fundamentals, asof_ts)

    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key or api_key.lower().startswith("your_"):
        return {}
    if fetch_state.get("count", 0) >= MAX_ESTIMATE_FETCH:
        return {}

    try:
        import requests
    except Exception:
        logger.warning("requests is not installed; skipping FMP estimate enrichment")
        return {}

    raw: dict[str, Any] = {"fetched_at": datetime.now().isoformat(), "quarterly": [], "annual": []}
    for period, key in (("quarter", "quarterly"), ("annual", "annual")):
        if fetch_state.get("count", 0) >= MAX_ESTIMATE_FETCH:
            break
        try:
            response = requests.get(
                f"https://financialmodelingprep.com/api/v3/analyst-estimates/{symbol}",
                params={"period": period, "limit": 12 if period == "quarter" else 8, "apikey": api_key},
                timeout=20,
            )
            response.raise_for_status()
            fetch_state["count"] = fetch_state.get("count", 0) + 1
            payload = response.json()
            if isinstance(payload, list):
                raw[key] = payload
        except Exception as exc:
            logger.warning("FMP estimate fetch failed for %s period=%s: %s", symbol, period, exc)
    cache[symbol] = raw
    return _compute_estimates(raw, fundamentals, asof_ts)


def _fetch_missing_profiles(symbols: list[str], profiles: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key or api_key.lower().startswith("your_"):
        return profiles

    missing = [
        s
        for s in symbols
        if s not in profiles or not (profiles[s].get("industry") or profiles[s].get("sector"))
    ][:MAX_PROFILE_FETCH]
    if not missing:
        return profiles

    try:
        import requests
    except Exception:
        logger.warning("requests is not installed; skipping FMP profile enrichment")
        return profiles

    logger.info("Fetching FMP profiles for %d symbols...", len(missing))
    for i in range(0, len(missing), 50):
        chunk = missing[i : i + 50]
        url = f"https://financialmodelingprep.com/api/v3/profile/{','.join(chunk)}"
        try:
            response = requests.get(url, params={"apikey": api_key}, timeout=30)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                continue
            for item in payload:
                symbol = _clean_text(item.get("symbol")).upper()
                if not symbol:
                    continue
                profiles[symbol] = {
                    **profiles.get(symbol, {}),
                    "symbol": symbol,
                    "companyName": _clean_text(item.get("companyName") or item.get("companyName")),
                    "sector": _clean_text(item.get("sector")),
                    "industry": _clean_text(item.get("industry")),
                    "country": _clean_text(item.get("country")),
                    "mktCap": _safe_float(item.get("mktCap"), math.nan),
                    "exchange": _clean_text(item.get("exchangeShortName") or item.get("exchange")),
                }
        except Exception as exc:
            logger.warning("FMP profile fetch failed for %s: %s", ",".join(chunk[:3]), exc)
    _save_profile_cache(profiles)
    return profiles


def _is_biotech(profile: dict[str, Any]) -> bool:
    text = f"{profile.get('sector', '')} {profile.get('industry', '')} {profile.get('companyName', '')}".lower()
    return any(term in text for term in BIOTECH_TERMS)


def _is_structural_industry(industry: str, sector: str) -> bool:
    text = f"{industry} {sector}".lower()
    return any(term in text for term in STRUCTURAL_INDUSTRY_TERMS)


def _to_series(frame: pd.DataFrame, symbol: str) -> pd.Series:
    if symbol not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[symbol], errors="coerce").dropna()


def _last_return(series: pd.Series, days: int) -> float:
    if len(series) <= days:
        return math.nan
    base = series.iloc[-days - 1]
    if not np.isfinite(base) or base <= 0:
        return math.nan
    return float(series.iloc[-1] / base - 1)


def _up_down_volume_ratio(close: pd.Series, volume: pd.Series, days: int = 20) -> float:
    close = close.tail(days + 1)
    volume = volume.reindex(close.index).tail(days + 1)
    if len(close) < 6:
        return math.nan
    change = close.diff()
    up = float(volume[change > 0].sum())
    down = float(volume[change < 0].sum())
    if down <= 0:
        return 9.99 if up > 0 else math.nan
    return up / down


def _atr_pct(high: pd.Series, low: pd.Series, close: pd.Series, days: int = 14) -> float:
    common = close.dropna().index.intersection(high.dropna().index).intersection(low.dropna().index)
    if len(common) < days + 2:
        return math.nan
    high_v = high.reindex(common).to_numpy(dtype=np.float64)
    low_v = low.reindex(common).to_numpy(dtype=np.float64)
    close_v = close.reindex(common).to_numpy(dtype=np.float64)
    tr = _true_range_numba(high_v, low_v, close_v)
    atr = float(pd.Series(tr, index=common).tail(days).mean())
    last_close = float(close_v[-1])
    return atr / last_close if last_close > 0 else math.nan


def _find_signal_date(close: pd.Series, volume: pd.Series, spy_close: pd.Series | None) -> pd.Timestamp | None:
    if len(close) < 80:
        return None
    sma10 = close.rolling(10).mean()
    sma20 = close.rolling(20).mean()
    vol20 = volume.rolling(20).mean()
    spy_ret60 = spy_close.pct_change(60).reindex(close.index) if spy_close is not None and len(spy_close) else None
    ret20 = close.pct_change(20)
    ret60 = close.pct_change(60)
    dollar_volume = close * volume
    avg_dv20 = dollar_volume.rolling(20).mean()

    start = max(60, len(close) - 126)
    candidates: list[pd.Timestamp] = []
    for idx in range(start, len(close)):
        date = close.index[idx]
        resid_spy = ret60.iloc[idx] - (spy_ret60.loc[date] if spy_ret60 is not None and date in spy_ret60.index else 0.0)
        gap_or_displacement = ret20.iloc[idx] > 0.18 or close.iloc[idx] / sma20.iloc[idx] > 1.12
        volume_confirm = volume.iloc[idx] / vol20.iloc[idx] >= 1.4 or avg_dv20.iloc[idx] >= MIN_DOLLAR_VOLUME20
        trend_ok = close.iloc[idx] > sma10.iloc[idx] and close.iloc[idx] > sma20.iloc[idx]
        if trend_ok and gap_or_displacement and volume_confirm and ret60.iloc[idx] > 0.15 and resid_spy > -0.05:
            candidates.append(date)
    return candidates[-1] if candidates else None


def _find_pullback_entry(close: pd.Series, low: pd.Series, signal_date: pd.Timestamp) -> pd.Timestamp:
    signal_pos = close.index.get_loc(signal_date)
    sma10 = close.rolling(10).mean()
    signal_low = float(low.loc[signal_date]) if signal_date in low.index else math.nan
    for idx in range(signal_pos + 1, min(len(close), signal_pos + 31)):
        date = close.index[idx]
        if not np.isfinite(sma10.iloc[idx]):
            continue
        touched = low.iloc[idx] <= sma10.iloc[idx] * 1.025
        held = close.iloc[idx] >= sma10.iloc[idx] * 0.99
        above_signal_low = not np.isfinite(signal_low) or low.iloc[idx] > signal_low
        if touched and held and above_signal_low:
            return date
    return close.index[min(signal_pos + 1, len(close) - 1)]


def _classify_price(close: pd.Series) -> str:
    if len(close) < 200:
        return "early_trend_unconfirmed"
    sma10 = close.rolling(10).mean()
    sma20 = close.rolling(20).mean()
    sma50 = close.rolling(50).mean()
    sma150 = close.rolling(150).mean()
    sma200 = close.rolling(200).mean()
    last = close.iloc[-1]
    if last > sma10.iloc[-1] and sma10.iloc[-1] > sma20.iloc[-1] and sma50.iloc[-1] > sma150.iloc[-1] > sma200.iloc[-1]:
        if last / sma50.iloc[-1] > 1.45:
            return "extended_but_intact"
        return "strong"
    if last > sma20.iloc[-1] and last > sma50.iloc[-1] and sma50.iloc[-1] > sma150.iloc[-1]:
        return "constructive"
    if last < sma50.iloc[-1] or sma50.iloc[-1] < sma150.iloc[-1]:
        return "weakening"
    return "mixed"


def _classify_volume(volume_ratio20: float, dv_persistence: float, up_down_ratio: float) -> str:
    if volume_ratio20 >= 1.8 and dv_persistence >= 1.8 and up_down_ratio >= 1.3:
        return "durable_accumulation"
    if dv_persistence >= 1.25 and up_down_ratio >= 1.0:
        return "supportive"
    if dv_persistence < 0.75 or up_down_ratio < 0.65:
        return "fading"
    return "neutral"


def _classify_supply(profile: dict[str, Any], market_cap: float | None, avg_dv20: float) -> tuple[str, str]:
    if market_cap and np.isfinite(market_cap):
        if market_cap < 250_000_000 and avg_dv20 < 4_000_000:
            return "supply_watch", "medium"
        if market_cap < 75_000_000:
            return "supply_watch", "high"
    text = f"{profile.get('industry', '')} {profile.get('companyName', '')}".lower()
    if any(term in text for term in ("warrant", "spac", "blank check")):
        return "supply_watch", "medium"
    return "low_supply_risk", "low"


def _classify_fundamental(profile: dict[str, Any], ret126: float, ret252: float, fundamentals: dict[str, Any] | None = None) -> str:
    fundamentals = fundamentals or {}
    revenue_yoy = _safe_float(fundamentals.get("revenue_yoy"), math.nan)
    eps_yoy = _safe_float(fundamentals.get("eps_yoy"), math.nan)
    eps_qoq = _safe_float(fundamentals.get("eps_qoq"), math.nan)
    if (np.isfinite(revenue_yoy) and revenue_yoy >= 0.25) or (np.isfinite(eps_yoy) and eps_yoy >= 0.25):
        return "reported_growth_confirmed"
    if np.isfinite(eps_qoq) and eps_qoq >= 0.25:
        return "earnings_reacceleration_watch"
    # This repo keeps fundamentals as an enrichment layer. When FMP statement data is absent,
    # trend persistence plus industry context becomes a conservative proxy, not a hard score.
    industry = _clean_text(profile.get("industry"))
    sector = _clean_text(profile.get("sector"))
    if _is_structural_industry(industry, sector) and ret126 > 0.4:
        return "structural_proxy_confirmed"
    if ret252 > 1.0:
        return "price_led_needs_fundamental_check"
    return "unconfirmed"


def _classify_catalyst(profile: dict[str, Any], signal_ret20: float) -> str:
    industry = _clean_text(profile.get("industry"))
    sector = _clean_text(profile.get("sector"))
    if _is_structural_industry(industry, sector) and signal_ret20 > 0.18:
        return "structural_or_industry_rerating"
    if signal_ret20 > 0.25:
        return "price_volume_catalyst_detected"
    return "needs_news_context"


def _state(price_state: str, volume_state: str, supply_severity: str, data_integrity: str) -> tuple[str, str]:
    if data_integrity != "clean":
        return "thesis_mixed", "entity_or_data_check"
    if price_state in {"weakening"} or volume_state == "fading" or supply_severity == "high":
        return "thesis_damaged", "risk_dominant"
    if price_state in {"strong", "extended_but_intact"} and volume_state == "durable_accumulation" and supply_severity == "low":
        return "thesis_intact", "intact_volume_leader"
    if price_state in {"strong", "extended_but_intact", "constructive"} and volume_state in {"durable_accumulation", "supportive"}:
        return "thesis_mixed", "mixed_strong"
    return "thesis_mixed", "mixed_watch"


def _priority_points(
    price_state: str,
    volume_state: str,
    supply_severity: str,
    catalyst: str,
    fundamental: str,
    ret60_resid_spy: float,
    avg_dv20: float,
) -> int:
    points = 0
    points += {"extended_but_intact": 5, "strong": 5, "constructive": 3, "mixed": 1}.get(price_state, 0)
    points += {"durable_accumulation": 5, "supportive": 3, "neutral": 1}.get(volume_state, 0)
    points += {"low": 3, "medium": 1, "high": -4}.get(supply_severity, 0)
    points += {"structural_or_industry_rerating": 3, "price_volume_catalyst_detected": 2}.get(catalyst, 0)
    points += {"structural_proxy_confirmed": 3, "price_led_needs_fundamental_check": 1}.get(fundamental, 0)
    points += 3 if ret60_resid_spy > 0.25 else 1 if ret60_resid_spy > 0 else -2
    points += 2 if avg_dv20 >= 5_000_000 else 0
    return points


def _risk_notes(supply_severity: str, price_state: str, data_integrity: str, adr_or_non_us: bool) -> str:
    notes: list[str] = []
    if supply_severity in {"medium", "high"}:
        severity_ja = {"medium": "中程度", "high": "高い"}.get(supply_severity, supply_severity)
        notes.append(f"株式需給リスク{severity_ja}")
    if price_state == "extended_but_intact":
        notes.append("伸び切り監視")
    if data_integrity != "clean":
        notes.append("銘柄エンティティ確認")
    if adr_or_non_us:
        notes.append("米国外企業リスク")
    return "、".join(notes) if notes else "機械的な出口ルール優先。重大ニュースのみ見立て変更候補。"


def _summary(
    symbol: str,
    company: str,
    sector: str,
    industry: str,
    price_state: str,
    volume_state: str,
    supply_severity: str,
    catalyst: str,
    fundamental: str,
    thesis_state: str,
    thesis_substate: str,
    ret60_resid_spy: float,
    dv_persistence: float,
    up_down_ratio: float,
    ret_since_entry: float,
    ret126: float,
    ret252: float,
    revenue_yoy: float | None,
    revenue_qoq: float | None,
    revenue_yoy_prev: float | None,
    eps: float | None,
    eps_yoy: float | None,
    eps_qoq: float | None,
    eps_yoy_prev: float | None,
    eps_qoq_prev: float | None,
    next_quarter_revenue_growth_est: float | None,
    next_quarter_eps_growth_est: float | None,
    current_year_revenue_growth_est: float | None,
    current_year_eps_growth_est: float | None,
    next_year_revenue_growth_est: float | None,
    next_year_eps_growth_est: float | None,
    estimate_snapshot_date: str,
    market_cap: float | None,
    avg_dollar_volume20: float,
    news_text: str,
    adr_or_non_us: bool,
) -> str:
    return compose_seven_layer_summary(
        symbol=symbol,
        company=company,
        sector=sector,
        industry=industry,
        price_state=price_state,
        volume_state=volume_state,
        supply_severity=supply_severity,
        catalyst=catalyst,
        fundamental=fundamental,
        thesis_state=thesis_state,
        thesis_substate=thesis_substate,
        ret60_resid_spy=ret60_resid_spy,
        dv_persistence=dv_persistence,
        up_down_ratio=up_down_ratio,
        ret_since_entry=ret_since_entry,
        ret126=ret126,
        ret252=ret252,
        revenue_yoy=revenue_yoy,
        revenue_qoq=revenue_qoq,
        revenue_yoy_prev=revenue_yoy_prev,
        eps=eps,
        eps_yoy=eps_yoy,
        eps_qoq=eps_qoq,
        eps_yoy_prev=eps_yoy_prev,
        eps_qoq_prev=eps_qoq_prev,
        next_quarter_revenue_growth_est=next_quarter_revenue_growth_est,
        next_quarter_eps_growth_est=next_quarter_eps_growth_est,
        current_year_revenue_growth_est=current_year_revenue_growth_est,
        current_year_eps_growth_est=current_year_eps_growth_est,
        next_year_revenue_growth_est=next_year_revenue_growth_est,
        next_year_eps_growth_est=next_year_eps_growth_est,
        estimate_snapshot_date=estimate_snapshot_date,
        market_cap=market_cap,
        avg_dollar_volume20=avg_dollar_volume20,
        news_text=news_text,
        adr_or_non_us=adr_or_non_us,
    )


def build_recognition_gap_ranking(
    asof_date: str | None = None,
    top_n: int | None = DEFAULT_TOP_N,
    price_data: pd.DataFrame | None = None,
) -> dict[str, Any]:
    data = price_data if price_data is not None else _load_price_data()
    if asof_date:
        asof_ts = pd.Timestamp(asof_date)
        data = data[data.index <= asof_ts]
    if data.empty:
        raise ValueError("no price data available for ranking")

    close_df = _field(data, "Close")
    high_df = _field(data, "High")
    low_df = _field(data, "Low")
    volume_df = _field(data, "Volume")

    symbols = sorted(set(close_df.columns).intersection(volume_df.columns))
    spy_close = _to_series(close_df, "SPY") if "SPY" in close_df.columns else None
    spy_ret60 = _last_return(spy_close, 60) if spy_close is not None and len(spy_close) else 0.0

    profiles = _fetch_missing_profiles(symbols, _load_profile_cache())
    fundamental_cache = _load_fundamental_cache()
    fundamental_fetch_state = {"count": 0}
    news_cache = _load_news_cache()
    news_fetch_state = {"count": 0}
    estimate_cache = _load_estimate_cache()
    estimate_fetch_state = {"count": 0}
    rows: list[RecognitionGapRow] = []

    for symbol in symbols:
        if symbol in {"SPY", "QQQ", "IWM"}:
            continue
        close = _to_series(close_df, symbol)
        volume = _to_series(volume_df, symbol)
        high = _to_series(high_df, symbol)
        low = _to_series(low_df, symbol)
        common = close.index.intersection(volume.index).intersection(high.index).intersection(low.index)
        if len(common) < 220:
            continue
        close = close.reindex(common)
        volume = volume.reindex(common)
        high = high.reindex(common)
        low = low.reindex(common)

        last_close = float(close.iloc[-1])
        if not np.isfinite(last_close) or last_close < MIN_CLOSE:
            continue

        profile = profiles.get(symbol, {})
        if _is_biotech(profile):
            continue

        dollar_volume = close * volume
        avg_dv20 = float(dollar_volume.tail(20).mean())
        if avg_dv20 < MIN_DOLLAR_VOLUME20:
            continue

        ret20 = _last_return(close, 20)
        ret60 = _last_return(close, 60)
        ret126 = _last_return(close, 126)
        ret252 = _last_return(close, 252)
        if not np.isfinite(ret60) or ret60 < 0.15:
            continue

        signal_date = _find_signal_date(close, volume, spy_close)
        if signal_date is None:
            continue
        entry_date = _find_pullback_entry(close, low, signal_date)
        if entry_date > close.index[-1]:
            continue

        entry_price = float(close.loc[entry_date])
        ret_since_entry = last_close / entry_price - 1 if entry_price > 0 else math.nan
        signal_pos = close.index.get_loc(signal_date)
        pre_start = max(0, signal_pos - 20)
        pre_dv = float(dollar_volume.iloc[pre_start:signal_pos].mean()) if signal_pos > pre_start else math.nan
        post_dv = float(dollar_volume.iloc[signal_pos : min(len(dollar_volume), signal_pos + 20)].mean())
        dv_persistence = post_dv / pre_dv if pre_dv and np.isfinite(pre_dv) and pre_dv > 0 else math.nan
        volume_ratio20 = float(volume.iloc[-1] / volume.tail(20).mean()) if volume.tail(20).mean() > 0 else math.nan
        up_down = _up_down_volume_ratio(close, volume)
        atr14_pct = _atr_pct(high, low, close)
        ret60_resid_spy = ret60 - spy_ret60

        industry = _clean_text(profile.get("industry"))
        sector = _clean_text(profile.get("sector"))
        country = _clean_text(profile.get("country"))
        company = _clean_text(profile.get("companyName")) or symbol
        market_cap = _safe_float(profile.get("mktCap"), math.nan)
        market_cap_value: float | None = float(market_cap) if np.isfinite(market_cap) else None
        adr_or_non_us = bool(country and country.lower() not in {"united states", "usa", "us"})
        fundamentals = _fetch_fundamentals(
            symbol,
            fundamental_cache,
            fundamental_fetch_state,
            pd.Timestamp(data.index[-1]) if asof_date else None,
        )
        revenue_yoy = _safe_float(fundamentals.get("revenue_yoy"), math.nan)
        revenue_qoq = _safe_float(fundamentals.get("revenue_qoq"), math.nan)
        revenue_yoy_prev = _safe_float(fundamentals.get("revenue_yoy_prev"), math.nan)
        latest_eps = _safe_float(fundamentals.get("latest_eps"), math.nan)
        eps_yoy = _safe_float(fundamentals.get("eps_yoy"), math.nan)
        eps_qoq = _safe_float(fundamentals.get("eps_qoq"), math.nan)
        eps_yoy_prev = _safe_float(fundamentals.get("eps_yoy_prev"), math.nan)
        eps_qoq_prev = _safe_float(fundamentals.get("eps_qoq_prev"), math.nan)
        ttm_revenue = _safe_float(fundamentals.get("ttm_revenue"), math.nan)
        ttm_eps = _safe_float(fundamentals.get("ttm_eps"), math.nan)
        asof_ts = pd.Timestamp(data.index[-1])
        estimates = _fetch_estimates(symbol, estimate_cache, estimate_fetch_state, fundamentals, asof_ts)
        next_quarter_revenue_est = _safe_float(estimates.get("next_quarter_revenue_est"), math.nan)
        next_quarter_eps_est = _safe_float(estimates.get("next_quarter_eps_est"), math.nan)
        current_year_revenue_est = _safe_float(estimates.get("current_year_revenue_est"), math.nan)
        current_year_eps_est = _safe_float(estimates.get("current_year_eps_est"), math.nan)
        next_year_revenue_est = _safe_float(estimates.get("next_year_revenue_est"), math.nan)
        next_year_eps_est = _safe_float(estimates.get("next_year_eps_est"), math.nan)
        next_quarter_revenue_growth_est = _safe_float(estimates.get("next_quarter_revenue_growth_est"), math.nan)
        next_quarter_eps_growth_est = _safe_float(estimates.get("next_quarter_eps_growth_est"), math.nan)
        current_year_revenue_growth_est = _safe_float(estimates.get("current_year_revenue_growth_est"), math.nan)
        current_year_eps_growth_est = _safe_float(estimates.get("current_year_eps_growth_est"), math.nan)
        next_year_revenue_growth_est = _safe_float(estimates.get("next_year_revenue_growth_est"), math.nan)
        next_year_eps_growth_est = _safe_float(estimates.get("next_year_eps_growth_est"), math.nan)
        estimate_snapshot_date = _clean_text(estimates.get("estimate_snapshot_date"))
        news_from = signal_date - pd.Timedelta(days=NEWS_PRE_SIGNAL_DAYS)
        news_items = _load_symbol_news(symbol, news_cache, news_fetch_state, news_from, asof_ts)
        news_context = _classify_news(news_items, signal_date, asof_ts)
        news_evidence = _clean_text(news_context.get("news_evidence_ja"))
        post_news_titles = _join_titles(news_context.get("post_events", []), limit=6)
        recent_news_titles = _join_titles(news_context.get("recent_events", []), limit=4)
        pre_news_titles = _join_titles(news_context.get("pre_events", []), limit=4)
        news_text_for_summary = " | ".join(text for text in (news_evidence, post_news_titles, recent_news_titles) if text)

        data_integrity = "clean" if company and (industry or sector) else "needs_entity_check"
        price_state = _classify_price(close)
        volume_state = _classify_volume(volume_ratio20, dv_persistence if np.isfinite(dv_persistence) else 1.0, up_down)
        supply, supply_severity = _classify_supply(profile, market_cap_value, avg_dv20)
        catalyst = _classify_catalyst(profile, float(close.loc[signal_date] / close.iloc[max(0, signal_pos - 20)] - 1))
        fundamental = _classify_fundamental(profile, ret126, ret252, fundamentals)
        thesis_state, thesis_substate = _state(price_state, volume_state, supply_severity, data_integrity)
        flags = []
        if price_state == "extended_but_intact":
            flags.append("extended_price")
        if volume_state == "fading":
            flags.append("volume_fading")
        if supply_severity in {"medium", "high"}:
            flags.append(f"supply_risk_{supply_severity}")
        if data_integrity != "clean":
            flags.append("entity_check")
        if news_context.get("guidance_positive_signal"):
            flags.append("positive_guidance")
        elif news_context.get("guidance_signal"):
            flags.append("guidance_watch")

        priority = _priority_points(
            price_state,
            volume_state,
            supply_severity,
            catalyst,
            fundamental,
            ret60_resid_spy,
            avg_dv20,
        )

        rows.append(
            RecognitionGapRow(
                rank=0,
                symbol=symbol,
                company=company,
                entry_date=entry_date.strftime("%Y-%m-%d"),
                signal_date=signal_date.strftime("%Y-%m-%d"),
                asof_date=close.index[-1].strftime("%Y-%m-%d"),
                asof_close=round(last_close, 4),
                entry_price=round(entry_price, 4),
                return_since_entry=round(ret_since_entry, 6),
                sector=sector,
                industry=industry,
                country=country,
                market_cap=market_cap_value,
                adr_or_non_us=adr_or_non_us,
                revenue_yoy=round(revenue_yoy, 6) if np.isfinite(revenue_yoy) else None,
                revenue_qoq=round(revenue_qoq, 6) if np.isfinite(revenue_qoq) else None,
                revenue_yoy_prev=round(revenue_yoy_prev, 6) if np.isfinite(revenue_yoy_prev) else None,
                ttm_revenue=round(ttm_revenue, 2) if np.isfinite(ttm_revenue) else None,
                ttm_eps=round(ttm_eps, 6) if np.isfinite(ttm_eps) else None,
                latest_eps=round(latest_eps, 6) if np.isfinite(latest_eps) else None,
                eps_yoy=round(eps_yoy, 6) if np.isfinite(eps_yoy) else None,
                eps_qoq=round(eps_qoq, 6) if np.isfinite(eps_qoq) else None,
                eps_yoy_prev=round(eps_yoy_prev, 6) if np.isfinite(eps_yoy_prev) else None,
                eps_qoq_prev=round(eps_qoq_prev, 6) if np.isfinite(eps_qoq_prev) else None,
                news_pre_60d_titles=pre_news_titles,
                news_ep_to_asof_titles=post_news_titles,
                news_recent_titles=recent_news_titles,
                news_evidence_ja=news_evidence,
                backlog_signal=bool(news_context.get("backlog_signal")),
                contract_signal=bool(news_context.get("contract_signal")),
                order_signal=bool(news_context.get("order_signal")),
                institutional_signal=bool(news_context.get("institutional_signal")),
                warning_signal=bool(news_context.get("warning_signal")),
                guidance_signal=bool(news_context.get("guidance_signal")),
                guidance_positive_signal=bool(news_context.get("guidance_positive_signal")),
                next_quarter_revenue_est=round(next_quarter_revenue_est, 2) if np.isfinite(next_quarter_revenue_est) else None,
                next_quarter_eps_est=round(next_quarter_eps_est, 6) if np.isfinite(next_quarter_eps_est) else None,
                current_year_revenue_est=round(current_year_revenue_est, 2) if np.isfinite(current_year_revenue_est) else None,
                current_year_eps_est=round(current_year_eps_est, 6) if np.isfinite(current_year_eps_est) else None,
                next_year_revenue_est=round(next_year_revenue_est, 2) if np.isfinite(next_year_revenue_est) else None,
                next_year_eps_est=round(next_year_eps_est, 6) if np.isfinite(next_year_eps_est) else None,
                next_quarter_revenue_growth_est=round(next_quarter_revenue_growth_est, 6) if np.isfinite(next_quarter_revenue_growth_est) else None,
                next_quarter_eps_growth_est=round(next_quarter_eps_growth_est, 6) if np.isfinite(next_quarter_eps_growth_est) else None,
                current_year_revenue_growth_est=round(current_year_revenue_growth_est, 6) if np.isfinite(current_year_revenue_growth_est) else None,
                current_year_eps_growth_est=round(current_year_eps_growth_est, 6) if np.isfinite(current_year_eps_growth_est) else None,
                next_year_revenue_growth_est=round(next_year_revenue_growth_est, 6) if np.isfinite(next_year_revenue_growth_est) else None,
                next_year_eps_growth_est=round(next_year_eps_growth_est, 6) if np.isfinite(next_year_eps_growth_est) else None,
                estimate_snapshot_date=estimate_snapshot_date,
                data_integrity=data_integrity,
                price_trend=price_state,
                volume_demand_durability=volume_state,
                institutional_and_supply=supply,
                supply_risk_severity=supply_severity,
                catalyst_quality=catalyst,
                fundamental_confirmation=fundamental,
                thesis_state=thesis_state,
                thesis_substate=thesis_substate,
                daily_flags=flags,
                risk_notes_ja=_risk_notes(supply_severity, price_state, data_integrity, adr_or_non_us),
                seven_layer_summary_ja=_summary(
                    symbol,
                    company,
                    sector,
                    industry,
                    price_state,
                    volume_state,
                    supply_severity,
                    catalyst,
                    fundamental,
                    thesis_state,
                    thesis_substate,
                    ret60_resid_spy,
                    dv_persistence if np.isfinite(dv_persistence) else 1.0,
                    up_down,
                    ret_since_entry,
                    ret126,
                    ret252,
                    revenue_yoy if np.isfinite(revenue_yoy) else None,
                    revenue_qoq if np.isfinite(revenue_qoq) else None,
                    revenue_yoy_prev if np.isfinite(revenue_yoy_prev) else None,
                    latest_eps if np.isfinite(latest_eps) else None,
                    eps_yoy if np.isfinite(eps_yoy) else None,
                    eps_qoq if np.isfinite(eps_qoq) else None,
                    eps_yoy_prev if np.isfinite(eps_yoy_prev) else None,
                    eps_qoq_prev if np.isfinite(eps_qoq_prev) else None,
                    next_quarter_revenue_growth_est if np.isfinite(next_quarter_revenue_growth_est) else None,
                    next_quarter_eps_growth_est if np.isfinite(next_quarter_eps_growth_est) else None,
                    current_year_revenue_growth_est if np.isfinite(current_year_revenue_growth_est) else None,
                    current_year_eps_growth_est if np.isfinite(current_year_eps_growth_est) else None,
                    next_year_revenue_growth_est if np.isfinite(next_year_revenue_growth_est) else None,
                    next_year_eps_growth_est if np.isfinite(next_year_eps_growth_est) else None,
                    estimate_snapshot_date,
                    market_cap_value,
                    avg_dv20,
                    news_text_for_summary,
                    adr_or_non_us,
                ),
                recommendation_priority=priority,
                ret20=round(ret20, 6),
                ret60=round(ret60, 6),
                ret126=round(ret126, 6),
                ret252=round(ret252, 6),
                ret60_resid_spy=round(ret60_resid_spy, 6),
                volume_ratio20=round(volume_ratio20, 6) if np.isfinite(volume_ratio20) else math.nan,
                avg_dollar_volume20=round(avg_dv20, 2),
                post_signal_dv_persistence=round(dv_persistence, 6) if np.isfinite(dv_persistence) else math.nan,
                up_down_volume_ratio_20d=round(up_down, 6) if np.isfinite(up_down) else math.nan,
                atr14_pct=round(atr14_pct, 6) if np.isfinite(atr14_pct) else math.nan,
            )
        )

    rows.sort(
        key=lambda r: (
            r.thesis_state != "thesis_intact",
            -r.recommendation_priority,
            -r.ret60_resid_spy,
            -r.post_signal_dv_persistence if np.isfinite(r.post_signal_dv_persistence) else 0,
            -r.avg_dollar_volume20,
        )
    )
    row_limit = parse_top_n(top_n)
    if row_limit is not None:
        rows = rows[:row_limit]
    for idx, row in enumerate(rows, start=1):
        row.rank = idx
    if fundamental_fetch_state.get("count", 0):
        _save_fundamental_cache(fundamental_cache)
    if news_fetch_state.get("count", 0):
        _save_news_cache(news_cache)
    if estimate_fetch_state.get("count", 0):
        _save_estimate_cache(estimate_cache)

    asof = data.index[-1].strftime("%Y-%m-%d")
    result = {
        "date": asof,
        "asof_date": asof,
        "generated_at": datetime.now().isoformat(),
        "system": "認識ズレ型EP 7層ランキング",
        "entry_rule": "industry_theme_ep_ex_biotech",
        "entry_timing": "pullback10",
        "exit_rule": "stage2_or_atr8",
        "ranking": [asdict(row) for row in rows],
        "notes": [
            "ランキングは監視と乗り換え比較用であり、自動買付指示ではありません。",
            "出口は機械ルールを優先し、7層評価は持続力とリスクを説明します。",
            "過去日付で検証する場合、その日より後の情報は使いません。",
        ],
    }
    return result


def save_ranking(result: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RANKING_JSON_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    rows = result.get("ranking", [])
    pd.DataFrame(rows).to_csv(RANKING_CSV_PATH, index=False, encoding="utf-8-sig")
    logger.info("Saved %s and %s", RANKING_JSON_PATH, RANKING_CSV_PATH)


def run(asof_date: str | None = None, top_n: int | None = DEFAULT_TOP_N, save: bool = True) -> dict[str, Any]:
    result = build_recognition_gap_ranking(asof_date=asof_date, top_n=top_n)
    if save:
        save_ranking(result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Recognition Gap EP 7-layer ranking.")
    parser.add_argument("--asof-date", help="YYYY-MM-DD. Only data up to this date is used.")
    parser.add_argument("--top-n", default=os.getenv("RECOGNITION_GAP_TOP_N", "0"), help="Positive number caps rows; 0/all means all detected rows.")
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run(asof_date=args.asof_date, top_n=parse_top_n(args.top_n), save=not args.no_save)
    for row in result["ranking"][:20]:
        print(
            f"{row['rank']:>2} {row['symbol']:<6} entry={row['entry_date']} "
            f"return={_pct(row['return_since_entry'])} state={row['thesis_state']} "
            f"{row['seven_layer_summary_ja']}"
        )


if __name__ == "__main__":
    main()
