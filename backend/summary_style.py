from __future__ import annotations

import math
from typing import Any


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null", "nat"}:
        return ""
    return " ".join(text.split())


def safe_float(value: Any, default: float = math.nan) -> float:
    try:
        if value is None or value == "":
            return default
        if isinstance(value, str) and value.endswith("%"):
            return float(value[:-1]) / 100.0
        return float(value)
    except Exception:
        return default


def pct_text(value: Any) -> str:
    number = safe_float(value)
    if not math.isfinite(number):
        return "-"
    if abs(number) <= 20:
        return f"{number * 100:+.1f}%"
    return f"{number:+.1f}%"


def ratio_text(value: Any) -> str:
    number = safe_float(value)
    if not math.isfinite(number):
        return "-"
    return f"{number:.2f}倍"


SEVERITY_LABEL_JA = {
    "low": "低い",
    "medium": "中程度",
    "high": "高い",
}

EQUITY_SUPPLY_RISK_TERMS = (
    "offering",
    "public offering",
    "registered direct",
    "at-the-market",
    "atm",
    "shelf",
    "s-3",
    "s-1",
    "warrant",
    "convertible",
    "resale",
    "dilution",
    "secondary",
    "common stock",
    "insider sale",
    "form 144",
)

BUSINESS_DEMAND_TERMS = (
    "backlog",
    "order",
    "orders",
    "booking",
    "bookings",
    "contract",
    "award",
    "demand",
    "capacity",
    "data center",
    "hyperscale",
    "ai server",
    "optical",
    "photonics",
    "defense",
    "aerospace",
    "satellite",
    "semiconductor equipment",
    "hbm",
    "受注",
    "受注残",
    "契約",
    "需要",
)


def infer_supply_severity(
    explicit: str = "",
    *,
    market_cap: Any = None,
    avg_dollar_volume20: Any = None,
    news_text: str = "",
) -> str:
    explicit = clean_text(explicit).lower()
    if explicit in {"low", "medium", "high"}:
        return explicit
    if explicit in {"低", "低い"}:
        return "low"
    if explicit in {"中", "中程度"}:
        return "medium"
    if explicit in {"高", "高い"}:
        return "high"

    cap = safe_float(market_cap)
    adv = safe_float(avg_dollar_volume20)
    text = clean_text(news_text).lower()
    has_supply_news = any(term in text for term in EQUITY_SUPPLY_RISK_TERMS)

    if math.isfinite(cap):
        if cap < 250_000_000:
            return "high"
        if has_supply_news and cap < 1_500_000_000:
            return "high"
        if cap < 1_000_000_000:
            return "medium"
        if has_supply_news:
            return "medium"
        if math.isfinite(adv) and adv < 1_000_000:
            return "medium"
        return "low"

    if has_supply_news:
        return "medium"
    return ""


def trim_summary(text: str, max_chars: int = 300) -> str:
    text = clean_text(text)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip("、。,. ") + "…"


def theme_phrase(industry: str = "", sector: str = "") -> str:
    text = f"{industry} {sector}".lower()
    if "semiconductor" in text:
        if "equipment" in text:
            return "半導体製造装置/検査周辺。AI半導体投資の周辺銘柄"
        if "memory" in text:
            return "メモリ/半導体サイクル再評価の中心に近い銘柄"
        return "半導体/AIデータセンター・電子部品周辺の再評価候補"
    if "hardware" in text or "electronic" in text or "pcb" in text:
        return "電子部品/PCB/AIサーバー・防衛インフラ寄りの再評価候補"
    if "communication" in text or "telecom" in text or "satellite" in text:
        return "通信/衛星/防衛・インフラ更新に連動しやすい銘柄"
    if "aerospace" in text or "defense" in text:
        return "航空宇宙/防衛テーマの構造変化候補"
    if "oil" in text or "gas" in text or "energy" in text:
        return "エネルギー設備/資源サイクル寄りの再評価候補"
    if "machinery" in text or "industrial" in text or "construction" in text:
        return "産業機械/設備投資サイクルの再評価候補"
    if "software" in text or "information" in text:
        return "ソフトウェア/データ活用テーマの再評価候補"
    if "metal" in text or "mining" in text or "uranium" in text:
        return "素材/資源テーマのモメンタム候補"
    return "業種文脈は要確認"


def price_phrase(price_state: str, ret_since_entry: Any = None) -> str:
    state = clean_text(price_state)
    if not state:
        ret = safe_float(ret_since_entry)
        if math.isfinite(ret):
            if ret >= 3.0:
                return "価格は大化け級に伸びた実績があり、崩れは機械的な出口ルールで監視する段階"
            if ret >= 0.5:
                return "価格は入口後に十分伸びており、上昇継続を機械ルールで確認したい"
            if ret > 0:
                return "価格は入口後プラス圏だが、上位候補ほどの伸びはまだ限定的"
        return "価格構造は日足移動平均線と上昇局面で確認する段階"
    return {
        "extended_but_intact": "価格は大きく伸びているが主要移動平均線上で崩れておらず、見立ては維持",
        "strong": "価格トレンドは強く、10・20・50日移動平均線の構造も崩れていない",
        "constructive": "価格は初動後の押し目/再上昇確認段階で、まだ建設的",
        "mixed": "価格は強弱混在で、上位候補ほどの直線的な強さはまだ弱い",
        "weakening": "価格は弱まり始めており、機械的な出口ルールに近づくなら優先度は落ちる",
        "early_trend_unconfirmed": "価格は初動寄りで、長期移動平均線構造の確認はまだ不足",
    }.get(state, "価格構造は日足移動平均線と上昇局面で確認する段階")


def volume_phrase(volume_state: str, dv_persistence: Any = None, up_down_ratio: Any = None) -> str:
    state = clean_text(volume_state)
    details = []
    if math.isfinite(safe_float(dv_persistence)):
        details.append(f"出来高維持率{ratio_text(dv_persistence)}")
    if math.isfinite(safe_float(up_down_ratio)):
        details.append(f"上昇日出来高比{ratio_text(up_down_ratio)}")
    suffix = f"（{ '、'.join(details) }）" if details else ""
    if not state:
        return f"出来高は日足側の急騰起点後維持率で確認し、資金流入が残るかが焦点{suffix}"
    return {
        "durable_accumulation": f"出来高は急騰起点後も持続し、機関投資家の買い継続を疑える強さ{suffix}",
        "supportive": f"出来高は支えがあり、資金流入はまだ残っている{suffix}",
        "neutral": f"出来高は中立で、価格主導の候補として追加確認が必要{suffix}",
        "fading": f"出来高は鈍化気味で、認識ズレの持続力には注意{suffix}",
    }.get(state, f"出来高は日足側の急騰起点後維持率で確認し、資金流入が残るかが焦点{suffix}")


def business_demand_phrase(
    industry: str = "",
    sector: str = "",
    news_text: str = "",
    fundamental_state: str = "",
    revenue_yoy: Any = None,
) -> str:
    text = f"{industry} {sector} {news_text}".lower()
    rev = safe_float(revenue_yoy)
    has_demand_context = any(term in text for term in BUSINESS_DEMAND_TERMS)
    structural = any(
        term in text
        for term in (
            "semiconductor",
            "electronic",
            "hardware",
            "communication",
            "communications",
            "satellite",
            "aerospace",
            "defense",
            "energy",
            "industrial",
            "machinery",
        )
    )
    if has_demand_context:
        return "事業需給は強い。受注・契約・顧客需要の裏取りが再評価を支える"
    if fundamental_state == "structural_proxy_confirmed" or (math.isfinite(rev) and rev >= 0.3 and structural):
        return "事業需給は強め。業種需要と売上変化の両面で確認したい"
    if structural:
        return "事業需給は確認中。受注残や顧客需要が次の決算で続くかを見る"
    return "事業需給は未確認。ニュースより決算・受注・受注残で裏取りしたい"


def supply_phrase(
    supply_severity: str,
    adr_or_non_us: bool = False,
    *,
    market_cap: Any = None,
    avg_dollar_volume20: Any = None,
    news_text: str = "",
) -> str:
    severity = infer_supply_severity(
        supply_severity,
        market_cap=market_cap,
        avg_dollar_volume20=avg_dollar_volume20,
        news_text=news_text,
    )
    label = SEVERITY_LABEL_JA.get(severity)
    base = {
        "low": f"株式需給リスクは{label}。増資や売り出しより価格・出来高を優先して見やすい",
        "medium": f"株式需給リスクは{label}。増資、売り出し、大株主売りの監視は必要",
        "high": f"株式需給リスクは{label}。価格が強くても資金調達や売り圧力を優先監視",
    }.get(severity, "株式需給リスクは未確定。増資、売り出し、大株主売りは別途確認")
    if adr_or_non_us:
        base += "。米国外企業要因も確認"
    return base


def fundamental_phrase(
    fundamental_state: str,
    ret126: Any = None,
    ret252: Any = None,
    revenue_yoy: Any = None,
    eps: Any = None,
) -> str:
    rev = safe_float(revenue_yoy)
    eps_value = safe_float(eps)
    if math.isfinite(rev):
        eps_part = f"、1株利益{eps_value:g}" if math.isfinite(eps_value) else ""
        if rev >= 0.3:
            return f"直近売上は前年同期比{pct_text(rev)}{eps_part}で、業績変化も価格を支えている"
        if rev > 0:
            return f"直近売上は前年同期比{pct_text(rev)}{eps_part}。成長はあるが、大化けには継続確認が必要"
        return f"直近売上は前年同期比{pct_text(rev)}{eps_part}で、業績面はまだ慎重に見る"
    ret126_text = pct_text(ret126)
    if fundamental_state == "structural_proxy_confirmed":
        return f"業種文脈と中期上昇が揃い、構造変化候補として見やすい（126日{ret126_text}）"
    if fundamental_state == "price_led_needs_fundamental_check":
        return "価格主導の再評価が先行しており、次の決算・受注・受注残確認が重要"
    if fundamental_state == "unconfirmed":
        return "業績確認はまだ弱く、決算・受注・受注残で裏取りしたい"
    return clean_text(fundamental_state) or "業績確認は要確認"


def thesis_phrase(thesis_state: str, thesis_substate: str) -> str:
    if thesis_state == "thesis_intact":
        return "見立ては良好で、保有監視の質は高い"
    if thesis_substate == "mixed_strong":
        return "見立ては強弱混在だが、価格と出来高は上位候補に近い"
    if thesis_state == "thesis_damaged":
        return "見立ては傷み気味で、監視は機械的な出口ルール優先"
    if not clean_text(thesis_state):
        return ""
    return "見立ては強弱混在。強い点と確認不足が同居している"


def compose_seven_layer_summary(
    *,
    symbol: str = "",
    company: str = "",
    sector: str = "",
    industry: str = "",
    price_state: str = "",
    volume_state: str = "",
    supply_severity: str = "",
    catalyst: str = "",
    fundamental: str = "",
    thesis_state: str = "",
    thesis_substate: str = "",
    ret60_resid_spy: Any = None,
    dv_persistence: Any = None,
    up_down_ratio: Any = None,
    ret_since_entry: Any = None,
    ret126: Any = None,
    ret252: Any = None,
    revenue_yoy: Any = None,
    eps: Any = None,
    market_cap: Any = None,
    avg_dollar_volume20: Any = None,
    news_text: str = "",
    adr_or_non_us: bool = False,
    max_chars: int = 300,
) -> str:
    market_part = ""
    if math.isfinite(safe_float(ret60_resid_spy)):
        market_part = f"市場比60日超過分は{pct_text(ret60_resid_spy)}"
    parts = [
        theme_phrase(industry, sector),
        price_phrase(price_state, ret_since_entry),
        volume_phrase(volume_state, dv_persistence, up_down_ratio),
        business_demand_phrase(industry, sector, news_text, fundamental, revenue_yoy),
        market_part,
        fundamental_phrase(fundamental, ret126, ret252, revenue_yoy, eps),
        supply_phrase(
            supply_severity,
            adr_or_non_us,
            market_cap=market_cap,
            avg_dollar_volume20=avg_dollar_volume20,
            news_text=news_text,
        ),
        thesis_phrase(thesis_state, thesis_substate),
        f"入口後{pct_text(ret_since_entry)}だが、売買判断ではなく機械的な出口ルールを優先",
    ]
    return trim_summary("。".join(part.rstrip("。") for part in parts if clean_text(part)) + "。", max_chars)
