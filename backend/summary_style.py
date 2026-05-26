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
    if "communications" in text or "telecom" in text or "satellite" in text:
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
    return clean_text(industry or sector) or "業種文脈は要確認"


def price_phrase(price_state: str, ret_since_entry: Any = None) -> str:
    state = clean_text(price_state)
    if not state:
        ret = safe_float(ret_since_entry)
        if math.isfinite(ret):
            if ret >= 3.0:
                return "価格は大化け級に伸びた実績があり、崩れは機械exitで監視する段階"
            if ret >= 0.5:
                return "価格はEntry後に十分伸びており、トレンド継続を機械ルールで確認したい"
            if ret > 0:
                return "価格はEntry後プラス圏だが、上位候補ほどの伸びはまだ限定的"
        return "価格構造は日足MAとstage2状態で確認する段階"
    return {
        "extended_but_intact": "価格は大きく伸びているが主要MA上で崩れておらず、伸び切り監視をしながらthesisは維持",
        "strong": "価格トレンドは強く、10/20/50MAの構造も崩れていない",
        "constructive": "価格は初動後の押し目/再上昇確認段階で、まだ建設的",
        "mixed": "価格はややmixedで、上位候補ほどの直線的な強さはまだ弱い",
        "weakening": "価格は弱まり始めており、機械exitに近づくなら優先度は落ちる",
        "early_trend_unconfirmed": "価格は初動寄りで、長期MA構造の確認はまだ不足",
    }.get(state, "価格構造は日足MAとstage2状態で確認する段階")


def volume_phrase(volume_state: str, dv_persistence: Any = None, up_down_ratio: Any = None) -> str:
    state = clean_text(volume_state)
    details = []
    if math.isfinite(safe_float(dv_persistence)):
        details.append(f"出来高維持率{ratio_text(dv_persistence)}")
    if math.isfinite(safe_float(up_down_ratio)):
        details.append(f"up/down volume {ratio_text(up_down_ratio)}")
    suffix = f"（{ '、'.join(details) }）" if details else ""
    if not state:
        return f"出来高は日足側のEP後維持率で確認し、資金流入が残るかが焦点{suffix}"
    return {
        "durable_accumulation": f"出来高はEP後も持続し、機関投資家の買い継続を疑える強さ{suffix}",
        "supportive": f"出来高はsupportiveで、資金流入はまだ残っている{suffix}",
        "neutral": f"出来高は中立で、価格主導の候補として追加確認が必要{suffix}",
        "fading": f"出来高は鈍化気味で、認識ズレの持続力には注意{suffix}",
    }.get(state, f"出来高は日足側のEP後維持率で確認し、資金流入が残るかが焦点{suffix}")


def supply_phrase(supply_severity: str, adr_or_non_us: bool = False) -> str:
    base = {
        "low": "供給リスクはlowで、希薄化よりトレンド継続を優先して見やすい",
        "medium": "供給リスクはmedium。offeringや大株主売りのニュース監視は必要",
        "high": "供給リスクはhighで、テクニカルが強くても監視優先度は下げる",
    }.get(clean_text(supply_severity), "供給リスクは未判定で、offeringや大株主売りは別途監視")
    if adr_or_non_us:
        base += "。ADR/非米国要因も確認"
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
        eps_part = f"、EPS {eps_value:g}" if math.isfinite(eps_value) else ""
        if rev >= 0.3:
            return f"直近売上YoY {pct_text(rev)}{eps_part}で、業績変化も価格を支えている"
        if rev > 0:
            return f"直近売上YoY {pct_text(rev)}{eps_part}。成長はあるが、大化けには継続確認が必要"
        return f"直近売上YoY {pct_text(rev)}{eps_part}で、ファンダ面はまだ慎重に見る"
    ret126_text = pct_text(ret126)
    if fundamental_state == "structural_proxy_confirmed":
        return f"業種文脈と中期上昇が揃い、構造変化候補として見やすい（126日{ret126_text}）"
    if fundamental_state == "price_led_needs_fundamental_check":
        return "価格主導の再評価が先行しており、次の決算/受注/バックログ確認が重要"
    if fundamental_state == "unconfirmed":
        return "ファンダ確認はまだ弱く、決算・受注・バックログで裏取りしたい"
    return clean_text(fundamental_state) or "ファンダ確認は要確認"


def thesis_phrase(thesis_state: str, thesis_substate: str) -> str:
    if thesis_state == "thesis_intact":
        return "7層ではthesis_intact寄りで、保有監視の質は高い"
    if thesis_substate == "mixed_strong":
        return "7層ではmixedだが、価格/出来高は上位候補に近い"
    if thesis_state == "thesis_damaged":
        return "7層ではdamaged寄りで、監視は機械exit優先"
    if not clean_text(thesis_state):
        return ""
    return "7層ではmixed。強い点と確認不足が同居している"


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
    adr_or_non_us: bool = False,
    max_chars: int = 300,
) -> str:
    market_part = ""
    if math.isfinite(safe_float(ret60_resid_spy)):
        market_part = f"対SPY60日残差は{pct_text(ret60_resid_spy)}"
    parts = [
        theme_phrase(industry, sector),
        price_phrase(price_state, ret_since_entry),
        volume_phrase(volume_state, dv_persistence, up_down_ratio),
        market_part,
        fundamental_phrase(fundamental, ret126, ret252, revenue_yoy, eps),
        supply_phrase(supply_severity, adr_or_non_us),
        thesis_phrase(thesis_state, thesis_substate),
        f"Entry後{pct_text(ret_since_entry)}だが、売買判断ではなくstage2_or_atr8の機械exitを優先",
    ]
    return trim_summary("。".join(part.rstrip("。") for part in parts if clean_text(part)) + "。", max_chars)
