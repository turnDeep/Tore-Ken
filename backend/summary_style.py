from __future__ import annotations

import math
import re
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


def is_finite(value: Any) -> bool:
    return math.isfinite(safe_float(value))


def value_direction(current: Any, previous: Any) -> str:
    cur = safe_float(current)
    prev = safe_float(previous)
    if not math.isfinite(cur) or not math.isfinite(prev):
        return ""
    if cur < 0 and prev < 0:
        if cur > prev + 0.05:
            return "改善"
        if cur < prev - 0.05:
            return "悪化"
        return "横ばい"
    if cur < 0 <= prev:
        return "悪化"
    if cur >= 0 > prev:
        return "改善"
    if cur > prev + 0.05:
        return "加速"
    if cur < prev - 0.05:
        return "減速"
    return "横ばい"


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


def theme_phrase(industry: str = "", sector: str = "", symbol: str = "", company: str = "") -> str:
    text = f"{industry} {sector} {symbol} {company}".lower()
    ticker = clean_text(symbol).upper()
    if ticker in {"STX", "WDC"} or any(term in text for term in ("seagate", "western digital")):
        return "HDD/ストレージ。AIデータ保管需要とメモリ/ストレージ再評価に連動する銘柄"
    if ticker == "LITE" or "lumentum" in text:
        return "光通信部品/フォトニクス。AIデータセンター向け光接続需要の中心寄り"
    if ticker == "LWLG" or "lightwave" in text:
        return "光変調器/フォトニクス材料。商用化ニュースで評価が変わりやすい小型材料株"
    if ticker in {"VSAT", "IRDM", "SATS", "SATL"} or any(term in text for term in ("viasat", "iridium", "echostar", "satellogic")):
        return "衛星通信/地球観測。宇宙・通信インフラ更新の需要確認が焦点"
    if ticker == "TTMI" or "ttm technologies" in text:
        return "PCB/高密度基板。AIサーバー、防衛、通信インフラ向け需要に連動"
    if ticker in {"FORM", "KLIC", "UCTT"} or any(term in text for term in ("formfactor", "kulicke", "ultra clean")):
        return "半導体製造装置/検査周辺。AI半導体投資の波及を受けやすい銘柄"
    if ticker == "SIMO" or "silicon motion" in text:
        return "メモリコントローラ。NAND/ストレージサイクル再評価の受け皿"
    if ticker == "TSEM" or "tower semiconductor" in text:
        return "特殊プロセス半導体ファウンドリ。アナログ/産業向け需要の再評価候補"
    if ticker == "VSH" or "vishay" in text:
        return "ディスクリート/受動部品。産業・車載・電力周辺の在庫循環改善が焦点"
    if ticker == "MTSI" or "macom" in text:
        return "RF/光・通信半導体。データセンター、通信、防衛向け需要を追う銘柄"
    if ticker == "SMTC" or "semtech" in text:
        return "アナログ/接続半導体。データセンター、IoT、光通信寄りの再評価候補"
    if ticker == "WULF" or "terawulf" in text:
        return "電力付きデータセンター/暗号資産マイニング。AI計算需要への転換余地が焦点"
    if ticker == "BW" or "babcock" in text:
        return "産業用ボイラー/環境設備。大型案件とエネルギー設備更新で評価が変わる銘柄"
    if ticker == "FIX" or "comfort systems" in text:
        return "機械・電気設備工事。データセンター建設需要と受注残の厚さが焦点"
    if ticker == "CRS" or "carpenter" in text:
        return "特殊金属材料。航空宇宙、防衛、産業向け素材需要の再評価候補"
    if ticker in {"NBR", "KGS", "KOS"} or any(term in text for term in ("nabors", "kodiak gas", "kosmos")):
        return "エネルギー設備/資源開発。商品市況よりも稼働率・契約・キャッシュ創出を確認"
    if ticker == "MRCY" or "mercury systems" in text:
        return "防衛エレクトロニクス。防衛予算と受注回復が見立ての中心"
    if ticker == "NVT" or "nvent" in text:
        return "電気設備/筐体・接続部品。データセンターと産業設備投資の受益候補"
    if ticker == "CECO" or "ceco environmental" in text:
        return "環境・産業設備。排ガス処理や水処理など規制対応需要が焦点"
    if ticker == "BELFB" or "bel fuse" in text:
        return "電子部品/接続部品。電源・ネットワーク機器向け需要の回復を追う銘柄"
    if ticker == "GTX" or "garrett" in text:
        return "車載ターボ/自動車部品。商用車・ハイブリッド周辺の需要回復が焦点"
    if ticker == "NOK" or "nokia" in text:
        return "通信設備。ネットワーク更新と光/データセンター寄り事業の再評価候補"
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
    ret = safe_float(ret_since_entry)
    if math.isfinite(ret) and ret >= 5.0:
        return f"価格は入口後{pct_text(ret)}まで伸びた大化け組で、過熱より崩れの有無を優先確認"
    if state == "weakening":
        return "価格は弱まり始めており、上位保有候補からは一段落ちる"
    if state == "early_trend_unconfirmed":
        return "価格は初動寄りで、長期移動平均線の裏付けはまだ不足"
    if not state:
        return ""
    return {
        "extended_but_intact": "価格は伸び切り気味でも主要移動平均線上を維持",
        "strong": "価格は10・20・50日線の並びが良く、上昇局面を維持",
        "constructive": "価格は初動後の押し目から再上昇を試す段階",
        "mixed": "価格は強弱混在で、出来高や業績の裏取りが必要",
    }.get(state, "")


def volume_phrase(volume_state: str, dv_persistence: Any = None, up_down_ratio: Any = None) -> str:
    state = clean_text(volume_state)
    dv = safe_float(dv_persistence)
    up_down = safe_float(up_down_ratio)
    if math.isfinite(dv) and math.isfinite(up_down):
        if dv >= 2.0 and up_down >= 1.3:
            return f"出来高維持率{ratio_text(dv)}、上昇日出来高比{ratio_text(up_down)}で買い需要が濃い"
        if dv >= 1.25 and up_down >= 1.0:
            return f"出来高維持率{ratio_text(dv)}、上昇日優勢で急騰後の需要が残る"
        if dv < 0.8 or up_down < 0.7:
            return f"出来高維持率{ratio_text(dv)}、上昇日出来高比{ratio_text(up_down)}で資金流入は鈍化"
    if math.isfinite(dv):
        if dv >= 2.0:
            return f"出来高維持率{ratio_text(dv)}で、急騰後も売買代金が落ちにくい"
        if dv < 0.8:
            return f"出来高維持率{ratio_text(dv)}で、初動後の関心低下に注意"
    if state == "fading":
        return "出来高は鈍化気味で、認識ズレの持続力には注意"
    return ""


def news_signal_phrase(news_text: str = "") -> str:
    text = clean_text(news_text)
    lower = text.lower()
    if not lower:
        return ""
    if "ニュース・開示:" in text:
        return text.split("|", 1)[0].strip(" 。")
    signals = []
    contract_amount = re.search(r"\$([0-9]+(?:\.[0-9]+)?)\s*(million|billion)[^|.]{0,80}contract", lower)
    if contract_amount:
        unit = "億ドル" if contract_amount.group(2) == "billion" else "百万ドル"
        signals.append(f"{contract_amount.group(1)}{unit}契約")
    revenue_growth = re.search(r"(?:grew revenue|revenue grew|revenue growth)[^0-9]{0,20}([0-9]+(?:\.[0-9]+)?)%", lower)
    if revenue_growth:
        signals.append(f"売上{revenue_growth.group(1)}%成長報道")
    if "backlog" in lower or "受注残" in lower:
        signals.append("受注残")
    if any(term in lower for term in ("contract", "award", "order", "booking", "契約", "受注")) and not contract_amount:
        signals.append("受注・契約")
    if any(term in lower for term in ("fund disclosed", "institutional", "13f", "stake", "shares worth", "大量保有")):
        signals.append("機関投資家の買い")
    if any(term in lower for term in ("revenue", "sales", "売上")) and not revenue_growth:
        signals.append("売上成長")
    if any(term in lower for term in ("red flag", "class action", "lawsuit", "investigation", "overvalued")):
        signals.append("警戒見出し")
    if not signals:
        return ""
    unique = list(dict.fromkeys(signals))
    return "ニュース文脈は" + "・".join(unique[:3]) + "が焦点"


def market_size_phrase(market_cap: Any = None) -> str:
    cap = safe_float(market_cap)
    if not math.isfinite(cap):
        return ""
    if cap < 300_000_000:
        return "超小型で値幅は出やすいが、増資や流動性の確認が重要"
    if cap < 1_000_000_000:
        return "小型株で、材料が株価に反映されやすい"
    if cap > 50_000_000_000:
        return "大型株のため、大化けには業績加速の継続が必要"
    return ""


def business_demand_phrase(
    industry: str = "",
    sector: str = "",
    symbol: str = "",
    company: str = "",
    news_text: str = "",
    fundamental_state: str = "",
    revenue_yoy: Any = None,
) -> str:
    text = f"{industry} {sector} {symbol} {company} {news_text}".lower()
    ticker = clean_text(symbol).upper()
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
        if ticker in {"STX", "WDC"}:
            return "クラウド/AI向けデータ保管需要が強く、価格上昇と業績改善を支える"
        if ticker in {"LITE", "MTSI", "LWLG"}:
            return "AIデータセンターの光接続需要が、受注・契約ニュースの重要な裏取りになる"
        if ticker in {"TTMI", "FORM", "KLIC", "UCTT"}:
            return "AI半導体投資の周辺需要が強く、受注・稼働率の維持が見立ての核心"
        if ticker in {"VSAT", "IRDM", "SATS", "SATL"}:
            return "衛星サービスや通信インフラ契約が続くかが、再評価の持続条件"
        if ticker in {"FIX", "NVT", "CECO", "BW"}:
            return "設備投資案件と受注残の積み上がりが、業績再評価の確認材料"
        if ticker in {"NBR", "KGS", "KOS"}:
            return "エネルギー関連契約と稼働率が、資源市況だけに依存しない支えになる"
        return "受注・契約・顧客需要の裏取りが、再評価の持続力を支える"
    if fundamental_state == "structural_proxy_confirmed" and structural:
        return "業種需要と中期上昇が重なり、事業側の再評価余地がある"
    return ""


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
        "medium": f"株式需給リスクは{label}。増資、売り出し、大株主売りの監視は必要",
        "high": f"株式需給リスクは{label}。価格が強くても資金調達や売り圧力を優先監視",
    }.get(severity, "")
    if adr_or_non_us:
        base = (base + "。" if base else "") + "米国外企業のため、為替・上場形態・開示タイミングも確認"
    return base


def fundamental_phrase(
    fundamental_state: str,
    ret126: Any = None,
    ret252: Any = None,
    revenue_yoy: Any = None,
    revenue_qoq: Any = None,
    revenue_yoy_prev: Any = None,
    eps: Any = None,
    eps_yoy: Any = None,
    eps_qoq: Any = None,
    eps_yoy_prev: Any = None,
    eps_qoq_prev: Any = None,
) -> str:
    rev = safe_float(revenue_yoy)
    rev_qoq = safe_float(revenue_qoq)
    rev_direction = value_direction(revenue_yoy, revenue_yoy_prev)
    eps_yoy_value = safe_float(eps_yoy)
    eps_qoq_value = safe_float(eps_qoq)
    eps_yoy_direction = value_direction(eps_yoy, eps_yoy_prev)
    eps_qoq_direction = value_direction(eps_qoq, eps_qoq_prev)
    fragments: list[str] = []
    if math.isfinite(rev):
        direction = f"で{rev_direction}" if rev_direction else ""
        if rev >= 1.0:
            fragments.append(f"売上は前年同期比{pct_text(rev)}{direction}、別格の伸び")
        elif rev >= 0.5:
            fragments.append(f"売上は前年同期比{pct_text(rev)}{direction}、高成長が明確")
        elif rev >= 0.25:
            fragments.append(f"売上は前年同期比{pct_text(rev)}{direction}、価格上昇の裏付けあり")
        elif rev > 0:
            fragments.append(f"売上は前年同期比{pct_text(rev)}、成長はあるが加速確認は必要")
        else:
            fragments.append(f"売上は前年同期比{pct_text(rev)}で、業績面の裏付けは弱い")
    if math.isfinite(rev_qoq) and abs(rev_qoq) >= 0.08:
        fragments.append(f"売上は前期比{pct_text(rev_qoq)}")
    eps_parts = []
    if math.isfinite(eps_yoy_value):
        direction = f"で{eps_yoy_direction}" if eps_yoy_direction else ""
        eps_parts.append(f"前年同期比{pct_text(eps_yoy_value)}{direction}")
    if math.isfinite(eps_qoq_value):
        direction = f"で{eps_qoq_direction}" if eps_qoq_direction else ""
        eps_parts.append(f"前期比{pct_text(eps_qoq_value)}{direction}")
    if eps_parts:
        fragments.append("1株利益は" + "、".join(eps_parts))
    if fragments:
        return "。".join(fragments[:3])
    ret126_text = pct_text(ret126)
    if fundamental_state == "structural_proxy_confirmed":
        return f"業種文脈と中期上昇が揃い、構造変化候補として見やすい（126日{ret126_text}）"
    return ""


def estimate_phrase(
    *,
    next_quarter_revenue_growth: Any = None,
    next_quarter_eps_growth: Any = None,
    current_year_revenue_growth: Any = None,
    current_year_eps_growth: Any = None,
    next_year_revenue_growth: Any = None,
    next_year_eps_growth: Any = None,
    estimate_snapshot_date: str = "",
) -> str:
    nq_rev = safe_float(next_quarter_revenue_growth)
    cy_rev = safe_float(current_year_revenue_growth)
    cy_eps = safe_float(current_year_eps_growth)
    ny_rev = safe_float(next_year_revenue_growth)
    ny_eps = safe_float(next_year_eps_growth)

    parts: list[str] = []
    if math.isfinite(nq_rev) and abs(nq_rev) >= 0.08:
        parts.append(f"来期売上予想は直近四半期比{pct_text(nq_rev)}")
    if math.isfinite(cy_rev) and abs(cy_rev) >= 0.08:
        parts.append(f"今期売上予想は過去4四半期比{pct_text(cy_rev)}")
    if math.isfinite(cy_eps) and abs(cy_eps) >= 0.15:
        parts.append(f"今期1株利益予想は過去4四半期比{pct_text(cy_eps)}")
    if math.isfinite(ny_rev) and abs(ny_rev) >= 0.08:
        parts.append(f"来年売上予想は今期予想比{pct_text(ny_rev)}")
    if math.isfinite(ny_eps) and abs(ny_eps) >= 0.15:
        parts.append(f"来年1株利益予想は今期予想比{pct_text(ny_eps)}")
    if not parts:
        return ""
    suffix = ""
    if clean_text(estimate_snapshot_date):
        suffix = "（予想取得日あり）"
    return "市場予想: " + "、".join(parts[:3]) + suffix


def thesis_phrase(thesis_state: str, thesis_substate: str) -> str:
    if thesis_substate == "mixed_strong":
        return "見立ては強弱混在だが、価格と出来高は上位候補に近い"
    if thesis_state == "thesis_damaged":
        return "見立ては傷み気味で、監視は機械的な出口ルール優先"
    return ""


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
    revenue_qoq: Any = None,
    revenue_yoy_prev: Any = None,
    eps: Any = None,
    eps_yoy: Any = None,
    eps_qoq: Any = None,
    eps_yoy_prev: Any = None,
    eps_qoq_prev: Any = None,
    next_quarter_revenue_growth_est: Any = None,
    next_quarter_eps_growth_est: Any = None,
    current_year_revenue_growth_est: Any = None,
    current_year_eps_growth_est: Any = None,
    next_year_revenue_growth_est: Any = None,
    next_year_eps_growth_est: Any = None,
    estimate_snapshot_date: str = "",
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
        theme_phrase(industry, sector, symbol, company),
        fundamental_phrase(
            fundamental,
            ret126,
            ret252,
            revenue_yoy,
            revenue_qoq,
            revenue_yoy_prev,
            eps,
            eps_yoy,
            eps_qoq,
            eps_yoy_prev,
            eps_qoq_prev,
        ),
        estimate_phrase(
            next_quarter_revenue_growth=next_quarter_revenue_growth_est,
            next_quarter_eps_growth=next_quarter_eps_growth_est,
            current_year_revenue_growth=current_year_revenue_growth_est,
            current_year_eps_growth=current_year_eps_growth_est,
            next_year_revenue_growth=next_year_revenue_growth_est,
            next_year_eps_growth=next_year_eps_growth_est,
            estimate_snapshot_date=estimate_snapshot_date,
        ),
        news_signal_phrase(news_text),
        market_size_phrase(market_cap),
        volume_phrase(volume_state, dv_persistence, up_down_ratio),
        business_demand_phrase(industry, sector, symbol, company, news_text, fundamental, revenue_yoy),
        market_part if abs(safe_float(ret60_resid_spy, 0.0)) >= 0.15 else "",
        price_phrase(price_state, ret_since_entry),
        supply_phrase(
            supply_severity,
            adr_or_non_us,
            market_cap=market_cap,
            avg_dollar_volume20=avg_dollar_volume20,
            news_text=news_text,
        ),
        thesis_phrase(thesis_state, thesis_substate),
    ]
    return trim_summary("。".join(part.rstrip("。") for part in parts if clean_text(part)) + "。", max_chars)
