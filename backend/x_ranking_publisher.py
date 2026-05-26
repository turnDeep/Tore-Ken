from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont


load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_CSV = DATA_DIR / "recognition_gap_ranking.csv"
DEFAULT_OUT_ROOT = DATA_DIR / "x_ranking_posts"


def parse_limit(value: Any = None) -> int | None:
    """Return None for all rows; positive int for an explicit cap."""
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"", "0", "all", "none", "unlimited"}:
        return None
    number = int(text)
    return number if number > 0 else None


def clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat", "null"} else text


def fnum(value: Any, default: float = float("nan")) -> float:
    text = clean(value).replace(",", "")
    if not text:
        return default
    if text.endswith("%"):
        try:
            return float(text[:-1]) / 100.0
        except Exception:
            return default
    try:
        return float(text)
    except Exception:
        return default


def pct_text(value: Any) -> str:
    text = clean(value)
    if text.endswith("%"):
        return text
    number = fnum(value)
    if number != number:
        return "-"
    if abs(number) <= 20:
        return f"{number * 100:+.1f}%"
    return f"{number:+.1f}%"


def get_first(row: pd.Series, names: list[str]) -> str:
    for name in names:
        if name in row.index:
            value = clean(row.get(name))
            if value:
                return value
    return ""


def load_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    candidates = [
        r"C:\Windows\Fonts\meiryob.ttc" if bold else r"C:\Windows\Fonts\meiryo.ttc",
        r"C:\Windows\Fonts\YuGothB.ttc" if bold else r"C:\Windows\Fonts\YuGothM.ttc",
        r"C:\Windows\Fonts\msgothic.ttc",
        r"C:\Windows\Fonts\arialbd.ttf" if bold else r"C:\Windows\Fonts\arial.ttf",
    ]
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    if not text:
        return 0
    box = draw.textbbox((0, 0), text, font=font)
    return int(box[2] - box[0])


def wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int, max_lines: int) -> list[str]:
    text = clean(text).replace("\r", " ").replace("\n", " ")
    if not text:
        return ["-"]
    lines: list[str] = []
    current = ""
    for char in text:
        candidate = current + char
        if text_width(draw, candidate, font) <= max_width:
            current = candidate
            continue
        if current:
            lines.append(current)
        current = char
        if len(lines) >= max_lines:
            break
    if current and len(lines) < max_lines:
        lines.append(current)
    if len(lines) == max_lines and len("".join(lines)) < len(text):
        lines[-1] = lines[-1].rstrip("、。,. ") + "..."
    return lines[:max_lines]


def trim_chars(text: str, max_chars: int) -> str:
    text = clean(text).replace("\r", " ").replace("\n", " ")
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip("、。,. ") + "…"


def normalize_rows(csv_path: Path, top_n: int | None = None) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"ranking CSV not found: {csv_path}")
    df = pd.read_csv(csv_path)
    if "rank" in df.columns:
        df["_rank_sort"] = pd.to_numeric(df["rank"], errors="coerce")
        df = df.sort_values("_rank_sort", na_position="last")

    rows: list[dict[str, str]] = []
    limit = parse_limit(top_n)
    source = df if limit is None else df.head(limit)
    for idx, (_, row) in enumerate(source.iterrows(), start=1):
        symbol = get_first(row, ["symbol", "ticker", "銘柄"]).upper().lstrip("$")
        if not symbol:
            continue
        summary = get_first(row, ["seven_layer_summary_ja", "summary_ja", "summary", "要点"])
        if not summary:
            price = get_first(row, ["price_trend"])
            volume = get_first(row, ["volume_demand_durability", "volume_state"])
            supply = get_first(row, ["supply_risk_severity", "institutional_and_supply"])
            catalyst = get_first(row, ["catalyst_quality"])
            fundamental = get_first(row, ["fundamental_confirmation"])
            industry = get_first(row, ["industry", "Industry"])
            summary = (
                f"{industry}。価格は{price}、出来高は{volume}、供給リスクは{supply}。"
                f"カタリストは{catalyst}、ファンダ確認は{fundamental}。"
            )

        rows.append(
            {
                "rank": clean(row.get("rank")) or str(idx),
                "symbol": symbol,
                "entry": get_first(row, ["entry_date", "Entry", "entry"]),
                "return": pct_text(get_first(row, ["return_since_entry", "含み益", "return", "gain"])),
                "summary": trim_chars(summary, 300),
            }
        )
    return rows


def draw_ranking_image(rows: list[dict[str, str]], start_rank: int, end_rank: int, asof_label: str, output_path: Path) -> None:
    width, height = 1600, 2640
    bg = "#171717"
    line = "#303030"
    white = "#f4f4f4"
    subtle = "#a8a8a8"

    image = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(image)

    header_font = load_font(30, bold=True)
    cell_font = load_font(30)
    summary_font = load_font(25, bold=True)
    footer_font = load_font(22)

    margin_x = 24
    header_y = 20
    table_top = 64
    row_h = 500
    col_rank = 24
    col_symbol = 220
    col_entry = 430
    col_return = 690
    col_summary = 930
    right = width - 24

    draw.text((col_rank, header_y), "順位", font=header_font, fill=white)
    draw.text((col_symbol, header_y), "銘柄", font=header_font, fill=white)
    draw.text((col_entry, header_y), "Entry", font=header_font, fill=white)
    draw.text((col_return, header_y), "含み益", font=header_font, fill=white)
    draw.text((col_summary, header_y), "要点", font=header_font, fill=white)
    draw.line((margin_x, table_top, right, table_top), fill=line, width=2)

    for i, row in enumerate(rows):
        y = table_top + i * row_h
        draw.line((margin_x, y, right, y), fill=line, width=1)
        draw.text((col_rank, y + 24), row["rank"], font=cell_font, fill=white)
        draw.text((col_symbol, y + 24), row["symbol"], font=cell_font, fill=white)
        draw.text((col_entry, y + 24), row["entry"] or "-", font=cell_font, fill=white)
        draw.text((col_return, y + 24), row["return"], font=cell_font, fill=white)

        body_y = y + 20
        for text in wrap_text(draw, row["summary"], summary_font, right - col_summary - 8, 13):
            draw.text((col_summary, body_y), text, font=summary_font, fill=white)
            body_y += 34

    draw.line((margin_x, table_top + len(rows) * row_h, right, table_top + len(rows) * row_h), fill=line, width=1)
    footer = f"{asof_label} / Rank {start_rank}-{end_rank} / exitは機械ルール優先"
    draw.text((margin_x, height - 48), footer, font=footer_font, fill=subtle)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path, "PNG", optimize=True)


def render_images(rows: list[dict[str, str]], out_dir: Path, asof_label: str) -> list[Path]:
    paths: list[Path] = []
    chunk_count = (len(rows) + 4) // 5
    for chunk_index in range(chunk_count):
        start = chunk_index * 5
        chunk = rows[start : start + 5]
        if not chunk:
            break
        start_rank = start + 1
        end_rank = start + len(chunk)
        path = out_dir / f"x_ranking_{start_rank:02d}_{end_rank:02d}.png"
        draw_ranking_image(chunk, start_rank, end_rank, asof_label, path)
        paths.append(path)
    return paths


def build_post_text(
    rows: list[dict[str, str]],
    asof_label: str,
    include_title: bool = False,
    max_symbols: int | None = 20,
) -> str:
    post_rows = rows if max_symbols is None else rows[:max_symbols]
    tickers = " ".join(f"${row['symbol']}" for row in post_rows)
    if include_title:
        return f"Recognition Gap EP 7層ランキング {asof_label}\n{tickers}"
    return tickers


def env_first(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def post_to_x(text: str, image_paths: list[Path]) -> dict[str, Any]:
    try:
        import tweepy
    except Exception as exc:
        raise RuntimeError("tweepy is not installed. Run: pip install tweepy") from exc

    api_key = env_first("X_API_KEY", "TWITTER_API_KEY")
    api_secret = env_first("X_API_SECRET", "TWITTER_API_SECRET")
    access_token = env_first("X_ACCESS_TOKEN", "TWITTER_ACCESS_TOKEN")
    access_secret = env_first("X_ACCESS_TOKEN_SECRET", "TWITTER_ACCESS_TOKEN_SECRET")
    missing = [
        name
        for name, value in {
            "X_API_KEY": api_key,
            "X_API_SECRET": api_secret,
            "X_ACCESS_TOKEN": access_token,
            "X_ACCESS_TOKEN_SECRET": access_secret,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing X credentials: {', '.join(missing)}")

    auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_secret)
    api_v1 = tweepy.API(auth)
    media_ids = [api_v1.media_upload(str(path)).media_id_string for path in image_paths]

    client = tweepy.Client(
        consumer_key=api_key,
        consumer_secret=api_secret,
        access_token=access_token,
        access_token_secret=access_secret,
    )
    response = client.create_tweet(text=text, media_ids=media_ids)
    return response.data or {}


def publish(
    ranking_csv: Path = DEFAULT_CSV,
    asof_label: str | None = None,
    top_n: int | None = None,
    post_x: bool = False,
    include_title: bool = False,
    post_text_limit: int | None = 20,
    out_dir: Path | None = None,
) -> dict[str, Any]:
    rows = normalize_rows(ranking_csv, top_n=top_n)
    if asof_label is None:
        asof_label = datetime.now().strftime("%Y-%m-%d")
    out_dir = out_dir or DEFAULT_OUT_ROOT / asof_label.replace("-", "")
    image_paths = render_images(rows, out_dir, asof_label)
    text = build_post_text(rows, asof_label, include_title=include_title, max_symbols=post_text_limit)

    result: dict[str, Any] = {
        "asof_label": asof_label,
        "text": text,
        "images": [str(path) for path in image_paths],
        "posted": False,
    }
    if post_x:
        result["x_response"] = post_to_x(text, image_paths)
        result["posted"] = True
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Render and optionally post Recognition Gap EP ranking images to X.")
    parser.add_argument("--ranking-csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--asof-label", default=None)
    parser.add_argument("--top-n", default="0", help="Positive number caps rows; 0/all means all rows in the CSV.")
    parser.add_argument("--post-x", action="store_true")
    parser.add_argument("--include-title", action="store_true")
    parser.add_argument("--post-text-limit", type=int, default=20)
    parser.add_argument("--all-tickers-in-text", action="store_true")
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    result = publish(
        ranking_csv=args.ranking_csv,
        asof_label=args.asof_label,
        top_n=parse_limit(args.top_n),
        post_x=args.post_x,
        include_title=args.include_title,
        post_text_limit=None if args.all_tickers_in_text else args.post_text_limit,
        out_dir=args.out_dir,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
