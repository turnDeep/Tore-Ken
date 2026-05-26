from __future__ import annotations

import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

MODEL_IDS = [
    "opencode-go/kimi-k2.6",
    "opencode-go-minimax/minimax-m2.7",
    "opencode-go/glm-5.1",
    "opencode-go/deepseek-v4-pro",
]

CONSENSUS_SYNTHESIZER = "opencode-go/deepseek-v4-pro"


def build_consensus_prompt(ranking_rows: list[dict[str, Any]], asof_date: str) -> str:
    rows_json = json.dumps(ranking_rows, ensure_ascii=False, indent=2)
    return f"""# Recognition Gap EP 7層ランキング監査

asof_date: {asof_date}

あなたは売買命令を出す担当ではありません。役割は、機械抽出された候補について
「市場の認識ズレがまだ残っているか」「出来高が本当に維持されているか」
「決算・受注・バックログ・業種文脈に構造変化があるか」を監査し、
Codexがこれまで書いてきたような日本語の要点に圧縮することです。

## 使うモデル

- Kimi: 事実関係、銘柄エンティティ、ニュース混入チェック
- MiniMax: 価格、出来高、需給、供給リスクの違和感チェック
- GLM: 業種、競合、構造テーマ、ファンダ文脈チェック
- DeepSeek: まとめ役。各モデルの意見を統合して最終要点を作る

## 重要ルール

- asof_dateより後の情報は絶対に使わない。
- exit判断はしない。exitは stage2_or_atr8 の機械ルールに任せる。
- ティッカー、会社名、業種が一致しないニュースは除外する。
- 価格が伸び切っていても、出来高維持とthesisが崩れていなければ「売り」と書かない。
- 「買い」「売り」「目標株価」ではなく、保有監視・乗り換え比較の質を書く。
- 出来高維持、up/down volume、対SPY残差、供給リスクを重視する。
- 要点はFMPニュース見出しの貼り付けではなく、7層を統合した人間向けコメントにする。

## 要点の文体

良い例:

- `半導体/AIデータセンター周辺。価格は大きく伸びているが崩れておらず、出来高もEP後に持続。売上確認は次決算待ちだが、対SPY残差と業種文脈は強い。供給リスクlowならthesisの質は高く、exitは機械ルール優先。`
- `衛星/防衛寄りの小型テーマ株。出来高はdurableで価格も強いが、規模が小さく供給リスクは監視。大化け色はある一方、決算・受注の裏取りが続くかが焦点。`

悪い例:

- `FMP profileで会社を確認。ニュース文脈: ...`
- `Wall Street Analysts Think ...`
- `買いです。売りです。`

## 出力形式

JSON配列で返す。

```json
[
  {{
    "rank": 1,
    "symbol": "SIMO",
    "stable_thesis_state": "thesis_intact",
    "thesis_substate": "intact_volume_leader",
    "summary_ja": "300字以内の日本語要点",
    "risk_notes_ja": "注意点",
    "entity_check": "clean"
  }}
]
```

## 入力候補

```json
{rows_json}
```
"""


def write_consensus_prompt(ranking_result: dict[str, Any], output_path: Path | None = None) -> Path:
    output_path = output_path or DATA_DIR / "opencode_consensus_prompt.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    prompt = build_consensus_prompt(
        ranking_result.get("ranking", []),
        ranking_result.get("asof_date") or ranking_result.get("date") or "",
    )
    output_path.write_text(prompt, encoding="utf-8")
    return output_path
