# Tore-Ken - Recognition Gap EP 7層ランキング

Tore-Kenは、米国株約3000銘柄の日足データから「Recognition Gap EP」を抽出し、7層評価で監視優先順位を作るダッシュボードです。旧MomentumXのStrong Stocks機能は互換用に残しつつ、主出力はRecognition Gap EPランキングへ変更しています。

## 目的

大化け候補は、単に株価が上がった銘柄ではなく、次の条件が重なった銘柄として扱います。

- EPまたは強い価格変化を起点に、価格構造が崩れていない
- EP後も出来高とドル出来高が維持されている
- 10日移動平均線押し目型の入口条件を満たしている
- バイオなどイベント一点賭け銘柄を原則除外する
- 業種、決算、会社見通し、市場予想、受注、受注残、株式需給リスク、銘柄一致を7層で監査する
- 出口は生成AIではなく、あらかじめ決めた機械ルールを優先する

このランキングは自動売買ではありません。保有監査、乗り換え候補比較、X投稿用の研究リストです。

## 実装済みの主要機能

- `backend/recognition_gap_ranking.py`
  - `price_data_ohlcv.pkl` を読み、Recognition Gap EP候補を抽出します。
  - `industry_theme_ep_ex_biotech / pullback10 / stage2_or_atr8` 系の運用仕様に合わせた日足ランキングを作ります。
  - FMPの四半期決算、ニュース/プレスリリース、アナリスト予想を取得し、会社見通しと来期/今期/来年予想を構造化します。
  - numbaが利用可能な場合、True Range計算をJIT化します。
  - 出力:
    - `data/recognition_gap_ranking.json`
    - `data/recognition_gap_ranking.csv`

- `backend/opencode_consensus.py`
  - opencode goの4モデル合議用プロンプトを生成します。
  - 対象モデル:
    - `opencode-go/kimi-k2.6`
    - `opencode-go-minimax/minimax-m2.7`
    - `opencode-go/glm-5.1`
    - `opencode-go/deepseek-v4-pro`
  - まとめ役は現状 `opencode-go/deepseek-v4-pro` を推奨します。
  - 出力:
    - `data/opencode_consensus_prompt.md`

- `backend/x_ranking_publisher.py`
  - 検出された全銘柄を5銘柄ずつ白黒PNGに分割します。
  - 表示順はデフォルトで含み益順ではなく、7層の優先保有/監視順です。`--sort-by rank` でCSV順位をそのまま使えます。
  - 画像には `順位 / 銘柄 / 入口日 / 含み益 / 要点` を表示します。
  - 本文は `$SIMO $BW ...` のように20銘柄分のティッカーだけを投稿できます。
  - 検証時は `--all-tickers-in-text` でCSV内の全ティッカーを本文に含められます。
  - FMP見出し貼り付け型の要点は使わず、価格・出来高・業種文脈・業績・株式需給リスク・事業需給を統合した要点に寄せます。
  - `X_POST_ENABLED=false` の場合は投稿せず画像だけ作成します。

- `backend/summary_style.py`
  - 画像やランキングCSVに載せる要点の共通スタイルを定義します。
  - Codexがこれまで書いていたような、売買命令ではなく「見立ての質」を説明する300字要点を作ります。
  - 全銘柄に同じ定型文を並べず、売上前年比、売上前期比、1株利益の前年比/前期比、受注・契約、時価総額リスクなど、銘柄ごとに差が出る材料だけを採用します。
  - 1株利益は単体数値を出さず、前年比・前期比・加速/減速/改善/悪化として表現します。
  - ニュース/プレスリリースは、入口日前60日、入口日から判定日、直近14日の3区分で取得し、受注残・受注・契約・機関投資家買い・警戒見出しを構造化します。
  - 会社見通しや市場予想は、来期売上、今期売上/1株利益、来年売上/1株利益の伸びとして要点に入れます。
  - `supply_risk_severity` が無いCSVでも、時価総額、ドル出来高、増資、売り出し、ワラント、転換社債、大株主売り系ニュースから内部値を推定し、表示では `低い / 中程度 / 高い` に変換します。

- FastAPI
  - `/api/recognition-gap-ranking`
  - `/api/x-ranking-image/{yyyymmdd}/{filename}`
  - 既存の `/api/data` と `/api/daily/{date_key}` にも `recognition_gap_ranking` を含めます。

- Frontend
  - 画面の主セクションを「7層ランキング」に変更しました。
  - `順位 / 銘柄 / 入口日 / 含み益 / 状態 / 要点` を表示します。

## 必要API

`.env.example` を `.env` にコピーして値を入れてください。実キーをGitHubへコミットしないでください。

必須:

```text
FMP_API_KEY=
OPENCODE_GO_API_KEY=
```

X投稿を使う場合:

```text
X_API_KEY=
X_API_SECRET=
X_ACCESS_TOKEN=
X_ACCESS_TOKEN_SECRET=
X_POST_ENABLED=false
X_INCLUDE_TITLE=false
```

任意:

```text
DISCORD_BOT_TOKEN=
DISCORD_CHANNEL_ID=
OPENAI_API_KEY=
FRED_API_KEY=
RECOGNITION_GAP_ESTIMATE_FETCH_LIMIT=150
```

Webullの売買機能と売買ボタンは実装していません。将来、読み取り専用レポートを復活させる場合だけ `.env.example` のWebull項目を使います。

## 実行方法

依存関係:

```bash
pip install -r backend/requirements.txt
```

データ更新からランキング生成まで:

```bash
python -m backend.data_fetcher
```

ランキングだけ再生成:

```bash
python -m backend.recognition_gap_ranking --top-n 20
```

特定日までのデータでランキング:

```bash
python -m backend.recognition_gap_ranking --asof-date 2026-05-22 --top-n 20
```

X用画像だけ生成:

```bash
python -m backend.x_ranking_publisher --ranking-csv data/recognition_gap_ranking.csv --asof-label 2026-05-22
```

`--top-n` 未指定、`--top-n 0`、または `--top-n all` なら、CSVにある検出済み銘柄をすべて画像化します。X本文だけは通常20銘柄に制限し、検証時に全ティッカーを本文にも入れる場合は `--all-tickers-in-text` を使います。
画像と同じ優先保有順・要点でCSVも残す場合は `--out-csv` を使います。

Xへ投稿:

```bash
python -m backend.x_ranking_publisher --ranking-csv data/recognition_gap_ranking.csv --asof-label 2026-05-22 --post-x
```

サーバー起動:

```bash
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

## 毎日運用

市場クローズ後に以下を実行します。

1. FMPでユニバースと企業属性を更新
2. yfinance/FMP由来の日足OHLCVを更新
3. FMPの四半期損益から売上・1株利益の前年比/前期比を更新
4. FMPのニュース/プレスリリースから受注残、受注、契約、会社見通し、機関投資家買い、警戒見出しを更新
5. FMPのアナリスト予想から来期、今期、来年の売上・1株利益予想を更新
6. Recognition Gap EP候補を抽出
7. 7層評価ラベルを付与
8. opencode go合議用プロンプトを生成
9. 検出された全銘柄を5件ずつX用画像に分割して生成
10. `X_POST_ENABLED=true` の場合だけXへ投稿

## 7層評価

1. データ整合性: 銘柄、会社名、業種、米国外企業混入を確認
2. 価格トレンド: 10・20・50・150・200日移動平均線、上昇局面、伸び切りでも崩れていないか
3. 出来高・資金流入の持続: EP後の出来高維持、ドル出来高、上昇日出来高比
4. 株式需給リスクと流動性: 流動性、時価総額、増資、売り出し、ワラント、転換社債、大株主売り
5. 材料の質: 業種再評価、決算、会社見通し、受注、受注残、ニュース文脈
6. 業績・受注確認: 売上、1株利益、来期/今期/来年予想、構造変化、未確認なら保守的に扱う
7. 見立て状態: `thesis_intact / thesis_mixed / thesis_damaged` とサブ分類

## 注意

- ランキングは「大化け予想ランキング」ですが、売買命令ではありません。
- 先見バイアス防止のため、`--asof-date` 実行時はその日以前の価格データだけを使います。
- アナリスト予想は取得日のあるスナップショットとして扱います。古い判定日のバックテストでは、判定日から大きく後に取得した予想データを使いません。
- 生成AIの役割は説明と監査です。出口判断は機械ルールを優先します。
- ニュースを使う場合は、ティッカー、会社名、業種の一致確認を必須にします。
