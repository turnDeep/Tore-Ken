# Tore-ken (トレけん) Market Dashboard - MomentumX Edition

## 1. 概要 (Overview)

**Tore-ken (トレけん)** は、市場トレンドを把握し、MomentumXロジックに基づいて有望な銘柄を発掘するためのダッシュボードです。

従来のRDTシステムから刷新され、**MomentumX** のスクリーニングロジックを完全移植しました。ATRトレーリングストップ、RSパーセンタイル、ボラティリティ調整済みRS、Zone RSといった高度な指標を組み合わせ、強力なトレンド銘柄（Strong Stocks）を抽出します。

## 2. 主要機能

### 2.1 Strong Stocks (有望銘柄リスト)
市場全体の銘柄から、以下の厳格な基準を満たす銘柄を「Strong Stocks」として抽出します。ダッシュボードには、その中からさらに **ADR% (20日平均変動率) が 4.0% 以上** の「動きのある銘柄」のみが表示されます。

*   **スクリーニング基準 (Entry Criteria)**:
    1.  **ATR Trailing Stop**: Buy状態（強気トレンド）。
    2.  **RS Percentile (1M)**: 80以上（市場の上位20%の強さ）。
    3.  **RS Volatility Adjusted**: HMAの傾きが正（上昇モメンタム）。
    4.  **Zone RS**: Power Zone（Zone 3: RS比率 > 1 かつ モメンタム > 0）。

*   **リスト維持・除外基準 (Persistence/Exit Criteria)**:
    一度リスト入りした銘柄は、以下に該当するまで追跡され続けます。
    1.  **ATR Trailing Stop**: Sell状態（弱気トレンド）に転換。
    2.  **Zone RS**: Power Zone から脱落（Dead, Drift, Liftへ移動）。

*   **運用スケジュール (Weekend Screening)**:
    *   **週末 (金曜データ更新時)**: 新規採用・除外の判定を行います。週足確定ベースでリストを更新します。
    *   **平日 (月〜木)**: リストの銘柄は固定したまま、株価やADR%などの指標のみを毎日更新します。

*   **表示順序**:
    *   エントリー日（リスト入りした日）が新しい順に表示されます。

### 2.2 Market Analysis (市場分析)
S&P 500 (SPY) の日足チャートと独自のトレンド判定を表示します。
*   **Green Zone**: 上昇トレンド（積極投資推奨）。
*   **Red Zone**: 下落トレンド（守備的）。
*   **Neutral**: 中立。

### 2.3 詳細チャート分析
リスト内の銘柄を選択すると、詳細なテクニカルチャートが表示されます。
*   **メインチャート**: ローソク足 + ATR Trailing Stop (緑=Buy, 赤=Sell)。
*   **サブ指標**: Zone RS, RS Percentile, Volatility Adjusted RS, RTI (Range Tightening Indicator)。

### 2.4 リアルタイム監視
*   **Realtime RVol**: 市場開場中、WebSocketを通じてリアルタイムの相対出来高（RVol）を表示します。
    *   **最適化**: APIリソース節約のため、**RTIシグナル（オレンジドット: 嵐の前の静けさ）** が点灯している「ブレイクアウト直前の銘柄」のみをリアルタイム更新します。それ以外の銘柄は `--` と表示されます。

## 3. 技術スタック

- **Backend**: Python 3.12, FastAPI
- **Frontend**: HTML5, CSS3, Vanilla JavaScript (PWA対応)
- **Data Processing**: pandas, numpy, yfinance, mplfinance, numba
- **Database**: JSON storage (日次データ保存)

## 4. ディレクトリ構造

```
.
├── backend/
│   ├── main.py                     # FastAPIアプリケーションサーバー
│   ├── data_fetcher.py             # 全体オーケストレーション
│   ├── screener_service.py         # 新スクリーニング実行サービス (MomentumX)
│   ├── rdt_data_fetcher.py         # データ取得・増分更新ロジック
│   ├── chart_generator_mx.py       # MomentumX仕様のチャート生成
│   ├── market_analysis_logic.py    # 市場分析ロジック (SPY)
│   ├── market_chart_generator.py   # 市場分析チャート生成
│   ├── calculate_atr_trailing_stop.py      # ATR計算モジュール
│   ├── calculate_rs_percentile_histogram.py # RS Percentile計算モジュール
│   ├── calculate_rs_volatility_adjusted.py # Volatility Adj RS計算モジュール
│   ├── calculate_rti.py            # RTI計算モジュール
│   ├── calculate_zone_rs.py        # Zone RS計算モジュール
│   └── stock.csv                   # 監視対象銘柄リスト
├── frontend/
│   ├── index.html                  # ダッシュボードUI
│   ├── app.js                      # フロントエンドロジック
│   └── style.css                   # スタイルシート
├── data/                           # 生成されたデータ・チャート (Git対象外)
└── README.md
```

## 5. セットアップ手順

### 5.1 前提条件
- Python 3.12+
- `pip`

### 5.2 インストール

```bash
# 1. リポジトリをクローン
git clone <repository_url>
cd tore-ken

# 2. 依存関係のインストール
pip install -r backend/requirements.txt
```

### 5.3 データの初期化と実行

```bash
# 1. データの取得とスクリーニングの実行
# 初回は過去5年分のデータを取得するため時間がかかります
python -m backend.data_fetcher fetch

# 2. サーバーの起動
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

ブラウザで `http://localhost:8000` にアクセスしてください。

## 6. 運用

データは日次で更新することを推奨します。`backend/data_fetcher.py` をcronなどで定期実行することで、最新の市場データに基づいたスクリーニングが行われ、通知が送信されます。

## 7. ライセンス

本ソフトウェアは個人利用を目的としています。
