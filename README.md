# Tore-ken (トレけん) Market Dashboard

## 1. 概要 (Overview)

**Tore-ken (トレけん)**（サブタイトル: *トレードに失敗しないためにけんじつに勉強してみた*）は、個人投資家が市場トレンドを把握し、有望な銘柄を発掘するためのシンプルかつ強力なダッシュボードです。

RDT (Reality Down Theory) システムに基づき、S&P 500の市場サイクル（強気/弱気）を判定し、独自のフィルタリングロジックを通過した「Strong Stocks」を提示します。

## 2. 主要機能

### 2.1 Market Analysis (市場分析)
- **6ヶ月チャート**: SPY (S&P 500 ETF) の日足チャートを表示。
- **市場サイクル判定**:
    - **Green Zone**: 強気相場 (Bullish Phase) - 積極的なトレードを推奨。
    - **Red Zone**: 弱気相場 (Bearish Phase) - 防御的なポジションを推奨。
    - **Neutral**: 中立局面。
- **インジケーター**: TSV (Time Segmented Volume) 近似値や StochRSI を使用した独自ロジックにより背景色を自動判定。

### 2.2 Strong Stocks (有望銘柄リスト)
- **RDTスクリーナー**: 市場全体の銘柄から、以下の基準を満たす「強い銘柄」を抽出。
    - RRS (Relative RS Rating)
    - RVol (Relative Volume)
    - ADR% (Average Daily Range)
    - その他、移動平均線や出来高のフィルター
- **リスト表示**: 該当日付における抽出銘柄を一覧表示。

### 2.3 シンプルで直感的なUI
- **デザイン**: 白背景・黒文字を基調とした視認性の高い「シンプルデザイン」。
- **操作性**: 日付スライダーや前後ボタンで過去の分析結果に簡単にアクセス可能。
- **セキュリティ**: 6桁のPINコードによるアクセス制限。

## 3. 技術スタック

- **Backend**: Python 3.12, FastAPI
- **Frontend**: HTML5, CSS3, Vanilla JavaScript
- **Data Processing**: pandas, pandas-ta, yfinance, mplfinance
- **Database**: SQLite / JSON storage (for daily reports)

## 4. セットアップ手順 (Setup)

### 4.1 前提条件
- Python 3.12+
- `pip`

### 4.2 インストール

```bash
# 1. リポジトリをクローン
git clone <repository_url>
cd tore-ken

# 2. 依存関係のインストール
pip install -r requirements.txt
# または
pip install fastapi uvicorn pandas pandas-ta yfinance mplfinance playwright requests
```

### 4.3 環境設定

プロジェクトルートに `.env` ファイルを作成（任意）。設定しない場合はデフォルト値が使用されます。

```env
# 認証用6桁PIN (デフォルト: 123456)
AUTH_PIN=123456
```

### 4.4 データの初期化とサーバー起動

```bash
# 1. データの取得と分析の実行（初回）
python -m backend.data_fetcher fetch

# 2. サーバーの起動
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

ブラウザで `http://localhost:8000` にアクセスしてください。

## 5. 運用・データ更新

データは毎日更新する必要があります。以下のコマンドを手動で実行するか、cronに設定してください。

```bash
# データの更新・チャート生成・スクリーニング実行
python -m backend.data_fetcher fetch
```

### 自動更新 (Cron) の例
`backend/run_job.sh` を使用してcronジョブを設定できます。

```bash
# 平日の毎朝 6:15 に更新
15 6 * * 1-5 /path/to/tore-ken/backend/run_job.sh >> /path/to/tore-ken/logs/cron.log 2>&1
```

## 6. ディレクトリ構造

```
.
├── backend/
│   ├── main.py             # FastAPIアプリケーションサーバー
│   ├── data_fetcher.py     # データ取得・分析・保存のメインスクリプト
│   ├── chart_generator.py  # mplfinanceによるチャート画像生成
│   ├── rdt_logic.py        # 市場分析・スクリーニングのコアロジック
│   └── stock.csv           # 監視対象銘柄リスト
├── frontend/
│   ├── index.html          # ダッシュボードUI
│   ├── app.js              # フロントエンドロジック
│   ├── style.css           # スタイルシート
│   └── assets/             # 生成されたチャート画像など
├── data/                   # 生成されたJSONデータ (Git対象外)
└── README.md
```

## 7. ライセンス

本ソフトウェアは個人利用を目的としています。
