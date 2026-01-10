# HanaView Market Dashboard

## 1. 概要 (Overview)

HanaViewは、個人投資家が毎朝の市場チェックを効率化するための統合ダッシュボードです。
このアプリケーションは、VIX、Fear & Greed Index、米国10年債などの主要な市場指標、S&P 500とNASDAQ 100のヒートマップ、経済指標カレンダー、HWB戦略に基づく銘柄スキャン機能などを一元的に表示します。

データは毎日定時に自動で更新され、Push通知機能により最新情報をタイムリーに受け取ることができます。

## 2. 主要機能

### 2.1 市況分析
- **VIX指数・米国10年債チャート**: 4時間足チャートでボラティリティと金利動向を把握
- **Fear & Greed Index**: 市場センチメントをビジュアルゲージで表示
- **AI解説**: OpenAI APIによる市況サマリーの自動生成

### 2.2 ヒートマップ
- **NASDAQ 100 & S&P 500**: 1日・1週間・1ヶ月のパフォーマンスヒートマップ
- **セクターETF**: 主要11セクターのパフォーマンス
- **AI解説**: 市場トレンドの自動分析

### 2.3 ニュース分析
- **今朝の3行サマリー**: Yahoo Finance等から取得したニュースをAIが要約
- **主要トピック3選**: 重要ニュースの事実・解釈・市場影響を解説

### 2.4 経済指標・決算カレンダー
- **経済指標**: 重要度★★以上の米国経済指標（Monex提供）
- **注目決算**: 主要米国・日本企業の決算スケジュール
- **AI解説**: 各指標・決算の市場への影響分析

### 2.5 HWB 200MA戦略スキャナー（新機能）
- **Russell 3000銘柄の自動スキャン**: HWB（High-Water Mark Breakout）戦略に基づく銘柄検出
- **リアルタイムチャート**: lightweight-chartsによる高度なチャート表示
- **個別銘柄分析**: 任意のティッカーシンボルで詳細分析

### 2.6 Algoタブ（新機能）
- **IBDスタイルスクリーナー**: Momentum 97など6種類の強力なスクリーナー
- **AI戦略分析**: StageAlgoによるガンマ分析・ボラティリティ分析
- **Gemini 3 Flash連携**: 最新AIモデルによる具体的なトレーディング戦略解説
- **権限管理**: 特定ユーザー（ura権限）のみがアクセス可能

### 2.7 セキュリティ＆通知機能（新機能）
- **PIN認証**: 6桁のPINによるアクセス制御
- **Push通知**: データ更新時の自動通知（PWA対応）
- **自動ログイン**: JWTトークンによる30日間の認証維持

## 3. セットアップ手順 (Setup)

### 3.1 前提条件
- Docker & Docker Compose
- OpenAI APIキー（[こちら](https://platform.openai.com/api-keys)から取得）

### 3.2 環境変数の設定

プロジェクトルートに `.env` ファイルを作成し、以下の設定を行います：

```env
# ======================================
# 必須設定（2項目のみ）
# ======================================

# OpenAI APIキー（必須）
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# 認証用6桁PIN（必須）
# デフォルト: 123456（本番環境では必ず変更してください）
AUTH_PIN=123456

# ======================================
# オプション設定
# ======================================

# OpenAIモデル（任意）
OPENAI_MODEL=gpt-4.1

# Push通知の送信元メールアドレス（任意）
VAPID_SUBJECT=mailto:your-email@example.com

# JWTトークン有効期限（日数、デフォルト: 30）
JWT_ACCESS_TOKEN_EXPIRE_DAYS=30

# 特別ティッカー設定（任意）
SPECIAL_TICKERS='{"2025/04/16": ["ASML", "(エーエスエムエル・ホールディングス)"]}'

# Hanaメモファイルのパス（任意）
HANA_MEMO_FILE=backend/hana-memo-202509.txt

# Gemini APIキー（Algoタブで必須）
GEMINI_API_KEY=your_gemini_api_key_here

# FinancialModelingPrep APIキー（Algoタブで推奨）
# 未設定時はyfinanceによるフォールバックモードで動作します
FMP_API_KEY=your_fmp_api_key_here
```

**重要**: 以下のセキュリティキーは初回起動時に自動生成され、`data/security_keys.json`に保存されます。このファイルは必ずバックアップしてください。
- `JWT_SECRET_KEY` (JWTトークン署名用)
- `VAPID_PUBLIC_KEY` (Push通知用公開鍵)
- `VAPID_PRIVATE_KEY` (Push通知用秘密鍵)

### 3.3 起動手順

```bash
# 1. リポジトリをクローン
git clone <repository_url>
cd <repository_directory>

# 2. Dockerコンテナをビルド・起動
docker-compose up -d --build

# 3. 初回起動後、セキュリティキーが生成されたことを確認
# data/security_keys.json の存在を確認
ls -la data/security_keys.json
```

初回起動には数分かかることがあります。起動後、ブラウザで `http://localhost` にアクセスし、設定したPIN（デフォルト: 123456）でログインしてください。

## 4. 手動でのデータ更新 (Manual Data Update)

データはcronによって自動更新されますが、管理者は以下の手順で手動更新も可能です：

```bash
# 1. コンテナ内でbashセッションを開始
docker compose exec app bash

# 2. データ取得（5〜10分程度）
python -m backend.data_fetcher fetch

# 3. レポート生成（AI解説含む）
python -m backend.data_fetcher generate
```

**注意**: `fetch`コマンドは、S&P 500とNASDAQ 100の全銘柄（約600）の情報を取得するため、完了までに5〜10分程度かかります。

## 5. スキャナーの手動実行 (Running Scanners)

### 5.1 HWBスキャナー (200MAタブ)

HWB戦略に基づくRussell 3000銘柄のスキャンを手動で実行できます：

```bash
# コンテナ内で実行
docker compose exec app bash
python -m backend.hwb_scanner_cli
```

### 5.2 Algoスキャナー (Algoタブ)

Algoタブ（MarketAlgoX & StageAlgo）のスキャンを手動で実行できます：

```bash
# コンテナ内で実行
docker compose exec app bash
bash backend/cron_job_algo.sh
```

**注意**:
- `FMP_API_KEY` が未設定の場合、自動的にデモモード（yfinanceフォールバック）で動作します。
- `GEMINI_API_KEY` が未設定の場合、AI解説の生成はスキップされます。

スキャン結果は以下に保存されます：
- **個別銘柄データ**: `data/algo/symbols/{TICKER}.json`
- **デイリーサマリー**: `data/algo/daily/latest.json`

## 6. VPSへのデプロイ (Deployment to VPS)

### 6.1 前提条件
- VPS契約（Ubuntu/Debian推奨）
- ドメイン取得（任意）
- SSH接続環境

### 6.2 サーバー初期設定

```bash
# Ubuntu/Debianの場合
sudo apt-get update
sudo apt-get install -y docker.io docker-compose git

# Docker起動
sudo systemctl start docker
sudo systemctl enable docker
```

### 6.3 DNS設定（ドメイン使用時）

ドメインのDNS設定で、VPSのIPアドレスを指す**Aレコード**を作成します：
- **タイプ**: A
- **名前**: `@` または `yourdomain.com`
- **IPv4アドレス**: VPSのIPアドレス

### 6.4 アプリケーションのデプロイ

```bash
# 1. リポジトリをクローン
git clone <repository_url>
cd <repository_directory>

# 2. .envファイルを作成（必須項目を設定）
nano .env

# 3. 起動
sudo docker-compose up -d --build
```

デプロイ完了後、ブラウザで `http://<VPSのIPアドレス>` または `http://<ドメイン>` にアクセスしてください。

## 7. 自動更新スケジュール

以下のタイミングで自動実行されます：

| 時刻 | 処理 | 内容 |
|------|------|------|
| 6:15 JST | データ取得 | 市場データ・ニュース・経済指標の取得 |
| 6:28 JST | レポート生成 | AI解説・コラム生成、Push通知送信 |
| 6:35 JST | HWBスキャン | Russell 3000銘柄のスキャン |
| 8:00 JST | Algoスキャン | MarketAlgoX & StageAlgo分析の実行 |

**実行日**: 月曜〜金曜（市場営業日）

## 8. トラブルシューティング

### 8.1 コンテナが起動しない場合

```bash
# ログを確認
docker-compose logs -f

# コンテナを再起動
docker-compose restart
```

### 8.2 データが更新されない場合

```bash
# Cronログを確認
docker compose exec app cat /app/logs/cron.log

# 手動でデータ更新を実行
docker compose exec app python -m backend.data_fetcher fetch
docker compose exec app python -m backend.data_fetcher generate
```

### 8.3 認証できない場合

- `.env`ファイルの`AUTH_PIN`設定を確認
- ブラウザのキャッシュをクリア
- LocalStorageとIndexedDBをクリア

### 8.4 Push通知が届かない場合

```bash
# VAPIDキーの生成確認
docker compose exec app cat /app/data/security_keys.json

# 通知権限の確認（ブラウザ設定）
# サービスワーカーの登録確認（DevTools > Application > Service Workers）
```

## 9. セキュリティに関する注意事項

1. **PINの変更**: デフォルトPIN（123456）は必ず変更してください
2. **APIキーの管理**: `.env`ファイルは絶対にGitにコミットしないでください
3. **セキュリティキーのバックアップ**: `data/security_keys.json`は定期的にバックアップしてください
4. **本番環境**: HTTPS化（リバースプロキシ経由）を推奨します

## 10. ライセンス

本プロジェクトは個人利用を目的としています。商用利用については別途ご相談ください。

## 11. サポート

問題が発生した場合は、以下を確認してください：
- [ログファイル](logs/)
- [設計書](hanaview-design.md)
- [要件定義書](hanaview-requirements.md)
- [仕様書](hanaview-specification.md)