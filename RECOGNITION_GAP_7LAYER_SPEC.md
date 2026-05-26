# Recognition Gap EP 7層ランキング仕様

## 入力

- 米国株ユニバース: `stock.csv` またはFMP stock screenerから取得したNASDAQ/NYSE銘柄
- 日足OHLCV: `data/price_data_ohlcv.pkl`
- 企業属性: FMP profileまたは`stock.csv`内の `CompanyName / Sector / Industry / MarketCap / Country`
- 任意: ニュース、決算、受注、バックログ、保有者データ、米10年金利

## ライブランキングで使ってよい情報

`asof_date` 以前に取得可能な情報だけを使う。将来高値、将来ニュース、将来決算、将来のテーマ分類は使わない。

## Entry候補

基本構成:

```text
entry_rule: industry_theme_ep_ex_biotech
entry_timing: pullback10
exit_rule: stage2_or_atr8
```

抽出条件:

- バイオ/医薬イベント銘柄を原則除外
- 低流動性を除外
- 価格が10/20MAを上回り、20日または60日で強いdisplacementを持つ
- 対SPY60日残差が弱すぎない
- 出来高またはドル出来高の増加がある
- EP後、10MA接近または10MA維持による日足entryを採用

## ランキング

ランキングは機械的な優先順位で作る。LLMに数値スコアを任せない。

優先するもの:

- `thesis_intact`
- `price_trend` が `strong` または `extended_but_intact`
- `volume_demand_durability` が `durable_accumulation`
- `supply_risk_severity` が `low`
- `ret60_resid_spy` が強い
- `post_signal_dv_persistence` が高い
- 最低限のドル出来高がある

## 7層

1. Data Integrity
2. Price Trend
3. Volume Demand Durability
4. Institutional and Supply
5. Catalyst Quality
6. Fundamental Confirmation
7. Thesis State

`thesis_state` は毎日ゼロから揺らさず、実運用では前日状態、`daily_flags`、重大イベント、3営業日継続を使って昇格/降格させる。

## opencode go合議

4モデルの役割:

- Kimi: 銘柄エンティティ、ニュース混入、事実確認
- MiniMax: 価格、出来高、需給の違和感
- GLM: 業種、競合、構造テーマ、ファンダ文脈
- DeepSeek: まとめ役。各モデルの意見を統合して日本語要点を作る

出力は売買判断ではなく、300字以内の要点、リスク、entity check、thesis substateに限定する。

## X投稿

- 画像は白黒テーブル
- 検出された全銘柄を5銘柄ずつ分割する。例: 1-5、6-10、11-15。最後は余り件数でよい
- 表示順は含み益順ではなく、7層の優先保有/監視順にする
- 列: `順位 / 銘柄 / Entry / 含み益 / 要点`
- 本文は原則 `$TICKER` を20銘柄分だけ並べる。内部検証では全ティッカー出力も許可する
- 要点はニュース見出しの貼り付けではなく、価格、出来高、業種文脈、ファンダ、供給リスク、thesis状態を統合した300字以内の日本語にする
- `supply_risk_severity` は全銘柄で `low / medium / high` のいずれかを原則付与する。欠損時は時価総額、ドル出来高、offering/ATM/warrant/shelf/secondary系ニュースから推定する

## 禁止

- Webull注文機能
- 売買ボタン
- LLMによる直接exit判断
- 先見バイアス
- ニュースと別会社ティッカーの混入
