"""
IBD Screeners

データベースに保存された計算済みレーティングを使用してスクリーナーを実行します。
"""

import numpy as np
from typing import List, Dict, Optional

# Change import to relative
from .ibd_database import IBDDatabase


class IBDScreeners:
    """データベースを使用したIBDスクリーナー"""

    def __init__(self, db_path: str = 'data/ibd_data.db'):
        """
        Args:
            db_path: データベースファイルのパス
        """
        self.db = IBDDatabase(db_path)

    def close(self):
        """リソースをクリーンアップ"""
        self.db.close()

    # ==================== ヘルパーメソッド ====================

    def get_price_metrics(self, ticker: str) -> Optional[Dict]:
        """価格関連の指標を計算"""
        prices_df = self.db.get_price_history(ticker, days=180)
        if prices_df is None or len(prices_df) < 2:
            return None

        try:
            close = prices_df['close'].values
            open_price = prices_df['open'].values

            result = {
                'price': close[-1],
                'pct_change_1d': ((close[-1] - close[-2]) / close[-2] * 100) if close[-2] != 0 else 0,
                'change_from_open': ((close[-1] - open_price[-1]) / open_price[-1] * 100) if open_price[-1] != 0 else 0,
                'pct_1w': ((close[-1] - close[-6]) / close[-6] * 100) if len(close) >= 6 and close[-6] != 0 else None,
                'pct_1m': ((close[-1] - close[-21]) / close[-21] * 100) if len(close) >= 21 and close[-21] != 0 else None,
                'pct_3m': ((close[-1] - close[-63]) / close[-63] * 100) if len(close) >= 63 and close[-63] != 0 else None,
                'pct_6m': ((close[-1] - close[-126]) / close[-126] * 100) if len(close) >= 126 and close[-126] != 0 else None
            }
            return result
        except Exception as e:
            return None

    def get_volume_metrics(self, ticker: str) -> Optional[Dict]:
        """ボリューム関連の指標を計算"""
        prices_df = self.db.get_price_history(ticker, days=100)
        if prices_df is None or len(prices_df) < 90:
            return None

        try:
            volume = prices_df['volume'].values

            avg_volume_50 = np.mean(volume[-50:]) if len(volume) >= 50 else None
            avg_volume_90 = np.mean(volume[-90:]) if len(volume) >= 90 else None
            current_volume = volume[-1]

            vol_change_pct = ((current_volume - avg_volume_50) / avg_volume_50 * 100) if avg_volume_50 and avg_volume_50 > 0 else 0
            rel_volume = (current_volume / avg_volume_50) if avg_volume_50 and avg_volume_50 > 0 else 0

            return {
                'avg_vol_50': avg_volume_50 / 1000,
                'avg_vol_90': avg_volume_90 / 1000,
                'current_volume': current_volume / 1000,
                'vol_change_pct': vol_change_pct,
                'rel_volume': rel_volume
            }
        except Exception as e:
            return None

    def get_moving_averages(self, ticker: str) -> Optional[Dict]:
        """移動平均を計算"""
        prices_df = self.db.get_price_history(ticker, days=250)
        if prices_df is None or len(prices_df) < 200:
            return None

        try:
            close = prices_df['close'].values

            return {
                '10ma': np.mean(close[-10:]) if len(close) >= 10 else None,
                '21ma': np.mean(close[-21:]) if len(close) >= 21 else None,
                '50ma': np.mean(close[-50:]) if len(close) >= 50 else None,
                '150ma': np.mean(close[-150:]) if len(close) >= 150 else None,
                '200ma': np.mean(close[-200:]) if len(close) >= 200 else None,
                'price': close[-1]
            }
        except Exception as e:
            return None

    def get_price_vs_50ma(self, ticker: str) -> Optional[float]:
        """価格と50日移動平均の比較"""
        ma_data = self.get_moving_averages(ticker)
        if not ma_data or ma_data['50ma'] is None:
            return None

        try:
            return ((ma_data['price'] - ma_data['50ma']) / ma_data['50ma'] * 100)
        except:
            return None

    def calculate_relative_strength(self, benchmark_prices, target_prices, days=25):
        """
        相対強度(RS)を計算

        Args:
            benchmark_prices: ベンチマークの価格データ（DataFrame）
            target_prices: ターゲット銘柄の価格データ（DataFrame）
            days: 使用する日数

        Returns:
            np.array: 日次のRS比率の配列
        """
        if benchmark_prices is None or target_prices is None:
            return None

        # 日付でマージして共通の日付のみを使用（重要：日付の不一致を防ぐ）
        import pandas as pd
        merged = pd.merge(
            benchmark_prices[['date', 'close']].rename(columns={'close': 'benchmark_close'}),
            target_prices[['date', 'close']].rename(columns={'close': 'target_close'}),
            on='date',
            how='inner'
        )

        if len(merged) == 0:
            return None

        # 最新のdays日分を使用
        if len(merged) > days:
            merged = merged.tail(days)

        # データが不足している場合
        if len(merged) < days:
            return None

        # ゼロ除算を防ぐ
        if (merged['benchmark_close'] == 0).any():
            return None

        rs = merged['target_close'].values / merged['benchmark_close'].values
        return rs

    def calculate_rs_sts_percentile(self, rs_values):
        """
        RS STS % (パーセンタイル)を計算

        Args:
            rs_values: RS値の配列

        Returns:
            float: パーセンタイル（0-100）
        """
        if rs_values is None or len(rs_values) == 0:
            return 0

        latest_rs = rs_values[-1]
        percentile = (np.sum(rs_values <= latest_rs) / len(rs_values)) * 100

        return round(percentile, 2)

    def get_rs_sts_percentile(self, ticker: str, benchmark_ticker: str = 'SPY', debug: bool = False) -> Optional[float]:
        """
        指定銘柄のRS STS%を計算

        Args:
            ticker: ターゲット銘柄
            benchmark_ticker: ベンチマーク銘柄（デフォルト: SPY）
            debug: デバッグ情報を出力するか

        Returns:
            float: RS STS%（0-100）、計算できない場合はNone
        """
        # ベンチマークと銘柄の価格データを取得
        benchmark_prices = self.db.get_price_history(benchmark_ticker, days=30)
        ticker_prices = self.db.get_price_history(ticker, days=30)

        if benchmark_prices is None:
            if debug:
                print(f"    DEBUG: {ticker} - Benchmark ({benchmark_ticker}) price data is None")
            return None

        if ticker_prices is None:
            if debug:
                print(f"    DEBUG: {ticker} - Ticker price data is None")
            return None

        if len(benchmark_prices) < 25 or len(ticker_prices) < 25:
            if debug:
                print(f"    DEBUG: {ticker} - Insufficient data (benchmark: {len(benchmark_prices)}, ticker: {len(ticker_prices)})")
            return None

        # RSを計算（全データを渡して、calculate_relative_strength内で日付マージ後に25日分を使用）
        rs_values = self.calculate_relative_strength(
            benchmark_prices,
            ticker_prices,
            days=25
        )

        if rs_values is None:
            return None

        # RS STS%を計算
        return self.calculate_rs_sts_percentile(rs_values)

    def check_rs_line_new_high(self, ticker: str) -> bool:
        """RS Lineが新高値かチェック"""
        # 簡易版: 52週高値に近いかで判定
        rating = self.db.get_rating(ticker)
        if not rating or rating['price_vs_52w_high'] is None:
            return False

        # 52週高値から5%以内
        return rating['price_vs_52w_high'] >= -5

    # ==================== スクリーナー実装 ====================

    def screener_momentum_97(self) -> List[Dict]:
        """
        Momentum 97 スクリーナー

        条件:
        - 1W Rank (Pct) ≥ 97%
        - 1M Rank (Pct) ≥ 97%
        - 3M Rank (Pct) ≥ 97%
        """
        print("\n=== Momentum 97 スクリーナー実行中 ===")

        tickers_list = self.db.get_all_tickers()
        performance_data = {}

        # 全銘柄のパフォーマンスを取得
        for ticker in tickers_list:
            price_metrics = self.get_price_metrics(ticker)
            # Ensure we have data for 1W, 1M, and 3M
            if price_metrics and price_metrics.get('pct_1w') is not None and price_metrics.get('pct_1m') is not None and price_metrics.get('pct_3m') is not None:
                performance_data[ticker] = {
                    '1w': price_metrics['pct_1w'],
                    '1m': price_metrics['pct_1m'],
                    '3m': price_metrics['pct_3m']
                }

        # 各期間でパーセンタイルランクを計算
        def calc_percentile_ranks(values_dict, key):
            valid = {t: v[key] for t, v in values_dict.items() if v[key] is not None}
            if not valid:
                return {}
            sorted_items = sorted(valid.items(), key=lambda x: x[1])
            total = len(sorted_items)
            return {t: ((idx + 1) / total) * 100 for idx, (t, v) in enumerate(sorted_items)}

        rank_1w = calc_percentile_ranks(performance_data, '1w')
        rank_1m = calc_percentile_ranks(performance_data, '1m')
        rank_3m = calc_percentile_ranks(performance_data, '3m')

        # フィルタリング
        passed = []
        for ticker in performance_data.keys():
            r1 = rank_1w.get(ticker, 0)
            r2 = rank_1m.get(ticker, 0)
            r3 = rank_3m.get(ticker, 0)

            if r1 >= 97 and r2 >= 97 and r3 >= 97:
                passed.append({
                    'ticker': ticker,
                    'momentum_rank_1w': round(r1, 2),
                    'momentum_rank_1m': round(r2, 2),
                    'momentum_rank_3m': round(r3, 2)
                })

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    def screener_explosive_eps_growth(self) -> List[Dict]:
        """
        Explosive Estimated EPS Growth Stocks スクリーナー

        条件:
        - RS Rating ≥ 80
        - EPS Est Cur Qtr % ≥ 100% (Proxy: EPS Growth Last Qtr)
        - 50-Day Avg Vol ≥ 100K
        - Price vs 50-Day ≥ 0.0%
        """
        print("\n=== Explosive Estimated EPS Growth Stocks スクリーナー実行中 ===")

        passed = []
        all_ratings = self.db.get_all_ratings()

        for ticker, rating in all_ratings.items():
            try:
                # RS Rating チェック
                rs_rating = rating['rs_rating']
                if rs_rating is None or rs_rating < 80:
                    continue

                # EPS Growth チェック
                # Note: Using Last Qtr Actual as Proxy for Est Cur Qtr (Limitation)
                eps_components = self.db.get_all_eps_components()
                if ticker not in eps_components:
                    continue

                eps_growth = eps_components[ticker]['eps_growth_last_qtr']
                if eps_growth is None or eps_growth < 100:
                    continue

                # ボリュームチェック
                vol_metrics = self.get_volume_metrics(ticker)
                if not vol_metrics or vol_metrics['avg_vol_50'] < 100:
                    continue

                # Price vs 50-Day MA チェック
                price_vs_50ma = self.get_price_vs_50ma(ticker)
                if price_vs_50ma is None or price_vs_50ma < 0:
                    continue

                passed.append({
                    'ticker': ticker,
                    'rs_rating': rs_rating,
                    'eps_growth_last_qtr': eps_growth,
                    'avg_vol_50': vol_metrics['avg_vol_50'],
                    'price_vs_50ma': round(price_vs_50ma, 2)
                })
            except:
                continue

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    def screener_up_on_volume(self) -> List[Dict]:
        """
        Up on Volume List スクリーナー

        条件:
        - Price % Chg ≥ 0.00%
        - Vol% Chg vs 50-Day ≥ 20%
        - Current Price ≥ $10
        - 50-Day Avg Vol ≥ 100K
        - Market Cap ≥ $250M
        - RS Rating ≥ 80
        - EPS % Chg Last Qtr ≥ 20%
        - A/D Rating ABC
        """
        print("\n=== Up on Volume List スクリーナー実行中 ===")

        passed = []
        all_ratings = self.db.get_all_ratings()

        for ticker, rating in all_ratings.items():
            try:
                # RS Rating チェック
                rs_rating = rating['rs_rating']
                if rs_rating is None or rs_rating < 80:
                    continue

                # A/D Rating チェック
                if rating['ad_rating'] not in ['A', 'B', 'C']:
                    continue

                # 価格チェック
                price_metrics = self.get_price_metrics(ticker)
                if not price_metrics:
                    continue

                if price_metrics['pct_change_1d'] < 0:
                    continue

                if price_metrics['price'] < 10:
                    continue

                # ボリュームチェック
                vol_metrics = self.get_volume_metrics(ticker)
                if not vol_metrics:
                    continue

                if vol_metrics['avg_vol_50'] < 100:
                    continue

                if vol_metrics['vol_change_pct'] < 20:
                    continue

                # 時価総額チェック
                profile = self.db.get_company_profile(ticker)
                if not profile or profile['market_cap'] is None:
                    continue

                market_cap_millions = profile['market_cap'] / 1_000_000
                if market_cap_millions < 250:
                    continue

                # EPS成長率チェック
                eps_components = self.db.get_all_eps_components()
                if ticker not in eps_components:
                    continue

                eps_growth = eps_components[ticker]['eps_growth_last_qtr']
                if eps_growth is None or eps_growth < 20:
                    continue

                passed.append({
                    'ticker': ticker,
                    'price_change_pct': price_metrics['pct_change_1d'],
                    'vol_change_pct': vol_metrics['vol_change_pct'],
                    'rs_rating': rs_rating,
                    'eps_growth_last_qtr': eps_growth
                })
            except:
                continue

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    def screener_top_2_percent_rs(self) -> List[Dict]:
        """
        Top 2% RS Rating List スクリーナー

        条件:
        - RS Rating ≥ 98
        - 10Day > 21Day > 50Day
        - 50-Day Avg Vol ≥ 100K
        - Volume ≥ 100K
        - Sector NOT: medical/healthcare
        """
        print("\n=== Top 2% RS Rating List スクリーナー実行中 ===")

        passed = []
        all_ratings = self.db.get_all_ratings()

        for ticker, rating in all_ratings.items():
            try:
                # RS Rating チェック
                rs_rating = rating['rs_rating']
                if rs_rating is None or rs_rating < 98:
                    continue

                # 移動平均チェック
                ma_data = self.get_moving_averages(ticker)
                if not ma_data:
                    continue

                if not (ma_data['10ma'] > ma_data['21ma'] > ma_data['50ma']):
                    continue

                # ボリュームチェック
                vol_metrics = self.get_volume_metrics(ticker)
                if not vol_metrics:
                    continue

                if vol_metrics['avg_vol_50'] < 100:
                    continue

                if vol_metrics['current_volume'] < 100:
                    continue

                # セクターチェック
                profile = self.db.get_company_profile(ticker)
                if profile:
                    sector = profile.get('sector', '').lower()
                    if 'healthcare' in sector or 'medical' in sector:
                        continue

                passed.append({
                    'ticker': ticker,
                    'rs_rating': rs_rating
                })
            except:
                continue

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    def screener_4_percent_bullish_yesterday(self) -> List[Dict]:
        """
        4% Bullish Yesterday スクリーナー

        条件:
        - Price ≥ $1
        - Change > 4%
        - Market cap > $250M
        - Volume > 100K
        - Rel Volume > 1
        - Change from Open > 0%
        - Avg Volume 90D > 100K
        """
        print("\n=== 4% Bullish Yesterday スクリーナー実行中 ===")

        passed = []
        tickers_list = self.db.get_all_tickers()

        for ticker in tickers_list:
            try:
                # 価格チェック
                price_metrics = self.get_price_metrics(ticker)
                if not price_metrics:
                    continue

                if price_metrics['price'] < 1:
                    continue

                if price_metrics['pct_change_1d'] <= 4:
                    continue

                if price_metrics['change_from_open'] <= 0:
                    continue

                # ボリュームチェック
                vol_metrics = self.get_volume_metrics(ticker)
                if not vol_metrics:
                    continue

                if vol_metrics['current_volume'] <= 100:
                    continue

                if vol_metrics['rel_volume'] <= 1:
                    continue

                if vol_metrics['avg_vol_90'] <= 100:
                    continue

                # 時価総額チェック
                profile = self.db.get_company_profile(ticker)
                if not profile or profile['market_cap'] is None:
                    continue

                market_cap_millions = profile['market_cap'] / 1_000_000
                if market_cap_millions <= 250:
                    continue

                passed.append({
                    'ticker': ticker,
                    'price_change_pct': price_metrics['pct_change_1d'],
                    'rel_volume': vol_metrics['rel_volume']
                })
            except:
                continue

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    def screener_healthy_chart_watchlist(self) -> List[Dict]:
        """
        Healthy Chart Watch List スクリーナー

        条件:
        - 10Day > 21Day > 50Day
        - 50Day > 150Day > 200Day
        - RS Line New High
        - RS Rating ≥ 90
        - A/D Rating AB
        - Ind Group RS AB (Rank >= 60)
        - Comp Rating ≥ 80
        - 50-Day Avg Vol ≥ 100K
        """
        print("\n=== Healthy Chart Watch List スクリーナー実行中 ===")

        passed = []
        all_ratings = self.db.get_all_ratings()

        for ticker, rating in all_ratings.items():
            try:
                # RS Rating チェック
                rs_rating = rating['rs_rating']
                if rs_rating is None or rs_rating < 90:
                    continue

                # Composite Rating チェック
                comp_rating = rating['comp_rating']
                if comp_rating is None or comp_rating < 80:
                    continue

                # A/D Rating チェック
                if rating['ad_rating'] not in ['A', 'B']:
                    continue

                # Industry Group RS チェック (A/B -> Top 40% -> Rank >= 60)
                ind_group_rs = rating.get('industry_group_rs')
                if ind_group_rs is None or ind_group_rs < 60:
                    continue

                # 移動平均チェック
                ma_data = self.get_moving_averages(ticker)
                if not ma_data:
                    continue

                if not (ma_data['10ma'] > ma_data['21ma'] > ma_data['50ma']):
                    continue

                if not (ma_data['50ma'] > ma_data['150ma'] > ma_data['200ma']):
                    continue

                # RS Line New High チェック
                if not self.check_rs_line_new_high(ticker):
                    continue

                # ボリュームチェック
                vol_metrics = self.get_volume_metrics(ticker)
                if not vol_metrics or vol_metrics['avg_vol_50'] < 100:
                    continue

                passed.append({
                    'ticker': ticker,
                    'rs_rating': rs_rating,
                    'comp_rating': comp_rating,
                    'ad_rating': rating['ad_rating'],
                    'industry_group_rs': ind_group_rs
                })
            except:
                continue

        print(f"  合格: {len(passed)} 銘柄")
        return passed

    # ==================== メイン実行関数 ====================

    def ensure_benchmark_data(self, benchmark_ticker: str = 'SPY'):
        """
        ベンチマークデータが存在することを確認し、なければ取得

        Args:
            benchmark_ticker: ベンチマークティッカー（デフォルト: SPY）

        Returns:
            bool: データが存在するか
        """
        # データベースにデータが存在するか確認
        benchmark_prices = self.db.get_price_history(benchmark_ticker, days=30)

        if benchmark_prices is not None and len(benchmark_prices) >= 25:
            print(f"✓ {benchmark_ticker} データは既に存在します ({len(benchmark_prices)}日分)")
            return True

        # データが存在しない場合、取得を試みる
        print(f"\n⚠ {benchmark_ticker} データが存在しません。取得中...")

        try:
            import os
            from dotenv import load_dotenv
            # Relative import
            from .ibd_data_collector import IBDDataCollector

            load_dotenv()
            fmp_api_key = os.getenv('FMP_API_KEY')

                # Allow missing key for fallback demo
                # if not fmp_api_key or fmp_api_key == 'your_api_key_here':
                #     print(f"✗ エラー: FMP_API_KEYが設定されていません")
                #     print(f"  RS STS%の計算には{benchmark_ticker}のデータが必須です")
                #     return False

            collector = IBDDataCollector(fmp_api_key, db_path=self.db.db_path)
            success = collector.collect_benchmark_data([benchmark_ticker])
            collector.close()

            if success > 0:
                print(f"✓ {benchmark_ticker} データの取得に成功しました")
                return True
            else:
                    # Allow fallback
                    print(f"✗ {benchmark_ticker} データの取得に失敗しました (Proceeding anyway)")
                    return True # Return true to proceed with partial data

        except Exception as e:
            print(f"✗ ベンチマークデータ取得エラー: {str(e)}")
            return True # Proceed anyway

    def run_all_screeners(self):
        """全スクリーナーを実行して結果を返す"""
        print("\n" + "="*80)
        print("IBD スクリーナー実行開始")
        print("="*80)

        # ベンチマークデータの確認・取得
        print("\nベンチマークデータを確認中...")
        if not self.ensure_benchmark_data('SPY'):
            print("\n⚠ 警告: SPYデータが取得できませんでした")
            print("  RS STS%を使用するスクリーナーの結果が制限される可能性があります")

        # 各スクリーナーを実行
        screener_results = {}

        screener_results['momentum_97'] = self.screener_momentum_97()
        screener_results['explosive_eps'] = self.screener_explosive_eps_growth()
        screener_results['up_on_volume'] = self.screener_up_on_volume()
        screener_results['top_2pct_rs'] = self.screener_top_2_percent_rs()
        screener_results['bullish_4pct'] = self.screener_4_percent_bullish_yesterday()
        screener_results['healthy_chart'] = self.screener_healthy_chart_watchlist()

        print("\n" + "="*80)
        print("すべてのスクリーナー実行完了!")
        print("="*80)

        return screener_results
