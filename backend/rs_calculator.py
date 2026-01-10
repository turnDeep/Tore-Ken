"""
RS Rating (Relative Strength Rating) 計算モジュール（改善版）
William O'NeilのIBD理論に基づく完全実装

【理論的基盤】
- IBD RS Rating: 12ヶ月の価格パフォーマンスを加重平均
- RS Line: ベンチマークに対する相対的強度
- マルチタイムフレーム分析: 1M, 3M, 6M, 9M, 12M

【重要な改善点】
1. IBD式の正確な加重平均計算（40-20-20-20）
2. 複数時間軸でのRS評価
3. RS Line新高値検出の精度向上
4. Stage分析との統合
5. より詳細な解釈とアクション推奨
"""
import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional


class RSCalculator:
    """
    RS Rating計算システム（IBD/O'Neil方式）
    
    計算式（IBD方式）:
    RS Score = 40% × ROC(63日) + 20% × ROC(126日) + 20% × ROC(189日) + 20% × ROC(252日)
    
    ここで:
    - ROC = Rate of Change（変化率）
    - 63日 ≈ 1四半期（3ヶ月）
    - 126日 ≈ 2四半期（6ヶ月）
    - 189日 ≈ 3四半期（9ヶ月）
    - 252日 ≈ 4四半期（12ヶ月）
    """
    
    def __init__(self, df: pd.DataFrame, benchmark_df: pd.DataFrame):
        """
        Args:
            df: 指標計算済みのDataFrame（銘柄データ）
            benchmark_df: ベンチマークデータ（通常はSPY）
        """
        self.df = df.copy()
        self.benchmark_df = benchmark_df.copy()
        self.latest = df.iloc[-1]
        
    def calculate_roc(self, series: pd.Series, period: int) -> pd.Series:
        """
        Rate of Change (変化率)を計算
        
        Args:
            series: 価格シリーズ
            period: 期間(営業日)
            
        Returns:
            ROC (%)
        """
        if len(series) < period + 1:
            return pd.Series([0] * len(series), index=series.index)
        
        roc = (series / series.shift(period) - 1) * 100
        return roc.fillna(0)
    
    def calculate_ibd_rs_score(self) -> pd.Series:
        """
        IBD方式のRS Scoreを計算（時系列）
        
        加重平均:
        - 40% × 直近3ヶ月（63日）
        - 20% × 直近6ヶ月（126日）
        - 20% × 直近9ヶ月（189日）
        - 20% × 直近12ヶ月（252日）
        
        Returns:
            pd.Series: RS Score時系列
        """
        close = self.df['close']
        
        # 各期間のROCを計算
        roc_63 = self.calculate_roc(close, 63)    # 3ヶ月
        roc_126 = self.calculate_roc(close, 126)  # 6ヶ月
        roc_189 = self.calculate_roc(close, 189)  # 9ヶ月
        roc_252 = self.calculate_roc(close, 252)  # 12ヶ月
        
        # IBD式加重平均
        rs_score = (
            0.40 * roc_63 +
            0.20 * roc_126 +
            0.20 * roc_189 +
            0.20 * roc_252
        )
        
        return rs_score
    
    def calculate_percentile_rating(self, rs_score: float, window: int = 252) -> float:
        """
        RS Scoreをパーセンタイルレーティング（1-99）に変換
        
        ※ 注: 理想的には全銘柄と比較すべきだが、計算時間の問題により
        　　　過去252日間の自己データ内でのパーセンタイルランクで代用
        
        Args:
            rs_score: 現在のRS Score
            window: 比較期間（デフォルト252日=約1年）
            
        Returns:
            float: パーセンタイルレーティング（1-99）
        """
        if len(self.df) < window:
            window = len(self.df)
        
        # 過去データからRS Scoreを計算
        rs_score_series = self.calculate_ibd_rs_score()
        recent_scores = rs_score_series.tail(window)
        
        # 有効なスコアのみを使用
        valid_scores = recent_scores[recent_scores != 0].dropna()
        
        if len(valid_scores) == 0:
            return 50  # デフォルト値
        
        # パーセンタイルランク計算
        rank = (valid_scores < rs_score).sum()
        percentile = (rank / len(valid_scores)) * 98 + 1  # 1-99にマッピング
        
        return min(99, max(1, percentile))
    
    def calculate_rs_line(self) -> pd.Series:
        """
        RS Line (相対強度線)を計算
        
        RS Line = (株価 / ベンチマーク価格) × 100
        
        Returns:
            pd.Series: RS Line時系列
        """
        # インデックスを揃える
        common_index = self.df.index.intersection(self.benchmark_df.index)
        
        if len(common_index) == 0:
            return pd.Series([100] * len(self.df), index=self.df.index)
        
        stock_close = self.df.loc[common_index, 'close']
        benchmark_close = self.benchmark_df.loc[common_index, 'close']
        
        # RS Line計算
        rs_line = (stock_close / benchmark_close) * 100
        
        # 欠損値を補間
        rs_line_full = pd.Series(index=self.df.index, dtype=float)
        rs_line_full.loc[common_index] = rs_line
        rs_line_full = rs_line_full.fillna(method='ffill').fillna(method='bfill').fillna(100)
        
        return rs_line_full
    
    def check_rs_line_new_high(self, rs_line: pd.Series, lookback_days: int = 252) -> Dict:
        """
        RS Lineが新高値を更新しているかチェック（精度向上版）
        
        Args:
            rs_line: RS Line時系列
            lookback_days: 確認期間
            
        Returns:
            dict: 新高値情報
        """
        if len(rs_line) < lookback_days + 1:
            return {
                'is_new_high': False,
                'reason': 'データ不足',
                'days_since_high': None,
                'percent_from_high': None
            }
        
        current_rs = rs_line.iloc[-1]
        historical_data = rs_line.iloc[-lookback_days:-1]
        historical_max = historical_data.max()
        
        # 新高値判定（現在値が過去最大値より大きい）
        is_new_high = current_rs > historical_max
        
        # 過去最高値からの日数と距離
        if historical_max > 0:
            days_since_high = len(rs_line) - historical_data.idxmax() - 1
            percent_from_high = ((current_rs - historical_max) / historical_max) * 100
        else:
            days_since_high = None
            percent_from_high = None
        
        return {
            'is_new_high': is_new_high,
            'current_rs_line': current_rs,
            'historical_max': historical_max,
            'days_since_high': days_since_high if not is_new_high else 0,
            'percent_from_high': percent_from_high,
            'strength': self._interpret_rs_line_strength(percent_from_high) if percent_from_high is not None else 'Unknown'
        }
    
    def _interpret_rs_line_strength(self, percent_from_high: float) -> str:
        """RS Lineの強さを解釈"""
        if percent_from_high > 5:
            return 'Excellent - Strong Breakout'
        elif percent_from_high > 2:
            return 'Very Strong - New High'
        elif percent_from_high > 0:
            return 'Strong - At New High'
        elif percent_from_high > -5:
            return 'Good - Near High'
        elif percent_from_high > -10:
            return 'Moderate - Some Weakness'
        else:
            return 'Weak - Significant Pullback'
    
    def calculate_multi_timeframe_rs(self) -> Dict:
        """
        複数時間軸でのRS評価
        
        Returns:
            dict: 各時間軸のRS情報
        """
        close = self.df['close']
        
        timeframes = {
            '1M': 21,    # 約1ヶ月
            '3M': 63,    # 約3ヶ月
            '6M': 126,   # 約6ヶ月
            '9M': 189,   # 約9ヶ月
            '12M': 252   # 約12ヶ月
        }
        
        results = {}
        
        for name, period in timeframes.items():
            if len(close) >= period + 1:
                roc = self.calculate_roc(close, period).iloc[-1]
                
                # ベンチマークとの比較
                if len(self.benchmark_df) >= period + 1:
                    benchmark_roc = self.calculate_roc(
                        self.benchmark_df['close'], period
                    ).iloc[-1] if len(self.benchmark_df['close']) > period else 0
                    
                    outperformance = roc - benchmark_roc
                else:
                    benchmark_roc = 0
                    outperformance = roc
                
                results[name] = {
                    'roc': roc,
                    'benchmark_roc': benchmark_roc,
                    'outperformance': outperformance,
                    'rating': self._rate_performance(roc, outperformance)
                }
            else:
                results[name] = {
                    'roc': 0,
                    'benchmark_roc': 0,
                    'outperformance': 0,
                    'rating': 'N/A'
                }
        
        # 一貫性チェック（すべての時間軸でプラス）
        all_positive = all(
            results[tf]['outperformance'] > 0 
            for tf in timeframes.keys() 
            if results[tf]['rating'] != 'N/A'
        )
        
        results['consistency'] = {
            'all_timeframes_positive': all_positive,
            'strength': 'Excellent' if all_positive else 'Mixed'
        }
        
        return results
    
    def _rate_performance(self, roc: float, outperformance: float) -> str:
        """パフォーマンスを評価"""
        if outperformance > 20 and roc > 20:
            return 'A+'
        elif outperformance > 15 and roc > 15:
            return 'A'
        elif outperformance > 10 and roc > 10:
            return 'B+'
        elif outperformance > 5 and roc > 5:
            return 'B'
        elif outperformance > 0:
            return 'C'
        else:
            return 'D'
    
    def analyze_rs_with_stage(self, current_stage: int, current_substage: str) -> Dict:
        """
        Stage分析と統合したRS評価
        
        Args:
            current_stage: 現在のステージ（1-4）
            current_substage: サブステージ（例: "2A"）
            
        Returns:
            dict: Stage統合RS分析結果
        """
        # 基本RS計算
        rs_score_series = self.calculate_ibd_rs_score()
        current_rs_score = rs_score_series.iloc[-1]
        rs_rating = self.calculate_percentile_rating(current_rs_score)
        
        # RS Line計算
        rs_line = self.calculate_rs_line()
        rs_line_analysis = self.check_rs_line_new_high(rs_line)
        
        # マルチタイムフレーム分析
        multi_tf = self.calculate_multi_timeframe_rs()
        
        result = {
            'rs_score': current_rs_score,
            'rs_rating': rs_rating,
            'rs_line_current': rs_line_analysis['current_rs_line'],
            'rs_line_new_high': rs_line_analysis['is_new_high'],
            'rs_line_strength': rs_line_analysis['strength'],
            'multi_timeframe': multi_tf,
            'stage': current_stage,
            'substage': current_substage
        }
        
        # RS Rating評価
        result['rs_grade'] = self._grade_rs_rating(rs_rating)
        result['rs_category'] = self._categorize_rs_rating(rs_rating)
        
        # Stage別の統合解釈
        result['integrated_analysis'] = self._integrate_with_stage(
            rs_rating, 
            rs_line_analysis, 
            multi_tf,
            current_stage, 
            current_substage
        )
        
        return result
    
    def _grade_rs_rating(self, rs_rating: float) -> str:
        """RS Ratingをグレード化"""
        if rs_rating >= 95:
            return 'A++'
        elif rs_rating >= 90:
            return 'A+'
        elif rs_rating >= 85:
            return 'A'
        elif rs_rating >= 80:
            return 'B+'
        elif rs_rating >= 70:
            return 'B'
        elif rs_rating >= 60:
            return 'C'
        else:
            return 'D'
    
    def _categorize_rs_rating(self, rs_rating: float) -> str:
        """RS Ratingをカテゴリ化"""
        if rs_rating >= 90:
            return 'Top 10% - Market Leader'
        elif rs_rating >= 85:
            return 'Top 15% - Strong Leader'
        elif rs_rating >= 80:
            return 'Top 20% - Above Average'
        elif rs_rating >= 70:
            return 'Top 30% - Average+'
        else:
            return 'Below Average'
    
    def _integrate_with_stage(self, rs_rating: float, rs_line_analysis: Dict,
                             multi_tf: Dict, stage: int, substage: str) -> Dict:
        """
        StageとRSを統合した詳細な解釈
        
        Returns:
            dict: 統合分析結果
        """
        analysis = {
            'assessment': '',
            'action': '',
            'confidence': '',
            'key_factors': []
        }
        
        # Stage 1: ベース形成期
        if stage == 1:
            if substage == '1B' and rs_rating >= 80 and rs_line_analysis['is_new_high']:
                analysis['assessment'] = 'Excellent Stage 1B Setup'
                analysis['action'] = '最優先監視 - ブレイクアウト準備完了、高RS Rating + RS Line新高値'
                analysis['confidence'] = 'Very High'
                analysis['key_factors'] = [
                    f'RS Rating {rs_rating:.0f} - トップ20%以内',
                    'RS Line新高値更新 - 機関投資家の蓄積',
                    'Stage 1B - ブレイクアウト直前'
                ]
            
            elif substage == '1B' and rs_rating >= 70:
                analysis['assessment'] = 'Good Stage 1B Candidate'
                analysis['action'] = '優先監視 - RS改善を期待'
                analysis['confidence'] = 'High'
                analysis['key_factors'] = [
                    f'RS Rating {rs_rating:.0f} - 平均以上',
                    f'RS Line: {rs_line_analysis["strength"]}',
                    'Stage 1B - 準備段階'
                ]
            
            elif substage in ['1', '1A']:
                if rs_rating >= 80:
                    analysis['assessment'] = 'Accumulating Strength'
                    analysis['action'] = '継続監視 - Stage 1B移行を待つ'
                    analysis['confidence'] = 'Medium'
                else:
                    analysis['assessment'] = 'Early Stage 1'
                    analysis['action'] = 'さらなる時間が必要'
                    analysis['confidence'] = 'Low'
        
        # Stage 2: 上昇期
        elif stage == 2:
            if substage == '2A':
                if rs_rating >= 90:
                    analysis['assessment'] = 'Elite Stage 2A Leader'
                    analysis['action'] = '積極的エントリー推奨 - トップ10%のリーダー'
                    analysis['confidence'] = 'Very High'
                    analysis['key_factors'] = [
                        f'RS Rating {rs_rating:.0f} - トップ10%',
                        'Stage 2A - 上昇初期',
                        f'マルチTF一貫性: {multi_tf["consistency"]["strength"]}'
                    ]
                
                elif rs_rating >= 80:
                    analysis['assessment'] = 'Strong Stage 2A Stock'
                    analysis['action'] = 'エントリー検討 - 押し目を待つ'
                    analysis['confidence'] = 'High'
                
                else:
                    analysis['assessment'] = 'Moderate Stage 2A'
                    analysis['action'] = 'RS改善を待つか見送り'
                    analysis['confidence'] = 'Medium'
            
            elif substage == '2':
                if rs_rating >= 85:
                    analysis['assessment'] = 'Strong Stage 2 Stock'
                    analysis['action'] = 'ホールド継続、健全な押し目でエントリー'
                    analysis['confidence'] = 'High'
                elif rs_rating >= 70:
                    analysis['assessment'] = 'Good Stage 2 Stock'
                    analysis['action'] = 'ホールド、RS弱体化に注意'
                    analysis['confidence'] = 'Medium'
                else:
                    analysis['assessment'] = 'Weakening RS in Stage 2'
                    analysis['action'] = '利確検討、Stage 3移行の可能性'
                    analysis['confidence'] = 'Low'
            
            elif substage == '2B':
                if rs_rating >= 80:
                    analysis['assessment'] = 'Late Stage 2 - Still Strong'
                    analysis['action'] = 'タイトなストップロス、利確準備'
                    analysis['confidence'] = 'Medium'
                else:
                    analysis['assessment'] = 'Late Stage 2 - Weakening'
                    analysis['action'] = '利確推奨、新規エントリー非推奨'
                    analysis['confidence'] = 'Low'
        
        # Stage 3: 天井形成期
        elif stage == 3:
            analysis['assessment'] = 'Stage 3 Distribution'
            analysis['action'] = '利確・撤退推奨、RSに関わらずリスク高'
            analysis['confidence'] = 'High (Sell Signal)'
            analysis['key_factors'] = [
                'Stage 3 - 分配フェーズ',
                'RSは回復しない可能性が高い',
                '新規エントリー絶対回避'
            ]
        
        # Stage 4: 下降期
        elif stage == 4:
            if rs_rating < 50:
                analysis['assessment'] = 'Stage 4 - Severe Weakness'
                analysis['action'] = 'ロング完全回避、Stage 1入り遠い'
                analysis['confidence'] = 'Very High (Avoid)'
            else:
                analysis['assessment'] = 'Stage 4 - Unusual RS Strength'
                analysis['action'] = 'Stage 1移行の可能性、慎重に監視'
                analysis['confidence'] = 'Medium'
        
        # RSとRS Lineの整合性チェック
        if rs_rating >= 80 and not rs_line_analysis['is_new_high'] and stage in [1, 2]:
            analysis['key_factors'].append(
                '⚠ RS Ratingは高いがRS Lineは新高値未更新 - 注意深く監視'
            )
        
        return analysis
    
    def generate_comprehensive_report(self, current_stage: int, 
                                     current_substage: str) -> str:
        """
        包括的なRSレポートを生成
        
        Args:
            current_stage: 現在のステージ
            current_substage: サブステージ
            
        Returns:
            str: 詳細レポート
        """
        analysis = self.analyze_rs_with_stage(current_stage, current_substage)
        
        report = []
        report.append("=" * 70)
        report.append("RS (Relative Strength) 包括的分析レポート")
        report.append("=" * 70)
        
        # 基本情報
        report.append(f"\n【基本RS指標】")
        report.append(f"  RS Score (IBD式): {analysis['rs_score']:.2f}%")
        report.append(f"  RS Rating (Percentile): {analysis['rs_rating']:.0f}/99")
        report.append(f"  Grade: {analysis['rs_grade']}")
        report.append(f"  Category: {analysis['rs_category']}")
        
        # RS Line
        report.append(f"\n【RS Line分析】")
        report.append(f"  Current Value: {analysis['rs_line_current']:.2f}")
        report.append(f"  New High: {'✓ YES' if analysis['rs_line_new_high'] else '✗ NO'}")
        report.append(f"  Strength: {analysis['rs_line_strength']}")
        
        # マルチタイムフレーム
        report.append(f"\n【マルチタイムフレームRS】")
        multi_tf = analysis['multi_timeframe']
        for tf in ['1M', '3M', '6M', '9M', '12M']:
            if tf in multi_tf and multi_tf[tf]['rating'] != 'N/A':
                data = multi_tf[tf]
                report.append(
                    f"  {tf}: ROC={data['roc']:+.1f}% | "
                    f"vs Benchmark={data['outperformance']:+.1f}% | "
                    f"Grade={data['rating']}"
                )
        
        report.append(f"  Consistency: {multi_tf['consistency']['strength']}")
        
        # Stage統合分析
        report.append(f"\n【Stage統合分析】")
        report.append(f"  Current Stage: Stage {analysis['stage']} ({analysis['substage']})")
        
        integrated = analysis['integrated_analysis']
        report.append(f"  Assessment: {integrated['assessment']}")
        report.append(f"  Confidence: {integrated['confidence']}")
        report.append(f"  Recommended Action: {integrated['action']}")
        
        if integrated['key_factors']:
            report.append(f"\n  Key Factors:")
            for factor in integrated['key_factors']:
                report.append(f"    • {factor}")
        
        # IBD基準との比較
        report.append(f"\n【IBD/O'Neil基準】")
        if analysis['rs_rating'] >= 87:
            report.append(f"  ✓ O'Neil推奨基準達成 (平均87以上)")
        elif analysis['rs_rating'] >= 80:
            report.append(f"  ✓ Minervini最低基準達成 (80以上)")
        elif analysis['rs_rating'] >= 70:
            report.append(f"  △ 許容範囲 (70以上)")
        else:
            report.append(f"  ✗ 基準未達 (70未満)")
        
        report.append("=" * 70)
        
        return "\n".join(report)


if __name__ == '__main__':
    # テスト用
    from data_fetcher import fetch_stock_data
    from indicators import calculate_all_basic_indicators
    from stage_detector import StageDetector
    
    print("RS Calculator（改善版）のテストを開始...")
    print("=" * 70)
    
    test_tickers = ['AAPL', 'NVDA', 'TSLA']
    
    # ベンチマーク取得
    print("\nベンチマーク(SPY)データを取得中...")
    _, benchmark_df = fetch_stock_data('SPY', period='2y')
    
    if benchmark_df is not None:
        benchmark_df = calculate_all_basic_indicators(benchmark_df)
        print("✓ ベンチマークデータ取得完了")
        
        for ticker in test_tickers:
            print(f"\n{'=' * 70}")
            print(f"{ticker} のRS分析:")
            print(f"{'=' * 70}")
            
            stock_df, _ = fetch_stock_data(ticker, period='2y')
            
            if stock_df is not None:
                indicators_df = calculate_all_basic_indicators(stock_df)
                indicators_df = indicators_df.dropna()
                
                if len(indicators_df) >= 252:
                    # RS Calculator初期化
                    rs_calc = RSCalculator(indicators_df, benchmark_df)
                    
                    # Stage判定
                    stage_detector = StageDetector(indicators_df, benchmark_df)
                    current_stage, current_substage = stage_detector.determine_stage()
                    
                    # 包括的レポート生成
                    report = rs_calc.generate_comprehensive_report(
                        current_stage, 
                        current_substage
                    )
                    
                    print(report)
                else:
                    print(f"  データ不足（252日以上必要）")
            else:
                print(f"  データ取得失敗")
    else:
        print("エラー: ベンチマークデータの取得に失敗しました")