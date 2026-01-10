"""
Algo Scanner for HanaView
MarketAlgoXのデータを取得し、StageAlgoで分析を実行
"""

import os
import sys
import json
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional
import subprocess
from pathlib import Path

# Adjust imports to use the local modules we just created
from .gemini_client import gemini_client
from .algo_data_manager import AlgoDataManager

# Import MarketAlgoX modules
# Since we are inside backend package, we can use relative imports
from .market_algo_x.ibd_screeners import IBDScreeners
from .market_algo_x.ibd_data_collector import IBDDataCollector
from .market_algo_x.ibd_database import IBDDatabase

# Import StageAlgo modules
from .stage_algo.gamma_plotter import GammaPlotter
from .stage_algo.quantlib_ai_analyzer import QuantLibAnalyzer
from .stage_algo.quantlib_timeseries_analyzer import TimeSeriesQuantLibAnalyzer

logger = logging.getLogger(__name__)

# Paths
CHARTS_ALGO_PATH = os.getenv("CHARTS_ALGO_PATH", "/app/frontend/charts/algo")

# Metric Descriptions for Gemini Prompt
METRIC_DESCRIPTIONS = {
    "momentum_rank_1w": "1週間モメンタムランク (0-100)",
    "momentum_rank_1m": "1ヶ月モメンタムランク (0-100)",
    "momentum_rank_3m": "3ヶ月モメンタムランク (0-100)",
    "rs_rating": "RS Rating (IBD相対強度, 1-99, 99が最強)",
    "rs_sts_percentile": "RS STS Percentile (短期相対強度パーセンタイル, 0-100)",
    "eps_growth_last_qtr": "最新四半期EPS成長率(%)",
    "avg_vol_50": "50日平均出来高",
    "price_vs_50ma": "50日移動平均乖離率(%)",
    "price_change_pct": "前日比騰落率(%)",
    "vol_change_pct": "出来高変化率(%)",
    "rel_volume": "相対出来高 (Relative Volume)",
    "comp_rating": "Composite Rating (IBD総合評価, 1-99)",
    "ad_rating": "A/D Rating (機関投資家売買動向, A=買い集め, E=売り抜け)",
    "gamma_flip": "ガンマフリップレベル (市場のボラティリティ性質が変わる価格分岐点)",
    "volatility_regime": "ボラティリティ環境 (contraction=収縮/transition=移行/expansion=拡大)",
    "expected_move_30d": "30日予想変動幅(%)"
}

class AlgoScanner:
    def __init__(self):
        self.data_manager = AlgoDataManager()
        os.makedirs(CHARTS_ALGO_PATH, exist_ok=True)
        # Ensure FMP API Key is present
        self.fmp_api_key = os.getenv("FMP_API_KEY")
        if not self.fmp_api_key:
            logger.warning("FMP_API_KEY is not set. MarketAlgoX data collection will fail.")

    async def run_scan(self) -> Dict:
        """
        Algoスキャンを実行

        Returns:
            スキャン結果のサマリー
        """
        logger.info("Starting Algo scan...")

        # 1. MarketAlgoXのスクリーニング実行 (Data Collection + Screening)
        market_data = await self.run_market_algox()

        if not market_data:
            raise Exception("MarketAlgoX execution failed or returned no data")

        # 2. 各スクリーナーの銘柄を分析
        summary = {}
        volatility_distribution = {"contraction": 0, "transition": 0, "expansion": 0}

        # Initialize Database connection for profile fetching
        db = IBDDatabase()

        try:
            for screener_key, items in market_data.items():
                # items is now list of dicts: [{'ticker': 'AAPL', 'rank_1w': 99}, ...]
                logger.info(f"Analyzing screener: {screener_key} ({len(items)} symbols)")

                analyzed_symbols = []

                for item in items:
                    # Robustly handle both dict (new format) and string (old format)
                    if isinstance(item, dict):
                        ticker = item.get('ticker')
                    else:
                        ticker = str(item)
                        item = {} # Empty dict if simple string

                    if not ticker:
                        continue

                    try:
                        # Fetch profile
                        profile = db.get_company_profile(ticker)
                        sector = profile.get('sector', 'Unknown') if profile else 'Unknown'
                        industry = profile.get('industry', 'Unknown') if profile else 'Unknown'

                        # Fetch current price (for portfolio entry price)
                        price_data = db.get_price_history(ticker, days=1)
                        current_price = price_data.iloc[0]['close'] if price_data is not None and not price_data.empty else None

                        # StageAlgoで分析
                        analysis_result = await self.analyze_symbol(ticker)

                        if analysis_result:
                            # スクリーナー情報をマージ
                            # item (from screener) + profile + analysis_result
                            merged_data = {
                                'symbol': ticker, # Keep compatibility
                                'sector': sector,
                                'industry': industry,
                                'price': current_price,
                                **item, # Include all metrics from screener
                                **analysis_result
                            }
                            analyzed_symbols.append(merged_data)
                        # ボラティリティ分布を集計
                        regime = analysis_result.get('volatility_regime', 'transition')
                        volatility_distribution[regime] = volatility_distribution.get(regime, 0) + 1

                    except Exception as e:
                        logger.error(f"Error analyzing {ticker}: {e}")
                        continue

                # バッチでGemini解説を生成
                if analyzed_symbols:
                    gemini_results = await self.generate_batch_gemini_analysis(screener_key, analyzed_symbols)

                    # 結果を統合して保存
                    for symbol_data in analyzed_symbols:
                        ticker = symbol_data['ticker']
                        gemini_analysis = gemini_results.get(ticker)

                        # リスト内のデータにも解説を追加（フロントエンド表示用）
                        symbol_data['gemini_analysis'] = gemini_analysis

                        # 個別銘柄データを保存
                        self.data_manager.save_symbol_data(ticker, {
                            **symbol_data,
                            'gemini_analysis': gemini_analysis,
                            'screener_sources': [screener_key],
                            'last_updated': datetime.now().isoformat()
                        })

                summary[screener_key] = analyzed_symbols
        finally:
            if 'db' in locals():
                db.close()

        # 3. ポートフォリオ生成 (全スクリーナーの結果から)
        all_analyzed_symbols = []
        seen_tickers = set()
        for symbols in summary.values():
            for symbol_data in symbols:
                if symbol_data['ticker'] not in seen_tickers:
                    all_analyzed_symbols.append(symbol_data)
                    seen_tickers.add(symbol_data['ticker'])

        portfolios = await self.generate_portfolio_recommendations(all_analyzed_symbols)

        # 4. サマリーを保存
        summary_data = {
            'portfolios': portfolios,
            'scan_date': datetime.now().strftime('%Y-%m-%d'),
            'scan_time': datetime.now().strftime('%H:%M:%S'),
            'total_scanned': sum(len(symbols) for symbols in summary.values()),
            'summary': summary,
            'volatility_distribution': volatility_distribution,
            'updated_at': datetime.now().isoformat()
        }

        self.data_manager.save_daily_summary(summary_data)

        logger.info(f"Algo scan completed: {summary_data['total_scanned']} symbols analyzed")

        return summary_data

    async def run_market_algox(self) -> Dict[str, List[Dict]]:
        """
        MarketAlgoXのデータ収集とスクリーニングを実行
        Returns:
            Dict[screener_name, List[Dict]]
        """
        try:
            logger.info("Running MarketAlgoX Data Collection...")

            # Run in thread pool to avoid blocking async loop
            loop = asyncio.get_event_loop()

            def collect_and_screen():
                # 1. Data Collection
                collector = IBDDataCollector(self.fmp_api_key, debug=True)
                # For full run: collector.run_full_collection(use_full_dataset=False)
                # But that takes time. Let's assume we fetch tickers and run.
                # For now, let's run a lighter version or full version depending on env?
                # Let's run full collection but with limited scope if needed.
                # Warning: running full collection might take time.
                # For this implementation, we will trust IBDDataCollector logic.
                collector.run_full_collection(use_full_dataset=True, max_workers=5)
                collector.close()

                # 2. Screening
                logger.info("Running MarketAlgoX Screeners...")
                screeners = IBDScreeners()
                results = screeners.run_all_screeners()
                screeners.close()
                return results

            results = await loop.run_in_executor(None, collect_and_screen)
            return results

        except Exception as e:
            logger.error(f"MarketAlgoX run failed: {e}")
            return {}

    async def analyze_symbol(self, ticker: str) -> Optional[Dict]:
        """
        StageAlgoで銘柄を分析
        """
        try:
            loop = asyncio.get_event_loop()

            def run_stage_algo_tools():
                # 1. Gamma Plotter
                gp = GammaPlotter(ticker)
                if gp.fetch_data():
                    gp.calculate_current_gamma_levels()
                    gp.calculate_historical_metrics()
                    gamma_plot_path = gp.plot_analysis(output_dir=CHARTS_ALGO_PATH)
                    gamma_flip = gp.gamma_flip
                else:
                    gamma_plot_path = None
                    gamma_flip = None

                # 2. QuantLib AI Analyzer
                qa = QuantLibAnalyzer(ticker)
                ai_strategy = qa.run() # Returns dict

                # 3. Time Series Analyzer
                ts = TimeSeriesQuantLibAnalyzer(ticker)
                if ts.fetch_history():
                    ts.calculate_metrics()
                    ts_plot_path = ts.plot_analysis(output_dir=CHARTS_ALGO_PATH)
                    ts_report = ts.generate_report()
                    volatility_regime = ts_report.get('cycle_phase', 'transition').lower()
                    if 'contraction' in volatility_regime: volatility_regime = 'contraction'
                    elif 'expansion' in volatility_regime: volatility_regime = 'expansion'
                    else: volatility_regime = 'transition'

                    expected_move = ts_report.get('expected_move_30d')
                else:
                    ts_plot_path = None
                    volatility_regime = 'transition'
                    expected_move = None

                return {
                    'volatility_regime': volatility_regime,
                    'gamma_flip': gamma_flip,
                    'expected_move_30d': expected_move,
                    'analysis_data': {
                        'gamma_plot': f'/charts/algo/{os.path.basename(gamma_plot_path)}' if gamma_plot_path else None,
                        'timeseries_plot': f'/charts/algo/{os.path.basename(ts_plot_path)}' if ts_plot_path else None,
                        'ai_strategy': ai_strategy
                    }
                }

            return await loop.run_in_executor(None, run_stage_algo_tools)

        except Exception as e:
            logger.error(f"Error analyzing {ticker}: {e}")
            return None

    async def generate_portfolio_recommendations(self, all_symbols: List[Dict]) -> Dict:
        """
        全抽出銘柄から3つのポートフォリオ（Aggressive, Balanced, Defensive）を生成
        """
        if not all_symbols:
            return {}

        try:
            # 1. 過去のポートフォリオを取得（差分分析用）
            prev_summary = self.data_manager.load_previous_daily_summary()
            prev_portfolios = prev_summary.get('portfolios', {}) if prev_summary else {}

            # 2. プロンプト用データ構築
            candidates = []
            for item in all_symbols:
                candidates.append({
                    "ticker": item['ticker'],
                    "price": item.get('price'), # 現在価格（終値）
                    "sector": item.get('sector'),
                    "industry": item.get('industry'),
                    "volatility_regime": item.get('volatility_regime'),
                    "gemini_analysis": item.get('gemini_analysis') # 個別分析結果も参考にする
                })

            # 前回データがない場合のメッセージ
            if not prev_portfolios:
                prev_portfolio_text = "（データなし：今回は初回構築、または前回データが不足しているため、すべての銘柄を新規採用の候補として扱ってください）"
            else:
                prev_portfolio_text = json.dumps(prev_portfolios, ensure_ascii=False, indent=2)

            prompt = f"""
あなたはプロのポートフォリオマネージャーです。
以下の抽出された有望銘柄リスト（候補銘柄）から、リスク許容度の異なる3つのモデルポートフォリオ（Aggressive, Balanced, Defensive）を構築してください。
また、前回のポートフォリオ構成と比較して、変更点（新規採用、除外、継続）とその理由を解説してください。

【候補銘柄リスト (今回の購入可能銘柄)】
{json.dumps(candidates, ensure_ascii=False, indent=2)}

【前回のポートフォリオ構成】
{prev_portfolio_text}

【要件】
1. **Aggressive Portfolio**: ハイリスク・ハイリターン。成長性やモメンタムを重視。
2. **Balanced Portfolio**: リスクとリターンのバランスを重視。分散投資。
3. **Defensive Portfolio**: 安定性重視。ボラティリティが低い、またはディフェンシブなセクター。

【制約】
- ショート（空売り）は絶対に行わず、ロング（買い）のみで構成すること。
- 各ポートフォリオは最大10銘柄まで（良い銘柄がなければ少なくても可、0でも可）。
- 各ポートフォリオ内での配分比率（percentage）を決めること（合計100%になるように）。
- **entry_price** には、候補銘柄リストにある `price` を使用すること。
- **commentary** には、前回のポートフォリオからの変更理由を記述すること。
  - 例: 「NVDAを新規採用（モメンタム強）、AAPLは候補から外れたため除外（利確/損切り）」など。
  - 今回が初回（前回データなし）の場合は、すべての銘柄を新規採用として扱い、選定理由を記述すること。
- 出力は**必ず以下のJSON形式**のみとすること。Markdownコードブロックは不要。

{{
  "aggressive": {{
    "allocations": [
        {{ "ticker": "AAPL", "percentage": 40, "entry_price": 150.5 }},
        {{ "ticker": "NVDA", "percentage": 60, "entry_price": 400.0 }}
    ],
    "commentary": "ポートフォリオの変更点と選定理由の解説..."
  }},
  "balanced": {{
    "allocations": [ ... ],
    "commentary": "..."
  }},
  "defensive": {{ ... }}
}}
"""
            response_text = gemini_client.generate_content(prompt)
            if not response_text:
                return {}

            clean_text = response_text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_text)

        except Exception as e:
            logger.error(f"Error generating portfolio recommendations: {e}")
            return {}

    async def generate_batch_gemini_analysis(self, screener_key: str, symbols_data: List[Dict]) -> Dict[str, str]:
        """Gemini APIで一括解説生成"""
        try:
            # プロンプト用のデータを構築
            prompt_data = []
            for item in symbols_data:
                # Include all relevant metrics in the prompt
                # Filter out heavy objects like 'analysis_data' to keep prompt clean, but keep specific AI strategy

                # Base data for prompt
                p_item = {
                    "ticker": item['ticker'],
                    "gamma_flip": item.get('gamma_flip'),
                    "volatility_regime": item.get('volatility_regime'),
                    "expected_move_30d": item.get('expected_move_30d'),
                    "ai_strategy": item.get('analysis_data', {}).get('ai_strategy', {}),
                    "sector": item.get('sector'),
                    "industry": item.get('industry')
                }

                # Add all screener-specific metrics (keys not present in standard set)
                standard_keys = {'ticker', 'symbol', 'gamma_flip', 'volatility_regime', 'expected_move_30d', 'analysis_data', 'gemini_analysis', 'screener_sources', 'last_updated', 'sector', 'industry'}

                for k, v in item.items():
                    if k not in standard_keys:
                        p_item[k] = v

                prompt_data.append(p_item)

            # Definitions for the prompt
            definitions_text = "\n".join([f"- {k}: {v}" for k, v in METRIC_DESCRIPTIONS.items()])

            prompt = f"""
あなたはプロのテクニカル株式トレーダーです。以下の銘柄リスト（スクリーナー: {screener_key}）について、各銘柄の分析とトレーディング戦略を日本語で作成してください。

【入力データ】
{json.dumps(prompt_data, ensure_ascii=False, indent=2)}

【データの定義】
入力データに含まれる主要な指標の意味は以下の通りです：
{definitions_text}

【要件】
1. オプション分析の専門用語（ガンマ、GEX、ボラティリティ環境など）は使わず、価格アクション、支持線・抵抗線、トレンドなどのテクニカル分析用語を用いて、なぜその銘柄が有望かを説明すること。
2. スクリーナー指標（Momentum, RS Rating等）の強さを具体的な根拠として挙げること。
3. 明確なエントリーポイント、利確目標（Take Profit）、損切りライン（Stop Loss）を含めた具体的なトレードシナリオを提案すること。
4. 初心者にも分かりやすく、かつプロトレーダーの視点（需給、モメンタム）を取り入れた文章にすること。
5. 出力は**必ず以下のJSON形式**のみとすること。Markdownのコードブロックなどは含めないこと。
{{
  "TICKER": "解説テキスト（400文字以内）",
  ...
}}
"""

            response_text = gemini_client.generate_content(prompt)

            if not response_text:
                return {}

            # JSONパース（Markdownのバッククォートが含まれている場合の除去処理）
            clean_text = response_text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_text)

        except Exception as e:
            logger.error(f"Error generating batch Gemini analysis: {e}")
            return {}

# グローバルインスタンス
algo_scanner = AlgoScanner()

async def run_algo_scan() -> Dict:
    """Algoスキャンを実行（エントリーポイント）"""
    return await algo_scanner.run_scan()

async def analyze_single_ticker_algo(ticker: str) -> Optional[Dict]:
    """単一銘柄を分析（検索機能用）"""
    result = await algo_scanner.analyze_symbol(ticker)
    if result:
         db = None
         try:
             db = IBDDatabase()
             profile = db.get_company_profile(ticker)
             if profile:
                 result['sector'] = profile.get('sector')
                 result['industry'] = profile.get('industry')
         except Exception as e:
             logger.error(f"Error fetching profile for {ticker}: {e}")
         finally:
             if db:
                 db.close()
    return result
