import json
import os
import asyncio
import concurrent.futures
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import pandas as pd
import numpy as np
import uuid
from dotenv import load_dotenv
from .hwb_data_manager import HWBDataManager
import logging
import warnings
from .rs_calculator import RSCalculator
from .image_generator import generate_stock_chart

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)
load_dotenv()

# --- Constants ---
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '10'))

# Rule 1: Trend Filter
WEEKLY_TREND_THRESHOLD = float(os.getenv('WEEKLY_TREND_THRESHOLD', '0.0'))

# Rule 2: Setup
SETUP_LOOKBACK_DAYS = int(os.getenv('SETUP_LOOKBACK_DAYS', '30'))
INITIAL_SCAN_MIN_HISTORY_DAYS = int(os.getenv('INITIAL_SCAN_MIN_HISTORY_DAYS', '1000'))

# Rule 3: FVG Detection (bot_hwb.py方式)
FVG_MIN_GAP_PERCENTAGE = float(os.getenv('FVG_MIN_GAP_PERCENTAGE', '0.001'))  # 0.1%
FVG_MAX_SEARCH_DAYS = int(os.getenv('FVG_MAX_SEARCH_DAYS', '20'))
PROXIMITY_PERCENTAGE = float(os.getenv('PROXIMITY_PERCENTAGE', '0.05'))  # 5%
FVG_ZONE_PROXIMITY = float(os.getenv('FVG_ZONE_PROXIMITY', '0.10'))  # 10%

# Rule 4: Breakout (bot_hwb.py方式)
BREAKOUT_THRESHOLD = float(os.getenv('BREAKOUT_THRESHOLD', '0.001'))  # 0.1%


class HWBAnalyzer:
    """HWB分析エンジン（bot_hwb.py方式に統一）"""
    
    def __init__(self):
        self.market_regime = 'TRENDING'
        self.params = self._adaptive_parameters()

    def _adaptive_parameters(self):
        """市場環境に応じたパラメータ調整"""
        params = {
            'TRENDING': {
                'setup_lookback': 30, 
                'fvg_search_days': 20, 
                'ma_proximity': 0.05, 
                'breakout_threshold': 0.001  # 固定0.1%
            },
        }
        return params.get(self.market_regime, params['TRENDING'])

    def optimized_rule1(self, df_daily: pd.DataFrame, df_weekly: pd.DataFrame) -> bool:
        """Rule ①: 週足トレンドフィルター（現時点チェック用）"""
        if df_weekly is None or df_weekly.empty:
            return False
        if 'sma200' not in df_weekly.columns or df_weekly['sma200'].isna().all():
            return False

        latest_weekly = df_weekly.iloc[-1]
        if pd.isna(latest_weekly['sma200']) or latest_weekly['sma200'] == 0:
            return False

        weekly_deviation = (latest_weekly['close'] - latest_weekly['sma200']) / latest_weekly['sma200']
        return weekly_deviation >= WEEKLY_TREND_THRESHOLD

    def check_weekly_trend_at_date(self, df_weekly: pd.DataFrame, check_date: pd.Timestamp) -> bool:
        """特定日時点での週足トレンドフィルター"""
        if df_weekly is None or df_weekly.empty:
            return False
        
        df_weekly_historical = df_weekly[df_weekly.index <= check_date]
        
        if df_weekly_historical.empty:
            return False
        
        if 'sma200' not in df_weekly_historical.columns or df_weekly_historical['sma200'].isna().all():
            return False
        
        latest_weekly_at_date = df_weekly_historical.iloc[-1]
        
        if pd.isna(latest_weekly_at_date['sma200']) or latest_weekly_at_date['sma200'] == 0:
            return False
        
        weekly_deviation = (
            (latest_weekly_at_date['close'] - latest_weekly_at_date['sma200']) 
            / latest_weekly_at_date['sma200']
        )
        
        return weekly_deviation >= WEEKLY_TREND_THRESHOLD

    def optimized_rule2_setups(
        self, 
        df_daily: pd.DataFrame, 
        df_weekly: pd.DataFrame,
        full_scan: bool = False,
        scan_start_date: Optional[pd.Timestamp] = None
    ) -> List[Dict]:
        """Rule ②: セットアップ検出（週足フィルター統合＋全期間対応版）"""
        setups = []
        
        # スキャン範囲の決定
        if full_scan:
            scan_start_index = max(0, INITIAL_SCAN_MIN_HISTORY_DAYS)
            logger.info(f"セットアップ検出：全期間スキャン（{scan_start_index}日目〜{len(df_daily)}日目）")
        elif scan_start_date:
            try:
                scan_start_index = df_daily.index.searchsorted(scan_start_date)
            except:
                scan_start_index = max(0, len(df_daily) - SETUP_LOOKBACK_DAYS)
        else:
            scan_start_index = max(0, len(df_daily) - SETUP_LOOKBACK_DAYS)
        
        if scan_start_index >= len(df_daily):
            return setups
        
        # ATR計算（全期間）
        atr = (df_daily['high'] - df_daily['low']).rolling(14).mean()
        
        for i in range(scan_start_index, len(df_daily)):
            row = df_daily.iloc[i]
            setup_date = df_daily.index[i]
            
            # この日付時点で週足200MAフィルターをチェック
            if not self.check_weekly_trend_at_date(df_weekly, setup_date):
                continue
            
            if pd.isna(row.get('sma200')) or pd.isna(row.get('ema200')):
                continue

            # MAゾーン計算
            zone_width = abs(row['sma200'] - row['ema200'])
            if atr.iloc[i] > 0:
                zone_width = max(zone_width, row['close'] * (atr.iloc[i] / row['close']) * 0.5)

            zone_upper = max(row['sma200'], row['ema200']) + zone_width * 0.2
            zone_lower = min(row['sma200'], row['ema200']) - zone_width * 0.2

            # セットアップ判定
            if zone_lower <= row['open'] <= zone_upper and zone_lower <= row['close'] <= zone_upper:
                setup = {
                    'id': str(uuid.uuid4()),
                    'date': setup_date,
                    'type': 'PRIMARY',
                    'status': 'active',
                    'weekly_deviation': self._get_weekly_deviation_at_date(df_weekly, setup_date)
                }
                setups.append(setup)
            elif (zone_lower <= row['open'] <= zone_upper) or (zone_lower <= row['close'] <= zone_upper):
                body_center = (row['open'] + row['close']) / 2
                if zone_lower <= body_center <= zone_upper:
                    setup = {
                        'id': str(uuid.uuid4()),
                        'date': setup_date,
                        'type': 'SECONDARY',
                        'status': 'active',
                        'weekly_deviation': self._get_weekly_deviation_at_date(df_weekly, setup_date)
                    }
                    setups.append(setup)
        
        logger.info(f"セットアップ検出完了：{len(setups)}件")
        return setups

    def _get_weekly_deviation_at_date(self, df_weekly: pd.DataFrame, check_date: pd.Timestamp) -> Optional[float]:
        """指定日時点での週足200MAからの乖離率を取得（記録用）"""
        try:
            df_weekly_historical = df_weekly[df_weekly.index <= check_date]
            if df_weekly_historical.empty:
                return None
            
            latest = df_weekly_historical.iloc[-1]
            if pd.isna(latest['sma200']) or latest['sma200'] == 0:
                return None
            
            return (latest['close'] - latest['sma200']) / latest['sma200']
        except:
            return None

    def _check_fvg_ma_proximity(self, candle_3: pd.Series, candle_1: pd.Series) -> bool:
        """
        FVGがMA近接条件を満たすかチェック（bot_hwb.py方式）
        
        条件A: 3本目の始値or終値がMA±5%以内
        条件B: FVGゾーンの中心がMA±10%以内
        """
        if pd.isna(candle_3.get('sma200')) or pd.isna(candle_3.get('ema200')):
            return False
        
        # 条件A: 3本目の始値or終値がMA±5%以内
        for price in [candle_3['open'], candle_3['close']]:
            sma_deviation = abs(price - candle_3['sma200']) / candle_3['sma200']
            ema_deviation = abs(price - candle_3['ema200']) / candle_3['ema200']
            if sma_deviation <= PROXIMITY_PERCENTAGE or ema_deviation <= PROXIMITY_PERCENTAGE:
                return True
        
        # 条件B: FVGゾーンの中心がMA±10%以内
        fvg_center = (candle_1['high'] + candle_3['low']) / 2
        sma_deviation = abs(fvg_center - candle_3['sma200']) / candle_3['sma200']
        ema_deviation = abs(fvg_center - candle_3['ema200']) / candle_3['ema200']
        
        return sma_deviation <= FVG_ZONE_PROXIMITY or ema_deviation <= FVG_ZONE_PROXIMITY

    def optimized_fvg_detection(self, df_daily: pd.DataFrame, setup: Dict) -> List[Dict]:
        """
        Rule ③: FVG検出（bot_hwb.py方式、スコアリング削除）
        
        条件:
        1. candle_3のlow > candle_1のhigh (ギャップ存在)
        2. ギャップ率 > 0.1%
        3. MA近接条件を満たす
        """
        fvgs = []
        setup_date = setup['date']
        
        try:
            setup_idx = df_daily.index.get_loc(setup_date)
        except KeyError:
            return fvgs

        max_days = self.params['fvg_search_days']
        search_end = min(setup_idx + max_days, len(df_daily) - 1)

        for i in range(setup_idx + 2, search_end):
            if i >= len(df_daily):
                break
                
            candle_1 = df_daily.iloc[i-2]
            candle_3 = df_daily.iloc[i]
            
            # FVG条件: candle_3のlowがcandle_1のhighより上
            if candle_3['low'] <= candle_1['high']:
                continue

            # ギャップ率チェック（0.1%以上）
            gap_percentage = (candle_3['low'] - candle_1['high']) / candle_1['high']
            if gap_percentage < FVG_MIN_GAP_PERCENTAGE:
                continue

            # MA近接条件チェック（bot_hwb.py方式）
            if not self._check_fvg_ma_proximity(candle_3, candle_1):
                continue

            # FVGとして認識（スコア不要）
            fvg = {
                'id': str(uuid.uuid4()),
                'setup_id': setup['id'],
                'formation_date': df_daily.index[i],
                'gap_percentage': gap_percentage,
                'lower_bound': candle_1['high'],
                'upper_bound': candle_3['low'],
                'status': 'active'
            }
            fvgs.append(fvg)
        
        return fvgs

    def optimized_breakout_detection_all_periods(
        self, 
        df_daily: pd.DataFrame, 
        setup: Dict, 
        fvg: Dict
    ) -> Optional[Dict]:
        """
        Rule ④: ブレイクアウト検出（bot_hwb.py方式、スコアリング削除）
        
        条件:
        1. レジスタンス = セットアップ〜FVG間の最高値
        2. 終値 > レジスタンス * (1 + 0.1%)
        3. FVG下限が破られていない
        """
        try:
            setup_idx = df_daily.index.get_loc(setup['date'])
            fvg_idx = df_daily.index.get_loc(fvg['formation_date'])
        except KeyError:
            return None

        # レジスタンスレベル計算（bot_hwb.py方式：単純な最高値）
        resistance_start_idx = setup_idx + 1
        resistance_end_idx = fvg_idx
        
        if resistance_end_idx <= resistance_start_idx:
            resistance_start_idx = max(0, setup_idx - 10)
            resistance_end_idx = setup_idx + 1
        
        resistance_data = df_daily.iloc[resistance_start_idx:resistance_end_idx]
        
        if resistance_data.empty:
            return None

        # シンプルな最高値をレジスタンスとする
        resistance_high = resistance_data['high'].max()

        # FVG違反チェック
        post_fvg_data = df_daily.iloc[fvg_idx:]
        if post_fvg_data['low'].min() < fvg['lower_bound'] * 0.98:
            return {
                'status': 'violated', 
                'violated_date': post_fvg_data['low'].idxmin()
            }

        # ブレイクアウトチェック（FVG形成日から現在まで、固定閾値0.1%）
        for i in range(fvg_idx + 1, len(df_daily)):
            current = df_daily.iloc[i]

            # bot_hwb.py方式：固定閾値0.1%
            if current['close'] > resistance_high * (1 + BREAKOUT_THRESHOLD):
                breakout_date = df_daily.index[i]

                # 出来高増加率を計算
                volume_metrics = self._calculate_volume_increase_at_date(df_daily, breakout_date)

                result = {
                    'status': 'breakout',
                    'breakout_date': breakout_date,
                    'breakout_price': current['close'],
                    'resistance_price': resistance_high,
                    'breakout_percentage': (current['close'] / resistance_high - 1) * 100
                }

                # 出来高情報を追加
                if volume_metrics:
                    result['breakout_volume'] = volume_metrics['breakout_volume']
                    result['avg_volume_20d'] = volume_metrics['avg_volume_20d']
                    result['volume_increase_pct'] = volume_metrics['volume_increase_pct']

                return result

        return None

    def _calculate_volume_increase_at_date(self, df_daily: pd.DataFrame, target_date: pd.Timestamp) -> Optional[Dict]:
        """
        ブレイクアウト日の出来高増加率を計算（20日平均との比較）

        Args:
            df_daily: 日次データ
            target_date: ブレイクアウト日

        Returns:
            {
                'breakout_volume': ブレイクアウト時の出来高,
                'avg_volume_20d': 20日平均出来高,
                'volume_increase_pct': 増加率（パーセント）
            }
        """
        try:
            # volumeカラムの確認
            if 'volume' not in df_daily.columns:
                logger.warning(f"'volume' column not found in dataframe. Available columns: {df_daily.columns.tolist()}")
                return None

            # target_date以前のデータを取得
            df_historical = df_daily[df_daily.index <= target_date].copy()

            # 最低21日のデータが必要（20日平均を計算するため）
            if len(df_historical) < 21:
                logger.debug(f"Insufficient data for volume calculation at {target_date}")
                return None

            # ブレイクアウト日の出来高
            breakout_volume = df_historical.iloc[-1]['volume']

            # 20日平均出来高（ブレイクアウト日の前日までの20日間）
            avg_volume_20d = df_historical.iloc[-21:-1]['volume'].mean()

            if avg_volume_20d == 0 or pd.isna(avg_volume_20d):
                logger.warning(f"Invalid average volume at {target_date}")
                return None

            # 増加率を計算（パーセント）
            volume_increase_pct = ((breakout_volume / avg_volume_20d) - 1) * 100

            logger.debug(f"Volume increase at {target_date}: {volume_increase_pct:.1f}% (breakout: {breakout_volume:,.0f}, avg: {avg_volume_20d:,.0f})")

            return {
                'breakout_volume': int(breakout_volume),
                'avg_volume_20d': int(avg_volume_20d),
                'volume_increase_pct': round(volume_increase_pct, 1)
            }

        except Exception as e:
            logger.error(f"Error calculating volume increase: {e}", exc_info=True)
            return None


class HWBScanner:
    """メインスキャナー（bot_hwb.py方式に統一）"""
    
    def __init__(self):
        self.data_manager = HWBDataManager()
        self.analyzer = HWBAnalyzer()
        self.benchmark_df = None  # ベンチマークデータをキャッシュ

    def _get_benchmark_data(self):
        """S&P500（SPY）データをベンチマークとして取得"""
        if self.benchmark_df is not None:
            return self.benchmark_df

        try:
            logger.info("Loading S&P500 (SPY) benchmark data...")
            data = self.data_manager.get_stock_data_with_cache('SPY', lookback_years=10)
            if data:
                self.benchmark_df, _ = data
                logger.info(f"Benchmark data loaded: {len(self.benchmark_df)} days")
            return self.benchmark_df
        except Exception as e:
            logger.error(f"Failed to load benchmark data: {e}")
            return None

    def _calculate_rs_rating_at_date(self, df_daily: pd.DataFrame, target_date: pd.Timestamp) -> Optional[float]:
        """指定日時点でのRS Ratingを計算"""
        try:
            # ✅ カラム名の確認
            if 'close' not in df_daily.columns:
                logger.warning(f"'close' column not found in dataframe. Available columns: {df_daily.columns.tolist()}")
                return None

            benchmark_df = self._get_benchmark_data()
            if benchmark_df is None or 'close' not in benchmark_df.columns:
                logger.warning("Benchmark data not available or missing 'close' column")
                return None

            # target_date以前のデータのみを使用
            df_historical = df_daily[df_daily.index <= target_date].copy()
            benchmark_historical = benchmark_df[benchmark_df.index <= target_date].copy()

            # 最低252日のデータが必要
            if len(df_historical) < 252 or len(benchmark_historical) < 252:
                logger.debug(f"Insufficient data for RS calculation at {target_date}")
                return None

            # RSCalculatorを使用してRS Ratingを計算
            rs_calc = RSCalculator(df_historical, benchmark_historical)
            rs_score_series = rs_calc.calculate_ibd_rs_score()
            current_rs_score = rs_score_series.iloc[-1]
            rs_rating = rs_calc.calculate_percentile_rating(current_rs_score)

            logger.debug(f"RS Rating calculated: {rs_rating:.0f}")
            return round(rs_rating)

        except Exception as e:
            logger.error(f"Error calculating RS rating: {e}", exc_info=True)
            return None

    async def scan_all_symbols(self, progress_callback=None):
        """全シンボルスキャン"""
        symbols = list(self.data_manager.get_russell3000_symbols())
        total = len(symbols)
        logger.info(f"スキャン開始: {total}銘柄")
        scan_start_time = datetime.now()

        all_results = []
        processed_count = 0
        
        for i in range(0, total, BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_symbol = {
                    executor.submit(self._analyze_and_save_symbol, symbol): symbol 
                    for symbol in batch
                }
                for future in concurrent.futures.as_completed(future_to_symbol):
                    processed_count += 1
                    try:
                        result = future.result()
                        if result:
                            all_results.extend(result)
                    except Exception as exc:
                        logger.error(f"エラー: {future_to_symbol[future]} - {exc}", exc_info=True)
                    if progress_callback:
                        await progress_callback(processed_count, total)
            await asyncio.sleep(0.1)

        summary = self._create_daily_summary(all_results, total, scan_start_time)
        self.data_manager.save_daily_summary(summary)
        logger.info("スキャン完了")
        return summary

    def _analyze_and_save_symbol(self, symbol: str) -> Optional[List[Dict]]:
        """単一銘柄分析（状態ベース差分処理版）"""
        try:
            data = self.data_manager.get_stock_data_with_cache(symbol)
            if not data:
                return None
            
            df_daily, df_weekly = data
            if df_daily.empty or df_weekly.empty:
                return None

            df_daily.index = pd.to_datetime(df_daily.index)
            df_weekly.index = pd.to_datetime(df_weekly.index)
            df_daily = df_daily[~df_daily.index.duplicated(keep='last')]
            df_weekly = df_weekly[~df_weekly.index.duplicated(keep='last')]

            latest_market_date = df_daily.index[-1].date()

            # Rule ①: 現時点のトレンドフィルター（初期チェック）
            if not self.analyzer.optimized_rule1(df_daily, df_weekly):
                return None

            # 既存データ確認
            existing_data = self.data_manager.load_symbol_data(symbol)
            
            if existing_data:
                result = self._differential_analysis(symbol, df_daily, df_weekly, existing_data, latest_market_date)
            else:
                result = self._full_analysis(symbol, df_daily, df_weekly, latest_market_date)
            
            return result

        except Exception as e:
            logger.error(f"分析エラー: {symbol} - {e}", exc_info=True)
            return None

    def _differential_analysis(self, symbol: str, df_daily: pd.DataFrame, df_weekly: pd.DataFrame,
                              existing_data: dict, latest_market_date: datetime.date) -> Optional[List[Dict]]:
        """差分分析（RS Rating追加版）"""
        existing_setups = existing_data.get('setups', [])
        existing_fvgs = existing_data.get('fvgs', [])
        existing_signals = existing_data.get('signals', [])
        
        for item in existing_setups + existing_fvgs + existing_signals:
            if 'date' in item:
                item['date'] = pd.to_datetime(item['date'])
            if 'formation_date' in item:
                item['formation_date'] = pd.to_datetime(item['formation_date'])
            if 'breakout_date' in item:
                item['breakout_date'] = pd.to_datetime(item['breakout_date'])
        
        active_setups = [s for s in existing_setups if s.get('status') == 'active']
        active_fvgs = [f for f in existing_fvgs if f.get('status') == 'active']
        
        last_analyzed_date = pd.to_datetime(existing_data.get('last_updated', '2000-01-01')).date()
        
        if latest_market_date <= last_analyzed_date:
            logger.debug(f"{symbol}: 新しいデータなし")
            return self._create_summary_from_existing(existing_data, latest_market_date)
        
        logger.info(f"{symbol}: 差分分析 ({last_analyzed_date} → {latest_market_date})")
        
        updated = False
        new_fvgs_found = []
        
        # アクティブセットアップからFVG探索
        if active_setups:
            for setup in active_setups:
                setup_date = setup['date']
                setup_idx = df_daily.index.get_loc(setup_date)
                
                setup_fvgs = [f for f in existing_fvgs if f.get('setup_id') == setup['id']]
                if setup_fvgs:
                    last_fvg_date = max(f['formation_date'] for f in setup_fvgs)
                    search_start_date = last_fvg_date + pd.Timedelta(days=1)
                    search_start = df_daily.index.searchsorted(search_start_date)
                else:
                    search_start = setup_idx + 2
                
                search_end = min(setup_idx + FVG_MAX_SEARCH_DAYS, len(df_daily) - 1)
                new_data_start = df_daily.index.searchsorted(pd.Timestamp(last_analyzed_date) + pd.Timedelta(days=1))
                search_start = max(search_start, new_data_start)
                
                if search_start >= search_end:
                    continue
                
                new_fvgs = self._detect_fvg_in_range(df_daily, setup, search_start, search_end)
                
                if new_fvgs:
                    existing_data['fvgs'].extend(new_fvgs)
                    new_fvgs_found.extend(new_fvgs)
                    updated = True
        
        # ブレイクアウトチェック
        all_active_fvgs = active_fvgs + new_fvgs_found
        
        if all_active_fvgs:
            for fvg in all_active_fvgs:
                setup = next((s for s in existing_setups if s['id'] == fvg['setup_id']), None)
                if not setup or setup.get('status') == 'consumed':
                    continue
                
                fvg_date = fvg['formation_date']
                fvg_idx = df_daily.index.get_loc(fvg_date)
                new_data_start = df_daily.index.searchsorted(pd.Timestamp(last_analyzed_date) + pd.Timedelta(days=1))
                check_start = max(fvg_idx + 1, new_data_start)
                
                if check_start >= len(df_daily):
                    continue
                
                breakout = self._check_breakout_in_range(df_daily, setup, fvg, check_start, len(df_daily))
                
                if breakout and breakout.get('status') == 'breakout':
                    # ✅ RS Ratingを計算
                    breakout_date = pd.to_datetime(breakout['breakout_date'])
                    rs_rating = self._calculate_rs_rating_at_date(df_daily, breakout_date)

                    signal = {**fvg, **breakout}
                    if rs_rating is not None:
                        signal['rs_rating'] = rs_rating
                        logger.info(f"{symbol}: RS Rating at breakout = {rs_rating}")

                    existing_data['signals'].append(signal)
                    
                    setup['status'] = 'consumed'
                    for related_fvg in existing_data['fvgs']:
                        if related_fvg.get('setup_id') == setup['id']:
                            related_fvg['status'] = 'consumed'
                    
                    updated = True
                    break
                
                elif breakout and breakout.get('status') == 'violated':
                    fvg['status'] = 'violated'
                    fvg['violated_date'] = breakout.get('violated_date')
                    updated = True
        
        # 新規セットアップ探索
        if not active_setups or all(s.get('status') == 'consumed' for s in existing_setups):
            logger.info(f"{symbol}: 新セットアップ探索")
            new_start_date = pd.Timestamp(last_analyzed_date) + pd.Timedelta(days=1)
            new_setups = self.analyzer.optimized_rule2_setups(df_daily, df_weekly, full_scan=False, scan_start_date=new_start_date)
            
            if new_setups:
                existing_data['setups'].extend(new_setups)
                updated = True
                logger.info(f"{symbol}: {len(new_setups)}件の新セットアップ")
        
        if updated:
            existing_data['last_updated'] = datetime.now().isoformat()
            self._save_symbol_data_with_chart(symbol, existing_data, df_daily, df_weekly)
        
        return self._create_summary_from_existing(existing_data, latest_market_date)

    def _full_analysis(self, symbol: str, df_daily: pd.DataFrame, df_weekly: pd.DataFrame,
                      latest_market_date: datetime.date) -> Optional[List[Dict]]:
        """初回フルスキャン（RS Rating追加版）"""
        logger.info(f"{symbol}: 初回フルスキャン（全期間：{len(df_daily)}日分）")
        
        setups = self.analyzer.optimized_rule2_setups(df_daily, df_weekly, full_scan=True)
        
        if not setups:
            logger.info(f"{symbol}: セットアップなし（全期間）")
            return None

        consumed_setups = set()
        consumed_fvgs = set()
        all_fvgs = []
        all_signals = []

        for s in setups:
            s['date'] = pd.to_datetime(s['date'])

        for setup in setups:
            if setup['id'] in consumed_setups:
                setup['status'] = 'consumed'
                continue

            fvgs = self.analyzer.optimized_fvg_detection(df_daily, setup)
            signal_found_for_this_setup = False
            
            for fvg in fvgs:
                if fvg['id'] in consumed_fvgs:
                    fvg['status'] = 'consumed'
                    all_fvgs.append(fvg)
                    continue

                breakout = self.analyzer.optimized_breakout_detection_all_periods(df_daily, setup, fvg)

                if breakout:
                    if breakout.get('status') == 'breakout':
                        # ✅ RS Ratingを計算（ブレイクアウト時点）
                        breakout_date = pd.to_datetime(breakout['breakout_date'])
                        rs_rating = self._calculate_rs_rating_at_date(df_daily, breakout_date)

                        signal = {**fvg, **breakout}
                        if rs_rating is not None:
                            signal['rs_rating'] = rs_rating
                            logger.info(f"{symbol}: RS Rating at breakout = {rs_rating}")
                        
                        all_signals.append(signal)
                        consumed_setups.add(setup['id'])
                        consumed_fvgs.add(fvg['id'])
                        fvg['status'] = 'consumed'
                        setup['status'] = 'consumed'
                        signal_found_for_this_setup = True
                        break
                    
                    elif breakout.get('status') == 'violated':
                        fvg['status'] = 'violated'
                        fvg['violated_date'] = breakout.get('violated_date')
                else:
                    fvg['status'] = 'active'
                
                all_fvgs.append(fvg)
            
            if not signal_found_for_this_setup:
                setup['status'] = 'active'

        if not all_signals and not any(f['status'] == 'active' for f in all_fvgs):
            logger.info(f"{symbol}: アクティブなFVG/シグナルなし")
            return None

        def stringify_dates(d):
            for k, v in d.items():
                if isinstance(v, pd.Timestamp):
                    d[k] = v.strftime('%Y-%m-%d')
            return d

        symbol_data = {
            "symbol": symbol,
            "last_updated": datetime.now().isoformat(),
            "market_regime": self.analyzer.market_regime,
            "setups": [stringify_dates(s.copy()) for s in setups],
            "fvgs": [stringify_dates(f.copy()) for f in all_fvgs],
            "signals": [stringify_dates(s.copy()) for s in all_signals]
        }
        
        logger.info(
            f"{symbol}: 完了 - セットアップ:{len(setups)}, "
            f"FVG:{len(all_fvgs)}, シグナル:{len(all_signals)}"
        )
        
        self._save_symbol_data_with_chart(symbol, symbol_data, df_daily, df_weekly)
        return self._create_summary_from_data(symbol, all_signals, all_fvgs, latest_market_date)

    def _detect_fvg_in_range(self, df_daily: pd.DataFrame, setup: Dict, start_idx: int, end_idx: int) -> List[Dict]:
        """指定範囲内でFVG検出（bot_hwb.py方式）"""
        fvgs = []
        
        for i in range(start_idx, end_idx):
            if i < 2:
                continue
            
            candle_1 = df_daily.iloc[i-2]
            candle_3 = df_daily.iloc[i]
            
            if candle_3['low'] <= candle_1['high']:
                continue
            
            gap_percentage = (candle_3['low'] - candle_1['high']) / candle_1['high']
            if gap_percentage < FVG_MIN_GAP_PERCENTAGE:
                continue
            
            # MA近接条件チェック（bot_hwb.py方式）
            if not self.analyzer._check_fvg_ma_proximity(candle_3, candle_1):
                continue
            
            fvg = {
                'id': str(uuid.uuid4()),
                'setup_id': setup['id'],
                'formation_date': df_daily.index[i],
                'gap_percentage': gap_percentage,
                'lower_bound': candle_1['high'],
                'upper_bound': candle_3['low'],
                'status': 'active'
            }
            fvgs.append(fvg)
        
        return fvgs

    def _check_breakout_in_range(self, df_daily: pd.DataFrame, setup: Dict, fvg: Dict,
                                 start_idx: int, end_idx: int) -> Optional[Dict]:
        """指定範囲内でブレイクアウトチェック（RS Rating追加版）"""
        try:
            setup_idx = df_daily.index.get_loc(setup['date'])
            fvg_idx = df_daily.index.get_loc(fvg['formation_date'])
        except KeyError:
            return None
        
        resistance_start_idx = setup_idx + 1
        resistance_end_idx = fvg_idx
        
        if resistance_end_idx <= resistance_start_idx:
            resistance_start_idx = max(0, setup_idx - 10)
            resistance_end_idx = setup_idx + 1
        
        resistance_data = df_daily.iloc[resistance_start_idx:resistance_end_idx]
        
        if resistance_data.empty:
            return None
        
        resistance_high = resistance_data['high'].max()
        
        for i in range(start_idx, end_idx):
            current = df_daily.iloc[i]
            if current['close'] > resistance_high * (1 + BREAKOUT_THRESHOLD):
                breakout_date = df_daily.index[i]

                # 出来高増加率を計算
                volume_metrics = self.analyzer._calculate_volume_increase_at_date(df_daily, breakout_date)

                result = {
                    'status': 'breakout',
                    'breakout_date': breakout_date,
                    'breakout_price': current['close'],
                    'resistance_price': resistance_high,
                    'breakout_percentage': (current['close'] / resistance_high - 1) * 100
                }

                # 出来高情報を追加
                if volume_metrics:
                    result['breakout_volume'] = volume_metrics['breakout_volume']
                    result['avg_volume_20d'] = volume_metrics['avg_volume_20d']
                    result['volume_increase_pct'] = volume_metrics['volume_increase_pct']

                return result

        return None

    def _create_summary_from_data(self, symbol: str, signals: list, fvgs: list,
                                 latest_market_date: datetime.date) -> List[Dict]:
        """シグナルとFVGからサマリー作成（RS Rating保持）"""
        summary_results = []
        today = latest_market_date
        
        business_days_back = 0
        current_date = today
        while business_days_back < 5:
            current_date -= timedelta(days=1)
            if current_date.weekday() < 5:
                business_days_back += 1
        five_business_days_ago = current_date

        for signal in signals:
            breakout_date_str = signal.get('breakout_date')
            if breakout_date_str:
                try:
                    breakout_date = pd.to_datetime(breakout_date_str).date()

                    summary_item = {
                        "symbol": symbol,
                        "signal_type": "",
                        "category": ""
                    }

                    # ✅ RS Ratingを含める
                    if 'rs_rating' in signal:
                        summary_item['rs_rating'] = signal['rs_rating']

                    # ✅ 出来高情報を含める
                    if 'volume_increase_pct' in signal:
                        summary_item['volume_increase_pct'] = signal['volume_increase_pct']
                    if 'breakout_volume' in signal:
                        summary_item['breakout_volume'] = signal['breakout_volume']
                    if 'avg_volume_20d' in signal:
                        summary_item['avg_volume_20d'] = signal['avg_volume_20d']

                    if breakout_date == today:
                        summary_item["signal_type"] = "signal_today"
                        summary_item["signal_date"] = breakout_date_str
                        summary_item["category"] = "当日ブレイクアウト"
                        summary_results.append(summary_item)
                    elif five_business_days_ago <= breakout_date < today:
                        summary_item["signal_type"] = "signal_recent"
                        summary_item["signal_date"] = breakout_date_str
                        summary_item["category"] = "直近5営業日以内"
                        summary_results.append(summary_item)
                except Exception as e:
                    logger.warning(f"Failed to parse breakout_date for {symbol}: {e}")
        
        for fvg in fvgs:
            if fvg.get('status') == 'active':
                formation_date_str = fvg.get('formation_date')
                if formation_date_str:
                    try:
                        formation_date = pd.to_datetime(formation_date_str).date()

                        if five_business_days_ago <= formation_date <= today:
                            summary_results.append({
                                "symbol": symbol,
                                "signal_type": "candidate",
                                "fvg_date": formation_date_str,
                                "category": "監視銘柄"
                            })
                    except Exception as e:
                        logger.warning(f"Failed to parse formation_date for {symbol}: {e}")
        
        return summary_results

    def _create_summary_from_existing(self, existing_data: dict, latest_market_date: datetime.date) -> List[Dict]:
        """
        既存データから最新の日付基準でサマリーを再作成

        Args:
            existing_data: 既存の銘柄分析データ
            latest_market_date: 最新の市場日付

        Returns:
            サマリーリスト
        """
        signals = existing_data.get('signals', [])
        fvgs = existing_data.get('fvgs', [])
        symbol = existing_data.get('symbol', 'UNKNOWN')

        # _create_summary_from_dataを再利用
        return self._create_summary_from_data(symbol, signals, fvgs, latest_market_date)

    def _save_symbol_data_with_chart(self, symbol: str, symbol_data: dict,
                                 df_daily: pd.DataFrame, df_weekly: pd.DataFrame):
        """
        シンボルデータとチャートデータを保存
        """
        try:
            # チャートデータ生成
            chart_data = self._generate_lightweight_chart_data(symbol_data, df_daily, df_weekly)
            symbol_data['chart_data'] = chart_data

            # データ保存
            self.data_manager.save_symbol_data(symbol, symbol_data)
            logger.info(f"✅ Saved data for {symbol}")

            # 静的チャート画像の生成（対象銘柄のみ）
            self._generate_static_chart_if_needed(symbol, symbol_data, df_daily)

        except Exception as e:
            logger.error(f"Failed to save data for {symbol}: {e}", exc_info=True)

    def _generate_static_chart_if_needed(self, symbol: str, symbol_data: dict, df_daily: pd.DataFrame):
        """
        対象カテゴリ（当日ブレイクアウト、直近5営業日、監視銘柄）に含まれる場合、
        静的なチャート画像を生成する。
        """
        try:
            latest_date = df_daily.index[-1].date()

            # 5営業日前を計算（簡易的）
            # 正確には _create_summary_from_data と同じロジックが必要だが、
            # ここでは直近10日(暦日)以内程度で判定して生成しておく（広めに生成しても問題ない）
            check_threshold_date = latest_date - timedelta(days=10)

            is_target = False

            # シグナルチェック (当日 or 直近)
            for s in symbol_data.get('signals', []):
                if 'breakout_date' in s:
                    b_date = pd.to_datetime(s['breakout_date']).date()
                    if b_date >= check_threshold_date:
                        is_target = True
                        break

            # 監視銘柄チェック (FVG)
            if not is_target:
                for f in symbol_data.get('fvgs', []):
                    if f.get('status') == 'active' and 'formation_date' in f:
                        f_date = pd.to_datetime(f['formation_date']).date()
                        if f_date >= check_threshold_date:
                            is_target = True
                            break

            if is_target:
                output_dir = os.path.join(os.path.dirname(__file__), '..', 'frontend', 'charts')
                generate_stock_chart(symbol, df_daily, output_dir, symbol_data)
                logger.debug(f"Generated chart for {symbol}")

        except Exception as e:
            logger.error(f"Failed to generate static chart for {symbol}: {e}")

    def _generate_lightweight_chart_data(self, symbol_data: dict, df_daily: pd.DataFrame, df_weekly: pd.DataFrame) -> dict:
        """チャートデータ生成"""
        df_plot = df_daily.copy()
        df_plot['weekly_sma200_val'] = df_weekly['sma200'].reindex(df_plot.index, method='ffill')

        def format_series(df, col):
            s = df[[col]].dropna()
            return [{"time": i.strftime('%Y-%m-%d'), "value": r[col]} for i, r in s.iterrows()]

        def clean_np_types(d):
            for k, v in d.items():
                if isinstance(v, (np.int64, np.int32)):
                    d[k] = int(v)
                if isinstance(v, (np.float64, np.float32)):
                    d[k] = float(v)
            return d

        candles = [{
            "time": i.strftime('%Y-%m-%d'),
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close
        } for i, r in df_plot.iterrows()]

        volume_data = []
        for i, r in df_plot.iterrows():
            color = '#26a69a' if r['close'] >= r['open'] else '#ef5350'
            volume_data.append({
                "time": i.strftime('%Y-%m-%d'),
                "value": r['volume'],
                "color": color
            })

        markers = []

        for fvg in symbol_data.get('fvgs', []):
            try:
                formation_date = pd.to_datetime(fvg['formation_date'])
                if formation_date in df_plot.index:
                    formation_idx = df_plot.index.get_loc(formation_date)
                    if formation_idx >= 1:
                        middle_candle_date = df_plot.index[formation_idx - 1]
                        color_map = {
                            'active': '#FFD700',
                            'consumed': '#9370DB',
                            'violated': '#808080'
                        }
                        markers.append({
                            "time": middle_candle_date.strftime('%Y-%m-%d'),
                            "position": "inBar",
                            "color": color_map.get(fvg.get('status'), '#FFD700'),
                            "shape": "circle",
                            "text": "🐮"
                        })
            except Exception as e:
                logger.warning(f"FVGマーカー生成エラー: {symbol_data.get('symbol', 'N/A')} - {e}")

        for s in symbol_data.get('signals', []):
            markers.append({
                "time": s['breakout_date'],
                "position": "belowBar",
                "color": "#FF00FF",
                "shape": "arrowUp",
                "text": "Break"
            })

        return {
            'candles': candles,
            'sma200': format_series(df_plot, 'sma200'),
            'ema200': format_series(df_plot, 'ema200'),
            'weekly_sma200': format_series(df_plot, 'weekly_sma200_val'),
            'volume': [clean_np_types(v) for v in volume_data],
            'markers': [clean_np_types(m) for m in markers]
        }

    def _create_daily_summary(self, results: List[Dict], total_scanned: int, start_time: datetime) -> Dict:
        """日次サマリー作成（3カテゴリ対応、スコアリング削除）"""
        end_time = datetime.now()

        def _merge_and_sort(items: List[Dict], date_key: str) -> List[Dict]:
            """重複を除去してソート（スコアリング削除）"""
            merged = {}
            for item in items:
                if date_key not in item or 'symbol' not in item:
                    continue
                key = (item['symbol'], item[date_key])
                if key not in merged:
                    merged[key] = item
            # スコアリング削除：シンボル名でソート
            return sorted(list(merged.values()), key=lambda x: x.get('symbol', ''))

        # カテゴリ別に分類
        signals_today = [r for r in results if r.get('signal_type') == 'signal_today']
        signals_recent = [r for r in results if r.get('signal_type') == 'signal_recent']
        candidates = [r for r in results if r.get('signal_type') == 'candidate']

        unique_signals_today = _merge_and_sort(signals_today, 'signal_date')
        unique_signals_recent = _merge_and_sort(signals_recent, 'signal_date')
        unique_candidates = _merge_and_sort(candidates, 'fvg_date')
        
        return {
            "scan_date": end_time.strftime('%Y-%m-%d'),
            "scan_time": end_time.strftime('%H:%M:%S'),
            "scan_duration_seconds": (end_time - start_time).total_seconds(),
            "total_scanned": total_scanned,
            "summary": {
                "signals_today_count": len(unique_signals_today),
                "signals_recent_count": len(unique_signals_recent),
                "candidates_count": len(unique_candidates),
                "signals_today": unique_signals_today,
                "signals_recent": unique_signals_recent,
                "candidates": unique_candidates
            },
            "performance": {
                "avg_time_per_symbol_ms": ((end_time - start_time).total_seconds() / total_scanned * 1000) if total_scanned > 0 else 0
            }
        }


async def run_hwb_scan(progress_callback=None):
    """スキャン実行エントリーポイント"""
    scanner = HWBScanner()
    summary = await scanner.scan_all_symbols(progress_callback)
    
    logger.info(
        f"完了 - 当日: {summary['summary']['signals_today_count']}, "
        f"直近: {summary['summary']['signals_recent_count']}, "
        f"監視: {summary['summary']['candidates_count']}"
    )
    return summary


async def analyze_single_ticker(symbol: str) -> Optional[Dict]:
    """単一銘柄分析"""
    scanner = HWBScanner()
    scanner._analyze_and_save_symbol(symbol)
    return scanner.data_manager.load_symbol_data(symbol)