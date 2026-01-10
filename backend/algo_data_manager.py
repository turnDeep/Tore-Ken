"""
Algo Data Manager for HanaView
Algoスキャンデータの読み書きと管理
"""

import os
import json
import math
import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class AlgoDataManager:
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = data_dir
        self.algo_dir = os.path.join(data_dir, 'algo')
        self.daily_dir = os.path.join(self.algo_dir, 'daily')
        self.symbols_dir = os.path.join(self.algo_dir, 'symbols')

        # ディレクトリ作成
        os.makedirs(self.daily_dir, exist_ok=True)
        os.makedirs(self.symbols_dir, exist_ok=True)

    def save_daily_summary(self, summary_data: Dict) -> bool:
        """デイリーサマリーを保存"""
        try:
            # latest.json
            latest_path = os.path.join(self.daily_dir, 'latest.json')
            with open(latest_path, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, ensure_ascii=False, indent=2)

            # アーカイブ
            scan_date = summary_data.get('scan_date', datetime.now().strftime('%Y-%m-%d'))
            archive_path = os.path.join(self.daily_dir, f'algo_{scan_date}.json')
            with open(archive_path, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, ensure_ascii=False, indent=2)

            logger.info(f"Daily summary saved: {latest_path}, {archive_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving daily summary: {e}")
            return False

    def _sanitize_data(self, data):
        """Recursively replace NaN and Infinity with None to ensure JSON compliance."""
        if isinstance(data, dict):
            return {k: self._sanitize_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._sanitize_data(v) for v in data]
        elif isinstance(data, float):
            if math.isnan(data) or math.isinf(data):
                return None
        return data

    def load_latest_summary(self) -> Optional[Dict]:
        """最新のサマリーをロード"""
        try:
            latest_path = os.path.join(self.daily_dir, 'latest.json')

            if not os.path.exists(latest_path):
                return None

            with open(latest_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return self._sanitize_data(data)

        except Exception as e:
            logger.error(f"Error loading latest summary: {e}")
            return None

    def save_symbol_data(self, symbol: str, symbol_data: Dict) -> bool:
        """個別銘柄データを保存"""
        try:
            symbol_path = os.path.join(self.symbols_dir, f'{symbol.upper()}.json')

            with open(symbol_path, 'w', encoding='utf-8') as f:
                json.dump(symbol_data, f, ensure_ascii=False, indent=2)

            logger.info(f"Symbol data saved: {symbol_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving symbol data for {symbol}: {e}")
            return False

    def load_symbol_data(self, symbol: str) -> Optional[Dict]:
        """個別銘柄データをロード"""
        try:
            symbol_path = os.path.join(self.symbols_dir, f'{symbol.upper()}.json')

            if not os.path.exists(symbol_path):
                return None

            with open(symbol_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return self._sanitize_data(data)

        except Exception as e:
            logger.error(f"Error loading symbol data for {symbol}: {e}")
            return None

    def load_previous_daily_summary(self) -> Optional[Dict]:
        """
        前回のデイリーサマリーをロード
        latest.json以外の最新の日付ファイルを探す
        """
        try:
            files = [f for f in os.listdir(self.daily_dir) if f.startswith('algo_') and f.endswith('.json')]
            if not files:
                return None

            # 日付順にソート (ファイル名は algo_YYYY-MM-DD.json なので文字列ソートでOK)
            files.sort(reverse=True)

            # 最新のファイル（今日のがあればそれを除外したいが、実行タイミングによる）
            # ここではシンプルに最新のアーカイブを返す（今日実行済みなら今日のが返るかもしれないが、
            # ポートフォリオ生成前なら昨日のものになるはず...いや、save_daily_summaryは最後に呼ばれるので、
            # この関数を呼ぶ時点では今日のアーカイブはまだ作成されていないはず）

            latest_archive = files[0]
            archive_path = os.path.join(self.daily_dir, latest_archive)

            with open(archive_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return self._sanitize_data(data)

        except Exception as e:
            logger.error(f"Error loading previous summary: {e}")
            return None
