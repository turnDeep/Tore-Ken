#!/usr/bin/env python
"""AlgoスキャナーCLI実行用スクリプト"""

import asyncio
import sys
import logging
import os
from .algo_scanner import run_algo_scan

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    """メイン実行関数"""
    print("==========================================")
    print("Algo Scanner Starting: " + os.popen('date').read().strip())
    print("==========================================")

    try:
        # スキャン実行
        result = await run_algo_scan()

        total_signals = result.get('total_scanned', 0)

        print(f"スキャン完了: {total_signals} signals")

        # Push通知送信
        try:
            from .data_fetcher import MarketDataFetcher
            fetcher = MarketDataFetcher()

            notification_data = {
                "title": "Algoスキャン完了",
                "body": f"新規シグナル: {total_signals}件",
                "type": "algo-scan"
            }

            sent_count = fetcher.send_push_notifications(notification_data)
            print(f"Push通知送信: {sent_count}件")
        except Exception as e:
            print(f"通知送信エラー: {e}")

        print("==========================================")
        print("Algo Scanner Finished: " + os.popen('date').read().strip())
        print("==========================================")
        return 0

    except Exception as e:
        print(f"❌ Algo scan failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
