#!/usr/bin/env python
"""HWBスキャナーCLI実行用スクリプト"""

import asyncio
import sys
import logging
from .hwb_scanner import run_hwb_scan

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    """メイン実行関数"""
    print("HWBスキャン開始...")

    try:
        result = await run_hwb_scan()
        
        # 正しいキー名を使用
        signals_today_count = result['summary']['signals_today_count']
        signals_recent_count = result['summary']['signals_recent_count']
        candidates_count = result['summary']['candidates_count']
        
        print(f"スキャン完了:")
        print(f"  🚀 当日ブレイクアウト: {signals_today_count}件")
        print(f"  📈 直近5営業日以内: {signals_recent_count}件")
        print(f"  📍 監視銘柄: {candidates_count}件")
        
        # Push通知送信
        try:
            from .data_fetcher import MarketDataFetcher
            fetcher = MarketDataFetcher()
            
            notification_data = {
                "title": "200MAスキャン完了",
                "body": f"当日: {signals_today_count}件 | 直近: {signals_recent_count}件 | 監視: {candidates_count}件",
                "type": "hwb-scan"
            }
            
            sent_count = fetcher.send_push_notifications(notification_data)
            print(f"Push通知送信: {sent_count}件")
        except Exception as e:
            print(f"通知送信エラー: {e}")
        
        return 0

    except Exception as e:
        print(f"エラー: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)