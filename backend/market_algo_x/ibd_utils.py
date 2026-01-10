"""
IBD Utilities

共通のユーティリティクラスと関数を提供します。
"""

import time
import threading


class RateLimiter:
    """API rate limit を管理するクラス（マルチスレッド対応）"""

    def __init__(self, max_calls_per_minute=750):
        """
        Args:
            max_calls_per_minute: 1分間の最大コール数
        """
        self.max_calls_per_minute = max_calls_per_minute
        self.min_interval = 60.0 / max_calls_per_minute
        self.lock = threading.Lock()
        self.request_times = []

    def wait_if_needed(self):
        """必要に応じて待機（スレッドセーフ）"""
        with self.lock:
            current_time = time.time()

            # 60秒以内のリクエストタイムスタンプをフィルタ
            self.request_times = [t for t in self.request_times if current_time - t < 60]

            if len(self.request_times) >= self.max_calls_per_minute:
                # 最も古いリクエストから60秒経過するまで待機
                sleep_time = 60 - (current_time - self.request_times[0]) + 0.1
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    current_time = time.time()
                    self.request_times = [t for t in self.request_times if current_time - t < 60]

            self.request_times.append(current_time)
