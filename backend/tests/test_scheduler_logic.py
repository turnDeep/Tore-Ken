import unittest
from unittest.mock import MagicMock, patch
import datetime
import pytz
import sys
import os

# Ensure backend is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Mock backend.rvol_logic BEFORE importing backend.cron_scheduler
# This prevents ImportErrors for libraries like yfinance/pandas if they aren't installed
mock_rvol = MagicMock()
sys.modules["backend.rvol_logic"] = mock_rvol

from backend import cron_scheduler

class TestCronScheduler(unittest.TestCase):

    def setUp(self):
        # We need to ensure we are testing the refactored function
        if not hasattr(cron_scheduler, 'should_run_at'):
            self.fail("cron_scheduler module does not have 'should_run_at' function. Refactor likely failed.")

    def test_should_run_at(self):
        # Helper to create JST time
        jst = pytz.timezone('Asia/Tokyo')

        def create_jst(year, month, day, hour, minute):
            return jst.localize(datetime.datetime(year, month, day, hour, minute))

        # Test Case 1: Winter (Jan 2026), Tuesday 06:15 JST -> Should Run
        # US is Standard Time. Market closes 06:00 JST.
        # Jan 13 2026 is Tuesday.
        dt = create_jst(2026, 1, 13, 6, 15)
        self.assertTrue(cron_scheduler.should_run_at(dt), "Winter Tue 06:15 should run")

        # Test Case 2: Winter, Tuesday 05:15 JST -> Should NOT Run (Too early)
        dt = create_jst(2026, 1, 13, 5, 15)
        self.assertFalse(cron_scheduler.should_run_at(dt), "Winter Tue 05:15 should NOT run")

        # Test Case 3: Summer (July 2026), Tuesday 05:15 JST -> Should Run
        # US is DST. Market closes 05:00 JST.
        # July 14 2026 is Tuesday.
        dt = create_jst(2026, 7, 14, 5, 15)
        self.assertTrue(cron_scheduler.should_run_at(dt), "Summer Tue 05:15 should run")

        # Test Case 4: Summer, Tuesday 06:15 JST -> Should NOT Run (Too late/wrong hour)
        dt = create_jst(2026, 7, 14, 6, 15)
        self.assertFalse(cron_scheduler.should_run_at(dt), "Summer Tue 06:15 should NOT run")

        # Test Case 5: Winter, Monday 06:15 JST -> Should NOT Run (Sunday US)
        # Jan 12 2026 is Monday.
        dt = create_jst(2026, 1, 12, 6, 15)
        self.assertFalse(cron_scheduler.should_run_at(dt), "Winter Mon 06:15 should NOT run")

        # Test Case 6: Winter, Saturday 06:15 JST -> Should Run (Friday US)
        # Jan 10 2026 is Saturday.
        dt = create_jst(2026, 1, 10, 6, 15)
        self.assertTrue(cron_scheduler.should_run_at(dt), "Winter Sat 06:15 should run")

        # Test Case 7: Winter, Sunday 06:15 JST -> Should NOT Run (Saturday US)
        # Jan 11 2026 is Sunday.
        dt = create_jst(2026, 1, 11, 6, 15)
        self.assertFalse(cron_scheduler.should_run_at(dt), "Winter Sun 06:15 should NOT run")

        # Test Case 8: Check UTC mapping scenario (just to be sure logic handles the 'time' part correctly)
        # If it is 20:15 UTC (previous day), it corresponds to 05:15 JST.
        # The script receives JST datetime object.
        # So we just test JST datetime.
        pass

if __name__ == '__main__':
    unittest.main()
