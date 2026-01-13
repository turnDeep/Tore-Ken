import unittest
from datetime import datetime
import pytz
from unittest.mock import patch, MagicMock
import sys
import os

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.cron_scheduler import main

class TestCronScheduler(unittest.TestCase):

    @patch('backend.cron_scheduler.run_fetch_job')
    @patch('backend.cron_scheduler.datetime')
    def test_summer_time_run(self, mock_datetime, mock_run_job):
        # Mock Summer Time (June) at 05:15 JST
        # US is in DST
        tz_jst = pytz.timezone('Asia/Tokyo')
        mock_now = datetime(2025, 6, 15, 5, 15, 0, tzinfo=tz_jst)
        mock_datetime.now.return_value = mock_now

        main()

        mock_run_job.assert_called_once()
        print("Summer Time (05:15) Test Passed: Job Triggered")

    @patch('backend.cron_scheduler.run_fetch_job')
    @patch('backend.cron_scheduler.datetime')
    def test_summer_time_skip_6am(self, mock_datetime, mock_run_job):
        # Mock Summer Time (June) at 06:15 JST
        # Should NOT run (already ran at 5)
        tz_jst = pytz.timezone('Asia/Tokyo')
        mock_now = datetime(2025, 6, 15, 6, 15, 0, tzinfo=tz_jst)
        mock_datetime.now.return_value = mock_now

        main()

        mock_run_job.assert_not_called()
        print("Summer Time (06:15) Test Passed: Job Skipped")

    @patch('backend.cron_scheduler.run_fetch_job')
    @patch('backend.cron_scheduler.datetime')
    def test_winter_time_run(self, mock_datetime, mock_run_job):
        # Mock Winter Time (January) at 06:15 JST
        # US is NOT in DST
        tz_jst = pytz.timezone('Asia/Tokyo')
        mock_now = datetime(2025, 1, 15, 6, 15, 0, tzinfo=tz_jst)
        mock_datetime.now.return_value = mock_now

        main()

        mock_run_job.assert_called_once()
        print("Winter Time (06:15) Test Passed: Job Triggered")

    @patch('backend.cron_scheduler.run_fetch_job')
    @patch('backend.cron_scheduler.datetime')
    def test_winter_time_skip_5am(self, mock_datetime, mock_run_job):
        # Mock Winter Time (January) at 05:15 JST
        # Should NOT run (too early)
        tz_jst = pytz.timezone('Asia/Tokyo')
        mock_now = datetime(2025, 1, 15, 5, 15, 0, tzinfo=tz_jst)
        mock_datetime.now.return_value = mock_now

        main()

        mock_run_job.assert_not_called()
        print("Winter Time (05:15) Test Passed: Job Skipped")

if __name__ == '__main__':
    unittest.main()
