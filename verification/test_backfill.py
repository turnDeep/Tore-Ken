import unittest
from unittest.mock import patch, MagicMock
import os
import json
import sys
from datetime import datetime

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the module to test
from backend.data_fetcher import fetch_and_notify

class TestBackfill(unittest.TestCase):
    @patch('backend.data_fetcher.get_market_analysis_data')
    @patch('backend.data_fetcher.generate_market_chart')
    @patch('backend.data_fetcher.run_screener_for_tickers')
    @patch('backend.data_fetcher.send_push_notifications')
    @patch('backend.data_fetcher.load_tickers')
    def test_backfill_logic(self, mock_load, mock_notify, mock_screener, mock_gen_chart, mock_market_data):
        # Mock Market Data (Today + 2 Past Days)
        mock_market_data.return_value = ([
            {"date": "2026/01/11", "date_key": "20260111", "market_status": "Red", "status_text": "Red"},
            {"date": "2026/01/12", "date_key": "20260112", "market_status": "Green", "status_text": "Green"},
            {"date": "2026/01/13", "date_key": "20260113", "market_status": "Green", "status_text": "Green"} # Latest
        ], MagicMock()) # spy_df

        mock_load.return_value = ["AAPL"]
        mock_screener.return_value = [] # Return empty strong stocks for simplicity

        # Ensure DATA_DIR exists
        data_dir = os.path.join(os.getcwd(), 'data')
        os.makedirs(data_dir, exist_ok=True)

        # Scenario:
        # 20260113.json (Latest) - Should be created by main logic
        # 20260112.json - Exists (Don't touch)
        # 20260111.json - Missing (Should be backfilled)

        # Setup existing files
        with open(os.path.join(data_dir, "20260112.json"), 'w') as f:
            f.write("{}")

        # Ensure target missing file is gone
        missing_file = os.path.join(data_dir, "20260111.json")
        if os.path.exists(missing_file):
            os.remove(missing_file)

        # Run Function
        fetch_and_notify()

        # Assertions

        # 1. Main screener call (for 20260113)
        # It's called implicitly for the latest date first in the main block.
        # Then backfill loop might skip it if it exists (created just before).

        # 2. Backfill call (for 20260111)
        # Check if run_screener_for_tickers was called with target_date="2026-01-11"

        found_backfill_call = False
        for call in mock_screener.call_args_list:
            # call.kwargs might contain target_date
            if call.kwargs.get('target_date') == "2026-01-11":
                found_backfill_call = True
                break

        if not found_backfill_call:
             print("Calls:", mock_screener.call_args_list)

        self.assertTrue(found_backfill_call, "Backfill should be triggered for 2026-01-11")

        # 3. File existence
        self.assertTrue(os.path.exists(missing_file), "20260111.json should be created")

        print("Test Passed: Backfill triggered and file created.")

if __name__ == '__main__':
    unittest.main()
