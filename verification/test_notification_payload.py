import unittest
from unittest.mock import patch, MagicMock
import json
import sys
import os

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.data_fetcher import send_push_notifications

class TestNotificationPayload(unittest.TestCase):

    @patch('backend.data_fetcher.webpush')
    @patch('backend.data_fetcher.security_manager')
    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='{"sub1": {"endpoint": "https://test.com", "keys": {"p256dh": "key", "auth": "auth"}}}')
    @patch('os.path.exists', return_value=True)
    def test_payload_format(self, mock_exists, mock_open, mock_sec_manager, mock_webpush):
        # Mock data
        daily_data = {
            "date": "2026-01-13",
            "status_text": "Green",
            "setup_stocks": ["AAPL", "TSLA"]
        }

        send_push_notifications(daily_data)

        # Verify webpush call arguments
        args, kwargs = mock_webpush.call_args
        data_sent = json.loads(kwargs['data'])

        expected_body = "Date: 2026-01-13\nStatus: Green / Stocks: 2"
        print(f"Actual Body: {data_sent['body']}")

        self.assertEqual(data_sent['body'], expected_body)
        self.assertEqual(data_sent['title'], "Market Data Updated")

if __name__ == '__main__':
    unittest.main()
