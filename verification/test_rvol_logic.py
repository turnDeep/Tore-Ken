import sys
import os
import pandas as pd
from datetime import datetime, time

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.rvol_logic import MarketSchedule, generate_volume_profile, RealTimeRvolAnalyzer

def test_market_schedule():
    print("Testing Market Schedule...")
    is_open = MarketSchedule.is_market_open()
    start_jst = MarketSchedule.get_market_start_jst()
    print(f"Is Market Open? {is_open}")
    print(f"Market Start JST: {start_jst}")

def test_profile_generation():
    print("\nTesting Profile Generation (AAPL)...")
    # This might fail if no internet or yfinance issues, but good to try.
    try:
        profile = generate_volume_profile("AAPL", lookback_days=5)
        print("Profile Head:")
        print(profile.head())
        return profile
    except Exception as e:
        print(f"Profile generation failed: {e}")
        return pd.DataFrame()

def test_analyzer(profile):
    print("\nTesting Analyzer...")
    if profile.empty:
        print("Skipping analyzer test due to empty profile.")
        return

    analyzer = RealTimeRvolAnalyzer("AAPL", profile)

    # Mock message 1: 09:30:00, Vol 1000
    msg1 = {
        'id': 'AAPL',
        'time': int(datetime.now().timestamp() * 1000), # Use current time for simplicity, or mock
        'dayVolume': 1000,
        'lastSize': 100
    }
    # Hack: Inject start time to match profile index if needed,
    # but the logic uses current message timestamp.
    # Let's trust the logic handles current time.

    analyzer.process_message(msg1)
    print(f"RVol after msg1: {analyzer.current_rvol}")

if __name__ == "__main__":
    test_market_schedule()
    profile = test_profile_generation()
    test_analyzer(profile)
