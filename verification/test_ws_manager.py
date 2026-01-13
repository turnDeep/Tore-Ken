import asyncio
import os
import sys
import logging

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.ws_manager import WebSocketManager

# Setup basic logging
logging.basicConfig(level=logging.INFO)

async def test_manager():
    print("Testing WebSocketManager...")
    manager = WebSocketManager.get_instance()

    # 1. Load Tickers
    manager.load_tickers()
    print(f"Tickers: {manager.tickers}")
    if not manager.tickers:
        print("No tickers loaded. Check data/latest.json")
        return

    # 2. Initialize Analyzers
    # This calls generate_volume_profile which hits the network
    print("Initializing analyzers (network call)...")
    await manager.initialize_analyzers()
    print(f"Analyzers: {manager.analyzers.keys()}")

    # 3. Test get_rvols
    rvols = manager.get_all_rvols()
    print(f"Initial RVols: {rvols}")

    # 4. Start (short run)
    # We won't actually connect to WS if market is closed, unless we set DEBUG_WS
    os.environ["DEBUG_WS"] = "true"

    print("Starting background task (running for 5 seconds)...")
    await manager.start()

    await asyncio.sleep(5)

    await manager.stop()
    print("Stopped.")

if __name__ == "__main__":
    asyncio.run(test_manager())
