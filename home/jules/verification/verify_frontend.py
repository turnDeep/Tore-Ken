from playwright.sync_api import sync_playwright, expect
import time
import os

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()

    # 1. Load Page
    page.goto("http://localhost:8080")

    # 2. Auth
    # Type PIN '123456'
    # Pin inputs: #pin-inputs input
    inputs = page.locator("#pin-inputs input")
    # There are 6 inputs
    for i in range(6):
        inputs.nth(i).fill(str(i+1)) # 123456

    # Click auth button
    page.get_by_role("button", name="認証").click()

    # 3. Wait for Dashboard
    # Dashboard container class '.container'
    expect(page.locator(".container")).to_be_visible()

    # 4. Wait for Strong Stocks to render
    expect(page.locator("#strong-stocks-content")).to_contain_text("Matches Found:")
    expect(page.locator("text=AAPL")).to_be_visible()
    expect(page.locator("text=NVDA")).to_be_visible()

    # 5. Wait for RealTime RVol update (app.js polls every 60s, but also calls on load?)
    # In app.js: "fetchRealTimeRVol(); // Initial fetch" in renderStrongStocks?
    # No, it's called at the end of renderStrongStocks inside:
    # if (!window.rvolInterval) { fetchRealTimeRVol(); ... }
    # So it should be called immediately after rendering.

    # Give it a moment for fetch to complete and UI to update
    # The mock server responds quickly.

    # Check AAPL text
    # text = `RVol: ${rvol.toFixed(2)}x`;
    # if (price > 0) text += ` | Pr: ${price.toFixed(2)}`;
    # if (breakout) ...

    # AAPL: rvol 1.8, pr 150.25, DAILY_BREAKOUT
    expect(page.locator("#rvol-AAPL")).to_contain_text("RVol: 1.80x")
    expect(page.locator("#rvol-AAPL")).to_contain_text("Pr: 150.25")
    expect(page.locator("#rvol-AAPL")).to_contain_text("[DAILY BO!]")

    # NVDA: rvol 3.2, pr 450.00, ORB_5M
    expect(page.locator("#rvol-NVDA")).to_contain_text("RVol: 3.20x")
    expect(page.locator("#rvol-NVDA")).to_contain_text("Pr: 450.00")
    expect(page.locator("#rvol-NVDA")).to_contain_text("[ORB 5m]")

    # 6. Screenshot
    page.screenshot(path="/home/jules/verification/verification.png")
    print("Verification screenshot saved.")

    browser.close()

if __name__ == "__main__":
    with sync_playwright() as playwright:
        run(playwright)
