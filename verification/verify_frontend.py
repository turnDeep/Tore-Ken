from playwright.sync_api import sync_playwright, expect
import time

def verify(page):
    page.goto("http://localhost:8002/")

    # Auth
    print("Entering PIN...")
    # Find inputs
    # Assuming inputs are inside #pin-inputs
    # Type '1' in first, '2' in second, etc.
    pin = "123456"
    # Wait for inputs to be visible
    page.wait_for_selector("#pin-inputs input")

    inputs = page.locator("#pin-inputs input").all()
    for i, input_el in enumerate(inputs):
        if i < len(pin):
            input_el.fill(pin[i])

    # Click button (might need to find by ID if text varies)
    page.locator("#auth-submit-button").click()

    # Wait for dashboard
    print("Waiting for dashboard...")
    page.wait_for_selector("#strong-stocks-content", timeout=15000)

    # Wait for RVol update (polled every 60s, but initial fetch is immediate)
    print("Waiting for RVol span...")
    # Ticker is AAPL (from my dummy latest.json)
    # RVol span id is #rvol-AAPL
    try:
        page.wait_for_selector("#rvol-AAPL", state="attached", timeout=10000)
        print("RVol span found.")

        # Wait a bit for fetch to complete
        time.sleep(3)

        text = page.locator("#rvol-AAPL").text_content()
        print(f"RVol Text: '{text}'")

        # Verify color is magenta
        color = page.locator("#rvol-AAPL").evaluate("el => getComputedStyle(el).color")
        print(f"Color: {color}")
        # rgb(255, 0, 255) is magenta

    except Exception as e:
        print(f"Error waiting for RVol: {e}")

    page.screenshot(path="verification/frontend_screenshot.png")
    print("Screenshot saved.")

if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        try:
            verify(page)
        except Exception as e:
            print(f"Verification failed: {e}")
            page.screenshot(path="verification/failure.png")
        finally:
            browser.close()
