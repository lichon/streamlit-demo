from playwright.async_api import async_playwright
import asyncio
import os

# Streamlit app URL from environment variable (or default)
STREAMLIT_URL = os.environ.get("STREAMLIT_APP_URL", "https://lichon.streamlit.app/")

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--window-size=1920,1080'
        ])
        page = await browser.new_page()
        try:
            await page.goto(STREAMLIT_URL, timeout=30000)
            print(f"Opened {STREAMLIT_URL}")

            # Try to find the wake-up button
            button = await page.query_selector("//button[contains(text(),'Yes, get this app back up')]")
            if button:
                print("Wake-up button found. Clicking...")
                await button.click()
                # Wait for the button to disappear
                try:
                    await page.wait_for_selector("//button[contains(text(),'Yes, get this app back up')]", state="detached", timeout=15000)
                    print("Button clicked and disappeared ✅ (app should be waking up)")
                except Exception:
                    print("Button was clicked but did NOT disappear ❌ (possible failure)")
                    exit(1)
            else:
                print("No wake-up button found. Assuming app is already awake ✅")
        except Exception as e:
            print(f"Unexpected error: {e}")
            exit(1)
        finally:
            await browser.close()
            print("Script finished.")

if __name__ == "__main__":
   asyncio.run(main())
