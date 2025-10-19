from playwright.async_api import async_playwright
import subprocess
import atexit
import asyncio
import sys
import os

# Streamlit app URL from environment variable (or default)
STREAMLIT_URL = os.environ.get("STREAMLIT_APP_URL", "https://lichon.streamlit.app/")


async def click_wake_up_button(page):
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

            # await click_wake_up_button(page)
            while True:
                await asyncio.sleep(60)  # Keep the script running
        except Exception as e:
            print(f"Unexpected error: {e}")
            exit(1)
        finally:
            await browser.close()
            print("Script finished.")


class KeepAlive:
    def __init__(self):
        self.proc: subprocess.Popen | None = None

    def __call__(
        self,
        secrets: dict = None,
    ) -> None:
        if not self.is_alive():
            self.start(secrets)
        atexit.register(self.proc.terminate)

    def terminate(self) -> None:
        if self.proc:
            self.proc.terminate()
        atexit.unregister(self.proc.terminate)

    def is_alive(self) -> bool:
        return self.proc and self.proc.poll() is None

    def start(self, secrets: dict) -> None:
        print("Starting Playwright...")
        self.proc = subprocess.Popen(
            [sys.executable, "streamlit_wakeup.py"],
            shell=False
        )


keepAlive = KeepAlive()


if __name__ == "__main__":
    os.system("playwright install")
    asyncio.run(main())
