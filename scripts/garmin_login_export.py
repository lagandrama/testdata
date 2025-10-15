# scripts/garmin_login_export.py
from __future__ import annotations
from pathlib import Path
from playwright.sync_api import sync_playwright

STATE_FILE = Path("./state/garmin.json")               # what your code reads later
PROFILE_DIR = Path("./state/playwright-chrome-profile")  # keeps the live session between runs
PROFILE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

START_URL = "https://connect.garmin.com/modern/"

def main():
    with sync_playwright() as pw:
        # Use persistent context so it behaves like a real profile
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
            ],
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )

        # Hide navigator.webdriver early
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)

        page = context.new_page()
        page.goto(START_URL)

        print(
            "\n1) In the window that opened, sign in to Garmin (SSO)."
            "\n2) Complete 2FA if prompted."
            "\n3) Wait until the Modern dashboard loads fully."
        )
        input("\nWhen the dashboard is visible, press ENTER here to save cookies... ")

        context.storage_state(path=str(STATE_FILE))
        print(f"\n✅ Saved cookie state to: {STATE_FILE.resolve()}")
        context.close()

if __name__ == "__main__":
    main()
