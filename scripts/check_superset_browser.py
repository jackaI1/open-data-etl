"""Render the imported dashboard in Chromium, using only this owned browser."""

import os
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

completed, errors = [], []


def chart_response(response):
    if "/api/v1/chart/data" not in response.url or response.request.method != "POST":
        return
    body = response.json()
    if response.status != 200 or any(r.get("status") != "success" for r in body.get("result", [])):
        errors.append(str(body)[:1500])
    else:
        completed.append(body)


with sync_playwright() as playwright:
    browser = playwright.chromium.launch()
    page = None
    try:
        page = browser.new_page(viewport={"width": 1440, "height": 2200})
        page.on("response", chart_response)
        page.goto("http://127.0.0.1:18088/login/")
        page.locator("#username").fill("admin")
        page.locator("#password").fill(os.environ["SUPERSET_ADMIN_PASSWORD"])
        page.get_by_role("button", name="Sign In").click()
        page.goto("http://127.0.0.1:18088/superset/dashboard/catalogue-quality/")
        deadline = time.monotonic() + 120
        while len(completed) < 11 and not errors and time.monotonic() < deadline:
            page.wait_for_timeout(500)
        page.wait_for_load_state("networkidle", timeout=60000)
        assert not errors, errors
        assert len(completed) >= 11, f"only {len(completed)} successful browser queries"
        assert page.get_by_text("Products processed", exact=True).count() >= 1
        Path("data/superset").mkdir(parents=True, exist_ok=True)
        page.screenshot(path="data/superset/dashboard.png", full_page=True)
        print(f"Chromium rendered dashboard after {len(completed)} successful query responses")
    finally:
        if page is not None:
            page.close()
        browser.close()
