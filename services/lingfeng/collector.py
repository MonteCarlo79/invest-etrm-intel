"""
LingFeng SaaS data collector.

Uses Playwright to log in to https://lingfeng-saas.tradingthink.cn,
navigate to 电力交易 → 数据咨询, configure market / indicator / date range,
click 导出 and capture the downloaded Excel file.

Typical usage:
    from services.lingfeng.collector import collect
    path = collect(
        username="your_user",
        password="your_pass",
        market="山东",
        indicator="市场供需数据",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 5, 9),
        download_dir=Path("/tmp/lingfeng"),
    )
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

_LOGIN_URL = "https://lingfeng-saas.tradingthink.cn/#/login"
_DATA_URL  = "https://lingfeng-saas.tradingthink.cn/#/powerTrading/sass/data-consultation"

# Element UI select option click timeout (ms)
_TIMEOUT = 20_000


# ---------------------------------------------------------------------------
# Internal async implementation
# ---------------------------------------------------------------------------

async def _collect_async(
    username: str,
    password: str,
    market: str,
    indicator: str,
    start_date: date,
    end_date: date,
    download_dir: Path,
    headless: bool,
) -> Path:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    download_dir.mkdir(parents=True, exist_ok=True)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(accept_downloads=True)
        page    = await context.new_page()
        page.set_default_timeout(_TIMEOUT)

        # ── 1. Login ──────────────────────────────────────────────────────
        logger.info("Opening login page …")
        await page.goto(_LOGIN_URL)
        await page.wait_for_load_state("networkidle")

        # The login form has two inputs: username (index 0) and password (index 1).
        # The form may have a "账号登录" tab — click it first to make sure we're
        # on the password-based tab, not the SMS tab.
        try:
            acct_tab = page.locator("div.login-tab-item", has_text="账号登录")
            if await acct_tab.count() > 0:
                await acct_tab.first.click()
                await page.wait_for_timeout(300)
        except Exception:
            pass

        inputs = page.locator("input")
        await inputs.nth(0).fill(username)
        await inputs.nth(1).fill(password)
        await page.locator("button", has_text="登录").first.click()
        await page.wait_for_load_state("networkidle")
        logger.info("Login submitted — waiting for redirect …")

        # Wait until we are no longer on the login page
        try:
            await page.wait_for_url(lambda u: "/login" not in u, timeout=15_000)
        except PWTimeout:
            # Check for error message
            err = page.locator("div.el-message--error")
            if await err.count() > 0:
                msg = await err.first.inner_text()
                raise RuntimeError(f"Login failed: {msg}")
            raise RuntimeError("Login did not redirect away from login page within 15 s.")

        logger.info(f"Logged in — current URL: {page.url}")

        # ── 2. Navigate to Data Consultation ─────────────────────────────
        # Some SPA shells require clicking the nav item before the route activates
        # the form — try clicking 电力交易 first, then navigate directly.
        try:
            nav_item = page.locator("li, a, span, div").filter(has_text="电力交易").first
            if await nav_item.count() > 0:
                await nav_item.click()
                await page.wait_for_timeout(800)
        except Exception:
            pass

        await page.goto(_DATA_URL)
        await page.wait_for_load_state("networkidle")

        # Wait for Vue to mount the form — poll for the first el-select
        logger.info("Waiting for form to render …")
        await page.wait_for_selector("div.el-select", timeout=30_000)
        await page.wait_for_timeout(500)   # brief extra settle
        logger.info("On data-consultation page.")

        # ── 3. Select market (市场交易) — first el-select ─────────────────
        selects = page.locator("div.el-select")
        count = await selects.count()
        logger.info(f"Found {count} el-select element(s).")

        await selects.nth(0).click()
        await page.wait_for_timeout(400)
        # Wait for dropdown to open
        await page.wait_for_selector(
            "li.el-select-dropdown__item", timeout=10_000
        )
        await page.locator("li.el-select-dropdown__item").filter(
            has_text=market
        ).first.click()
        await page.wait_for_timeout(500)
        logger.info(f"Selected market: {market}")

        # ── 4. Select indicator (指标选择) — second el-select ────────────
        # Re-query selects after DOM may have updated
        selects = page.locator("div.el-select")
        await selects.nth(1).click()
        await page.wait_for_timeout(400)
        await page.wait_for_selector(
            "li.el-select-dropdown__item", timeout=10_000
        )
        await page.locator("li.el-select-dropdown__item").filter(
            has_text=indicator
        ).first.click()
        await page.wait_for_timeout(500)
        logger.info(f"Selected indicator: {indicator}")

        # ── 5. Set date range ─────────────────────────────────────────────
        # Element UI date range picker has two inputs inside el-date-editor.
        date_inputs = page.locator(".el-date-editor input")
        # Start date
        await date_inputs.nth(0).triple_click()
        await date_inputs.nth(0).fill(start_str)
        await date_inputs.nth(0).press("Tab")
        await page.wait_for_timeout(200)
        # End date
        await date_inputs.nth(1).triple_click()
        await date_inputs.nth(1).fill(end_str)
        await date_inputs.nth(1).press("Enter")
        await page.wait_for_timeout(500)
        logger.info(f"Date range set: {start_str} → {end_str}")

        # Close any open date picker popup by clicking outside
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(300)

        # ── 6. Click 导出 and capture download ───────────────────────────
        logger.info("Clicking 导出 …")
        async with page.expect_download(timeout=60_000) as dl_info:
            await page.locator("button", has_text="导出").first.click()
        download = await dl_info.value

        suggested = download.suggested_filename or ""
        dest_name = suggested if suggested.endswith(".xlsx") else (
            f"{market}_{indicator}_{start_str}_{end_str}.xlsx"
        )
        dest = download_dir / dest_name
        await download.save_as(str(dest))
        logger.info(f"Downloaded → {dest}")

        await browser.close()

    return dest


# ---------------------------------------------------------------------------
# Public sync API
# ---------------------------------------------------------------------------

def collect(
    username: str,
    password: str,
    market: str,
    indicator: str,
    start_date: date,
    end_date: date,
    download_dir: Path,
    headless: bool = True,
) -> Path:
    """
    Login to LingFeng SaaS, download the specified data export, return local Path.

    Parameters
    ----------
    username     : LingFeng account username
    password     : LingFeng account password
    market       : 市场交易 dropdown value, e.g. "山东"
    indicator    : 指标选择 dropdown value, e.g. "市场供需数据"
    start_date   : date range start
    end_date     : date range end
    download_dir : local folder where the Excel will be saved
    headless     : run browser without a visible window (default True)

    Returns
    -------
    Path to the downloaded Excel file.
    """
    return asyncio.run(
        _collect_async(
            username, password, market, indicator,
            start_date, end_date, download_dir, headless,
        )
    )
