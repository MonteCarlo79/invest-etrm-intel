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
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_LOGIN_URL = "https://lingfeng-saas.tradingthink.cn/#/login"

# Substrings that indicate wrong credentials (not a transient/network error).
# When any of these appear in the login-page error message the run halts
# immediately to avoid triggering the account-lock mechanism.
_CREDENTIAL_ERROR_PHRASES = (
    "密码错误", "账号或密码", "用户名或密码", "账号不存在",
    "密码不正确", "password", "incorrect", "invalid",
    "账户已被锁定", "账号已锁定", "locked",
)


class CredentialError(RuntimeError):
    """Raised when the LingFeng login fails due to wrong credentials.

    Signals to the pipeline that it must halt immediately without retrying,
    to prevent the account-lock mechanism from triggering.
    """
_DATA_URL  = "https://lingfeng-saas.tradingthink.cn/#/powerTrading/market"

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
            # Save debug screenshot to diagnose what's blocking the login
            _debug_dir = Path(__file__).resolve().parent.parent.parent / "debug" / "lingfeng"
            _debug_dir.mkdir(parents=True, exist_ok=True)
            _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            _shot = _debug_dir / f"login_fail_{_ts}.png"
            try:
                await page.screenshot(path=str(_shot), full_page=True)
                logger.error(f"Login failure screenshot saved → {_shot}")
            except Exception:
                pass
            # Check for error message (Element UI and Ant Design variants)
            _login_err_msg = ""
            for _sel in ("div.el-message--error", ".ant-message-error", ".ant-alert-error",
                         "div[class*='error']", "span[class*='error']"):
                _err = page.locator(_sel)
                if await _err.count() > 0:
                    try:
                        _login_err_msg = (await _err.first.inner_text()).strip()
                    except Exception:
                        pass
                    if _login_err_msg:
                        break
            # Also scan body text for credential error phrases
            _body = ""
            try:
                _body = await page.locator("body").inner_text()
                logger.error(f"Page body (first 400 chars): {_body[:400]!r}")
            except Exception:
                pass
            _combined = (_login_err_msg + " " + _body).lower()
            if any(ph.lower() in _combined for ph in _CREDENTIAL_ERROR_PHRASES):
                hint = _login_err_msg or "(see page body above)"
                raise CredentialError(
                    f"Login credentials rejected by LingFeng — pipeline halted to "
                    f"prevent account lockout. Error: {hint!r}. "
                    f"Update LINGFENG_PASSWORD in config/.env and delete "
                    f"services/lingfeng/CREDENTIAL_HALT to resume."
                )
            if _login_err_msg:
                raise RuntimeError(f"Login failed: {_login_err_msg}")
            raise RuntimeError("Login did not redirect away from login page within 15 s.")

        logger.info(f"Logged in — current URL: {page.url}")

        # ── 2. Navigate to Data Consultation ─────────────────────────────
        await page.goto(_DATA_URL)
        await page.wait_for_load_state("networkidle")

        # Form uses Ant Design — wait for .ant-select-selector to be visible
        logger.info("Waiting for form to render …")
        await page.wait_for_selector(".ant-select-selector", timeout=30_000)
        await page.wait_for_timeout(300)
        logger.info("On data-consultation page.")

        # ── 3. Select market (市场交易) — first ant-select ────────────────
        # Only change if current value differs from requested market
        current_market = await page.locator(
            ".ant-select-selection-item"
        ).nth(0).inner_text()
        if current_market.strip() != market.strip():
            logger.info(f"Current market '{current_market}' ≠ '{market}' — selecting …")
            await page.locator(".ant-select-selector").nth(0).click()
            await page.wait_for_selector(".ant-select-dropdown", timeout=10_000)
            await page.wait_for_timeout(300)
            # Scroll through the virtual list until the target option is rendered
            _list = page.locator(".rc-virtual-list-holder").first
            _found = False
            for _step in range(50):
                _opt = page.locator(".ant-select-item-option-content").filter(has_text=market)
                if await _opt.count() > 0:
                    await _opt.first.click()
                    _found = True
                    break
                await _list.evaluate("el => { el.scrollTop += 120; }")
                await page.wait_for_timeout(80)
            if not _found:
                raise RuntimeError(f"Market '{market}' not found in dropdown after scrolling")
            await page.wait_for_timeout(400)
        else:
            logger.info(f"Market already set to '{market}' — no change needed.")
        logger.info(f"Market: {market}")

        # ── 4. Select indicator (指标选择) — second ant-select ───────────
        current_indicator = await page.locator(
            ".ant-select-selection-item"
        ).nth(1).inner_text()
        if current_indicator.strip() != indicator.strip():
            logger.info(f"Current indicator '{current_indicator}' ≠ '{indicator}' — selecting …")
            await page.locator(".ant-select-selector").nth(1).click()
            await page.wait_for_selector(".ant-select-item-option-content", timeout=10_000)
            await page.locator(".ant-select-item-option-content").filter(
                has_text=indicator
            ).first.click()
            await page.wait_for_timeout(400)
        else:
            logger.info(f"Indicator already set to '{indicator}' — no change needed.")
        logger.info(f"Indicator: {indicator}")

        # ── 5. Set date range ─────────────────────────────────────────────
        # Ant Design range picker: input[date-range="start"] and input[date-range="end"]
        start_input = page.locator("input[date-range='start']").first
        end_input   = page.locator("input[date-range='end']").first

        await start_input.click()
        await page.wait_for_timeout(300)
        await start_input.fill(start_str)
        await start_input.press("Tab")
        await page.wait_for_timeout(300)

        await end_input.click()
        await end_input.fill(end_str)
        await end_input.press("Enter")
        await page.wait_for_timeout(400)

        # Close any open date picker popup by clicking outside
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(300)
        logger.info(f"Date range set: {start_str} → {end_str}")

        # ── 6. Click 导出 and capture download ───────────────────────────
        # Button text is "导 出" (with a space) and has class ant-btn-primary
        logger.info("Clicking 导 出 …")
        async with page.expect_download(timeout=60_000) as dl_info:
            await page.locator("button.ant-btn-primary").first.click()
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
# Session-based: login once, download all chunks for one market
# ---------------------------------------------------------------------------

async def _collect_province_async(
    username: str,
    password: str,
    market: str,
    indicator: str,
    chunks: list,       # list[tuple[date, date]]
    download_dir: Path,
    headless: bool,
) -> list:             # list[tuple[date, date, Path]]
    """Login once and download all date chunks for a single market.

    Returns list of (chunk_start, chunk_end, path) for successfully downloaded chunks.
    Failed chunks are logged and skipped; page state is recovered before the next chunk.
    """
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    download_dir.mkdir(parents=True, exist_ok=True)
    results: list = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(accept_downloads=True)
        page    = await context.new_page()
        page.set_default_timeout(_TIMEOUT)

        # ── 1. Login (once) ───────────────────────────────────────────────
        logger.info(f"[{market}] Opening login page …")
        await page.goto(_LOGIN_URL)
        await page.wait_for_load_state("networkidle")

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
        logger.info(f"[{market}] Login submitted — waiting for redirect …")

        try:
            await page.wait_for_url(lambda u: "/login" not in u, timeout=15_000)
        except PWTimeout:
            _debug_dir = Path(__file__).resolve().parent.parent.parent / "debug" / "lingfeng"
            _debug_dir.mkdir(parents=True, exist_ok=True)
            _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            _shot = _debug_dir / f"login_fail_{market}_{_ts}.png"
            try:
                await page.screenshot(path=str(_shot), full_page=True)
                logger.error(f"[{market}] Login failure screenshot → {_shot}")
            except Exception:
                pass
            _login_err_msg = ""
            for _sel in ("div.el-message--error", ".ant-message-error", ".ant-alert-error",
                         "div[class*='error']", "span[class*='error']"):
                _err = page.locator(_sel)
                if await _err.count() > 0:
                    try:
                        _login_err_msg = (await _err.first.inner_text()).strip()
                    except Exception:
                        pass
                    if _login_err_msg:
                        break
            _body = ""
            try:
                _body = await page.locator("body").inner_text()
                logger.error(f"[{market}] Page body (first 400 chars): {_body[:400]!r}")
            except Exception:
                pass
            _combined = (_login_err_msg + " " + _body).lower()
            if any(ph.lower() in _combined for ph in _CREDENTIAL_ERROR_PHRASES):
                hint = _login_err_msg or "(see page body above)"
                raise CredentialError(
                    f"Login credentials rejected by LingFeng — pipeline halted to "
                    f"prevent account lockout. Error: {hint!r}. "
                    f"Update LINGFENG_PASSWORD in config/.env and delete "
                    f"services/lingfeng/CREDENTIAL_HALT to resume."
                )
            if _login_err_msg:
                raise RuntimeError(f"Login failed: {_login_err_msg}")
            raise RuntimeError("Login did not redirect away from login page within 15 s.")

        logger.info(f"[{market}] Logged in — current URL: {page.url}")

        # ── Helper: navigate to data page and select market + indicator ───
        async def _goto_data_page() -> None:
            await page.goto(_DATA_URL)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_selector(".ant-select-selector", timeout=30_000)
            await page.wait_for_timeout(800)  # extra time for form to fully initialise

            # Select market
            _current_market = ""
            try:
                _current_market = (await page.locator(".ant-select-selection-item").nth(0).inner_text()).strip()
            except Exception:
                pass
            if _current_market != market.strip():
                await page.locator(".ant-select-selector").nth(0).click()
                # Wait for dropdown AND at least one option to be rendered
                await page.wait_for_selector(".ant-select-dropdown:not(.ant-select-dropdown-hidden)", timeout=10_000)
                await page.wait_for_selector(".ant-select-item-option-content", timeout=10_000)
                await page.wait_for_timeout(500)
                _list = page.locator(".rc-virtual-list-holder")
                # Scroll to top first so we start searching from the beginning
                if await _list.count() > 0:
                    await _list.first.evaluate("el => { el.scrollTop = 0; }")
                    await page.wait_for_timeout(200)
                _found = False
                for _step in range(80):
                    _opt = page.locator(".ant-select-item-option-content").filter(has_text=market)
                    if await _opt.count() > 0:
                        await _opt.first.click()
                        _found = True
                        break
                    if await _list.count() > 0:
                        await _list.first.evaluate("el => { el.scrollTop += 100; }")
                    await page.wait_for_timeout(100)
                if not _found:
                    await page.keyboard.press("Escape")
                    raise RuntimeError(f"Market '{market}' not found in dropdown after scrolling")
                await page.wait_for_timeout(400)

            # Select indicator
            _current_indicator = ""
            try:
                _current_indicator = (await page.locator(".ant-select-selection-item").nth(1).inner_text()).strip()
            except Exception:
                pass
            if _current_indicator != indicator.strip():
                await page.locator(".ant-select-selector").nth(1).click()
                await page.wait_for_selector(".ant-select-item-option-content", timeout=10_000)
                await page.locator(".ant-select-item-option-content").filter(
                    has_text=indicator
                ).first.click()
                await page.wait_for_timeout(400)

        # ── 2. Navigate to Data Consultation and set market + indicator ───
        await _goto_data_page()
        logger.info(f"[{market}] Ready — market & indicator selected, starting chunk loop.")

        # ── 3. Loop over chunks ───────────────────────────────────────────
        for idx, (chunk_start, chunk_end) in enumerate(chunks):
            start_str = chunk_start.strftime("%Y-%m-%d")
            end_str   = chunk_end.strftime("%Y-%m-%d")
            logger.info(f"[{market}] Chunk {idx+1}/{len(chunks)}: {start_str} → {end_str}")

            try:
                start_input = page.locator("input[date-range='start']").first
                end_input   = page.locator("input[date-range='end']").first

                await start_input.click()
                await page.wait_for_timeout(300)
                await start_input.fill(start_str)
                await start_input.press("Tab")
                await page.wait_for_timeout(300)

                await end_input.click()
                await end_input.fill(end_str)
                await end_input.press("Enter")
                await page.wait_for_timeout(400)

                await page.keyboard.press("Escape")
                await page.wait_for_timeout(300)

                logger.info(f"[{market}] Clicking 导 出 …")
                async with page.expect_download(timeout=60_000) as dl_info:
                    await page.locator("button.ant-btn-primary").first.click()
                download = await dl_info.value

                # Always include date range in filename to avoid collisions across chunks
                suggested = download.suggested_filename or ""
                base = suggested.rsplit(".", 1)[0] if suggested.endswith(".xlsx") else f"{market}_{indicator}"
                dest_name = f"{base}_{start_str}_{end_str}.xlsx"
                dest = download_dir / dest_name
                await download.save_as(str(dest))
                logger.info(f"[{market}] Downloaded → {dest}")

                results.append((chunk_start, chunk_end, dest))
                # Brief pause between chunks to avoid rapid-fire requests
                await page.wait_for_timeout(800)

            except Exception as exc:
                logger.error(f"[{market}] Chunk {start_str}–{end_str} failed: {exc}")
                # Recover page state for the next chunk
                try:
                    logger.info(f"[{market}] Recovering page state …")
                    await _goto_data_page()
                    logger.info(f"[{market}] Page recovered — continuing.")
                except Exception as recover_exc:
                    logger.warning(f"[{market}] Page recovery failed: {recover_exc} — aborting remaining chunks.")
                    break

        await browser.close()

    return results


def collect_province(
    username: str,
    password: str,
    market: str,
    indicator: str,
    chunks: list,       # list[tuple[date, date]]
    download_dir: Path,
    headless: bool = True,
) -> list:             # list[tuple[date, date, Path]]
    """Login once and download all date chunks for a single market.

    Returns list of (chunk_start, chunk_end, path) for successfully downloaded chunks.
    Reduces login count from N_chunks to 1 per province, avoiding rate-limiting.
    """
    return asyncio.run(
        _collect_province_async(
            username, password, market, indicator, chunks, download_dir, headless,
        )
    )


# ---------------------------------------------------------------------------
# Public sync API (single chunk — kept for backward compatibility)
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
