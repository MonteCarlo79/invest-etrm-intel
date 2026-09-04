"""
Quick diagnostic: logs in, navigates to market page, clicks 查询,
then dumps every button/anchor element with its text and class.

Run:
    py services/lingfeng/debug_buttons.py
"""
import asyncio
import os
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent.parent
_DEBUG_DIR = _REPO / "debug" / "lingfeng"
_DEBUG_DIR.mkdir(parents=True, exist_ok=True)

_env_file = _REPO / "config" / ".env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(str(_env_file))
    except ImportError:
        pass

USERNAME = os.environ.get("LINGFENG_USERNAME", "")
PASSWORD = os.environ.get("LINGFENG_PASSWORD", "")
_LOGIN_URL = "https://lingfeng-saas.tradingthink.cn/#/login"
_DATA_URL  = "https://lingfeng-saas.tradingthink.cn/#/powerTrading/market"
MARKET = "山西"


async def run():
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page    = await context.new_page()
        page.set_default_timeout(20_000)

        # Login
        print("[1] Logging in …")
        await page.goto(_LOGIN_URL)
        await page.wait_for_load_state("networkidle")
        try:
            tab = page.locator("div.login-tab-item", has_text="账号登录")
            if await tab.count() > 0:
                await tab.first.click()
                await page.wait_for_timeout(300)
        except Exception:
            pass
        inputs = page.locator("input")
        await inputs.nth(0).fill(USERNAME)
        await inputs.nth(1).fill(PASSWORD)
        await page.locator("button", has_text="登录").first.click()
        await page.wait_for_url(lambda u: "/login" not in u, timeout=15_000)
        print(f"    Logged in → {page.url}")

        # Navigate to market page
        print(f"\n[2] Navigating to {_DATA_URL} …")
        await page.goto(_DATA_URL)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_selector(".ant-select-selector", timeout=30_000)
        await page.wait_for_timeout(800)
        print(f"    URL: {page.url}")

        # Select market
        print(f"\n[3] Selecting market '{MARKET}' …")
        current = (await page.locator(".ant-select-selection-item").nth(0).inner_text()).strip()
        if current != MARKET:
            await page.locator(".ant-select-selector").nth(0).click()
            await page.wait_for_selector(".ant-select-dropdown:not(.ant-select-dropdown-hidden)", timeout=10_000)
            await page.wait_for_timeout(300)
            _list = page.locator(".rc-virtual-list-holder").first
            for _ in range(80):
                _opt = page.locator(".ant-select-item-option-content").filter(has_text=MARKET)
                if await _opt.count() > 0:
                    await _opt.first.click()
                    break
                await _list.evaluate("el => { el.scrollTop += 100; }")
                await page.wait_for_timeout(80)
            await page.wait_for_timeout(400)
        print(f"    Market set.")

        # Set date range
        print("\n[4] Setting date range (2026-07-05 → 2026-07-06) …")
        start_input = page.locator("input[date-range='start']").first
        end_input   = page.locator("input[date-range='end']").first
        await start_input.click()
        await page.wait_for_timeout(300)
        await start_input.fill("2026-07-05")
        await start_input.press("Tab")
        await page.wait_for_timeout(300)
        await end_input.click()
        await end_input.fill("2026-07-06")
        await end_input.press("Enter")
        await page.wait_for_timeout(400)
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(300)
        print("    Dates set.")

        # Click 查询
        print("\n[5] Clicking 查询 (ant-btn-primary) …")
        await page.locator("button.ant-btn-primary").first.click()
        await page.wait_for_timeout(3000)
        print("    Done waiting after 查询.")

        # Dump ALL visible elements containing 导出 / export
        print("\n[6] All elements containing '导' (any tag):")
        for tag in ["button", "span", "div", "a", "li", "i"]:
            elems = await page.locator(tag).filter(has_text="导").all()
            for i, el in enumerate(elems):
                try:
                    txt  = repr((await el.inner_text()).strip()[:60])
                    cls  = (await el.get_attribute("class") or "")[:60]
                    role = await el.get_attribute("role") or ""
                    vis  = await el.is_visible()
                    if vis:
                        print(f"    <{tag}>[{i}] visible={vis} role='{role}' cls='{cls}' text={txt}")
                except Exception:
                    pass

        # Also check role="button" elements
        print("\n[7] Elements with role='button':")
        role_btns = await page.locator("[role='button']").all()
        for i, el in enumerate(role_btns):
            try:
                txt = repr((await el.inner_text()).strip()[:60])
                cls = (await el.get_attribute("class") or "")[:60]
                vis = await el.is_visible()
                print(f"    [{i}] visible={vis} cls='{cls}' text={txt}")
            except Exception:
                pass

        # Dump all VISIBLE buttons for reference
        print("\n[8] All visible <button> elements:")
        btns = await page.locator("button:visible").all()
        for i, btn in enumerate(btns):
            try:
                txt  = repr((await btn.inner_text()).strip())
                cls  = (await btn.get_attribute("class") or "")[:60]
                print(f"    [{i}] cls='{cls}'  text={txt}")
            except Exception as e:
                print(f"    [{i}] error: {e}")

        # Screenshot
        shot = _DEBUG_DIR / "debug_buttons.png"
        await page.screenshot(path=str(shot), full_page=False)
        print(f"\n[8] Screenshot → {shot}")

        print("\n[DONE] Close the browser window or press Ctrl+C to exit.")
        await page.wait_for_timeout(30_000)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(run())
