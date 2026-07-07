"""
Debug: clicks 导出 span and inspects what appears (dropdown? direct download?)

Run:
    py services/lingfeng/debug_export.py
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

        # Navigate
        print(f"\n[2] Navigating to {_DATA_URL} …")
        await page.goto(_DATA_URL)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_selector(".ant-select-selector", timeout=30_000)
        await page.wait_for_timeout(800)

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

        # Set date range
        print("\n[4] Setting date range …")
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

        # Click 查询
        print("\n[5] Clicking 查询 …")
        await page.locator("button.ant-btn-primary").first.click()
        await page.wait_for_timeout(3000)
        print("    Done waiting.")

        # Print full HTML of down-load-container area
        print("\n[6] HTML around span.down-load-container:")
        spans = await page.locator("span.down-load-container").all()
        print(f"    Found {len(spans)} span.down-load-container element(s)")
        for i, sp in enumerate(spans):
            try:
                html = await sp.evaluate("el => el.outerHTML")
                print(f"    [{i}] outerHTML: {html[:500]}")
                vis = await sp.is_visible()
                print(f"    [{i}] visible: {vis}")
            except Exception as e:
                print(f"    [{i}] error: {e}")

        # Screenshot before clicking
        await page.screenshot(path=str(_DEBUG_DIR / "export_before_click.png"))
        print("\n    Screenshot → export_before_click.png")

        # Click the span and immediately check for new elements
        print("\n[7] Clicking span.down-load-container …")
        await page.locator("span.down-load-container").first.click()
        await page.wait_for_timeout(1500)

        # Screenshot after clicking
        await page.screenshot(path=str(_DEBUG_DIR / "export_after_click.png"))
        print("    Screenshot → export_after_click.png")

        # Check for dropdown / popover / menu that appeared
        print("\n[8] Elements that appeared after click (dropdowns/menus/popovers):")
        for sel in [
            ".ant-dropdown:not(.ant-dropdown-hidden)",
            ".ant-popover:not(.ant-popover-hidden)",
            ".ant-menu-submenu-popup",
            "[class*='dropdown']:not([class*='hidden'])",
            "[class*='popup']:not([class*='hidden'])",
            "[class*='overlay']:not([class*='hidden'])",
        ]:
            cnt = await page.locator(sel).count()
            if cnt > 0:
                print(f"    '{sel}': {cnt} found")
                elems = await page.locator(sel).all()
                for el in elems:
                    try:
                        txt = (await el.inner_text()).strip()[:200]
                        html = await el.evaluate("el => el.outerHTML")
                        print(f"      text: {repr(txt)}")
                        print(f"      html: {html[:400]}")
                    except Exception:
                        pass

        # Dump all newly visible elements with 导/Excel/xls/csv
        print("\n[9] All visible elements with 导/Excel/xls/csv after click:")
        for keyword in ["导", "Excel", "xls", "csv", "下载"]:
            for tag in ["button", "span", "div", "a", "li"]:
                elems = await page.locator(tag).filter(has_text=keyword).all()
                for i, el in enumerate(elems):
                    try:
                        vis = await el.is_visible()
                        if vis:
                            txt = repr((await el.inner_text()).strip()[:60])
                            cls = (await el.get_attribute("class") or "")[:60]
                            print(f"    <{tag}>[{i}] cls='{cls}' text={txt}")
                    except Exception:
                        pass

        # Full page HTML snapshot
        html = await page.content()
        html_path = _DEBUG_DIR / "export_page.html"
        html_path.write_text(html, encoding="utf-8")
        print(f"\n[10] Full HTML → debug/lingfeng/export_page.html")

        print("\n[DONE] Browser open — inspect manually. Close or wait 60s.")
        await page.wait_for_timeout(60_000)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(run())
