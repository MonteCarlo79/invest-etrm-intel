"""
Debug inspector — logs in, navigates step-by-step, takes screenshots,
and prints what nav items / form elements are found on each page.

Run:
    py services/lingfeng/debug_inspect.py
"""
import asyncio
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent.parent
_DEBUG_DIR = _REPO / "debug" / "lingfeng"
_DEBUG_DIR.mkdir(parents=True, exist_ok=True)

# Load .env
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
_DATA_URL  = "https://lingfeng-saas.tradingthink.cn/#/powerTrading/sass/data-consultation"


async def inspect():
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page    = await context.new_page()
        page.set_default_timeout(15_000)

        # ── Step 1: Login ──────────────────────────────────────────────────
        print("\n[1] Opening login page …")
        await page.goto(_LOGIN_URL)
        await page.wait_for_load_state("networkidle")
        await page.screenshot(path=str(_DEBUG_DIR / "01_login.png"))
        print(f"    Screenshot → debug/lingfeng/01_login.png")
        print(f"    URL: {page.url}")

        # Fill credentials
        inputs = page.locator("input")
        await inputs.nth(0).fill(USERNAME)
        await inputs.nth(1).fill(PASSWORD)
        await page.locator("button", has_text="登录").first.click()
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2_000)
        await page.screenshot(path=str(_DEBUG_DIR / "02_after_login.png"))
        print(f"\n[2] After login")
        print(f"    URL: {page.url}")
        print(f"    Screenshot → debug/lingfeng/02_after_login.png")

        # Print all visible text in nav/menu areas
        nav_texts = await page.locator("nav, header, .el-menu, .sidebar, .menu, ul.menu").all_inner_texts()
        print(f"    Nav/menu text found: {nav_texts[:5]}")

        # Print all <a> and clickable elements with Chinese text
        links = await page.locator("a, li, .menu-item").all()
        print(f"    Clickable elements (up to 20):")
        for el in links[:20]:
            try:
                txt = (await el.inner_text()).strip()
                if txt:
                    print(f"      '{txt}'")
            except Exception:
                pass

        # ── Step 2: Navigate directly to data-consultation ─────────────────
        print(f"\n[3] Navigating directly to DATA_URL …")
        await page.goto(_DATA_URL)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(3_000)
        await page.screenshot(path=str(_DEBUG_DIR / "03_data_url_direct.png"))
        print(f"    URL: {page.url}")
        print(f"    Screenshot → debug/lingfeng/03_data_url_direct.png")

        # Check what's actually on the page
        all_divs = await page.locator("div[class]").count()
        print(f"    Total div[class] elements: {all_divs}")

        for selector in ["div.el-select", "select", ".el-form", "form", ".el-form-item",
                         "input", "button", ".v-select", ".ant-select"]:
            cnt = await page.locator(selector).count()
            print(f"    '{selector}': {cnt} found")

        # Print page title and h1/h2
        title = await page.title()
        print(f"    Page title: {title}")
        for sel in ["h1", "h2", "h3", ".title", ".page-title"]:
            elems = await page.locator(sel).all_inner_texts()
            if elems:
                print(f"    {sel}: {elems}")

        # Save full page HTML for inspection
        html = await page.content()
        html_path = _DEBUG_DIR / "03_page.html"
        html_path.write_text(html, encoding="utf-8")
        print(f"    Full HTML → debug/lingfeng/03_page.html")

        # ── Step 3: Try clicking 电力交易 if visible ─────────────────────
        print(f"\n[4] Looking for 电力交易 / 数据咨询 nav links …")
        for keyword in ["电力交易", "数据咨询", "数据", "交易"]:
            cnt = await page.locator(f"text={keyword}").count()
            print(f"    text='{keyword}': {cnt} match(es)")

        # Try clicking 电力交易
        try:
            el = page.locator("text=电力交易").first
            if await el.count() > 0:
                print("    Clicking 电力交易 …")
                await el.click()
                await page.wait_for_timeout(2_000)
                await page.screenshot(path=str(_DEBUG_DIR / "04_after_click_nav.png"))
                print(f"    URL after click: {page.url}")
                print(f"    Screenshot → debug/lingfeng/04_after_click_nav.png")

                for selector in ["div.el-select", "select", ".el-form-item", "input"]:
                    cnt = await page.locator(selector).count()
                    print(f"    '{selector}': {cnt} found")
        except Exception as exc:
            print(f"    Could not click 电力交易: {exc}")

        input("\n[PAUSED] Browser is open. Manually navigate to 数据咨询 form, then press Enter here …")

        # After manual navigation, inspect the form
        await page.screenshot(path=str(_DEBUG_DIR / "05_manual_nav.png"))
        print(f"\n[5] After manual navigation")
        print(f"    URL: {page.url}")
        print(f"    Screenshot → debug/lingfeng/05_manual_nav.png")

        for selector in ["div.el-select", "select", ".el-form", ".el-form-item",
                         "input", "button", ".v-select", ".ant-select", "[class*='select']"]:
            cnt = await page.locator(selector).count()
            if cnt > 0:
                print(f"    '{selector}': {cnt} found ✓")

        # Print all class attributes on divs that contain form elements
        form_html = await page.locator("form, .el-form, .form, [class*='form'], [class*='query']").first.inner_html()
        form_path = _DEBUG_DIR / "05_form.html"
        form_path.write_text(form_html, encoding="utf-8")
        print(f"    Form HTML → debug/lingfeng/05_form.html")

        await browser.close()

    print(f"\nAll screenshots saved to: {_DEBUG_DIR}")


if __name__ == "__main__":
    asyncio.run(inspect())
