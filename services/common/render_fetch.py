# -*- coding: utf-8 -*-
"""JS-rendered fetch for pages behind anti-bot JS challenges (Aliyun WAF etc.).

Plain requests gets an empty webpack shell from sites like news.bjx.com.cn —
the Aliyun WAF computes an acw_sc__v2 cookie in JavaScript and reloads before
serving real content. This module renders the page in headless Chromium and
waits out the challenge. Heavy dependency (playwright + browser): use only
when a plain fetch returns a shell — check with looks_like_js_shell().
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_WAF_MARKERS = ("aliyun_waf", "acw_sc__")

_STEALTH_JS = (
    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def looks_like_js_shell(html: str) -> bool:
    """True if the HTML is an anti-bot JS-challenge shell, not real content."""
    head = html[:3000]
    return any(m in head for m in _WAF_MARKERS)


def render(url: str, *, timeout_ms: int = 45000, wait_selector: str | None = None) -> str:
    """Render `url` in headless Chromium and return the final page HTML.

    Polls page content until the WAF shell markers disappear (the challenge
    reloads the page once the acw cookie is set), then waits for network idle
    or an optional selector. Raises on playwright/navigation errors.
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        try:
            ctx = browser.new_context(user_agent=_UA, locale="zh-CN")
            ctx.add_init_script(_STEALTH_JS)
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            for _ in range(max(1, timeout_ms // 1000)):
                if not looks_like_js_shell(page.content()):
                    break
                page.wait_for_timeout(1000)
            if wait_selector:
                page.wait_for_selector(wait_selector, timeout=timeout_ms)
            else:
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass  # networkidle is best-effort; content is what matters
            html = page.content()
            if looks_like_js_shell(html):
                raise RuntimeError(f"WAF challenge not cleared within timeout for {url}")
            return html
        finally:
            browser.close()
