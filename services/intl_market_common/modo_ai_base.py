"""Parameterised Modo Energy AI agent distillation.

Identical Playwright logic as gb_knowledge/modo_ai.py but driven by a MarketConfig
so any market's question set can be used without code duplication.

URL scheme:
  Standard / research : modo_ai://{cfg.code}/{YYYY-MM-DD}/q{NN}
  Research             : modo_ai://{cfg.code}/{YYYY-MM-DD}/research{NN}
  Foundational         : modo_ai://{cfg.code}/foundational/q{NN}
  Gap questions        : modo_ai://{cfg.code}/gap-questions/{YYYY-MM-DD}/q{NN}

All stored in intl_market.{cfg.table_prefix}knowledge_docs via the market's own
`base.upsert_doc` / `ensure_table`.
"""
from __future__ import annotations

import logging
import os
import random
import time
from datetime import date
from typing import Iterator

logger = logging.getLogger(__name__)

# Timeouts (ms)
_NAV_TIMEOUT = 30_000
_ELEMENT_TIMEOUT = 15_000
_RESPONSE_TIMEOUT = 90_000
_SETTLE_POLLS = 8


class ModoAIConnector:
    """Distills daily BESS market intelligence from Modo Energy's AI agent.

    Pass a MarketConfig to drive market-specific questions and URL namespacing.
    """

    def __init__(self, cfg, email: str | None = None, password: str | None = None):
        from services.intl_market_common.market_config import MarketConfig  # avoid circular
        self._cfg = cfg
        self._email    = email    or os.environ.get("MODO_EMAIL",    "")
        self._password = password or os.environ.get("MODO_PASSWORD", "")

    # ------------------------------------------------------------------
    # Public fetch interface
    # ------------------------------------------------------------------

    def fetch(self) -> Iterator[dict]:
        """Yield all docs (standard + research + foundational) for today."""
        if not self._email or not self._password:
            logger.warning("[modo_ai:%s] MODO_EMAIL / MODO_PASSWORD not set — skipping", self._cfg.code)
            return

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.warning("[modo_ai:%s] playwright not installed — skipping", self._cfg.code)
            return

        today = date.today()
        cfg = self._cfg

        all_questions = [
            (q, f"modo_ai://{cfg.code}/{today.isoformat()}/q{i:02d}", False)
            for i, q in enumerate(cfg.standard_questions)
        ] + [
            (q, f"modo_ai://{cfg.code}/{today.isoformat()}/research{i:02d}", False)
            for i, q in enumerate(cfg.research_questions)
        ] + [
            (q, f"modo_ai://{cfg.code}/foundational/q{i:02d}", False)
            for i, q in enumerate(cfg.foundational_questions)
        ]
        if all_questions:
            all_questions[-1] = (all_questions[-1][0], all_questions[-1][1], True)

        with sync_playwright() as pw:
            browser, ctx, page = self._launch_browser(pw)
            try:
                from playwright_stealth import stealth_sync
                stealth_sync(page)
            except ImportError:
                pass

            try:
                if not self._login(page):
                    logger.error("[modo_ai:%s] Login failed — aborting", cfg.code)
                    return

                total_q = len(all_questions)
                for idx, (question, url, is_last) in enumerate(all_questions):
                    logger.info("[modo_ai:%s] Q %d/%d: %s…", cfg.code, idx + 1, total_q, question[:60])
                    try:
                        answer = self._ask_fresh(page, question)
                    except Exception as exc:
                        logger.warning("[modo_ai:%s] Q%d error: %s", cfg.code, idx + 1, exc)
                        continue

                    if not answer or len(answer) < 30:
                        continue

                    yield {
                        "doc_type":       "ai_insight",
                        "title":          f"Modo AI ({cfg.code.upper()}) — {question[:80]}",
                        "url":            url,
                        "published_date": today,
                        "content":        f"Q: {question}\n\nA: {answer}",
                    }

                    if not is_last:
                        time.sleep(random.uniform(15, 45))

            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass

    def fetch_custom(self, questions: list[str], url_prefix: str = "gap-questions") -> Iterator[dict]:
        """Send a custom list of questions (for gap interviews)."""
        if not self._email or not self._password:
            return

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            return

        today = date.today()
        cfg = self._cfg

        with sync_playwright() as pw:
            browser, ctx, page = self._launch_browser(pw)
            try:
                from playwright_stealth import stealth_sync
                stealth_sync(page)
            except ImportError:
                pass

            try:
                if not self._login(page):
                    return

                for i, question in enumerate(questions):
                    url = f"modo_ai://{cfg.code}/{url_prefix}/{today.isoformat()}/q{i:02d}"
                    logger.info("[modo_ai:%s] Custom Q %d/%d: %s…", cfg.code, i + 1, len(questions), question[:60])
                    try:
                        answer = self._ask_fresh(page, question)
                    except Exception as exc:
                        logger.warning("[modo_ai:%s] Custom Q%d error: %s", cfg.code, i, exc)
                        continue

                    if not answer or len(answer) < 30:
                        continue

                    yield {
                        "doc_type":       "ai_insight",
                        "title":          f"Modo AI ({cfg.code.upper()} gap) — {question[:80]}",
                        "url":            url,
                        "published_date": today,
                        "content":        f"Q: {question}\n\nA: {answer}",
                        "_question":      question,
                        "_answer":        answer,
                    }

                    if i < len(questions) - 1:
                        time.sleep(random.uniform(10, 30))

            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Browser setup
    # ------------------------------------------------------------------

    def _launch_browser(self, pw):
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-GB",
            timezone_id="Asia/Singapore",
        )
        page = ctx.new_page()
        page.on("console", lambda _: None)
        return browser, ctx, page

    # ------------------------------------------------------------------
    # Login (identical to gb_knowledge/modo_ai.py)
    # ------------------------------------------------------------------

    def _login(self, page) -> bool:
        try:
            page.goto("https://modoenergy.com/home", timeout=_NAV_TIMEOUT, wait_until="domcontentloaded")
        except Exception as exc:
            logger.warning("[modo_ai] Could not load modoenergy.com/home: %s", exc)
            return False
        page.wait_for_timeout(3_000)
        _save_screenshot(page, "01_after_nav")
        if self._is_authenticated(page):
            return True

        email_sel = _first_visible(page, [
            'input[type="email"]', 'input[name="email"]',
            'input[id*="email" i]', 'input[placeholder*="email" i]',
            'input[autocomplete="email"]',
        ])
        if not email_sel:
            _save_screenshot(page, "02_no_email")
            return False
        page.fill(email_sel, self._email)

        pass_sel = _first_visible(page, [
            'input[type="password"]', 'input[name="password"]',
            'input[id*="password" i]', 'input[autocomplete*="password"]',
        ])
        if not pass_sel:
            next_sel = _first_visible(page, [
                'button[type="submit"]', 'button:has-text("Continue")',
                'button:has-text("Next")', 'button:has-text("Sign in")',
            ])
            if next_sel:
                page.click(next_sel)
            else:
                page.keyboard.press("Enter")
            page.wait_for_timeout(2_500)
            _save_screenshot(page, "03_after_email_submit")
            pass_sel = _first_visible(page, [
                'input[type="password"]', 'input[name="password"]',
                'input[id*="password" i]', 'input[autocomplete*="password"]',
            ])

        if not pass_sel:
            _save_screenshot(page, "04_no_pass")
            return False
        page.fill(pass_sel, self._password)

        submit_sel = _first_visible(page, [
            'button[type="submit"]', 'button:has-text("Sign in")',
            'button:has-text("Log in")', 'button:has-text("Continue")',
        ])
        if submit_sel:
            page.click(submit_sel)
        else:
            page.keyboard.press("Enter")

        try:
            page.wait_for_url(lambda url: "modoenergy.com/home" in url, timeout=_NAV_TIMEOUT)
        except Exception:
            pass
        page.wait_for_timeout(3_000)
        self._dismiss_cookie_banner(page)
        _save_screenshot(page, "05_after_submit")
        return self._is_authenticated(page)

    def _is_authenticated(self, page) -> bool:
        if "modoenergy.com/home" not in page.url:
            return False
        for sel in ['input[type="email"]', 'input[type="password"]']:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    return False
            except Exception:
                pass
        return True

    # ------------------------------------------------------------------
    # Ask a single question
    # ------------------------------------------------------------------

    def _ask_fresh(self, page, question: str) -> str | None:
        try:
            page.goto("https://modoenergy.com/home", timeout=_NAV_TIMEOUT, wait_until="domcontentloaded")
        except Exception as exc:
            logger.warning("[modo_ai] Nav failed: %s", exc)
        page.wait_for_timeout(2_000)
        self._dismiss_cookie_banner(page)
        self._try_open_chat(page)

        input_sel = _first_visible(page, [
            'input[placeholder*="looking" i]', 'input[placeholder*="what" i]',
            'textarea[placeholder*="ask" i]', 'textarea[placeholder*="question" i]',
            'textarea[placeholder*="message" i]', 'textarea[placeholder*="chat" i]',
            'textarea[placeholder*="type" i]', 'textarea[placeholder*="search" i]',
            'input[placeholder*="ask" i]', 'input[placeholder*="question" i]',
            'input[placeholder*="message" i]', 'input[placeholder*="search" i]',
            'div[contenteditable="true"][data-placeholder*="ask" i]',
            'div[contenteditable="true"]', 'textarea', 'input[type="text"]',
        ])
        if not input_sel:
            return None

        pre_text = self._extract_response_text(page)
        page.click(input_sel)
        page.fill(input_sel, question)

        send_sel = _first_visible(page, [
            'button[type="submit"]', 'button[aria-label*="send" i]',
            'button[data-testid*="send" i]', 'button:has-text("Send")',
            'button:has-text("Ask")', 'button:has-text("Submit")',
        ])
        if send_sel:
            page.click(send_sel)
        else:
            page.keyboard.press("Enter")

        return self._wait_for_settled_response(page, pre_text)

    def _dismiss_cookie_banner(self, page) -> None:
        dismiss_sel = _first_visible(page, [
            'button:has-text("Accept All")', 'button:has-text("Accept all")',
            'button:has-text("Accept")', 'button:has-text("Reject All")',
            'button:has-text("OK")', 'button:has-text("Got it")',
            '[aria-label*="accept" i]', '[aria-label*="cookie" i]',
        ])
        if dismiss_sel:
            try:
                page.click(dismiss_sel)
                page.wait_for_timeout(500)
            except Exception:
                pass

    def _try_open_chat(self, page) -> None:
        open_sel = _first_visible(page, [
            'button[aria-label*="chat" i]', 'button[aria-label*="ai" i]',
            'button[aria-label*="ask" i]', '[data-testid*="chat-open" i]',
        ])
        if open_sel:
            try:
                page.click(open_sel)
                page.wait_for_timeout(1_500)
            except Exception:
                pass

    def _wait_for_settled_response(self, page, pre_text: str) -> str | None:
        deadline = time.monotonic() + _RESPONSE_TIMEOUT / 1000
        stable_count = 0
        prev = pre_text or ""

        while time.monotonic() < deadline:
            page.wait_for_timeout(2_000)
            current = self._extract_response_text(page)

            if not current or current == prev:
                stable_count += 1
                if stable_count >= _SETTLE_POLLS and current and current != pre_text:
                    return _clean_response(current, pre_text)
            else:
                stable_count = 0

            prev = current

            if not self._is_streaming(page) and current and current != pre_text:
                page.wait_for_timeout(2_000)
                final = self._extract_response_text(page)
                return _clean_response(final or current, pre_text)

        final = self._extract_response_text(page)
        return _clean_response(final, pre_text) if final and final != pre_text else None

    def _is_streaming(self, page) -> bool:
        try:
            return page.evaluate("""() => {
                const stopBtn = document.querySelector(
                    'button[aria-label*="stop" i], button[title*="stop" i], '
                    '[data-testid*="stop" i], button:has-text("Stop")'
                );
                if (stopBtn && stopBtn.offsetParent !== null) return true;
                const spinner = document.querySelector(
                    '[class*="loading"], [class*="spinner"], [class*="typing"]'
                );
                return !!(spinner && spinner.offsetParent !== null);
            }""")
        except Exception:
            return False

    def _extract_response_text(self, page) -> str:
        candidates = [
            """(() => {
                const els = document.querySelectorAll(
                    '[data-role="assistant"], [data-message-role="assistant"], '
                    '[class*="assistant-message"], [class*="ai-message"]'
                );
                if (!els.length) return null;
                return els[els.length - 1].innerText?.trim() || null;
            })()""",
            """(() => {
                const all = document.querySelectorAll('[class*="message"]:not([class*="input"])');
                const nonUser = Array.from(all).filter(el => {
                    const cls = el.className || '';
                    return !cls.includes('user') && !cls.includes('human');
                });
                if (!nonUser.length) return null;
                return nonUser[nonUser.length - 1].innerText?.trim() || null;
            })()""",
            """(() => {
                const container = document.querySelector(
                    '[class*="chat-container"], [class*="conversation"], '
                    '[class*="messages-container"], [class*="chat-messages"]'
                );
                if (!container) return null;
                const paras = container.querySelectorAll('p, li, [class*="response"]');
                if (!paras.length) return container.innerText?.trim() || null;
                return Array.from(paras).slice(-30).map(e => e.innerText).join('\\n').trim() || null;
            })()""",
        ]
        for js in candidates:
            try:
                result = page.evaluate(js)
                if result and len(result.strip()) > 20:
                    return result.strip()
            except Exception:
                continue
        return ""


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def distill_gap_questions(questions: list[str], cfg, db_table_prefix: str) -> dict[str, str | None]:
    """Push gap questions to Modo AI; store in {prefix}knowledge_docs.

    Returns {question: answer_or_None}.
    """
    import os
    import psycopg2
    from services.gb_knowledge.base import get_db_conn

    results: dict[str, str | None] = {q: None for q in questions}

    # Ensure knowledge table for this market exists
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS intl_market.{db_table_prefix}knowledge_docs (
                    id              SERIAL PRIMARY KEY,
                    source          TEXT NOT NULL,
                    doc_type        TEXT NOT NULL,
                    title           TEXT,
                    url             TEXT UNIQUE,
                    published_date  DATE,
                    content         TEXT NOT NULL,
                    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    search_vector   TSVECTOR GENERATED ALWAYS AS (
                        to_tsvector('english',
                            coalesce(title,'') || ' ' || left(content,100000))
                    ) STORED
                )
            """)
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {db_table_prefix}knowledge_docs_fts "
                f"ON intl_market.{db_table_prefix}knowledge_docs USING GIN(search_vector)"
            )
        conn.commit()

        connector = ModoAIConnector(cfg)
        for doc in connector.fetch_custom(questions):
            q_text = doc.pop("_question", "")
            a_text = doc.pop("_answer", "")
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO intl_market.{db_table_prefix}knowledge_docs "
                    "(source, doc_type, title, url, published_date, content) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (url) DO NOTHING",
                    (
                        "modo_ai", doc["doc_type"], doc.get("title", ""),
                        doc.get("url"), doc.get("published_date"), doc["content"],
                    ),
                )
            conn.commit()
            if q_text and a_text:
                for orig_q in questions:
                    if orig_q == q_text or orig_q[:80] == q_text[:80]:
                        results[orig_q] = a_text
                        break

    except Exception as exc:
        logger.error("[modo_ai:%s] distill_gap_questions failed: %s", cfg.code, exc)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()

    answered = sum(1 for v in results.values() if v)
    logger.info("[modo_ai:%s] Gap distillation: %d/%d answered", cfg.code, answered, len(questions))
    return results


def _first_visible(page, selectors: list[str]) -> str | None:
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                return sel
        except Exception:
            continue
    return None


def _save_screenshot(page, name: str) -> None:
    try:
        page.screenshot(path=f"/tmp/modo_{name}.png")
    except Exception:
        pass


def _clean_response(text: str, pre_text: str) -> str:
    if not text:
        return text
    if pre_text and text.startswith(pre_text):
        text = text[len(pre_text):].strip()
    return text.strip()
