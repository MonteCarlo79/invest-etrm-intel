"""Modo Energy AI agent distillation via Playwright.

Logs into modoenergy.com with MODO_EMAIL + MODO_PASSWORD, navigates to
modoenergy.com/home where Modo's AI chat lives, asks a curated set of GB BESS
market questions, captures each response, and yields them as knowledge documents.

Source:   modo_ai
Doc type: ai_insight
URL key:  modo_ai://{YYYY-MM-DD}/q{NN}   (one row per question per day;
          ON CONFLICT DO NOTHING so re-running on the same day is a no-op)
"""
from __future__ import annotations

import logging
import os
import random
import time
from datetime import date
from typing import Iterator

from services.gb_knowledge.base import BaseConnector

logger = logging.getLogger(__name__)

# Path where the Playwright browser session (cookies + localStorage) is persisted
# between container runs.  Override via MODO_SESSION_PATH env var.
_SESSION_PATH = os.environ.get("MODO_SESSION_PATH", "/tmp/modo_session.json")

# ---------------------------------------------------------------------------
# Standard questions sent to Modo AI each night
# ---------------------------------------------------------------------------
# Asked every night — time-sensitive market intelligence.
STANDARD_QUESTIONS: list[str] = [
    "What are the most important GB BESS market developments from the last 24 hours?",
    "Which revenue streams are performing best for GB BESS assets right now — "
    "BM, FFR, DCL, DCH, or EPEX day-ahead?",
    "What is the current short-term outlook for GB BESS merchant revenues?",
    "Are there any significant regulatory or policy changes currently affecting GB BESS?",
    "How is GB system price and net imbalance volume (NIV) trending, and what does "
    "it mean for BESS dispatch strategy?",
    "Which GB BESS assets or operators are showing standout performance this week and why?",
    "What are the key market risks and opportunities for GB BESS investors right now?",
    "How is the GB grid stability and curtailment environment affecting BESS revenues?",
]

# Asked once (or when missing from KB) — foundational knowledge that doesn't change daily.
FOUNDATIONAL_QUESTIONS: list[str] = [
    "Give me a detailed explanation of how the UK power market mechanism works, "
    "covering the wholesale market, balancing mechanism, system operator role, "
    "settlement, and how prices are formed.",
    "Give me a comprehensive list of all BESS-relevant policies and regulations in the UK "
    "with a brief description of each — including capacity market, ancillary services "
    "framework, grid connection rules, planning policy, and any storage-specific legislation.",
    "How does NIV chasing work for GB BESS — what is net imbalance volume, how do "
    "batteries exploit it, what are the risks, and how has National Grid/NESO responded?",
    "Explain the GB Dynamic Containment, Dynamic Moderation, and Dynamic Regulation "
    "ancillary services — how they differ, procurement mechanisms, and typical BESS revenues.",
    "What is the GB Capacity Market and how does a BESS asset participate — T-4/T-1 auctions, "
    "de-rating factors, obligations, and penalty regime?",
    "How does EPEX day-ahead price formation work in Great Britain — auction timeline, "
    "participants, relationship to system price, and implications for BESS wholesale trading?",
]

# Asked every night — draw specifically on Modo's proprietary research, forecasts, and data.
MODO_RESEARCH_QUESTIONS: list[str] = [
    "Based on Modo's latest GB BESS revenue forecast, what is the outlook for total merchant "
    "revenues over the next 12 months, and which markets (wholesale, BM, ancillary) are "
    "expected to grow or decline?",
    "What are the key findings from Modo's most recent GB storage market report or outlook — "
    "including any changes to Modo's revenue index or forward curve assumptions?",
    "According to Modo's pipeline and deployment data, how much new GB BESS capacity is "
    "expected to come online in the next 12–24 months, and what does this mean for "
    "per-MW revenues as the market matures?",
    "What does Modo's research say about optimal BESS duration strategy in the current "
    "GB market — is 1h, 2h, or 4h duration showing better risk-adjusted returns, "
    "and how is this expected to shift?",
    "According to Modo's data, which ancillary service markets (DC, DR, FFR) currently "
    "offer the best risk-adjusted returns for GB BESS, and how have clearing prices trended?",
    "What does Modo's research show about the long-term impact of increasing BESS "
    "penetration on GB ancillary service prices — at what fleet size does Modo expect "
    "significant price cannibalisation?",
]

# Timeouts (ms)
_NAV_TIMEOUT = 30_000
_ELEMENT_TIMEOUT = 15_000
_RESPONSE_TIMEOUT = 90_000   # max wait for AI to start responding
_SETTLE_POLLS = 8            # poll count (×2 s each) with no change = done


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------

class ModoAIConnector(BaseConnector):
    """Distills daily GB BESS intelligence from Modo Energy's AI agent."""

    source = "modo_ai"

    def __init__(self, email: str | None = None, password: str | None = None):
        self._email    = email    or os.environ.get("MODO_EMAIL",    "")
        self._password = password or os.environ.get("MODO_PASSWORD", "")
        # Set to True when _login() detects that Modo sent a magic link instead of
        # showing a password field — prevents pointless retries that spam more emails.
        self._magic_link_sent = False

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    def fetch(self) -> Iterator[dict]:
        if not self._email or not self._password:
            logger.warning("[modo_ai] MODO_EMAIL / MODO_PASSWORD not set — skipping")
            return

        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            logger.warning("[modo_ai] playwright not installed — skipping")
            return

        today = date.today()

        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )

            # Load persisted session so we can skip login on re-runs
            saved_state = _load_session_state()
            ctx = browser.new_context(
                storage_state=saved_state,   # None = fresh context
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                locale="en-GB",
                timezone_id="Asia/Singapore",
            )
            page = ctx.new_page()
            # Suppress noisy console messages from the React app
            page.on("console", lambda _: None)

            # Patch headless fingerprint so navigator.webdriver = false,
            # missing browser APIs are shimmed, etc.
            try:
                from playwright_stealth import stealth_sync
                stealth_sync(page)
                logger.info("[modo_ai] Stealth mode applied")
            except ImportError:
                logger.warning("[modo_ai] playwright-stealth not installed — running without stealth")

            try:
                logged_in = False

                # ── 1. Try restored session ───────────────────────────────
                if saved_state:
                    logger.info("[modo_ai] Checking restored session from %s", _SESSION_PATH)
                    try:
                        page.goto(
                            "https://modoenergy.com/home",
                            timeout=_NAV_TIMEOUT,
                            wait_until="domcontentloaded",
                        )
                        page.wait_for_timeout(2_000)
                        if self._is_authenticated(page):
                            logged_in = True
                            logger.info("[modo_ai] Saved session is valid — login skipped")
                    except Exception as exc:
                        logger.warning("[modo_ai] Session restore check failed: %s", exc)

                # ── 2. Try magic link URL (set once via env var) ──────────
                if not logged_in:
                    magic_link_url = os.environ.get("MODO_MAGIC_LINK_URL", "").strip()
                    if magic_link_url:
                        logged_in = self._login_via_magic_link(page, magic_link_url)

                # ── 3. Fall back to password login ────────────────────────
                if not logged_in:
                    self._magic_link_sent = False
                    for _attempt in range(1, 3):   # max 2 attempts
                        logger.info("[modo_ai] Login attempt %d/2", _attempt)
                        logged_in = self._login(page)
                        if logged_in:
                            break
                        if self._magic_link_sent:
                            # Modo sent an auth email — retrying will only send more emails
                            logger.error(
                                "[modo_ai] Magic link auth detected — aborting without retry"
                            )
                            _notify_magic_link_required(page.url)
                            break
                        if _attempt < 2:
                            logger.warning(
                                "[modo_ai] Login attempt %d failed — retrying in 10s", _attempt
                            )
                            time.sleep(10)
                            try:
                                page.goto(
                                    "https://modoenergy.com/sign-in",
                                    timeout=_NAV_TIMEOUT,
                                    wait_until="domcontentloaded",
                                )
                                page.wait_for_timeout(2_000)
                            except Exception:
                                pass

                if not logged_in:
                    if not self._magic_link_sent:
                        logger.error("[modo_ai] Login failed after all attempts — aborting")
                        _notify_login_failure(2, page.url)
                    return

                # Persist session so tomorrow's run (same container) skips login
                _save_session_state(ctx)

                all_questions = [
                    # (question_text, url, is_last)
                    # Daily market intelligence — date-keyed, refreshed every night
                    (q, f"modo_ai://{today.isoformat()}/q{i:02d}", False)
                    for i, q in enumerate(STANDARD_QUESTIONS)
                ] + [
                    # Modo proprietary research — date-keyed, refreshed every night
                    (q, f"modo_ai://{today.isoformat()}/research{i:02d}", False)
                    for i, q in enumerate(MODO_RESEARCH_QUESTIONS)
                ] + [
                    # Foundational knowledge — fixed URL, inserted once only
                    (q, f"modo_ai://foundational/q{i:02d}", False)
                    for i, q in enumerate(FOUNDATIONAL_QUESTIONS)
                ]
                # Mark last question
                if all_questions:
                    all_questions[-1] = (all_questions[-1][0], all_questions[-1][1], True)

                total_q = len(all_questions)
                for idx, (question, url, is_last) in enumerate(all_questions):
                    logger.info(
                        "[modo_ai] Question %d/%d: %s…",
                        idx + 1, total_q, question[:60],
                    )
                    try:
                        answer = self._ask_fresh(page, question)
                    except Exception as exc:
                        logger.warning("[modo_ai] Question %d error: %s", idx + 1, exc)
                        continue

                    if not answer or len(answer) < 30:
                        logger.warning("[modo_ai] No substantive answer for q%d (got: %r)", idx, answer)
                        continue

                    yield {
                        "doc_type": "ai_insight",
                        "title":    f"Modo AI — {question[:80]}",
                        "url":      url,
                        "published_date": today,
                        "content":  f"Q: {question}\n\nA: {answer}",
                    }

                    # Random pause between questions (15–45 s) to avoid
                    # looking like automated traffic
                    if not is_last:
                        pause = random.uniform(15, 45)
                        logger.debug("[modo_ai] Pausing %.0fs before next question", pause)
                        time.sleep(pause)

            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Login
    # ------------------------------------------------------------------

    def _login_via_magic_link(self, page, url: str) -> bool:
        """Navigate to a Modo magic link URL to authenticate.

        Set MODO_MAGIC_LINK_URL=<url from email> in ECS env vars to use this.
        The session is saved after success so future runs reuse it.
        """
        try:
            logger.info("[modo_ai] Authenticating via MODO_MAGIC_LINK_URL")
            page.goto(url, timeout=_NAV_TIMEOUT, wait_until="domcontentloaded")
            page.wait_for_timeout(3_000)
            _save_screenshot(page, "magic_link_after_nav")
            try:
                page.wait_for_url(
                    lambda u: "modoenergy.com/home" in u,
                    timeout=15_000,
                )
            except Exception:
                pass
            page.wait_for_timeout(2_000)
            self._dismiss_cookie_banner(page)
            if self._is_authenticated(page):
                logger.info("[modo_ai] Magic link authentication successful")
                return True
            logger.error("[modo_ai] Magic link auth failed; URL after nav: %s", page.url)
            _save_screenshot(page, "magic_link_failed")
            return False
        except Exception as exc:
            logger.error("[modo_ai] Magic link nav error: %s", exc)
            return False

    def _login(self, page) -> bool:
        """Navigate to Modo sign-in page and authenticate via email + password.

        Handles known login flows:
          Flow A (original):  email field + "sign in with a password" link visible
                              → click link → password field → Continue
          Flow B (Auth0-style): email field + Continue button → password field → Continue
          Flow C (single page): email + password both visible → fill both → Continue
          Flow D (magic link):  after Continue Modo sends an auth email instead of
                              showing a password field → sets self._magic_link_sent
        """
        try:
            page.goto(
                "https://modoenergy.com/sign-in",
                timeout=_NAV_TIMEOUT,
                wait_until="domcontentloaded",
            )
        except Exception as exc:
            logger.warning("[modo_ai] Could not load modoenergy.com/sign-in: %s", exc)
            return False

        page.wait_for_timeout(3_000)
        _save_screenshot(page, "01_after_nav")

        if self._is_authenticated(page):
            logger.info("[modo_ai] Already authenticated")
            return True

        logger.info("[modo_ai] Sign-in page loaded (URL: %s)", page.url)

        # --- Step 1: fill email ---
        email_sel = _first_visible(page, [
            'input[type="email"]',
            'input[name="email"]',
            'input[id*="email" i]',
            'input[placeholder*="email" i]',
            'input[autocomplete="email"]',
        ])
        if not email_sel:
            logger.error("[modo_ai] Email input not found; see /tmp/modo_02_no_email.png")
            _save_screenshot(page, "02_no_email")
            return False
        page.fill(email_sel, self._email)
        logger.info("[modo_ai] Email filled")
        _save_screenshot(page, "02_email_filled")

        # --- Step 2: get to the password field ---
        # Check if password field is already visible (Flow C — single page form)
        pass_sel = _first_visible(page, [
            'input[type="password"]',
            'input[name="password"]',
            'input[id*="password" i]',
            'input[autocomplete*="password"]',
        ])

        if pass_sel:
            logger.info("[modo_ai] Password field already visible (single-page form)")
        else:
            # Try Flow A: "sign in with a password" link
            pw_link_sel = _first_visible(page, [
                'a:has-text("sign in with a password")',
                'button:has-text("sign in with a password")',
                'a:has-text("Use password")',
                'button:has-text("Use password")',
                'a:has-text("password")',
            ])
            if pw_link_sel:
                page.click(pw_link_sel)
                logger.info("[modo_ai] Clicked 'sign in with a password' link (Flow A)")
                page.wait_for_timeout(2_500)
                _save_screenshot(page, "03_after_pw_link_click")
                # Check for magic link page even after Flow A click
                if _is_magic_link_page(page):
                    logger.error("[modo_ai] Magic link page detected after pw-link click (Flow D)")
                    _save_screenshot(page, "03_magic_link_sent")
                    self._magic_link_sent = True
                    return False
            else:
                # Flow B: click Continue/Next to advance to password screen
                logger.info("[modo_ai] No password link found — trying Continue button (Flow B)")
                continue_sel = _first_visible(page, [
                    'button:has-text("Continue")',
                    'button:has-text("Next")',
                    'button[type="submit"]',
                    'button:has-text("Sign in")',
                    'button:has-text("Sign In")',
                    'button:has-text("Log in")',
                ])
                if continue_sel:
                    page.click(continue_sel)
                    logger.info("[modo_ai] Clicked Continue button")
                else:
                    page.keyboard.press("Enter")
                    logger.info("[modo_ai] Pressed Enter to advance")
                page.wait_for_timeout(2_500)
                _save_screenshot(page, "03_after_continue")

                # Flow D detection: Modo sent a magic link email — do NOT retry
                if _is_magic_link_page(page):
                    logger.error(
                        "[modo_ai] Magic link / passwordless auth page detected (Flow D) — "
                        "Modo sent an authorization email. Set MODO_MAGIC_LINK_URL to "
                        "authenticate, or use the Streamlit app's Manual Auth page."
                    )
                    _save_screenshot(page, "03_magic_link_sent")
                    self._magic_link_sent = True
                    return False

                # After advancing, check again for "sign in with a password" link
                pw_link_sel2 = _first_visible(page, [
                    'a:has-text("sign in with a password")',
                    'button:has-text("sign in with a password")',
                    'a:has-text("Use password")',
                    'button:has-text("Use password")',
                ])
                if pw_link_sel2:
                    page.click(pw_link_sel2)
                    logger.info("[modo_ai] Clicked 'sign in with a password' link (after Continue)")
                    page.wait_for_timeout(2_500)
                    _save_screenshot(page, "03b_after_pw_link_click")
                    # Check again for magic link
                    if _is_magic_link_page(page):
                        logger.error("[modo_ai] Magic link page detected after pw-link click (Flow D)")
                        _save_screenshot(page, "03b_magic_link_sent")
                        self._magic_link_sent = True
                        return False

            # Re-check for password field after navigation
            pass_sel = _first_visible(page, [
                'input[type="password"]',
                'input[name="password"]',
                'input[id*="password" i]',
                'input[autocomplete*="password"]',
            ])

        if not pass_sel:
            logger.error("[modo_ai] Password input not found after all flow attempts")
            _save_screenshot(page, "04_no_pass")
            # Log page source snippet for debugging
            try:
                snippet = page.evaluate("() => document.body.innerText.slice(0, 800)")
                logger.error("[modo_ai] Page text: %s", snippet)
            except Exception:
                pass
            return False

        page.fill(pass_sel, self._password)
        logger.info("[modo_ai] Password filled")

        # --- Step 3: submit ---
        submit_sel = _first_visible(page, [
            'button:has-text("Continue")',
            'button[type="submit"]',
            'button:has-text("Sign in")',
            'button:has-text("Sign In")',
            'button:has-text("Log in")',
        ])
        if submit_sel:
            page.click(submit_sel)
        else:
            page.keyboard.press("Enter")
        logger.info("[modo_ai] Submit clicked — waiting for redirect to /home")

        try:
            page.wait_for_url(
                lambda url: "modoenergy.com/home" in url,
                timeout=_NAV_TIMEOUT,
            )
        except Exception:
            pass  # fall through; _is_authenticated() decides

        page.wait_for_timeout(3_000)
        self._dismiss_cookie_banner(page)
        _save_screenshot(page, "05_after_submit")

        if self._is_authenticated(page):
            logger.info("[modo_ai] Login successful")
            return True

        logger.error("[modo_ai] Login failed; URL after submit: %s — see /tmp/modo_05_after_submit.png", page.url)
        # Dump page text to help diagnose
        try:
            snippet = page.evaluate("() => document.body.innerText.slice(0, 800)")
            logger.error("[modo_ai] Page text at failure: %s", snippet)
        except Exception:
            pass
        return False

    def _is_authenticated(self, page) -> bool:
        """Return True if the page is the authenticated home (URL=/home, no login form visible)."""
        if "modoenergy.com/home" not in page.url:
            return False
        # If an email or password input is still visible, we're on the login screen
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
        """Reload home page, locate AI input, ask question, return answer text."""
        # Always start from home to get a fresh conversation context
        try:
            page.goto(
                "https://modoenergy.com/home",
                timeout=_NAV_TIMEOUT,
                wait_until="domcontentloaded",
            )
        except Exception as exc:
            logger.warning("[modo_ai] Navigation to home failed: %s", exc)

        # Brief settle for dynamic content to render
        page.wait_for_timeout(2_000)

        # Dismiss cookie consent banner if present
        self._dismiss_cookie_banner(page)

        # Try to open AI chat if there's a trigger button
        self._try_open_chat(page)

        # Find the chat input — Modo home uses "What are you looking for?"
        input_sel = _first_visible(page, [
            'input[placeholder*="looking" i]',   # "What are you looking for?"
            'input[placeholder*="what" i]',
            'textarea[placeholder*="ask" i]',
            'textarea[placeholder*="question" i]',
            'textarea[placeholder*="message" i]',
            'textarea[placeholder*="chat" i]',
            'textarea[placeholder*="type" i]',
            'textarea[placeholder*="search" i]',
            'input[placeholder*="ask" i]',
            'input[placeholder*="question" i]',
            'input[placeholder*="message" i]',
            'input[placeholder*="search" i]',
            'div[contenteditable="true"][data-placeholder*="ask" i]',
            'div[contenteditable="true"]',
            'textarea',
            'input[type="text"]',   # generic fallback
        ])
        if not input_sel:
            logger.warning("[modo_ai] Chat input not found — page source snippet:\n%s",
                           page.content()[:500])
            return None

        # Note: record the response area text before sending so we can detect new content
        pre_text = self._extract_response_text(page)

        # Type and send
        page.click(input_sel)
        page.fill(input_sel, question)

        send_sel = _first_visible(page, [
            'button[type="submit"]',
            'button[aria-label*="send" i]',
            'button[data-testid*="send" i]',
            'button:has-text("Send")',
            'button:has-text("Ask")',
            'button:has-text("Submit")',
        ])
        if send_sel:
            page.click(send_sel)
        else:
            page.keyboard.press("Enter")

        # Wait for response
        return self._wait_for_settled_response(page, pre_text)

    def _dismiss_cookie_banner(self, page) -> None:
        """Dismiss cookie consent popup if visible — prevents it blocking clicks."""
        dismiss_sel = _first_visible(page, [
            'button:has-text("Accept All")',
            'button:has-text("Accept all")',
            'button:has-text("Accept")',
            'button:has-text("Reject All")',
            'button:has-text("OK")',
            'button:has-text("Got it")',
            '[aria-label*="accept" i]',
            '[aria-label*="cookie" i]',
        ])
        if dismiss_sel:
            try:
                page.click(dismiss_sel)
                page.wait_for_timeout(500)
                logger.info("[modo_ai] Cookie banner dismissed")
            except Exception:
                pass

    def _try_open_chat(self, page) -> None:
        """Click a chat-open button if present (e.g. floating AI icon)."""
        open_sel = _first_visible(page, [
            'button[aria-label*="chat" i]',
            'button[aria-label*="ai" i]',
            'button[aria-label*="ask" i]',
            '[data-testid*="chat-open" i]',
            '[data-testid*="ai-toggle" i]',
        ])
        if open_sel:
            try:
                page.click(open_sel)
                page.wait_for_timeout(1_500)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Custom question list (gap-targeted queries)
    # ------------------------------------------------------------------

    def fetch_custom(self, questions: list[str], url_prefix: str = "gap-questions") -> Iterator[dict]:
        """Send a custom list of questions to Modo AI.

        URL scheme: modo_ai://{url_prefix}/{YYYY-MM-DD}/q{i:02d}
        Suitable for credit-efficient, gap-targeted queries.
        """
        if not self._email or not self._password:
            logger.warning("[modo_ai] MODO_EMAIL / MODO_PASSWORD not set — skipping")
            return

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.warning("[modo_ai] playwright not installed — skipping")
            return

        today = date.today()

        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )

            saved_state = _load_session_state()
            ctx = browser.new_context(
                storage_state=saved_state,
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
            try:
                from playwright_stealth import stealth_sync
                stealth_sync(page)
            except ImportError:
                pass

            try:
                logged_in = False

                # ── 1. Try restored session ───────────────────────────────
                if saved_state:
                    try:
                        page.goto(
                            "https://modoenergy.com/home",
                            timeout=_NAV_TIMEOUT,
                            wait_until="domcontentloaded",
                        )
                        page.wait_for_timeout(2_000)
                        if self._is_authenticated(page):
                            logged_in = True
                            logger.info("[modo_ai] Saved session is valid — login skipped")
                    except Exception as exc:
                        logger.warning("[modo_ai] Session restore check failed: %s", exc)

                # ── 2. Try magic link URL ─────────────────────────────────
                if not logged_in:
                    magic_link_url = os.environ.get("MODO_MAGIC_LINK_URL", "").strip()
                    if magic_link_url:
                        logged_in = self._login_via_magic_link(page, magic_link_url)

                # ── 3. Fall back to password login ────────────────────────
                if not logged_in:
                    self._magic_link_sent = False
                    for _attempt in range(1, 3):
                        logger.info("[modo_ai] Login attempt %d/2", _attempt)
                        logged_in = self._login(page)
                        if logged_in:
                            break
                        if self._magic_link_sent:
                            logger.error(
                                "[modo_ai] Magic link auth detected — aborting without retry"
                            )
                            _notify_magic_link_required(page.url)
                            break
                        if _attempt < 2:
                            logger.warning(
                                "[modo_ai] Login attempt %d failed — retrying in 10s", _attempt
                            )
                            time.sleep(10)
                            try:
                                page.goto(
                                    "https://modoenergy.com/sign-in",
                                    timeout=_NAV_TIMEOUT,
                                    wait_until="domcontentloaded",
                                )
                                page.wait_for_timeout(2_000)
                            except Exception:
                                pass

                if not logged_in:
                    if not self._magic_link_sent:
                        logger.error("[modo_ai] Login failed after all attempts — aborting custom distillation")
                        _notify_login_failure(2, page.url)
                    return

                _save_session_state(ctx)

                for i, question in enumerate(questions):
                    url = f"modo_ai://{url_prefix}/{today.isoformat()}/q{i:02d}"
                    logger.info(
                        "[modo_ai] Custom Q %d/%d: %s…",
                        i + 1, len(questions), question[:60],
                    )
                    try:
                        answer = self._ask_fresh(page, question)
                    except Exception as exc:
                        logger.warning("[modo_ai] Custom Q%d error: %s", i, exc)
                        continue

                    if not answer or len(answer) < 30:
                        logger.warning("[modo_ai] No substantive answer for custom Q%d", i)
                        continue

                    yield {
                        "doc_type": "ai_insight",
                        "title":    f"Modo AI (gap) — {question[:80]}",
                        "url":      url,
                        "published_date": today,
                        "content":  f"Q: {question}\n\nA: {answer}",
                        "_question": question,
                        "_answer":   answer,
                    }

                    if i < len(questions) - 1:
                        pause = random.uniform(10, 30)
                        logger.debug("[modo_ai] Pausing %.0fs", pause)
                        time.sleep(pause)

            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Response extraction
    # ------------------------------------------------------------------

    def _wait_for_settled_response(self, page, pre_text: str) -> str | None:
        """Poll until the AI response text is stable (streaming finished)."""
        deadline = time.monotonic() + _RESPONSE_TIMEOUT / 1000
        stable_count = 0
        prev = pre_text or ""

        while time.monotonic() < deadline:
            page.wait_for_timeout(2_000)
            current = self._extract_response_text(page)

            if not current or current == prev:
                stable_count += 1
                if stable_count >= _SETTLE_POLLS and current and current != pre_text:
                    # Text has been stable long enough and differs from before the question
                    return _clean_response(current, pre_text)
            else:
                stable_count = 0

            prev = current

            # Fast-path: streaming indicator gone and we have new text
            if not self._is_streaming(page) and current and current != pre_text:
                # Wait one more poll to be sure
                page.wait_for_timeout(2_000)
                final = self._extract_response_text(page)
                return _clean_response(final or current, pre_text)

        logger.warning("[modo_ai] Response timeout — returning partial text")
        final = self._extract_response_text(page)
        return _clean_response(final, pre_text) if final and final != pre_text else None

    def _is_streaming(self, page) -> bool:
        """Return True if the AI appears to still be generating output."""
        try:
            return page.evaluate("""() => {
                // Look for a "Stop generating" button or a streaming cursor/spinner
                const stopBtn = document.querySelector(
                    'button[aria-label*="stop" i], button[title*="stop" i], '
                    '[data-testid*="stop" i], button:has-text("Stop")'
                );
                if (stopBtn && stopBtn.offsetParent !== null) return true;
                // Animated spinner near the response area
                const spinner = document.querySelector(
                    '[class*="loading"], [class*="spinner"], [class*="typing"]'
                );
                return !!(spinner && spinner.offsetParent !== null);
            }""")
        except Exception:
            return False

    def _extract_response_text(self, page) -> str:
        """Extract the latest AI response text from the page via JS."""
        candidates = [
            # Most recent assistant/AI message by role
            """(() => {
                const els = document.querySelectorAll(
                    '[data-role="assistant"], [data-message-role="assistant"], '
                    '[class*="assistant-message"], [class*="ai-message"]'
                );
                if (!els.length) return null;
                return els[els.length - 1].innerText?.trim() || null;
            })()""",

            # Last non-user message bubble
            """(() => {
                const all = document.querySelectorAll('[class*="message"]:not([class*="input"])');
                const nonUser = Array.from(all).filter(el => {
                    const cls = el.className || '';
                    return !cls.includes('user') && !cls.includes('human');
                });
                if (!nonUser.length) return null;
                return nonUser[nonUser.length - 1].innerText?.trim() || null;
            })()""",

            # Entire chat / conversation container — last substantive block
            """(() => {
                const container = document.querySelector(
                    '[class*="chat-container"], [class*="conversation"], '
                    '[class*="messages-container"], [class*="chat-messages"]'
                );
                if (!container) return null;
                const paras = container.querySelectorAll('p, li, [class*="response"]');
                if (!paras.length) return container.innerText?.trim() || null;
                return Array.from(paras).slice(-30).map(e => e.innerText).join('\n').trim() || null;
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
# Session persistence helpers
# ---------------------------------------------------------------------------

def _load_session_state() -> dict | None:
    """Load Playwright storage state (cookies + localStorage) from disk.

    Returns the state dict, or None if the file doesn't exist or is unreadable.
    """
    import json as _json
    if not os.path.exists(_SESSION_PATH):
        return None
    try:
        with open(_SESSION_PATH) as f:
            state = _json.load(f)
        logger.info("[modo_ai] Loaded browser session from %s", _SESSION_PATH)
        return state
    except Exception as exc:
        logger.warning("[modo_ai] Could not load session state from %s: %s", _SESSION_PATH, exc)
        return None


def _save_session_state(ctx) -> None:
    """Persist Playwright storage state to disk after a successful login."""
    try:
        ctx.storage_state(path=_SESSION_PATH)
        logger.info("[modo_ai] Browser session saved to %s", _SESSION_PATH)
    except Exception as exc:
        logger.warning("[modo_ai] Could not save session state: %s", exc)


# ---------------------------------------------------------------------------
# Alert helpers
# ---------------------------------------------------------------------------

def _notify_login_failure(attempt: int, final_url: str) -> None:
    """Send a WeCom text alert when Modo login fails after max retries."""
    webhook_url = os.environ.get("WECOM_WEBHOOK_URL", "")
    if not webhook_url:
        logger.warning("[modo_ai] WECOM_WEBHOOK_URL not set — cannot send login-failure alert")
        return
    import requests as _req
    msg = (
        f"⚠️ Modo AI login failed after {attempt} attempt(s).\n"
        f"Final URL: {final_url}\n"
        f"Action needed: check MODO_EMAIL / MODO_PASSWORD or inspect debug screenshots at /tmp/modo_*.png."
    )
    for url in [u.strip() for u in webhook_url.split(",") if u.strip()]:
        try:
            resp = _req.post(
                url,
                json={"msgtype": "text", "text": {"content": msg}},
                timeout=10,
            )
            resp.raise_for_status()
            logger.info("[modo_ai] Login-failure alert sent to WeCom")
        except Exception as exc:
            logger.warning("[modo_ai] Could not send WeCom alert: %s", exc)


def _notify_magic_link_required(final_url: str) -> None:
    """Send a WeCom alert when Modo requires magic link authentication."""
    webhook_url = os.environ.get("WECOM_WEBHOOK_URL", "")
    if not webhook_url:
        logger.warning("[modo_ai] WECOM_WEBHOOK_URL not set — cannot send magic-link alert")
        return
    import requests as _req
    msg = (
        "⚠️ Modo AI login requires magic link (passwordless auth).\n"
        "Modo sent an authorization email — password login is unavailable.\n\n"
        "To re-authenticate:\n"
        "1. Open the authorization email from team@modoenergy.com\n"
        "2. Copy the magic link URL (the long https://modoenergy.com/sign-in/auth?... URL)\n"
        "3. Set MODO_MAGIC_LINK_URL=<paste URL> in the ECS task environment variables\n"
        "4. Redeploy the bess-gb-market task\n"
        "5. Once authenticated the session is saved — subsequent nightly runs will work automatically\n\n"
        f"Final URL at failure: {final_url}"
    )
    for url in [u.strip() for u in webhook_url.split(",") if u.strip()]:
        try:
            resp = _req.post(
                url,
                json={"msgtype": "text", "text": {"content": msg}},
                timeout=10,
            )
            resp.raise_for_status()
            logger.info("[modo_ai] Magic-link alert sent to WeCom")
        except Exception as exc:
            logger.warning("[modo_ai] Could not send WeCom alert: %s", exc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_magic_link_page(page) -> bool:
    """Return True if Modo is showing a 'check your email' magic link page."""
    try:
        text = page.evaluate("() => document.body.innerText.slice(0, 2000)").lower()
        return any(phrase in text for phrase in [
            "check your email",
            "magic link",
            "link has been sent",
            "we've sent you",
            "authorization link",
            "check your inbox",
            "email has been sent",
            "sent you a link",
        ])
    except Exception:
        return False


def _first_visible(page, selectors: list[str]) -> str | None:
    """Return the first selector that matches a currently visible element."""
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                return sel
        except Exception:
            continue
    return None


def _save_screenshot(page, name: str) -> None:
    """Save a debug screenshot to /tmp for login flow inspection."""
    try:
        path = f"/tmp/modo_{name}.png"
        page.screenshot(path=path)
        logger.info("[modo_ai] Screenshot saved: %s", path)
    except Exception as exc:
        logger.debug("[modo_ai] Screenshot failed (%s): %s", name, exc)


def distill_gap_questions(questions: list[str]) -> dict[str, str | None]:
    """Push gap questions to Modo AI, store answers in gb_knowledge_docs.

    Returns {question: answer} for each question; value is None if Modo
    gave no substantive answer (< 30 chars or timeout).
    Upserts via base.upsert_doc so re-runs on the same day are no-ops.
    """
    from services.gb_knowledge.base import get_db_conn, ensure_table, upsert_doc

    results: dict[str, str | None] = {q: None for q in questions}

    connector = ModoAIConnector()
    conn = get_db_conn()
    try:
        ensure_table(conn)
        for doc in connector.fetch_custom(questions):
            q_text   = doc.pop("_question", "")
            a_text   = doc.pop("_answer",   "")
            inserted = upsert_doc(
                conn, "modo_ai",
                doc["doc_type"], doc.get("title", ""),
                doc.get("url"), doc.get("published_date"), doc["content"],
            )
            if q_text and a_text:
                # Match back to original question (exact or prefix)
                for orig_q in questions:
                    if orig_q == q_text or orig_q[:80] == q_text[:80]:
                        results[orig_q] = a_text
                        break
            logger.info("[modo_ai] Gap doc stored=%s url=%s", inserted, doc.get("url"))
    except Exception as exc:
        logger.error("[modo_ai] distill_gap_questions failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()

    answered = sum(1 for v in results.values() if v)
    logger.info("[modo_ai] Gap distillation: %d/%d questions answered", answered, len(questions))
    return results


def request_magic_link_email(email: str | None = None) -> dict:
    """Navigate to Modo sign-in and submit the email to trigger a magic link email.

    Returns {"success": bool, "message": str}.
    Call this from the Streamlit UI (Step 1 of re-authentication).
    """
    _email = email or os.environ.get("MODO_EMAIL", "")
    if not _email:
        return {"success": False, "message": "No email address set (MODO_EMAIL)"}
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"success": False, "message": "playwright not installed"}

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
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
            try:
                from playwright_stealth import stealth_sync
                stealth_sync(page)
            except ImportError:
                pass

            # Use a connector instance so we can call _dismiss_cookie_banner
            connector = ModoAIConnector(email=_email)
            try:
                page.goto(
                    "https://modoenergy.com/sign-in",
                    timeout=_NAV_TIMEOUT,
                    wait_until="domcontentloaded",
                )
                page.wait_for_timeout(3_000)
                # Dismiss cookie banner immediately — it can block button clicks
                connector._dismiss_cookie_banner(page)
                page.wait_for_timeout(500)

                email_sel = _first_visible(page, [
                    'input[type="email"]',
                    'input[name="email"]',
                    'input[id*="email" i]',
                    'input[placeholder*="email" i]',
                    'input[autocomplete="email"]',
                ])
                if not email_sel:
                    _save_screenshot(page, "request_link_no_email_input")
                    return {"success": False, "message": "Email input not found on sign-in page"}

                page.fill(email_sel, _email)
                page.wait_for_timeout(500)

                # Click Continue to submit the email and trigger the magic link
                continue_sel = _first_visible(page, [
                    'button:has-text("Continue")',
                    'button:has-text("Next")',
                    'button[type="submit"]',
                    'button:has-text("Sign in")',
                    'button:has-text("Log in")',
                ])
                if continue_sel:
                    page.click(continue_sel)
                else:
                    page.keyboard.press("Enter")

                page.wait_for_timeout(3_000)
                # Dismiss cookie banner again — it often reappears after navigation
                connector._dismiss_cookie_banner(page)
                page.wait_for_timeout(500)
                _save_screenshot(page, "request_link_after_submit")

                if _is_magic_link_page(page):
                    return {
                        "success": True,
                        "message": (
                            f"Magic link email sent to {_email}. "
                            "Check your inbox and paste the link URL below."
                        ),
                    }

                # Also check for a password field — could mean password auth is still available
                pass_sel = _first_visible(page, ['input[type="password"]', 'input[name="password"]'])
                if pass_sel:
                    return {
                        "success": False,
                        "message": (
                            "Modo showed a password field instead of sending a magic link. "
                            "Password auth may still work — try 'Run Modo AI Distillation Now' instead."
                        ),
                    }

                body_text = ""
                try:
                    body_text = page.evaluate("() => document.body.innerText.slice(0, 600)")
                except Exception:
                    pass
                return {
                    "success": False,
                    "message": (
                        f"Could not confirm magic link was sent. "
                        f"Page text: {body_text[:300]!r}"
                    ),
                }
            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass
    except Exception as exc:
        return {"success": False, "message": str(exc)}


def authenticate_with_magic_link(url: str) -> dict:
    """Use a magic link URL to authenticate Playwright and save the session.

    Returns {"success": bool, "message": str}.
    Call this from the Streamlit UI (Step 2 of re-authentication).
    After success, the saved session is used by all subsequent nightly runs
    until the container restarts.
    """
    if not url or "modoenergy.com" not in url:
        return {"success": False, "message": "Invalid URL — must be a modoenergy.com link"}
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"success": False, "message": "playwright not installed"}

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
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
            try:
                from playwright_stealth import stealth_sync
                stealth_sync(page)
            except ImportError:
                pass

            try:
                connector = ModoAIConnector()
                success = connector._login_via_magic_link(page, url)
                if success:
                    _save_session_state(ctx)
                    return {
                        "success": True,
                        "message": (
                            "Authenticated successfully. Session saved to "
                            f"{_SESSION_PATH} — tonight's nightly run will reuse it."
                        ),
                    }
                # Try to get some page context for diagnostics
                body_text = ""
                try:
                    body_text = page.evaluate("() => document.body.innerText.slice(0, 300)")
                except Exception:
                    pass
                return {
                    "success": False,
                    "message": (
                        "Magic link authentication failed — the link may have expired "
                        f"(URL: {page.url}). Page text: {body_text[:150]!r}"
                    ),
                }
            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass
    except Exception as exc:
        return {"success": False, "message": str(exc)}


def _clean_response(text: str, pre_text: str) -> str:
    """Strip the pre-existing text (from previous messages) from the captured text."""
    if not text:
        return text
    # If pre-existing text is a prefix, strip it
    if pre_text and text.startswith(pre_text):
        text = text[len(pre_text):].strip()
    # Remove leading/trailing whitespace artefacts
    return text.strip()
