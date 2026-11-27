"""Export picked Feishu documents through the UI's own "Download As" menu.

Usage: python export.py <url> [<url> ...]     (or no args: every line of picks.txt)
Saves to ~/feishu-ingest/downloads/<YYYY-MM-DD>/ and prints one line per document.
"""
import json
import os
import re
import sys
from datetime import date
from pathlib import Path

from playwright.sync_api import sync_playwright

H = Path(os.environ.get("FEISHU_INGEST_HOME", Path.home() / "feishu-ingest"))
OUT = H / "downloads" / date.today().isoformat()
# preferred formats per document kind, first visible match wins
PREF = {
    "docx": ["Word", "PDF"], "wiki": ["Word", "Excel", "PDF"], "doc": ["Word", "PDF"],
    "sheets": ["Excel", "CSV", "PDF"], "slides": ["PPTX", "PowerPoint", "PDF"],
}
_ZW = re.compile(r"[​-‏⁠-⁤﻿‪-‮]")


def kind(url: str) -> str:
    m = re.search(r"feishu\.cn/(\w+)/", url)
    return m.group(1) if m else "docx"


def _shot(pg) -> None:
    try:  # diagnostics only; never let a slow page kill the run
        pg.screenshot(path=str(H / "logs/export_fail.png"), timeout=5000)
    except Exception:  # noqa: BLE001
        pass


def _save(d, subdir: str, fmt: str = "original") -> str:
    folder = OUT / subdir if subdir else OUT
    folder.mkdir(parents=True, exist_ok=True)
    dest = folder / _ZW.sub("", d.suggested_filename)
    d.save_as(str(dest))
    return f"OK {dest.relative_to(OUT)} ({dest.stat().st_size / 1e6:.1f} MB, {fmt})"


def _wait_online(max_min: int = 60) -> None:
    import time, urllib.request
    for i in range(max_min * 6):
        try:
            urllib.request.urlopen(json.load(open(H / "config.json"))["base"] + "/", timeout=8)
            if i:
                print(f"  network back after {i * 10}s", flush=True)
            return
        except Exception as e:  # noqa: BLE001
            if "HTTP Error" in str(e):
                return  # server answered
            time.sleep(10)


_checked = {"n": 0}


def _session_ok(ctx) -> bool:
    """Every 25 items, confirm the drive home still loads signed-in (not the QR login page)."""
    _checked["n"] += 1
    if _checked["n"] % 25 != 1:
        return True
    pg = ctx.new_page()
    try:
        _wait_online()
        pg.goto(json.load(open(H / "config.json"))["home"], timeout=90000)
        pg.wait_for_timeout(6000)
        return "/drive/" in pg.url and pg.locator("text=/Log In With QR Code|扫码登录/").count() == 0
    except Exception:  # noqa: BLE001
        return True  # network trouble is handled per item; don't abort on it
    finally:
        pg.close()


def export_one(ctx, url: str, subdir: str = "") -> str:
    pg = ctx.new_page()
    try:
        pg.goto(url, timeout=90000)
        pg.wait_for_selector("[data-e2e=suite-more-btn]", timeout=60000)
        pg.wait_for_timeout(int(__import__("os").environ.get("FEISHU_SETTLE_MS", "3000")))
        title = _ZW.sub("", pg.title()).rsplit(" - ", 1)[0].strip()
        pg.locator("[data-e2e=suite-more-btn]").first.click()
        pg.wait_for_timeout(int(__import__("os").environ.get("FEISHU_MENU_MS", "1000")))
        if kind(url) == "file":  # uploaded file: plain "Download", original format
            item = pg.get_by_text("Download", exact=True)
            if not item.count():
                return f"FAIL {title}: no Download in the menu (download disabled?)"
            with pg.expect_download(timeout=300000) as dl:
                item.first.click()
            return _save(dl.value, subdir)
        dl_item = pg.get_by_text("Download As", exact=True)
        if not dl_item.count():  # wiki pages tuck it under "More"
            pg.get_by_text("More", exact=True).last.hover()
            pg.wait_for_timeout(1200)
        if not dl_item.count():
            _shot(pg)
            return f"FAIL {title}: no Download As in the menu (export disabled for this page?)"
        opts = []
        for _ in range(3):  # submenu can render slowly
            dl_item.first.hover()
            pg.wait_for_timeout(2500)
            opts = [o for o in PREF.get(kind(url), ["PDF"])
                    if pg.get_by_text(re.compile("^" + o)).count()]
            if opts:
                break
        if not opts:
            _shot(pg)
            try:
                pg.screenshot(path=str(H / f"logs/emptymenu_{re.sub(r'[^A-Za-z0-9]', '', url)[-12:]}.png"), timeout=5000)
            except Exception:  # noqa: BLE001
                pass
            shown = [t for t in pg.locator("[role=menuitem], li").all_inner_texts() if "(." in t or "PDF" in t]
            return f"FAIL {title}: no known format in Download As menu; shown={shown[:6]}"
        with pg.expect_download(timeout=300000) as dl:
            pg.get_by_text(re.compile("^" + opts[0])).last.click()
            pg.wait_for_timeout(2500)
            # some kinds open a confirm dialog (e.g. include comments) before exporting
            for btn in ("Export", "Download", "导出", "下载"):
                b = pg.get_by_role("button", name=btn)
                if b.count() and b.first.is_visible():
                    b.first.click()
                    break
        return _save(dl.value, subdir, opts[0])
    except Exception as e:  # noqa: BLE001
        _shot(pg)
        return f"FAIL {url}: {str(e)[:200]}"
    finally:
        pg.close()


def main() -> int:
    # args: URLs, or "--manifest <json>" with [{path, name, url}] (saves under the Feishu folder path)
    if sys.argv[1:2] == ["--manifest"]:
        items = json.load(open(sys.argv[2]))
    else:
        urls = sys.argv[1:] or [l.strip() for l in open(H / "picks.txt") if l.strip() and not l.startswith("#")]
        items = [{"path": "", "name": "", "url": u} for u in urls]
    results = []
    with sync_playwright() as p:
        br = p.chromium.launch(headless=True)
        ctx = br.new_context(storage_state=str(H / "state.json"), accept_downloads=True,
                             viewport={"width": 1400, "height": 1000})
        for it in items:
            if not _session_ok(ctx):
                print("ABORT: Feishu session expired - re-run login.py, then resume", flush=True)
                break
            for attempt in range(4):
                _wait_online()
                r = export_one(ctx, it["url"], it["path"])
                if not any(e in r for e in ("ERR_INTERNET_DISCONNECTED", "ERR_NETWORK_CHANGED",
                                            "ERR_FAILED", "Timeout")):
                    break
                print(f"  retry {attempt + 1} after: {r[:120]}", flush=True)
            results.append(r)
            print(r, flush=True)
            with open(H / "logs/export_results.jsonl", "a") as fh:
                fh.write(json.dumps({**it, "result": r}, ensure_ascii=False) + "\n")
        ctx.storage_state(path=str(H / "state.json"))  # keep refreshed cookies
        br.close()
    return 0 if all(r.startswith("OK") for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
