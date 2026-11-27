"""List a Feishu drive folder tree through its UI pages (read-only, no downloads).

Usage: python list_folder.py <folder-url> [max_depth]
Writes ~/feishu-ingest/logs/folder_tree.json: [{path, name, kind, url}]
"""
import json
import os
import re
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

H = Path(os.environ.get("FEISHU_INGEST_HOME", Path.home() / "feishu-ingest"))
_ZW = re.compile(r"[​-‏⁠-⁤﻿‪-‮]")


def kind(href: str) -> str:
    m = re.search(r"feishu\.cn/(?:drive/)?(\w+)/", href)
    return m.group(1) if m else "?"


def rows(pg, url):
    for attempt in range(3):
        try:
            pg.goto(url, timeout=90000)
            break
        except Exception:
            if attempt == 2:
                print(f"SKIP (timeout) {url}", flush=True)
                return []
    try:
        pg.wait_for_selector("a.file-item-link", timeout=20000)
    except Exception:
        return []  # empty folder (or no access)
    pg.wait_for_timeout(2500)
    seen, stable = {}, 0
    while stable < 4:  # scroll until no new rows appear
        for name, href in pg.eval_on_selector_all(
                "a.file-item-link", "els => els.map(e => [e.innerText, e.href])"):
            seen.setdefault(href.split("?")[0], _ZW.sub("", name.split("\n")[0]).strip())
        n = len(seen)
        # the list scrolls inside its own virtualised container, not the page
        pg.evaluate("""() => { let n = [...document.querySelectorAll('a.file-item-link')].pop();
            while (n) { const s = getComputedStyle(n);
                if ((s.overflowY == 'auto' || s.overflowY == 'scroll') && n.scrollHeight > n.clientHeight) {
                    n.scrollTop = n.scrollTop + n.clientHeight; break; }
                n = n.parentElement; } }""")
        pg.wait_for_timeout(1500)
        after = len({h.split("?")[0] for h in pg.eval_on_selector_all(
            "a.file-item-link", "els => els.map(e => e.href)")} | set(seen))
        stable = stable + 1 if after == n else 0
    return [(name, href) for href, name in seen.items()]


def main() -> None:
    root, depth = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 4
    only = set(sys.argv[3].split(",")) if len(sys.argv) > 3 else None  # top-level folder names
    dest = H / (sys.argv[4] if len(sys.argv) > 4 else "logs/folder_tree.json")
    out = []
    with sync_playwright() as p:
        br = p.chromium.launch(headless=True)
        ctx = br.new_context(storage_state=str(H / "state.json"), viewport={"width": 1400, "height": 1000})
        pg = ctx.new_page()

        def walk(url, path, d):
            for name, href in rows(pg, url):
                if not path and only and name not in only:
                    continue
                k = kind(href)
                out.append({"path": "/".join(path), "name": name, "kind": k, "url": href})
                print(f"{'  ' * len(path)}[{k}] {name}", flush=True)
                if k == "folder" and d < depth:
                    walk(href, path + [name], d + 1)
            json.dump(out, open(dest, "w"), ensure_ascii=False, indent=1)

        walk(root.split("?")[0], [], 1)
        br.close()
    json.dump(out, open(dest, "w"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
