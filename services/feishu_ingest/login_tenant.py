"""Headed sign-in for a cross-tenant Feishu share. Logs every tab URL + a screenshot each few
seconds (logs/login_tenant_*.png), saves the session whenever a tab is past the login pages,
and finishes when the user closes the window."""
import json, os, sys, time
from pathlib import Path
from playwright.sync_api import sync_playwright
H = Path(os.environ.get("FEISHU_INGEST_HOME", Path.home() / "feishu-ingest"))
url = sys.argv[1]
status = H / "logs/login_tenant_status.txt"
def say(s): status.write_text(s); print(s, flush=True)
with sync_playwright() as p:
    br = p.chromium.launch(headless=False)
    ctx = br.new_context(storage_state=str(H / "state.json"), viewport={"width": 1400, "height": 900})
    pg = ctx.new_page(); pg.goto(url, timeout=120000)
    say("WAITING_FOR_LOGIN (close the window once the file is open)")
    saved, last = 0, None
    while True:
        try:
            pages = [q for q in ctx.pages if not q.is_closed()]
        except Exception:
            break
        if not pages:
            break
        urls = [q.url[:110] for q in pages]
        if urls != last:
            print("TABS", urls, flush=True); last = urls
        try:
            pages[-1].screenshot(path=str(H / "logs/login_tenant_now.png"), timeout=3000)
        except Exception:
            pass
        if any(".feishu.cn/" in u and "accounts." not in u and "/login" not in u for u in urls):
            try:
                ctx.storage_state(path=str(H / "state.json")); os.chmod(H / "state.json", 0o600)
                if not saved: say("SIGNED_IN - session saved; close the window to finish")
                saved += 1
            except Exception:
                pass
        time.sleep(3)
    say("SAVED" if saved else "CLOSED_WITHOUT_LOGIN")
    try: br.close()
    except Exception: pass
