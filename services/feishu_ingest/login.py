"""Headed Feishu sign-in; saves the session (incl. session cookies) to state.json once logged in."""
import json
import os, os, time
from pathlib import Path
from playwright.sync_api import sync_playwright
H = Path(os.environ.get("FEISHU_INGEST_HOME", Path.home() / "feishu-ingest"))
cfg = json.load(open(H / "config.json"))
status = H / "logs/login_status.txt"
def say(s): status.write_text(s); print(s, flush=True)
with sync_playwright() as p:
    ctx = p.chromium.launch_persistent_context(str(H / "profile"), headless=False, viewport={"width": 1400, "height": 900})
    pg = ctx.pages[0] if ctx.pages else ctx.new_page()
    pg.goto(cfg["home"], timeout=90000)
    say("WAITING_FOR_LOGIN")
    for i in range(1800):
        time.sleep(2)
        try:
            page = ctx.pages[-1]
            names = {c["name"] for c in ctx.cookies(cfg["base"])}
            if i % 3 == 0:
                print(i, page.url[:90], "| session" , "session" in names, flush=True)
                page.screenshot(path=str(H / "logs/login.png"))
            if "session" in names and "accounts." not in page.url and "/login" not in page.url:
                break
        except Exception as e:
            print("err", str(e)[:120], flush=True)
    else:
        say("LOGIN_TIMEOUT"); ctx.close(); raise SystemExit(1)
    time.sleep(4)
    ctx.storage_state(path=str(H / "state.json"))
    os.chmod(H / "state.json", 0o600)
    say("SAVED")
    ctx.close()
