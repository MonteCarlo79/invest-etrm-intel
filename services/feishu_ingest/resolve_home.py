"""Open named items from Home -> Recent by clicking their rows; record URLs (all matches per name)."""
import json
import os, sys
from pathlib import Path
from playwright.sync_api import sync_playwright
H = Path(os.environ.get("FEISHU_INGEST_HOME", Path.home() / "feishu-ingest"))
out = []
with sync_playwright() as p:
    br = p.chromium.launch(headless=True)
    ctx = br.new_context(storage_state=str(H / "state.json"), viewport={"width": 1400, "height": 1000})
    home = ctx.new_page()
    for w in sys.argv[1:]:
        i = 0
        while True:
            home.goto(json.load(open(H / "config.json"))["home"], timeout=90000); home.wait_for_timeout(6000)
            loc = home.get_by_text(w, exact=False)
            for _ in range(40):  # virtualised list: scroll until the row renders
                if loc.count() > i: break
                home.mouse.move(700, 600); home.mouse.wheel(0, 1500); home.wait_for_timeout(1200)
            if loc.count() <= i: break
            try:
                with ctx.expect_page(timeout=15000) as np:
                    loc.nth(i).click()
                doc = np.value; doc.wait_for_timeout(5000); url = doc.url.split("?")[0]; doc.close()
            except Exception:
                home.wait_for_timeout(5000); url = home.url.split("?")[0]
            out.append({"path": "Home-recent", "name": w, "url": url}); print(w, "->", url, flush=True)
            i += 1
            if i >= 3: break
    json.dump(out, open(H / "logs/batch_home.json", "w"), ensure_ascii=False, indent=1)
    br.close()
