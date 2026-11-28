"""Weekly Feishu -> shared KB re-sync (Mac; launchd ai.pjh-etrm.feishu-weekly).

1. Check both Feishu sessions (Envision tenant + the Tradingthink cross-tenant share). If either has
   expired, notify and stop — the owner re-runs login.py / login_tenant.py (QR scan).
2. Re-list the ingested folders of the shared 电力市场文件库 (UI pages, read-only), with modified times.
3. Select: never-exported items, plus items modified after their last export. 官方-各省披露文件 keeps the
   agreed subset (policy/rules folders + quarterly/half-year/annual reports of this and last year).
4. Export through Feishu's own Download As / Download (3 workers when the list is long).
5. Stage + ingest in AWS (stage_ingest.py; the pre-step replaces a changed document's old KB entry),
   re-run once if any file failed, then notify with a summary.

Usage: python weekly_sync.py [--dry-run] [--reuse-guanfang <tree.json>]
  --dry-run: steps 1-3 only (selection written to logs/weekly/<date>/selected.json)
"""
from __future__ import annotations

import fcntl
import glob
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
H = Path(os.environ.get("FEISHU_INGEST_HOME", Path.home() / "feishu-ingest"))
PY = sys.executable
SCOPE = ["内部-各省专题研究", "外部-外部交流调研-收集的报告-会议纪要", "外部-外部客户交流材料"]
GUANFANG = "官方-各省披露文件"
REPORT = re.compile(r"季度|季报|上半年|年度|年报")
NATIVE = {"docx", "doc", "sheets", "slides", "wiki", "base"}
MON = {m: i for i, m in enumerate("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), 1)}
LOG = H / "logs/weekly_sync.log"


def log(msg: str) -> None:
    line = f"{datetime.now():%Y-%m-%d %H:%M} {msg}"
    print(line, flush=True)
    with open(LOG, "a") as fh:
        fh.write(line + "\n")


def notify(msg: str) -> None:
    subprocess.run(["osascript", "-e", f'display notification "{msg}" with title "Feishu weekly sync"'],
                   capture_output=True)


def parse_modified(text: str, now: datetime) -> datetime | None:
    """Feishu list 'modified' cell: '2:29 PM Nov 24', '18:05 Yesterday', 'Today', 'Dec 3, 2025', '2025-12-03'."""
    t = text.strip()
    if not t:
        return None
    if re.search(r"just now|minute|刚刚|分钟", t, re.I):
        return now
    hm = re.search(r"(\d{1,2}):(\d{2})\s*(AM|PM)?", t, re.I)
    hh, mm = (int(hm.group(1)), int(hm.group(2))) if hm else (23, 59)
    if hm and hm.group(3):
        hh = hh % 12 + (12 if hm.group(3).upper() == "PM" else 0)
    if re.search(r"today|今天", t, re.I):
        d = now.date()
    elif re.search(r"yesterday|昨天", t, re.I):
        d = now.date() - timedelta(days=1)
    elif m := re.search(r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})", t):
        d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    elif m := re.search(r"\b([A-Z][a-z]{2})\s+(\d{1,2})(?:,\s*(\d{4}))?", t):
        if m.group(1) not in MON:
            return None
        d = date(int(m.group(3) or now.year), MON[m.group(1)], int(m.group(2)))
        if not m.group(3) and d > now.date() + timedelta(days=1):
            d = d.replace(year=d.year - 1)
    elif m := re.search(r"(\d{1,2})月(\d{1,2})日", t):
        d = date(now.year, int(m.group(1)), int(m.group(2)))
    else:
        return None
    return datetime(d.year, d.month, d.day, hh, mm)


def exported() -> dict[str, float]:
    """url -> mtime of its newest local export (all export result logs)."""
    out: dict[str, float] = {}
    for f in glob.glob(str(H / "logs/export_results*.jsonl")) + glob.glob(str(H / "logs/runs/*.jsonl")):
        for line in open(f):
            r = json.loads(line)
            if not r["result"].startswith("OK "):
                continue
            rel = r["result"][3:].rsplit(" (", 1)[0]
            for p in sorted((H / "downloads").glob(f"*/{rel}"), reverse=True):
                out[r["url"]] = max(out.get(r["url"], 0), p.stat().st_mtime)
                break
    return out


def sessions_ok(tree_files: list[Path]) -> tuple[bool, str]:
    from playwright.sync_api import sync_playwright
    cfg = json.load(open(H / "config.json"))
    probes = [cfg["home"]]
    for tf in tree_files:  # one cross-tenant file from the last listing, if any
        if tf.exists():
            tt = next((x["url"] for x in json.load(open(tf)) if "tradingthink" in x["url"] and x["kind"] == "file"), None)
            if tt:
                probes.append(tt)
                break
    with sync_playwright() as p:
        br = p.chromium.launch(headless=True)
        ctx = br.new_context(storage_state=str(H / "state.json"))
        try:
            for url in probes:
                pg = ctx.new_page()
                pg.goto(url, timeout=120000)
                pg.wait_for_timeout(8000)
                if "accounts." in pg.url or "/login" in pg.url or pg.locator("text=/Log In With QR Code/").count():
                    return False, url.split("/")[2]
                pg.close()
        finally:
            br.close()
    return True, ""


def list_tree(root: str, depth: int, only: str, dest: Path, previous: list[dict]) -> list[dict]:
    """List a tree; a folder that shows no rows but had files in the previous listing means the page
    failed to load, not that the folder emptied -> re-list once, then give up (raise)."""
    for attempt in (1, 2):
        r = subprocess.run([PY, str(HERE / "list_folder.py"), root, str(depth), only, str(dest)],
                           capture_output=True, text=True, env={**os.environ, "FEISHU_INGEST_HOME": str(H)})
        if r.returncode != 0 or not dest.exists():
            raise RuntimeError(f"listing failed: {r.stderr[-300:]}")
        tree = json.load(open(dest))
        empty = set(json.load(open(str(dest) + ".empty.json")))
        before = {x["url"]: x for x in previous if x["kind"] == "folder"}
        had_files = {x["path"] for x in previous}
        lost = [before[u]["path"] + "/" + before[u]["name"] if before[u]["path"] else before[u]["name"]
                for u in empty if u in before]
        lost = [p for p in lost if p in had_files]
        if not lost:
            return tree
        log(f"listing attempt {attempt}: {len(lost)} folder(s) came back empty but had files: {lost[:5]}")
    raise RuntimeError(f"incomplete listing: {len(lost)} folders failed to load")


UNSUPPORTED = re.compile(r"\.(zip|rar|wps|svg|doc|xls|ppt)$", re.I)  # KB extractors cannot read these
KNOWN = H / "logs/known_urls.json"


def known_urls() -> set[str]:
    """URLs already seen in an earlier listing. Bootstrap = the listings the original ingest was based on
    (anything there that was not exported was skipped on purpose: already in the KB, unsupported, or
    outside the agreed subset)."""
    if KNOWN.exists():
        return set(json.load(open(KNOWN)))
    seed = set()
    for f in ("logs/tree_scope.json", "logs/tree_guanfang.json"):
        if (H / f).exists():
            seed |= {x["url"] for x in json.load(open(H / f))}
    return seed


def select(main: list[dict], guanfang: list[dict], done: dict[str, float], now: datetime) -> tuple[list, dict]:
    years = {str(now.year), str(now.year - 1)}
    known = known_urls()
    stats = {"new": 0, "changed": 0, "unparsed": 0}
    picks = []
    for x in main + guanfang:
        if x["kind"] == "folder" or UNSUPPORTED.search(x["name"]):
            continue
        if x["path"].startswith(GUANFANG):
            yrs = set(re.findall(r"20\d{2}", x["name"]))
            if not (re.search(r"政策|规则", x["path"]) or (REPORT.search(x["name"]) and yrs & years)):
                continue
        if x["url"] not in done:
            if x["url"] not in known:  # appeared since the last listing
                stats["new"] += 1
                picks.append(x)
            continue
        mod = parse_modified(x.get("modified", ""), now)
        if mod is None:
            stats["unparsed"] += 1
        elif mod.timestamp() > done[x["url"]] + 60:
            stats["changed"] += 1
            picks.append(x)
    return picks, stats


def export(picks: list[dict], wk: Path) -> dict:
    env = {**os.environ, "FEISHU_INGEST_HOME": str(H), "FEISHU_SETTLE_MS": "10000", "FEISHU_MENU_MS": "3000"}
    n = 3 if len(picks) > 60 else 1
    procs = []
    for k in range(n):
        mf = wk / f"export_manifest_{k}.json"
        json.dump(picks[k::n], open(mf, "w"), ensure_ascii=False, indent=1)
        lf = open(wk / f"export_{k}.log", "w")
        procs.append(subprocess.Popen([PY, str(HERE / "export.py"), "--manifest", str(mf)], cwd=str(H),
                                      stdout=lf, stderr=subprocess.STDOUT, env=env))
    for p in procs:
        p.wait()
    text = "".join(open(f).read() for f in sorted(wk.glob("export_*.log")))
    return {"ok": len(re.findall(r"^OK ", text, re.M)),
            "disabled": len(re.findall(r"^FAIL .*no Download", text, re.M)),
            "failed": len(re.findall(r"^FAIL ", text, re.M)) - len(re.findall(r"^FAIL .*no Download", text, re.M)),
            "abort": "ABORT" in text}


def aws(*args: str) -> str:
    for _ in range(10):
        r = subprocess.run(["aws", *args, "--region", "ap-southeast-1"], capture_output=True, text=True)
        if r.returncode == 0:
            return r.stdout.strip()
        time.sleep(30)
    return ""


def wait_batch(task: str) -> str:
    while aws("ecs", "describe-tasks", "--cluster", "bess-platform-cluster", "--tasks", task,
              "--query", "tasks[0].lastStatus", "--output", "text") != "STOPPED":
        time.sleep(60)
    out = aws("logs", "get-log-events", "--log-group-name", "/ecs/bess-platform", "--log-stream-name",
              f"spot-markets/spot-markets/{task}", "--start-from-head", "--query", "events[*].message",
              "--output", "text")
    m = re.findall(r"BATCH DONE ok=\d+ failed=\d+", out)
    return m[-1] if m else "no BATCH DONE line"


def ingest(batch: str) -> str:
    r = subprocess.run([PY, str(HERE / "stage_ingest.py"), batch, "--min", "1"], capture_output=True, text=True,
                       env={**os.environ, "FEISHU_INGEST_HOME": str(H)})
    m = re.search(r"TASK (\w+)", r.stdout)
    if not m:
        return "nothing staged" if "pending 0" in r.stdout else f"staging failed: {r.stderr[-200:]}"
    result = wait_batch(m.group(1))
    left = aws("s3", "ls", f"s3://bess-uploader-data-chen-singp-2026/tmp/{batch}/", "--recursive")
    if any(not l.split("/")[-1].startswith("_") for l in left.splitlines() if l.strip()):
        sys.path.insert(0, str(HERE))
        from stage_ingest import launch  # re-run once: only files still in S3 are processed
        result += f"; re-run: {wait_batch(launch(f'tmp/{batch}/'))}"
    return result


def main() -> int:
    dry = "--dry-run" in sys.argv
    lock = open(H / "logs/weekly.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log("another sync is running; exit")
        return 0
    now = datetime.now()
    wk = H / f"logs/weekly/{now:%Y-%m-%d}"
    wk.mkdir(parents=True, exist_ok=True)
    last = sorted((H / "logs/weekly").glob("*/tree_guanfang.json")) or [H / "logs/tree_guanfang.json"]
    ok, host = sessions_ok([last[-1]])
    if not ok:
        log(f"STOP: Feishu session expired on {host}")
        notify(f"Session expired on {host} - re-run login (QR scan), then run weekly_sync.py")
        return 2
    root = json.load(open(H / "config.json"))["shared_root"]
    log("listing folders")
    prev_dir = sorted(d for d in (H / "logs/weekly").glob("*") if d != wk and (d / "tree_main.json").exists())
    prev_main = json.load(open(prev_dir[-1] / "tree_main.json")) if prev_dir else json.load(open(H / "logs/tree_scope.json"))
    prev_gf = json.load(open(prev_dir[-1] / "tree_guanfang.json")) if prev_dir and (prev_dir[-1] / "tree_guanfang.json").exists() \
        else json.load(open(H / "logs/tree_guanfang.json"))
    try:
        main_tree = list_tree(root, 4, ",".join(SCOPE), wk / "tree_main.json", prev_main)
        if "--reuse-guanfang" in sys.argv:  # testing: skip the multi-hour 官方 walk, reuse a saved listing
            gf_tree = json.load(open(sys.argv[sys.argv.index("--reuse-guanfang") + 1]))
        else:
            gf_tree = list_tree(root, 7, GUANFANG, wk / "tree_guanfang.json", prev_gf)
    except RuntimeError as e:  # nothing selected or recorded from a partial view of the folders
        log(f"STOP: {e}")
        notify(f"Listing incomplete ({e}) - nothing changed; will retry next week or run weekly_sync.py")
        for f in wk.glob("tree_*.json*"):
            f.unlink()  # keep a failed listing from becoming next week's baseline
        return 3
    picks, stats = select(main_tree, gf_tree, exported(), now)
    log(f"listed {len(main_tree) + len(gf_tree)} entries; selected {len(picks)} "
        f"(new {stats['new']}, changed {stats['changed']}; modified unparsed {stats['unparsed']})")
    json.dump(picks, open(wk / "selected.json", "w"), ensure_ascii=False, indent=1)
    if not dry:  # everything listed now is "known" from here on
        json.dump(sorted(known_urls() | {x["url"] for x in main_tree + gf_tree}), open(KNOWN, "w"))
    if dry or not picks:
        if not picks:
            notify("No new or changed documents this week")
        return 0
    ex = export(picks, wk)
    log(f"export: {ex}")
    if ex["abort"]:
        notify("Feishu session expired during export - re-run login, then weekly_sync.py")
        return 2
    res = ingest(f"feishu-weekly-{now:%Y%m%d}")
    log(f"ingest: {res}")
    notify(f"{ex['ok']} exported ({stats['new']} new, {stats['changed']} changed); ingest {res}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
