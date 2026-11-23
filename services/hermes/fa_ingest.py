"""Financial Advisor attachment ingest — save property/tax/bank mail attachments
to OneDrive `Home/Financial Advisor/` (spec: Financial Advisor/Analysis 2026-11/
Hermes_attachment_spec.md, routing confirmed by Dipeng 2026-11-23).

Hermes only SAVES files; it never interprets them, never calls an LLM, and never
modifies mail (no mark-as-read, move, delete or forward).

Routing rules live in OneDrive, not in this repo (they carry addresses, policy
and account numbers): `<root>/_ingest_rules.json`. Shape:

    {"root": "Home/Financial Advisor", "since": "2024-04-01",
     "extensions": ["pdf", "csv", "xls", "xlsx"], "unsorted": "_inbox_unsorted",
     "watch": ["life residential", ...],          # in-scope trigger words
     "rules": [                                   # first match wins
       {"name": "OWP rent", "any": [...], "all": [...], "dest": "UK Properties/OWP/Rental Payment",
        "invoice_dest": "...",                    # optional: if "invoice" in subject/filename
        "subfolders": {"keyword": "subfolder"}},  # optional: dest/<subfolder> on keyword hit
       {"name": "HMRC", "any": [...], "dest": "UK tax/{tax_year}"}  # {tax_year} = 2025-2026 (6 Apr boundary)
     ]}

A message is in scope when its sender name/address, subject, body preview or an
attachment name contains a `watch` word (or any rule's `any` word). In-scope mail
with no matching rule goes to `unsorted`. Out-of-scope mail is ignored entirely.

Matching is case-insensitive substring. Dedupe is by SHA-256 across runs
(table fa_ingest_files). Watermark (last receivedDateTime) in hermes_settings.

CLI (one-off ECS task):  python -m services.hermes.fa_ingest [--dry-run] [--since 2024-04-01]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import PurePosixPath
from typing import Optional
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

_GRAPH = "https://graph.microsoft.com/v1.0"
RULES_PATH = "Home/Financial Advisor/_ingest_rules.json"
WATERMARK_KEY = "fa_ingest_watermark"
LOG_NAME = "_ingest_log.csv"
LOG_HEADER = ["saved_at", "email_date", "sender", "subject", "destination", "filename", "sha256"]

_DDL = """
CREATE TABLE IF NOT EXISTS fa_ingest_files (
    sha256      TEXT PRIMARY KEY,
    saved_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    email_date  TIMESTAMPTZ,
    destination TEXT NOT NULL,
    filename    TEXT NOT NULL
)"""


# ── pure helpers (unit-tested) ──────────────────────────────────────────────

def uk_tax_year(d: date) -> str:
    """UK tax year folder name: 6 Apr Y → 5 Apr Y+1 is 'Y-(Y+1)'."""
    start = d.year if (d.month, d.day) >= (4, 6) else d.year - 1
    return f"{start}-{start + 1}"


def _has(text: str, words) -> bool:
    return any(w.lower() in text for w in words)


@dataclass
class Route:
    dest: Optional[str]          # folder under root; None = out of scope
    rule: str = ""


def route_message(rules: dict, sender: str, subject: str, preview: str,
                  att_names: list[str], received: date) -> Route:
    """First matching rule → destination folder; in-scope but unrouted →
    rules['unsorted']; out of scope → Route(None)."""
    text = " ".join([sender, subject, preview, *att_names]).lower()
    subj_files = " ".join([subject, *att_names]).lower()
    for r in rules.get("rules", []):
        if r.get("any") and not _has(text, r["any"]):
            continue
        if r.get("all") and not all(w.lower() in text for w in r["all"]):
            continue
        dest = r["dest"]
        if r.get("invoice_dest") and "invoice" in subj_files:
            dest = r["invoice_dest"]
        for kw, sub in (r.get("subfolders") or {}).items():
            if kw.lower() in text:
                dest = f"{dest}/{sub}"
                break
        return Route(dest.replace("{tax_year}", uk_tax_year(received)), r.get("name", ""))
    scope = list(rules.get("watch", []))
    for r in rules.get("rules", []):
        scope += r.get("any", [])
    if scope and _has(text, scope):
        return Route(rules.get("unsorted", "_inbox_unsorted"), "unsorted")
    return Route(None)


def wanted(name: str, extensions) -> bool:
    return PurePosixPath(name.lower()).suffix.lstrip(".") in {e.lower() for e in extensions}


def dated_name(name: str, d: date) -> str:
    p = PurePosixPath(name)
    return f"{p.stem}_{d:%Y%m%d}{p.suffix}"


# ── Graph I/O ───────────────────────────────────────────────────────────────

def _check(resp: requests.Response) -> None:
    if not resp.ok:   # never echo URLs (may carry tempauth)
        raise requests.HTTPError(f"{resp.status_code} {resp.reason}", response=resp)


def _get(url: str, headers_fn, params=None, timeout: int = 60, attempts: int = 4) -> requests.Response:
    """GET with retry on 429/5xx/timeouts (Graph 504s seen mid-backfill 2026-11-23).
    headers_fn is called per attempt so a refreshed token is picked up."""
    import time
    for i in range(attempts):
        try:
            r = requests.get(url, headers=headers_fn(), params=params, timeout=timeout)
        except requests.RequestException:
            if i == attempts - 1:
                raise
        else:
            if r.status_code not in (429, 500, 502, 503, 504) or i == attempts - 1:
                return r
            time.sleep(float(r.headers.get("Retry-After", 0)) or 5 * (i + 1))
            continue
        time.sleep(5 * (i + 1))
    return r


def iter_messages(outlook, since_iso: str):
    """All mail folders, attachments only, oldest first, paged."""
    url = f"{_GRAPH}/me/messages"
    params = {
        "$filter": f"receivedDateTime ge {since_iso} and hasAttachments eq true",
        "$orderby": "receivedDateTime asc",
        "$select": "id,subject,from,receivedDateTime,bodyPreview",
        "$top": "50",
    }
    while url:
        r = _get(url, outlook._headers, params=params, timeout=60)
        _check(r)
        j = r.json()
        yield from j.get("value", [])
        url, params = j.get("@odata.nextLink"), None


def list_file_attachments(outlook, msg_id: str) -> list[dict]:
    r = _get(f"{_GRAPH}/me/messages/{msg_id}/attachments", outlook._headers,
             params={"$select": "id,name,size,isInline"}, timeout=30)
    _check(r)
    return [a for a in r.json().get("value", [])
            if a.get("@odata.type", "").endswith("fileAttachment")]


def _od_path(path: str) -> str:
    return "/".join(quote(s, safe="") for s in path.strip("/").split("/"))


def od_read(onedrive, path: str) -> Optional[bytes]:
    r = _get(f"{_GRAPH}/me/drive/root:/{_od_path(path)}:/content", onedrive._headers, timeout=120)
    if r.status_code == 404:
        return None
    _check(r)
    return r.content


# ── run ─────────────────────────────────────────────────────────────────────

@dataclass
class RunResult:
    scanned: int = 0
    in_scope: int = 0
    saved: list = field(default_factory=list)     # (dest, filename)
    duplicates: int = 0
    errors: list = field(default_factory=list)
    by_dest: dict = field(default_factory=dict)
    senders: dict = field(default_factory=dict)   # dry-run only: dest → {sender: n}


def run(outlook, onedrive, pg_url: str, *, dry_run: bool = False,
        since: Optional[str] = None, settings=None) -> RunResult:
    import psycopg2

    raw = od_read(onedrive, RULES_PATH)
    if raw is None:
        raise RuntimeError(f"rules file missing: OneDrive/{RULES_PATH}")
    rules = json.loads(raw.decode("utf-8-sig"))
    root = rules.get("root", "Home/Financial Advisor").strip("/")
    exts = rules.get("extensions", ["pdf", "csv", "xls", "xlsx"])

    conn = psycopg2.connect(pg_url)
    conn.autocommit = True
    cur = conn.cursor()
    if not dry_run:                       # dry run never changes the schema
        cur.execute(_DDL)
    cur.execute("SELECT to_regclass('fa_ingest_files')")
    if cur.fetchone()[0]:
        cur.execute("SELECT sha256 FROM fa_ingest_files")
        seen = {r[0] for r in cur.fetchall()}
    else:
        seen = set()

    def get_wm():
        cur.execute("SELECT value FROM hermes_settings WHERE key=%s", (WATERMARK_KEY,))
        row = cur.fetchone()
        return row[0] if row else None

    start = since or get_wm() or f"{rules.get('since', '2024-04-01')}T00:00:00Z"
    if len(start) == 10:
        start += "T00:00:00Z"
    res = RunResult()
    log_rows: list[list[str]] = []
    last_ok = None

    for m in iter_messages(outlook, start):
        res.scanned += 1
        sender = " ".join(filter(None, [(m.get("from") or {}).get("emailAddress", {}).get("name", ""),
                                        (m.get("from") or {}).get("emailAddress", {}).get("address", "")]))
        subject = m.get("subject") or ""
        received = datetime.fromisoformat(m["receivedDateTime"].replace("Z", "+00:00"))
        try:
            atts = [a for a in list_file_attachments(outlook, m["id"]) if wanted(a.get("name", ""), exts)]
            if not atts:
                last_ok = m["receivedDateTime"]
                continue
            route = route_message(rules, sender, subject, m.get("bodyPreview") or "",
                                  [a["name"] for a in atts], received.date())
            if route.dest is None:
                last_ok = m["receivedDateTime"]
                continue
            res.in_scope += 1
            folder = f"{root}/{route.dest}"
            for a in atts:
                if dry_run:
                    res.saved.append((route.dest, a["name"]))
                    res.senders.setdefault(route.dest, {}).setdefault(sender, 0)
                    res.senders[route.dest][sender] += 1
                    res.by_dest[route.dest] = res.by_dest.get(route.dest, 0) + 1
                    continue
                rr = _get(f"{_GRAPH}/me/messages/{m['id']}/attachments/{a['id']}/$value",
                          outlook._headers, timeout=120)
                _check(rr)
                data = rr.content
                h = hashlib.sha256(data).hexdigest()
                if h in seen:
                    res.duplicates += 1
                    continue
                name = a["name"]
                existing = od_read(onedrive, f"{folder}/{name}")
                if existing is not None:
                    if hashlib.sha256(existing).hexdigest() == h:
                        seen.add(h)
                        res.duplicates += 1
                        cur.execute("INSERT INTO fa_ingest_files (sha256, email_date, destination, filename) "
                                    "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                    (h, received, route.dest, name))
                        continue
                    name = dated_name(name, received.date())
                onedrive.upload_file(folder, name, data, conflict_behavior="rename")
                seen.add(h)
                cur.execute("INSERT INTO fa_ingest_files (sha256, email_date, destination, filename) "
                            "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                            (h, received, route.dest, name))
                now = datetime.now(timezone.utc).isoformat(timespec="seconds")
                log_rows.append([now, m["receivedDateTime"], sender, subject, route.dest, name, h])
                res.saved.append((route.dest, name))
                res.by_dest[route.dest] = res.by_dest.get(route.dest, 0) + 1
            last_ok = m["receivedDateTime"]
        except Exception as exc:   # one bad message must not stop the run; watermark stops here
            res.errors.append(f"{m['receivedDateTime']}: {type(exc).__name__}: {str(exc)[:160]}")
            logger.warning("fa_ingest message failed (%s): %s", m["receivedDateTime"], exc)
            break

    if not dry_run:
        if log_rows:
            prev = od_read(onedrive, f"{root}/{LOG_NAME}")
            buf = io.StringIO()
            if prev:
                buf.write(prev.decode("utf-8-sig"))
                if not buf.getvalue().endswith("\n"):
                    buf.write("\n")
            w = csv.writer(buf)
            if not prev:
                w.writerow(LOG_HEADER)
            w.writerows(log_rows)
            onedrive.upload_file(root, LOG_NAME, buf.getvalue().encode("utf-8-sig"))
        if last_ok:
            cur.execute("INSERT INTO hermes_settings (key, value, updated_at) VALUES (%s,%s,NOW()) "
                        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()",
                        (WATERMARK_KEY, last_ok))
    conn.close()
    return res


def summary_text(res: RunResult, dry_run: bool) -> str:
    head = "📎 财务附件归集" + ("（试运行，未保存）" if dry_run else "")
    lines = [f"{head}：扫描 {res.scanned} 封带附件邮件，命中 {res.in_scope} 封，"
             f"{'将保存' if dry_run else '保存'} {len(res.saved)} 个文件，重复跳过 {res.duplicates} 个"]
    for d, n in sorted(res.by_dest.items(), key=lambda x: -x[1]):
        lines.append(f"• {d}: {n}")
    for e in res.errors[:3]:
        lines.append(f"⚠️ {e}")
    return "\n".join(lines)


def scheduled_job(outlook, onedrive, pg_url: str, feishu=None, open_id: str = "") -> None:
    try:
        res = run(outlook, onedrive, pg_url)
        if feishu and open_id and (res.saved or res.errors):
            feishu.send_text(open_id, summary_text(res, False))
    except Exception as exc:
        logger.error("fa_ingest failed: %s", exc)
        if feishu and open_id:
            feishu.send_text(open_id, f"⚠️ 财务附件归集失败：{type(exc).__name__}: {str(exc)[:200]}")


def _main() -> None:
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--since", default=None)
    ap.add_argument("--list", action="store_true", help="print each file + destination (dry-run)")
    args = ap.parse_args()
    from services.hermes.onedrive_client import OneDriveClient
    from services.hermes.outlook_client import OutlookClient
    from services.hermes.onedrive_client import _save_setting
    pg = os.environ.get("PGURL") or os.environ["HERMES_DB_URL"]
    import psycopg2
    with psycopg2.connect(pg) as c, c.cursor() as cur:
        cur.execute("SELECT key, value FROM hermes_settings WHERE key IN ('onedrive_refresh_token','outlook_refresh_token')")
        s = dict(cur.fetchall())
    cid, sec = os.environ["ONEDRIVE_CLIENT_ID"], os.environ.get("ONEDRIVE_CLIENT_SECRET", "")
    # Refresh tokens rotate on use — persist like the service does, or the
    # live hermes process would be left holding a superseded token.
    od = OneDriveClient(client_id=cid, client_secret=sec,
                        refresh_token=s.get("onedrive_refresh_token") or os.environ.get("ONEDRIVE_REFRESH_TOKEN", ""),
                        on_token_rotated=lambda t: _save_setting(pg, "onedrive_refresh_token", t))
    ol = OutlookClient(client_id=cid, client_secret=sec,
                       refresh_token=s.get("outlook_refresh_token") or os.environ.get("OUTLOOK_REFRESH_TOKEN", ""),
                       on_token_rotated=lambda t: _save_setting(pg, "outlook_refresh_token", t))
    res = run(ol, od, pg, dry_run=args.dry_run, since=args.since)
    print(summary_text(res, args.dry_run), flush=True)
    if args.list:
        for d, n in res.saved:
            print(f"  {d} | {n}", flush=True)
        for d, ss in res.senders.items():
            for snd, n in sorted(ss.items(), key=lambda x: -x[1]):
                print(f"SENDER {d} | {n} | {snd}", flush=True)


if __name__ == "__main__":
    _main()
