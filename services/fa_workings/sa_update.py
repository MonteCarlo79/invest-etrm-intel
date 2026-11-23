"""Mac-side companion to services/hermes/fa_ingest.py (spec §5).

Scans OneDrive `Home/Financial Advisor/` for property documents that are new
since the last run, extracts money items (rent received, agent fees, service
charge, ground rent, insurance, repairs, other costs) with the LOCAL Ollama
model (nothing leaves the Mac), verifies every amount literally appears in the
document text, and appends them to a review sheet `New documents` in the SA
workings workbook of the item's tax year. It never edits the curated sheets:
items stay "to review" until moved by hand. Files it cannot read are listed too.

Config (personal, not in repo): `<root>/_workings_config.json`
    {"scan_dirs": ["UK Properties", "UK insurance"],
     "properties": {"Kinsale": {"owner": "Dipeng", "label": "4 Kinsale Close"}, ...},
     "workbooks": {"2025-26": "Analysis 2026-11/SA_2025-26_workings.xlsx"},
     "default_workbook": "SA_{ty}_workings.xlsx", "earliest_tax_year": "2025-26"}

Run:  python -m services.fa_workings.sa_update [--all] [--limit N] [--dry-run]
State + workbook backups: ~/Library/Application Support/fa-workings/
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests

ROOT = Path.home() / "Library/CloudStorage/OneDrive-Personal/Home/Financial Advisor"
STATE_DIR = Path.home() / "Library/Application Support/fa-workings"
OLLAMA = "http://127.0.0.1:11434"
MODEL = "qwen3:8b"
SHEET = "New documents"
HEADER = ["Added", "Tax year", "Owner", "Property", "Date", "Category", "Income/Cost",
          "Amount £", "Suggested SA105 box", "Description", "Verified in text",
          "Possibly already listed", "Source file", "Status"]
EXTS = {".pdf", ".csv", ".xls", ".xlsx"}
BOX = {"rent": 20, "service_charge": 24, "ground_rent": 24, "insurance": 24,
       "repair": 25, "agent_fee": 27, "other_cost": 29}

PROMPT = """You extract money items from one UK residential-letting document. Reply with JSON only:
{"doc_type": "landlord_statement|invoice|estimate|service_charge_demand|ground_rent_demand|receipt|insurance_schedule|info",
 "items": [{"date": "YYYY-MM-DD", "description": "...", "amount": 123.45,
            "category": "rent|agent_fee|repair|service_charge|ground_rent|insurance|other_cost",
            "direction": "income|cost"}]}
Rules:
- Use only numbers printed in the document. Never compute, guess or round. Amount is a positive number in GBP.
- Landlord/rental statement: one item per rent received (income) and one per deduction (agent fee, repair, etc.). Skip opening/closing balances, subtotals and the net amount paid to the landlord.
- Invoice or demand: one item for the total payable (category by what it is for); date = invoice/demand date.
- Estimates and quotations are not costs: doc_type "estimate", items [].
- Budgets, letters, notices, reminders, certificates, policy wording, terms and conditions: doc_type "info", items [].
- Dates as YYYY-MM-DD. If no date for an item, use the document date.

Document file name: {name}
Document text:
{text}"""


# ── pure helpers (unit-tested) ──────────────────────────────────────────────

def tax_year(d: date) -> str:
    """'2025-26' for 6 Apr 2025 – 5 Apr 2026."""
    y = d.year if (d.month, d.day) >= (4, 6) else d.year - 1
    return f"{y}-{str(y + 1)[2:]}"


def amount_in_text(amount: float, text: str) -> bool:
    """True if the amount is printed in the text (with or without thousands
    separators / pence). Guards against the model inventing numbers."""
    flat = re.sub(r"(?<=\d),(?=\d{3})", "", text)
    whole = f"{amount:.2f}"
    pats = [re.escape(whole)]
    if whole.endswith(".00"):
        pats.append(re.escape(whole[:-3]) + r"(?![.\d])")
    return any(re.search(r"(?<![\d.])" + p, flat) for p in pats)


_DROP = re.compile(r"payment to (the )?landlord|paid to (the )?landlord|amount (paid|due) to landlord|"
                   r"opening balance|closing balance|brought forward|carried forward|sub-?total", re.I)
_RECAT = [(re.compile(r"commission|letting fee|management fee|renewal fee|tenancy renewal|admin fee", re.I), "agent_fee"),
          (re.compile(r"insurance", re.I), "insurance"),
          (re.compile(r"ground rent", re.I), "ground_rent"),
          (re.compile(r"service charge", re.I), "service_charge")]
_ESTIMATE = re.compile(r"(^|[^a-z])(est\d|estimate|quotation|quote)", re.I)


def is_estimate(name: str, doc_type: str = "") -> bool:
    """Quotes/estimates are not costs (Kingfisher folder held many EST*.pdf)."""
    return doc_type == "estimate" or bool(_ESTIMATE.search(name))


def dup_key(r: "Row") -> Optional[tuple]:
    if r.amount is None or not r.item_date:
        return None
    return (r.prop, r.category, round(r.amount, 2), r.item_date)


_ONE_TOTAL = {"invoice", "service_charge_demand", "ground_rent_demand", "receipt"}


def clean_items(doc_type: str, items: list[dict], text: str) -> list[dict]:
    """Deterministic fixes to model output (seen on real documents 2026-11-23):
    drop net-to-landlord / balance lines, recategorise fees by keyword, and
    collapse an invoice's line items (or net + VAT) into its printed total."""
    out = []
    for it in items:
        desc = str(it.get("description") or "")
        if _DROP.search(desc):
            continue
        for rx, cat in _RECAT:
            if rx.search(desc):
                it = {**it, "category": cat}
                break
        out.append(it)
    costs = [it for it in out if it.get("direction") != "income"]
    if doc_type in _ONE_TOTAL and len(costs) > 1:
        try:
            total = round(sum(float(it["amount"]) for it in costs), 2)
        except (TypeError, ValueError, KeyError):
            return out
        if amount_in_text(total, text):
            merged = {**costs[0], "amount": total,
                      "description": "; ".join(str(it.get("description") or "") for it in costs)[:120]}
            out = [it for it in out if it.get("direction") == "income"] + [merged]
    return out


def property_for(path: Path, root: Path, properties: dict, text: str = "",
                 hints: Optional[dict] = None) -> tuple[str, str]:
    """(property key, owner): folder UK Properties/<key>/..., else the first
    config hint (e.g. a policy number) found in the file name or text."""
    parts = path.relative_to(root).parts
    for p in parts:
        if p in properties:
            return p, properties[p]["owner"]
    hay = f"{path.name}\n{text}".lower()
    for needle, key in (hints or {}).items():
        if needle.lower() in hay and key in properties:
            return key, properties[key]["owner"]
    return "", ""


def workbook_for(ty: str, cfg: dict) -> str:
    earliest = cfg.get("earliest_tax_year", "2025-26")
    if ty < earliest:
        ty = earliest
    return cfg.get("workbooks", {}).get(ty) or cfg.get("default_workbook", "SA_{ty}_workings.xlsx").replace("{ty}", ty)


# ── extraction ──────────────────────────────────────────────────────────────

def is_dataless(path: Path) -> bool:
    out = subprocess.run(["ls", "-lO", str(path)], capture_output=True, text=True).stdout
    return "dataless" in out


def file_text(path: Path) -> str:
    suf = path.suffix.lower()
    if suf == ".pdf":
        import pymupdf as fitz
        doc = fitz.open(path)
        pages = [p.get_text() for p in doc]
        if sum(len(t.strip()) for t in pages) < 40:          # scanned → OCR
            from rapidocr_onnxruntime import RapidOCR
            ocr = RapidOCR()
            pages = []
            for p in doc:
                img = p.get_pixmap(dpi=200).tobytes("png")
                res, _ = ocr(img)
                pages.append("\n".join(r[1] for r in (res or [])))
        return "\n".join(pages)
    import pandas as pd
    if suf == ".csv":
        return pd.read_csv(path, dtype=str).to_string()
    return "\n\n".join(f"[{n}]\n{df.to_string()}" for n, df in pd.read_excel(path, sheet_name=None, dtype=str).items())


def extract(name: str, text: str) -> dict:
    import requests
    r = requests.post(f"{OLLAMA}/api/chat", timeout=600, json={
        "model": MODEL, "stream": False, "format": "json", "think": False,
        "options": {"temperature": 0, "num_ctx": 16384},
        "messages": [{"role": "user", "content": PROMPT.replace("{name}", name).replace("{text}", text[:12000])}]})
    r.raise_for_status()
    return json.loads(r.json()["message"]["content"])


# ── workbook ────────────────────────────────────────────────────────────────

def _first_date(v) -> Optional[date]:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    m = re.match(r"(\d{4})-(\d{2})(?:-(\d{2}))?", str(v or ""))
    if not m:
        return None
    return date(int(m[1]), int(m[2]), int(m[3] or 15))


def listed_items(wb, owner: str) -> list[tuple[str, float, Optional[date], str]]:
    """(sheet tax year, amount, date paid, 'sheet!row') from the owner's curated
    sheets, e.g. 'Dipeng 2025-26' (columns: item, date paid, amount)."""
    out = []
    for ws in wb.worksheets:
        m = re.match(re.escape(owner) + r"\s+(\d{4}-\d{2})$", ws.title)
        if not m:
            continue
        for row in ws.iter_rows(min_row=2):
            try:
                amt = round(float(row[2].value), 2)
            except (TypeError, ValueError, IndexError):
                continue
            out.append((m[1], amt, _first_date(row[1].value), f"{ws.title}!{row[0].row}"))
    return out


def already_listed(listed, ty: str, amount: Optional[float], d: Optional[date], days: int = 60) -> str:
    """Same tax year, same amount, paid within ±days (or no usable date) → ref."""
    if amount is None:
        return ""
    for lty, amt, ld, ref in listed:
        if lty == ty and abs(amt - amount) < 0.005 and (ld is None or d is None or abs((ld - d).days) <= days):
            return ref
    return ""


@dataclass
class Row:
    ty: str
    owner: str
    prop: str
    item_date: str
    category: str
    direction: str
    amount: Optional[float]
    description: str
    verified: str
    source: str
    status: str


def write_rows(cfg: dict, rows: list[Row], dry_run: bool) -> list[str]:
    import openpyxl
    from openpyxl.styles import Font
    written = []
    by_wb: dict[str, list[Row]] = {}
    for r in rows:
        by_wb.setdefault(workbook_for(r.ty, cfg), []).append(r)
    for rel, rs in by_wb.items():
        path = ROOT / rel
        if (path.parent / f"~${path.name}").exists():
            raise RuntimeError(f"{path.name} is open in Excel — close it and re-run")
        if path.exists():
            backup = STATE_DIR / "backups" / f"{path.stem}_{datetime.now():%Y%m%d_%H%M%S}{path.suffix}"
            backup.parent.mkdir(parents=True, exist_ok=True)
            if not dry_run:
                shutil.copy2(path, backup)
            wb = openpyxl.load_workbook(path)
        else:
            wb = openpyxl.Workbook()
            wb.active.title = SHEET
        ws = wb[SHEET] if SHEET in wb.sheetnames else wb.create_sheet(SHEET)
        if ws.max_row <= 1 and ws["A1"].value is None:      # fresh sheet: header in row 1
            for j, h in enumerate(HEADER, 1):
                ws.cell(1, j, h).font = Font(bold=True)
        known = {o: listed_items(wb, o) for o in {r.owner for r in rs if r.owner}}
        hdr = [c.value for c in ws[1]]
        seen_keys: dict[tuple, int] = {}
        if "Source file" in hdr:                      # rows already on the sheet
            ci = {h: hdr.index(h) for h in ("Property", "Category", "Amount £", "Date")}
            for row in ws.iter_rows(min_row=2, values_only=True):
                try:
                    k = (row[ci["Property"]], row[ci["Category"]], round(float(row[ci["Amount £"]]), 2), str(row[ci["Date"]])[:10])
                except (TypeError, ValueError):
                    continue
                seen_keys.setdefault(k, 0)
        today = date.today().isoformat()
        for r in rs:
            d = date.fromisoformat(r.item_date) if r.item_date else None
            dup = already_listed(known.get(r.owner, []), r.ty, r.amount, d)
            k = dup_key(r)
            if k and k in seen_keys:
                r.status = "duplicate of an earlier row – ignore"
            elif k:
                seen_keys[k] = ws.max_row + 1
            ws.append([today, r.ty, r.owner, r.prop, r.item_date, r.category, r.direction, r.amount,
                       BOX.get(r.category, "") if r.amount is not None else "", r.description,
                       r.verified, dup or "", r.source, r.status])
        if not dry_run:
            wb.save(path)
        written.append(f"{rel}: +{len(rs)} rows")
    return written


# ── run ─────────────────────────────────────────────────────────────────────

def candidates(cfg: dict) -> list[Path]:
    files = []
    for d in cfg.get("scan_dirs", ["UK Properties", "UK insurance"]):
        for p in sorted((ROOT / d).rglob("*")):
            if p.is_file() and p.suffix.lower() in EXTS and not p.name.startswith(("~$", ".")):
                files.append(p)
    return files


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="ignore state, reprocess everything")
    ap.add_argument("--limit", type=int, default=0, help="process at most N new files")
    ap.add_argument("--dry-run", action="store_true", help="extract and print, write nothing")
    ap.add_argument("--since", default="2024-04-01", help="skip files whose items all predate this")
    args = ap.parse_args()

    import requests
    cfg = json.loads((ROOT / "_workings_config.json").read_text(encoding="utf-8"))
    try:
        requests.get(f"{OLLAMA}/api/version", timeout=5).raise_for_status()
    except requests.RequestException:
        raise SystemExit("Ollama is not running (http://127.0.0.1:11434) — nothing processed")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_path = STATE_DIR / "state.json"
    state = {} if args.all or not state_path.exists() else json.loads(state_path.read_text())
    done = state.setdefault("done", {})

    todo = [p for p in candidates(cfg)
            if done.get(str(p.relative_to(ROOT))) != f"{p.stat().st_size}:{int(p.stat().st_mtime)}"]
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(todo)} new/changed files to read", flush=True)

    rows: list[Row] = []
    newly_done: dict[str, str] = {}
    stats = {"items": 0, "info": 0, "unread": 0, "not_downloaded": 0, "unverified": 0}
    for i, p in enumerate(todo, 1):
        rel = str(p.relative_to(ROOT))
        prop, owner = property_for(p, ROOT, cfg.get("properties", {}))
        key = f"{p.stat().st_size}:{int(p.stat().st_mtime)}"
        if is_dataless(p):
            stats["not_downloaded"] += 1
            continue                     # retried next run; not marked done
        try:
            text = file_text(p)
            if len(text.strip()) < 40:
                raise ValueError("no readable text")
            res = extract(p.name, text)
        except requests.RequestException as exc:   # Ollama down: stop, retry these files next run
            print(f"Ollama unavailable ({type(exc).__name__}) — stopping; remaining files retried next run", flush=True)
            break
        except Exception as exc:
            stats["unread"] += 1
            ty = tax_year(datetime.fromtimestamp(p.stat().st_mtime).date())
            rows.append(Row(ty, owner, prop, "", "", "", None, f"{type(exc).__name__}: {str(exc)[:120]}",
                            "", rel, "COULD NOT READ"))
            newly_done[rel] = key
            continue
        if not prop:
            prop, owner = property_for(p, ROOT, cfg.get("properties", {}), text, cfg.get("hints"))
        items = clean_items(str(res.get("doc_type") or ""), res.get("items") or [], text)
        if res.get("doc_type") in ("info", "estimate") or not items or is_estimate(p.name, str(res.get("doc_type") or "")):
            stats["info"] += 1
            newly_done[rel] = key
            print(f"[{i}/{len(todo)}] info  {rel}", flush=True)
            continue
        for it in items:
            try:
                amt = round(float(it.get("amount")), 2)
                d = date.fromisoformat(str(it.get("date"))[:10])
            except (TypeError, ValueError):
                continue
            if d.isoformat() < args.since:
                continue
            ok = amount_in_text(amt, text)
            stats["items"] += 1
            stats["unverified"] += 0 if ok else 1
            rows.append(Row(tax_year(d), owner, prop, d.isoformat(), str(it.get("category", "")),
                            str(it.get("direction", "")), amt, str(it.get("description", ""))[:120],
                            "yes" if ok else "NO – check", rel,
                            "to review" if prop else "to review – property unknown"))
        newly_done[rel] = key
        print(f"[{i}/{len(todo)}] {len(items)} items  {rel}", flush=True)

    if args.dry_run:
        for r in rows:
            print(f"  {r.ty} {r.owner:6} {r.prop:10} {r.item_date} {r.category:14} {r.amount} "
                  f"{'' if r.verified == 'yes' else '[UNVERIFIED]'} {r.status} | {r.source}")
        print(stats)
        return
    written = write_rows(cfg, rows, dry_run=False) if rows else []
    done.update(newly_done)
    state["last_run"] = datetime.now().isoformat(timespec="seconds")
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=1))
    summary = (f"SA workings: {len(newly_done)} files read, {stats['items']} items "
               f"({stats['unverified']} unverified), {stats['info']} info docs, {stats['unread']} unreadable, "
               f"{stats['not_downloaded']} not downloaded yet. " + "; ".join(written))
    print(summary, flush=True)
    subprocess.run(["osascript", "-e", f'display notification "{summary[:200]}" with title "SA workings"'],
                   capture_output=True)


if __name__ == "__main__":
    main()
