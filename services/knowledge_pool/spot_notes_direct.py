"""
Standalone Obsidian note generator for spot market data.

Does NOT require the staging schema — reads directly from spot_daily
and (optionally) re-extracts summary text from the PDF file.

Usage:
    # Generate notes for a date range
    py -m services.knowledge_pool.spot_notes_direct --start 2026-01-01 --end 2026-04-30

    # Generate notes for a single date
    py -m services.knowledge_pool.spot_notes_direct --date 2026-02-14

    # Generate all dates that have DB data but no / outdated note
    py -m services.knowledge_pool.spot_notes_direct --missing

Output: knowledge/spot_market/01_daily_reports/YYYY-MM-DD.md
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    for _env in [_REPO / "config" / ".env", _REPO / ".env"]:
        if _env.exists():
            load_dotenv(_env)
except ImportError:
    pass

import psycopg2

_VAULT_DAILY = _REPO / "knowledge" / "spot_market" / "01_daily_reports"

# Province groupings for organised note sections
_GRID_GROUPS = {
    "North China (华北)":    ["Shandong", "Shanxi", "Hebei-North", "Hebei-South", "Hebei",
                               "Beijing", "Tianjin"],
    "Northeast (东北)":      ["Liaoning", "Jilin", "Heilongjiang", "Mengdong"],
    "Northwest (西北)":      ["Mengxi", "Gansu", "Ningxia", "Qinghai", "Xinjiang", "Shaanxi"],
    "East China (华东)":     ["Jiangsu", "Shanghai", "Zhejiang", "Anhui", "Fujian"],
    "Central China (华中)":  ["Hubei", "Hunan", "Henan", "Jiangxi", "Chongqing", "Sichuan"],
    "South China (南方)":    ["Guangdong", "Guangxi", "Yunnan", "Guizhou", "Hainan"],
}


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_conn():
    return psycopg2.connect(os.environ["PGURL"])


def fetch_prices(report_date: dt.date) -> List[dict]:
    """Return all spot_daily rows for a date, sorted by province_en."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT province_en, province_cn,
                          da_avg, da_max, da_min,
                          rt_avg, rt_max, rt_min
                   FROM spot_daily WHERE report_date = %s
                   ORDER BY province_en""",
                (report_date,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def dates_with_data(start: dt.date, end: dt.date) -> List[dt.date]:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT report_date FROM spot_daily "
                "WHERE report_date BETWEEN %s AND %s ORDER BY 1",
                (start, end),
            )
            return [r[0] for r in cur.fetchall()]


# ── PDF text extraction ───────────────────────────────────────────────────────

def _find_pdf_for_date(report_date: dt.date) -> Optional[Path]:
    """
    Heuristic: scan data/spot reports/<year>/*.pdf and find the PDF whose
    filename date range covers report_date.
    """
    year = report_date.year
    base = _REPO / "data" / "spot reports" / str(year)
    if not base.exists():
        return None
    for p in sorted(base.glob("*.pdf")):
        m = re.search(r"\(([^)）]+)\)", p.stem)
        if not m:
            continue
        # Parse range from e.g. "2.14" or "2.7-2.9"
        rng = m.group(1).strip().rstrip("）) ")
        mr = re.search(r"(\d{1,2})\.(\d{1,2})(?:-(\d{1,2})\.(\d{1,2}))?", rng)
        if not mr:
            continue
        try:
            start = dt.date(year, int(mr.group(1)), int(mr.group(2)))
            end = dt.date(year, int(mr.group(3) or mr.group(1)),
                          int(mr.group(4) or mr.group(2)))
            if start <= report_date <= end:
                return p
        except ValueError:
            continue
    return None


def _extract_summary_sentences(pdf_path: Path, report_date: dt.date,
                                max_chars: int = 2000) -> str:
    """
    Pull the narrative summary paragraph for report_date from the PDF.
    Looks for the paragraph that mentions the date and price statistics.
    Returns raw text (≤ max_chars).
    """
    try:
        import pdfplumber
    except ImportError:
        return "_pdfplumber not installed — pip install pdfplumber_"

    month = report_date.month
    day   = report_date.day
    date_pattern = re.compile(rf"{month}\s*月\s*{day}\s*日")

    collected: List[str] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if date_pattern.search(text):
                # Keep paragraphs that mention the date and contain price-like numbers
                for para in text.split("\n"):
                    if date_pattern.search(para) and re.search(r"\d+\.\d+\s*元", para):
                        collected.append(para.strip())
                        if sum(len(s) for s in collected) >= max_chars:
                            break
            if sum(len(s) for s in collected) >= max_chars:
                break

    return "\n\n".join(collected) if collected else "_No summary paragraphs found in PDF._"


# ── Note formatting ───────────────────────────────────────────────────────────

def _fmt(val, decimals: int = 4) -> str:
    if val is None:
        return "—"
    return f"{float(val):.{decimals}f}"


def _price_table(rows: List[dict], market: str) -> str:
    """Build a markdown table for DA or RT prices."""
    avg_col = f"{market}_avg"
    max_col = f"{market}_max"
    min_col = f"{market}_min"

    header = "| Province | CN | Avg | Max | Min |"
    sep    = "|---|---|---:|---:|---:|"
    lines  = [header, sep]

    for r in rows:
        if r[avg_col] is None and r[max_col] is None:
            continue
        lines.append(
            f"| {r['province_en']} | {r['province_cn']} "
            f"| {_fmt(r[avg_col])} | {_fmt(r[max_col])} | {_fmt(r[min_col])} |"
        )

    if len(lines) == 2:
        return "_No data._"
    return "\n".join(lines)


def _stats_summary(rows: List[dict], market: str) -> str:
    avg_col = f"{market}_avg"
    vals = [(r["province_en"], r[avg_col]) for r in rows if r[avg_col] is not None]
    if not vals:
        return "_No data._"

    vals.sort(key=lambda x: x[1])
    lowest  = vals[0]
    highest = vals[-1]
    mean    = sum(v for _, v in vals) / len(vals)

    label = "DA" if market == "da" else "RT"
    return (
        f"- **Highest {label}**: {highest[0]} @ {_fmt(highest[1])} ¥/kWh\n"
        f"- **Lowest {label}**: {lowest[0]} @ {_fmt(lowest[1])} ¥/kWh\n"
        f"- **National avg**: {mean:.4f} ¥/kWh  ({len(vals)} provinces)"
    )


def _spread_notes(rows: List[dict]) -> str:
    spreads = []
    for r in rows:
        if r["da_avg"] is not None and r["rt_avg"] is not None:
            spreads.append((r["province_en"], r["da_avg"] - r["rt_avg"]))
    if not spreads:
        return "_Not enough DA+RT pairs to compute spread._"
    spreads.sort(key=lambda x: abs(x[1]), reverse=True)
    lines = ["| Province | DA−RT Spread (¥/kWh) |", "|---|---:|"]
    for prov, spd in spreads[:10]:
        sign = "+" if spd >= 0 else ""
        lines.append(f"| {prov} | {sign}{spd:.4f} |")
    return "\n".join(lines)


def generate_note(report_date: dt.date, include_pdf_text: bool = True) -> str:
    """Generate a complete Obsidian markdown note for report_date."""
    rows = fetch_prices(report_date)

    if not rows:
        return (
            f"---\ntitle: Spot Market {report_date}\ndate: {report_date}\n"
            f"note_type: daily_report\ntags:\n  - spot-market\n  - daily-report\n---\n\n"
            f"# Spot Market — {report_date}\n\n_No price data in DB for this date._\n"
        )

    da_count = sum(1 for r in rows if r["da_avg"] is not None)
    rt_count = sum(1 for r in rows if r["rt_avg"] is not None)
    provinces_list = sorted({r["province_cn"] for r in rows})

    # Try to find source PDF
    pdf_path = _find_pdf_for_date(report_date)
    pdf_ref  = pdf_path.name if pdf_path else "unknown"

    # ── Frontmatter ───────────────────────────────────────────────────────────
    fm_provinces = "\n".join(f"  - {p}" for p in provinces_list)
    frontmatter = f"""---
title: Spot Market Daily Report {report_date}
date: {report_date}
source_file: {pdf_ref}
note_type: daily_report
provinces:
{fm_provinces}
tags:
  - spot-market
  - daily-report
  - {report_date.year}
---"""

    # ── Body ──────────────────────────────────────────────────────────────────
    body_lines: List[str] = [
        f"# Spot Market — {report_date}",
        "",
        "## Data Coverage",
        f"- **DA provinces**: {da_count}",
        f"- **RT provinces**: {rt_count}",
        f"- **Source PDF**: `{pdf_ref}`",
        "",
        "## Key Statistics",
        "",
        "**Day-Ahead (日前)**",
        _stats_summary(rows, "da"),
        "",
        "**Real-Time (实时)**",
        _stats_summary(rows, "rt"),
        "",
        "## DA−RT Spread (Top 10 by magnitude)",
        "",
        _spread_notes(rows),
        "",
        "## Day-Ahead (日前) Prices — ¥/kWh",
        "",
        _price_table(rows, "da"),
        "",
        "## Real-Time (实时) Prices — ¥/kWh",
        "",
        _price_table(rows, "rt"),
    ]

    # ── Regional breakdown ────────────────────────────────────────────────────
    body_lines += ["", "## Regional Breakdown", ""]
    for group_name, group_provs in _GRID_GROUPS.items():
        group_rows = [r for r in rows if r["province_en"] in group_provs]
        if not group_rows:
            continue
        body_lines.append(f"### {group_name}")
        body_lines.append("")
        body_lines.append("| Province | DA avg | RT avg |")
        body_lines.append("|---|---:|---:|")
        for r in group_rows:
            body_lines.append(
                f"| {r['province_en']} ({r['province_cn']}) "
                f"| {_fmt(r['da_avg'])} | {_fmt(r['rt_avg'])} |"
            )
        body_lines.append("")

    # ── PDF narrative summary ─────────────────────────────────────────────────
    if include_pdf_text and pdf_path:
        body_lines += [
            "## Market Summary (from PDF)",
            "",
            "_Extracted from source PDF — verbatim, not interpreted._",
            "",
            _extract_summary_sentences(pdf_path, report_date),
        ]

    body_lines += [
        "",
        "## Analyst Notes",
        "",
        "_Fill in market commentary, unusual observations, or links to related notes._",
        "",
        "## Links",
        "",
    ]
    # Auto-link prev/next day
    prev_d = report_date - dt.timedelta(days=1)
    next_d = report_date + dt.timedelta(days=1)
    body_lines.append(f"- [[{prev_d}]] ← previous")
    body_lines.append(f"- [[{next_d}]] → next")

    return frontmatter + "\n\n" + "\n".join(body_lines) + "\n"


def write_note(report_date: dt.date, include_pdf_text: bool = True,
               force: bool = False) -> Path:
    """Write note to vault. Returns the path written."""
    out_path = _VAULT_DAILY / f"{report_date}.md"
    if out_path.exists() and not force:
        # Check if it's a stub (old empty note)
        content = out_path.read_text(encoding="utf-8", errors="replace")
        if "Price data not available" not in content and "No price data" not in content:
            return out_path  # already has real data, skip

    note = generate_note(report_date, include_pdf_text=include_pdf_text)
    _VAULT_DAILY.mkdir(parents=True, exist_ok=True)
    out_path.write_text(note, encoding="utf-8")
    return out_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def _main():
    parser = argparse.ArgumentParser(
        description="Generate Obsidian notes from spot_daily DB data"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date",    help="Single date YYYY-MM-DD")
    group.add_argument("--start",   help="Start date YYYY-MM-DD (use with --end)")
    group.add_argument("--missing", action="store_true",
                       help="Generate/refresh all dates that have DB data")

    parser.add_argument("--end",    help="End date YYYY-MM-DD", default=None)
    parser.add_argument("--no-pdf", action="store_true",
                        help="Skip PDF text extraction (faster)")
    parser.add_argument("--force",  action="store_true",
                        help="Overwrite existing non-stub notes")
    args = parser.parse_args()

    include_pdf = not args.no_pdf

    if args.date:
        dates = [dt.date.fromisoformat(args.date)]
    elif args.start:
        start = dt.date.fromisoformat(args.start)
        end   = dt.date.fromisoformat(args.end) if args.end else dt.date.today()
        dates = dates_with_data(start, end)
    else:  # --missing
        dates = dates_with_data(dt.date(2024, 1, 1), dt.date.today())

    print(f"Generating {len(dates)} notes → {_VAULT_DAILY}")
    for d in dates:
        path = write_note(d, include_pdf_text=include_pdf, force=args.force)
        print(f"  {d} → {path.name}")

    print("Done.")


if __name__ == "__main__":
    _main()
