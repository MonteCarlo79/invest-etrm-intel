"""
Obsidian-compatible markdown note generation.

Three note types:
  daily_report  — one note per report date
  province      — one rolling note per province (accumulates cross-report)
  concept       — one note per recurring driver theme

Notes are written to knowledge/spot_market/{01_daily_reports|02_provinces|03_concepts}/
All notes have YAML frontmatter and explicitly separate source vs interpretation sections.
"""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import List, Optional

from .db import get_conn

# Repo root is 3 levels up from this file
_REPO_ROOT = Path(__file__).resolve().parents[2]
_VAULT_ROOT = _REPO_ROOT / "knowledge" / "spot_market"


# ── Common helpers ─────────────────────────────────────────────────────────────

def _frontmatter(**kwargs) -> str:
    lines = ["---"]
    for k, v in kwargs.items():
        if v is None:
            continue
        if isinstance(v, list):
            lines.append(f"{k}:")
            for item in v:
                lines.append(f"  - {item}")
        else:
            lines.append(f"{k}: {v}")
    lines.append("---")
    return "\n".join(lines)


def _register_note(
    note_type: str,
    note_key: str,
    note_path: Path,
    note_title: str,
    document_id: Optional[int],
    date_min: Optional[dt.date],
    date_max: Optional[dt.date],
):
    rel_path = str(note_path.relative_to(_REPO_ROOT)).replace("\\", "/")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging.spot_report_notes
                    (document_id, note_type, note_key, note_path, note_title,
                     report_date_min, report_date_max)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (note_type, note_key) DO UPDATE SET
                    note_path       = EXCLUDED.note_path,
                    note_title      = EXCLUDED.note_title,
                    report_date_min = LEAST(staging.spot_report_notes.report_date_min, EXCLUDED.report_date_min),
                    report_date_max = GREATEST(staging.spot_report_notes.report_date_max, EXCLUDED.report_date_max),
                    updated_at      = now()
                """,
                (document_id, note_type, note_key, rel_path, note_title,
                 date_min, date_max),
            )
        conn.commit()


# ── Daily report note ─────────────────────────────────────────────────────────

def generate_daily_report_note(
    report_date: dt.date,
    document_id: int,
    source_path: str,
) -> Path:
    """
    Generate knowledge/spot_market/01_daily_reports/YYYY-MM-DD.md
    """
    note_path = _VAULT_ROOT / "01_daily_reports" / f"{report_date}.md"

    # Fetch price facts from DB
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT province_cn, province_en, fact_type, metric_name, metric_value
                FROM staging.spot_report_facts
                WHERE document_id = %s AND report_date = %s
                  AND fact_type IN ('price_da', 'price_rt')
                ORDER BY province_cn, fact_type, metric_name
                """,
                (document_id, report_date),
            )
            price_rows = cur.fetchall()

            cur.execute(
                """
                SELECT province_cn, fact_text
                FROM staging.spot_report_facts
                WHERE document_id = %s AND report_date = %s
                  AND fact_type = 'driver'
                ORDER BY province_cn
                """,
                (document_id, report_date),
            )
            driver_rows = cur.fetchall()

    # Build price table
    prices: dict[str, dict] = {}
    for pcn, pen, ftype, metric, val in price_rows:
        key = pcn or pen or "Unknown"
        prices.setdefault(key, {})[f"{ftype}_{metric}"] = val

    price_lines = []
    if prices:
        price_lines.append("| Province | DA Avg | DA Max | DA Min | RT Avg | RT Max | RT Min |")
        price_lines.append("|----------|--------|--------|--------|--------|--------|--------|")
        for prov, vals in sorted(prices.items()):
            def _v(k):
                v = vals.get(k)
                return f"{v:.1f}" if v is not None else "—"
            price_lines.append(
                f"| {prov} | {_v('price_da_da_avg')} | {_v('price_da_da_max')} | "
                f"{_v('price_da_da_min')} | {_v('price_rt_rt_avg')} | "
                f"{_v('price_rt_rt_max')} | {_v('price_rt_rt_min')} |"
            )

    # Build drivers section
    driver_lines = []
    seen_provs = set()
    for pcn, fact_text in driver_rows:
        if pcn and pcn not in seen_provs:
            driver_lines.append(f"- **{pcn}**: {fact_text}")
            seen_provs.add(pcn)

    # Province links
    province_links = sorted(set(p for p, _, _, _, _ in price_rows if p))
    prov_link_lines = [f"- [[{p}]]" for p in province_links]

    # Assemble note
    fm = _frontmatter(
        title=f"Spot Market Daily Report {report_date}",
        date=str(report_date),
        source_path=source_path,
        document_id=document_id,
        note_type="daily_report",
        provinces=province_links,
        tags=["spot-market", "daily-report", str(report_date.year)],
    )

    sections = [
        fm,
        "",
        f"# Spot Market Daily Report — {report_date}",
        "",
        f"> Source: `{source_path}`",
        "",
        "## Source-backed Summary",
        "",
    ]

    if price_lines:
        sections += price_lines
    else:
        sections.append("_Price data not extracted for this date._")

    sections += [
        "",
        "## Market Drivers (Source-backed)",
        "",
    ]
    if driver_lines:
        sections += driver_lines
    else:
        sections.append("_No driver sentences extracted._")

    sections += [
        "",
        "## Province Notes",
        "",
    ]
    sections += prov_link_lines if prov_link_lines else ["_No province links._"]

    sections += [
        "",
        "## Analyst / LLM Interpretation",
        "",
        "_Not yet generated._",
        "",
        "## Open Questions",
        "",
        "_None recorded._",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")

    _register_note(
        "daily_report", str(report_date), note_path,
        f"Spot Market {report_date}", document_id,
        report_date, report_date,
    )

    return note_path


# ── Province rolling note ─────────────────────────────────────────────────────

def generate_province_note(province_cn: str, province_en: str) -> Path:
    """
    Generate/update knowledge/spot_market/02_provinces/{province_cn}.md
    Accumulates all report dates where this province appears.
    """
    note_path = _VAULT_ROOT / "02_provinces" / f"{province_cn}.md"

    with get_conn() as conn:
        with conn.cursor() as cur:
            # All dates with price data
            cur.execute(
                """
                SELECT DISTINCT report_date
                FROM staging.spot_report_facts
                WHERE province_cn = %s AND fact_type IN ('price_da','price_rt')
                ORDER BY report_date DESC
                LIMIT 60
                """,
                (province_cn,),
            )
            dates = [r[0] for r in cur.fetchall()]

            # Recent DA/RT avg by date
            cur.execute(
                """
                SELECT report_date,
                       MAX(CASE WHEN metric_name='da_avg' THEN metric_value END) AS da_avg,
                       MAX(CASE WHEN metric_name='rt_avg' THEN metric_value END) AS rt_avg
                FROM staging.spot_report_facts
                WHERE province_cn = %s AND fact_type IN ('price_da','price_rt')
                GROUP BY report_date
                ORDER BY report_date DESC
                LIMIT 30
                """,
                (province_cn,),
            )
            price_series = cur.fetchall()

            # Recent drivers
            cur.execute(
                """
                SELECT DISTINCT ON (report_date) report_date, fact_text
                FROM staging.spot_report_facts
                WHERE province_cn = %s AND fact_type = 'driver'
                ORDER BY report_date DESC, id
                LIMIT 20
                """,
                (province_cn,),
            )
            drivers = cur.fetchall()

    date_min = min(dates) if dates else None
    date_max = max(dates) if dates else None

    # Build price series table
    ps_lines = []
    if price_series:
        ps_lines.append("| Date | DA Avg | RT Avg |")
        ps_lines.append("|------|--------|--------|")
        for rd, da, rt in price_series:
            ps_lines.append(
                f"| {rd} | "
                f"{'%.1f' % da if da else '—'} | "
                f"{'%.1f' % rt if rt else '—'} |"
            )

    driver_lines = [f"- **{rd}**: {ft}" for rd, ft in drivers]

    # Report date links
    report_links = [f"- [[{d}]]" for d in sorted(dates, reverse=True)[:20]]

    fm = _frontmatter(
        title=f"Province: {province_cn} ({province_en})",
        province_cn=province_cn,
        province_en=province_en,
        note_type="province",
        date_range=f"{date_min} → {date_max}" if date_min else "unknown",
        report_count=len(dates),
        tags=["spot-market", "province", province_cn, province_en],
    )

    sections = [
        fm,
        "",
        f"# {province_cn} ({province_en}) — Province Knowledge Note",
        "",
        "## Scope",
        f"Covers all spot market daily reports mentioning **{province_cn}**.",
        f"Period: {date_min} → {date_max}. Reports with data: {len(dates)}.",
        "",
        "## Recent DA/RT Average Prices (source-backed)",
        "",
    ]
    sections += ps_lines if ps_lines else ["_No price data yet._"]
    sections += [
        "",
        "## Recent Market Drivers (source-backed)",
        "",
    ]
    sections += driver_lines if driver_lines else ["_No driver sentences extracted._"]
    sections += [
        "",
        "## Linked Daily Reports",
        "",
    ]
    sections += report_links if report_links else ["_No reports linked._"]
    sections += [
        "",
        "## Recurring Patterns",
        "",
        "_Accumulate observations here over time._",
        "",
        "## Open Questions / Unresolved",
        "",
        "_None recorded._",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")

    _register_note(
        "province", province_cn, note_path,
        f"Province: {province_cn}", None, date_min, date_max,
    )

    return note_path


# ── Concept note ──────────────────────────────────────────────────────────────

# Concept keywords and their display names
CONCEPT_PATTERNS = {
    "新能源出力下降": "新能源出力下降 (New Energy Output Decline)",
    "负荷增加": "负荷增加 (Load Increase)",
    "省间现货交易": "省间现货交易 (Interprovincial Spot Trading)",
    "实时价格波动": "实时价格波动 (Real-time Price Volatility)",
    "检修": "机组检修 (Unit Maintenance)",
    "水电": "水电出力 (Hydro Output)",
    "新能源消纳": "新能源消纳 (New Energy Absorption)",
}


def generate_concept_note(concept_key: str) -> Path:
    """
    Generate/update knowledge/spot_market/03_concepts/{concept_key}.md
    Searches driver facts for mentions of the concept keyword.
    """
    display_name = CONCEPT_PATTERNS.get(concept_key, concept_key)
    note_path = _VAULT_ROOT / "03_concepts" / f"{concept_key}.md"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_date, province_cn, fact_text
                FROM staging.spot_report_facts
                WHERE fact_type = 'driver'
                  AND fact_text ILIKE %s
                ORDER BY report_date DESC
                LIMIT 50
                """,
                (f"%{concept_key}%",),
            )
            evidence = cur.fetchall()

    date_min = min(r[0] for r in evidence) if evidence else None
    date_max = max(r[0] for r in evidence) if evidence else None

    evidence_lines = [
        f"- **{rd}** ({pcn or 'National'}): {ft[:200]}"
        for rd, pcn, ft in evidence[:30]
    ]

    fm = _frontmatter(
        title=display_name,
        concept=concept_key,
        note_type="concept",
        evidence_count=len(evidence),
        date_range=f"{date_min} → {date_max}" if date_min else "unknown",
        tags=["spot-market", "concept", concept_key],
    )

    sections = [
        fm,
        "",
        f"# Concept: {display_name}",
        "",
        "## Description",
        "",
        f"_Cross-report concept note for occurrences of **{concept_key}** in daily spot reports._",
        "",
        "## Evidence (Source-backed)",
        "",
        f"Found in {len(evidence)} fact records.",
        "",
    ]
    sections += evidence_lines if evidence_lines else ["_No evidence yet._"]
    sections += [
        "",
        "## Pattern Analysis",
        "",
        "_Accumulate patterns across reports here._",
        "",
        "## Related Concepts",
        "",
        "_Add [[links]] to related concept notes._",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")

    _register_note(
        "concept", concept_key, note_path,
        display_name, None, date_min, date_max,
    )

    return note_path


# ── Index note ────────────────────────────────────────────────────────────────

def generate_index_note() -> Path:
    """Generate knowledge/spot_market/04_indices/index.md — master index."""
    note_path = _VAULT_ROOT / "04_indices" / "index.md"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), MIN(report_date_min), MAX(report_date_max) "
                "FROM staging.spot_report_notes WHERE note_type = 'daily_report'"
            )
            daily_count, dmin, dmax = cur.fetchone()

            cur.execute(
                "SELECT note_key FROM staging.spot_report_notes "
                "WHERE note_type = 'province' ORDER BY note_key"
            )
            provinces = [r[0] for r in cur.fetchall()]

            cur.execute(
                "SELECT note_key FROM staging.spot_report_notes "
                "WHERE note_type = 'concept' ORDER BY note_key"
            )
            concepts = [r[0] for r in cur.fetchall()]

            cur.execute(
                "SELECT COUNT(*), COUNT(DISTINCT file_hash) "
                "FROM staging.spot_report_documents WHERE ingest_status = 'parsed'"
            )
            doc_row = cur.fetchone()
            doc_count = doc_row[0] if doc_row else 0

    sections = [
        "---",
        "title: Spot Market Knowledge Pool — Index",
        "note_type: index",
        f"generated_at: {dt.date.today()}",
        "tags:",
        "  - spot-market",
        "  - index",
        "---",
        "",
        "# Spot Market Knowledge Pool",
        "",
        "## Statistics",
        "",
        f"- **PDFs ingested**: {doc_count}",
        f"- **Daily report notes**: {daily_count or 0}",
        f"- **Date range**: {dmin} → {dmax}" if dmin else "- **Date range**: not yet available",
        f"- **Province notes**: {len(provinces)}",
        f"- **Concept notes**: {len(concepts)}",
        "",
        "## Province Notes",
        "",
    ]
    sections += [f"- [[{p}]]" for p in provinces] or ["_None yet._"]
    sections += [
        "",
        "## Concept Notes",
        "",
    ]
    sections += [f"- [[{c}]]" for c in concepts] or ["_None yet._"]
    sections += [
        "",
        "## Daily Reports",
        "",
        f"See [[01_daily_reports/]] for all {daily_count or 0} daily notes.",
    ]

    note_path.parent.mkdir(parents=True, exist_ok=True)
    note_path.write_text("\n".join(sections), encoding="utf-8")
    return note_path
