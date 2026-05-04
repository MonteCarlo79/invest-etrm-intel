"""
Shared tool implementations for the spot-market MCP server and Streamlit agent.

Each function is a pure callable that queries the DB or triggers the ingestion
pipeline.  Return values are always JSON-serialisable (dicts / lists of dicts /
primitives).

Used by:
  - services/spot_mcp/server.py  (MCP stdio server — Claude Desktop integration)
  - apps/spot-market/app.py      (Agent tab — in-app chat interface)
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

_log = logging.getLogger(__name__)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _rows_to_dicts(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _serial(obj: Any) -> Any:
    """JSON-serialise date/datetime and Decimal objects."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")


def _jsonify(rows: list[dict]) -> list[dict]:
    """Convert any date/Decimal values to plain Python types."""
    return json.loads(json.dumps(rows, default=_serial))


# ── Tool: get_spot_prices ─────────────────────────────────────────────────────

def get_spot_prices(
    start_date: str,
    end_date: str,
    provinces: list[str] | None = None,
) -> dict:
    """
    Query public.spot_daily for day-ahead and real-time clearing prices.

    Args:
        start_date: ISO date string, e.g. "2026-01-01"
        end_date:   ISO date string, e.g. "2026-04-30"
        provinces:  Optional list of province_en names to filter by.
                    If omitted, all provinces are returned.

    Returns:
        {"rows": [...], "count": int}
        Each row has: report_date, province_en, province_cn,
                      da_avg, da_max, da_min, rt_avg, rt_max, rt_min
        Price unit: ¥/kWh
    """
    from services.knowledge_pool.db import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            if provinces:
                cur.execute(
                    """
                    SELECT report_date::text, province_en, province_cn,
                           da_avg, da_max, da_min, rt_avg, rt_max, rt_min
                    FROM public.spot_daily
                    WHERE report_date BETWEEN %s AND %s
                      AND province_en = ANY(%s)
                    ORDER BY report_date, province_en
                    LIMIT 5000
                    """,
                    (start_date, end_date, provinces),
                )
            else:
                cur.execute(
                    """
                    SELECT report_date::text, province_en, province_cn,
                           da_avg, da_max, da_min, rt_avg, rt_max, rt_min
                    FROM public.spot_daily
                    WHERE report_date BETWEEN %s AND %s
                    ORDER BY report_date, province_en
                    LIMIT 5000
                    """,
                    (start_date, end_date),
                )
            rows = _jsonify(_rows_to_dicts(cur))
    return {"rows": rows, "count": len(rows)}


# ── Tool: get_interprov_flow ──────────────────────────────────────────────────

def get_interprov_flow(start_date: str, end_date: str) -> dict:
    """
    Query staging.spot_interprov_flow for inter-provincial spot trading data.

    Returns daily peak/floor average prices and volumes for exporting (送端)
    and importing (受端) provinces.

    Args:
        start_date: ISO date string
        end_date:   ISO date string

    Returns:
        {"rows": [...], "count": int}
        Each row: report_date, direction, metric_type, province_cn,
                  province_share (%), price_yuan_kwh, price_chg_pct (%),
                  time_period, total_vol_100gwh (亿kWh)
    """
    from services.knowledge_pool.db import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_date::text, direction, metric_type,
                       province_cn, province_share,
                       price_yuan_kwh, price_chg_pct,
                       time_period, total_vol_100gwh
                FROM staging.spot_interprov_flow
                WHERE report_date BETWEEN %s AND %s
                ORDER BY report_date, direction, metric_type
                LIMIT 5000
                """,
                (start_date, end_date),
            )
            rows = _jsonify(_rows_to_dicts(cur))
    return {"rows": rows, "count": len(rows)}


# ── Tool: get_market_summaries ────────────────────────────────────────────────

def get_market_summaries(start_date: str, end_date: str) -> dict:
    """
    Query staging.spot_report_summaries for AI-generated daily market narratives.

    Each summary is a 2-3 paragraph English text covering price levels, drivers,
    inter-provincial flows, and notable events for that trading day.

    Args:
        start_date: ISO date string
        end_date:   ISO date string

    Returns:
        {"summaries": [...], "count": int}
        Each item: report_date, summary_text, model, source_pdf,
                   prompt_tokens, completion_tokens
    """
    from services.knowledge_pool.db import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_date::text, summary_text, model, source_pdf,
                       prompt_tokens, completion_tokens
                FROM staging.spot_report_summaries
                WHERE report_date BETWEEN %s AND %s
                ORDER BY report_date DESC
                LIMIT 365
                """,
                (start_date, end_date),
            )
            rows = _jsonify(_rows_to_dicts(cur))
    return {"summaries": rows, "count": len(rows)}


# ── Tool: run_pipeline ────────────────────────────────────────────────────────

def run_pipeline(pdf_path: str, dry_run: bool = False) -> dict:
    """
    Run the full spot-market ingestion pipeline for one PDF file.

    Steps performed:
      1. Parse DA/RT prices from the PDF
      2. Cross-check against Excel reference data
      3. Upsert to public.spot_daily (COALESCE — never clobbers existing data)
      4. Sync to Excel (fill blanks only)
      5. Parse 省间现货交易 table → staging.spot_interprov_flow
      6. Generate AI summary → staging.spot_report_summaries
      7. Knowledge-pool ingestion (chunks, facts, Obsidian notes)

    Args:
        pdf_path: Absolute path or repo-relative path to the PDF file.
        dry_run:  If True, parse and cross-check only — no writes to DB or Excel.

    Returns:
        {
          "pdf":           str,
          "dates":         [str, ...],
          "provinces":     int,
          "upserted":      int,
          "discrepancies": [str, ...],
          "errors":        [str, ...]
        }
    """
    watcher_dir = str(_REPO / "apps" / "spot-watcher")
    if watcher_dir not in sys.path:
        sys.path.insert(0, watcher_dir)

    import pipeline as _pipeline_mod  # noqa: PLC0415

    p = Path(pdf_path)
    if not p.is_absolute():
        p = _REPO / pdf_path

    if not p.exists():
        return {"error": f"File not found: {p}", "pdf": str(p.name)}

    result = _pipeline_mod.run(p, dry_run=dry_run)
    # Dates are date objects — convert to strings
    result["dates"] = [d.isoformat() if hasattr(d, "isoformat") else str(d)
                       for d in result.get("dates", [])]
    return result
