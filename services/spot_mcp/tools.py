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


# ── Tool: ingest_kb_document ──────────────────────────────────────────────────

def ingest_kb_document(
    s3_key: str | None = None,
    file_path: str | None = None,
    category: str | None = None,
    app: str = "shared",
) -> dict:
    """
    Ingest a reference document (Excel, PDF, PPTX, DOCX, TXT, image, …) into the
    knowledge base so it can be searched via search_reference_docs.

    Provide exactly one of:
      s3_key   — object key in the uploads S3 bucket, e.g. "uploads/report.xlsx"
      file_path — repo-relative or absolute local path, e.g. "data/market-fundamentals/report.xlsx"

    Supports all file types: pdf, xlsx, xls, pptx, ppt, docx, doc, txt, png, jpg, jpeg, webp.
    If the file was already ingested (same SHA-256 hash), returns the existing doc_id without
    re-processing.

    Args:
        s3_key:    S3 object key (relative to the uploads bucket root).
        file_path: Local path (absolute, or relative to the repo root).
        category:  Optional manual category override:
                   market_rules | annual_report | policy_doc | technical_spec |
                   research_report | other.  Omit to auto-detect.
        app:       Document scope — 'shared' (all agents) or 'strategist'.  Default 'shared'.

    Returns:
        {
          "doc_id":   int,
          "is_new":   bool,   # False if already existed in KB
          "category": str,
          "filename": str,
          "status":   "ingested" | "duplicate" | "error",
          "message":  str
        }
    """
    import os as _os

    if not s3_key and not file_path:
        return {"status": "error", "message": "Provide either s3_key or file_path."}

    # ── Resolve file bytes ──────────────────────────────────────────────────────
    if s3_key:
        try:
            import boto3 as _boto3
            bucket = _os.environ.get("UPLOADS_BUCKET", "")
            if not bucket:
                return {"status": "error", "message": "UPLOADS_BUCKET env var not set."}
            _s3 = _boto3.client("s3", region_name=_os.environ.get("AWS_REGION", "ap-southeast-1"))
            obj = _s3.get_object(Bucket=bucket, Key=s3_key)
            file_bytes = obj["Body"].read()
            filename = s3_key.split("/")[-1]
        except Exception as exc:
            return {"status": "error", "message": f"S3 download failed: {exc}"}
    else:
        fp = Path(file_path)
        if not fp.is_absolute():
            fp = _REPO / file_path
        if not fp.exists():
            return {"status": "error", "message": f"File not found: {fp}"}
        try:
            file_bytes = fp.read_bytes()
        except Exception as exc:
            return {"status": "error", "message": f"File read failed: {exc}"}
        filename = fp.name

    # ── Ingest ─────────────────────────────────────────────────────────────────
    try:
        from services.knowledge_pool.knowledge_docs import register_and_ingest as _rai
        api_key = _os.environ.get("ANTHROPIC_API_KEY", "")
        doc_id, is_new, detected_category = _rai(
            file_bytes=file_bytes,
            filename=filename,
            category_override=category,
            app=app,
            api_key=api_key,
            synthesize=bool(api_key),
        )
        status = "ingested" if is_new else "duplicate"
        msg = (
            f"Document ingested successfully (doc_id={doc_id}, category={detected_category})."
            if is_new
            else f"Document already in knowledge base (doc_id={doc_id}). No re-processing needed."
        )
        return {
            "doc_id":   doc_id,
            "is_new":   is_new,
            "category": detected_category,
            "filename": filename,
            "status":   status,
            "message":  msg,
        }
    except Exception as exc:
        _log.error("ingest_kb_document failed: %s", exc, exc_info=True)
        return {"status": "error", "message": str(exc)}


# ── Tool: get_market_fundamentals ─────────────────────────────────────────────

def get_market_fundamentals(
    provinces: list[str] | None = None,
    year: int = 2025,
) -> dict:
    """
    Return market fundamentals (installed capacity, generation mix, peak load)
    for Chinese electricity provinces, parsed from the Excel reference file.

    Data covers:
      - Installed capacity by fuel type (万kW): Wind, Solar, Thermal, Hydro, Nuclear, Storage
      - Generation by fuel type (亿kWh)
      - Peak load by season (summer 6–9 月 / winter 12–2 月) in MW

    Args:
        provinces: Optional list of province_en names (e.g. ['Shandong', 'Guangdong']).
                   If omitted, all provinces are returned.
        year:      2024 or 2025 (defaults to 2025).

    Returns:
        {
          "year": int,
          "provinces": [
            {
              "province_cn": str,
              "province_en": str,
              "capacity_10kw":     {"Wind": float, "Solar": float, ...},
              "capacity_share":    {"Wind": float, ...},   # 0–1
              "generation_100gwh": {"Wind": float, ...},
              "generation_share":  {"Wind": float, ...},
              "peak_summer_mw":    float | None,
              "peak_winter_mw":    float | None,
            },
            ...
          ]
        }
    """
    from services.market_fundamentals.loader import get_fundamentals_summary as _gfs
    return _gfs(provinces=provinces, year=year)


# ── Tool: search_reference_docs ───────────────────────────────────────────────

def search_reference_docs(
    query: str,
    category: str | None = None,
    app: str | None = "shared",
    limit: int = 5,
) -> dict:
    """
    Full-text search over the knowledge base (staging.spot_knowledge_chunks).

    Searches all reference documents ingested into the spot-market knowledge
    pool — PDFs, Excel files, policy documents, annual reports, research
    papers, etc.

    Both English/Latin and Chinese (CJK) queries are supported.
    For CJK queries, bigram ILIKE search is used.
    For Latin queries, PostgreSQL FTS with 'simple' config is used.

    Args:
        query:    Free-text search query (English or Chinese).
        category: Optional filter — one of: market_rules | annual_report |
                  policy_doc | technical_spec | research_report | other.
                  Omit to search all categories.
        app:      Scope filter — 'shared' returns documents visible to all
                  agents; 'strategist' returns strategist-private docs plus
                  shared docs.  Defaults to 'shared'.
        limit:    Maximum number of chunk results to return (default 5, max 20).

    Returns:
        {
          "results": [
            {
              "doc_id":     int,
              "file_name":  str,
              "category":   str,
              "app":        str,
              "page_no":    int | None,
              "chunk_text": str,
              "rank":       float
            },
            ...
          ],
          "count": int
        }
    """
    try:
        from services.knowledge_pool.knowledge_docs import search_reference_docs as _srd
        rows = _srd(query=query, category=category, app=app, limit=min(limit, 20))
        return {"results": rows, "count": len(rows)}
    except Exception as exc:
        _log.error("search_reference_docs failed: %s", exc, exc_info=True)
        return {"results": [], "count": 0, "error": str(exc)}
