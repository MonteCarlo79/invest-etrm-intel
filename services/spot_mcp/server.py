"""
Spot Market MCP Server
======================
Exposes 4 tools over the Model Context Protocol (MCP) stdio transport so that
Claude Desktop (or any MCP-compatible client) can query spot market data and
trigger ingestion.

Usage
-----
Run directly:
    python services/spot_mcp/server.py

Claude Desktop config (add to claude_desktop_config.json):
    {
      "mcpServers": {
        "spot-market": {
          "command": "python",
          "args": ["C:/path/to/bess-platform/services/spot_mcp/server.py"],
          "env": {
            "PGURL": "postgresql://user:pass@host:5432/db?sslmode=require"
          }
        }
      }
    }

Requirements:
    pip install mcp>=1.0
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

# Load .env so the server can find PGURL when launched by Claude Desktop
try:
    from dotenv import load_dotenv
    for _env in [
        _REPO / "config" / ".env",
        _REPO / ".env",
    ]:
        if _env.exists():
            load_dotenv(_env)
except ImportError:
    pass

from mcp.server.fastmcp import FastMCP  # pip install mcp

from services.spot_mcp.tools import (
    get_spot_prices,
    get_interprov_flow,
    get_market_summaries,
    run_pipeline,
    ingest_kb_document,
    get_market_fundamentals,
    search_reference_docs,
)

mcp = FastMCP(
    "spot-market",
    instructions=(
        "You have access to China's spot electricity market data. "
        "Prices are in ¥/kWh. Provinces use English names "
        "(e.g. Shandong, Guangdong, Mengxi). "
        "Use spot_get_prices for DA/RT clearing prices, "
        "spot_get_interprov_flow for 省间现货交易 data, "
        "spot_get_market_summaries for AI narrative summaries, "
        "spot_run_pipeline to ingest a new PDF report, "
        "spot_get_market_fundamentals for installed capacity / generation mix / peak load, "
        "spot_search_reference_docs to search uploaded reference documents (policy, annual reports, Excel data), "
        "and spot_ingest_kb_document to add a new reference document to the knowledge base."
    ),
)


@mcp.tool()
def spot_get_prices(
    start_date: str,
    end_date: str,
    provinces: list[str] | None = None,
) -> dict:
    """
    Fetch day-ahead (DA) and real-time (RT) spot electricity clearing prices
    from public.spot_daily.

    Args:
        start_date: Start of date range, ISO format e.g. "2026-01-01"
        end_date:   End of date range, ISO format e.g. "2026-04-30"
        provinces:  Optional list of province English names to filter by,
                    e.g. ["Shandong", "Guangdong"]. Omit for all provinces.

    Returns:
        {"rows": [...], "count": int}
        Price unit: ¥/kWh
    """
    return get_spot_prices(start_date, end_date, provinces)


@mcp.tool()
def spot_get_interprov_flow(start_date: str, end_date: str) -> dict:
    """
    Fetch inter-provincial spot trading data (省间现货交易情况) from
    staging.spot_interprov_flow.

    Includes daily peak/floor average prices and volumes for exporting (送端)
    and importing (受端) provinces.

    Args:
        start_date: Start of date range, ISO format
        end_date:   End of date range, ISO format

    Returns:
        {"rows": [...], "count": int}
        price_yuan_kwh unit: ¥/kWh; total_vol_100gwh unit: 亿kWh
    """
    return get_interprov_flow(start_date, end_date)


@mcp.tool()
def spot_get_market_summaries(start_date: str, end_date: str) -> dict:
    """
    Fetch AI-generated daily market narrative summaries from
    staging.spot_report_summaries.

    Each summary is a 2-3 paragraph English narrative covering price levels,
    key drivers, inter-provincial flows, and notable market events.

    Args:
        start_date: Start of date range, ISO format
        end_date:   End of date range, ISO format

    Returns:
        {"summaries": [...], "count": int}
    """
    return get_market_summaries(start_date, end_date)


@mcp.tool()
def spot_run_pipeline(pdf_path: str, dry_run: bool = False) -> dict:
    """
    Run the spot market ingestion pipeline for a single PDF report file.

    This parses the PDF, cross-checks against Excel, upserts prices to
    public.spot_daily, parses inter-provincial data, generates an AI summary,
    and populates the knowledge pool.

    Args:
        pdf_path: Absolute path or repo-relative path to the PDF file.
        dry_run:  If True, parse and validate only — no DB or Excel writes.

    Returns:
        {
          "pdf": str, "dates": [str], "provinces": int,
          "upserted": int, "discrepancies": [...], "errors": [...]
        }
    """
    return run_pipeline(pdf_path, dry_run=dry_run)


@mcp.tool()
def spot_get_market_fundamentals(
    provinces: list[str] | None = None,
    year: int = 2025,
) -> dict:
    """
    Return market fundamentals for Chinese electricity provinces.

    Data covers installed capacity by fuel type (万kW: Wind, Solar, Thermal,
    Hydro, Nuclear, Storage), generation mix (亿kWh), and seasonal peak loads
    (MW).  Available for 2024 and 2025.

    Args:
        provinces: Optional list of province English names to filter by,
                   e.g. ["Shandong", "Guangdong"]. Omit for all provinces.
        year:      Data year — 2024 or 2025 (default 2025).

    Returns:
        {
          "year": int,
          "provinces": [
            {
              "province_cn": str, "province_en": str,
              "capacity_10kw": {"Wind": float, ...},
              "capacity_share": {"Wind": float, ...},
              "generation_100gwh": {"Wind": float, ...},
              "generation_share": {"Wind": float, ...},
              "peak_summer_mw": float | None,
              "peak_winter_mw": float | None
            }, ...
          ]
        }
    """
    return get_market_fundamentals(provinces=provinces, year=year)


@mcp.tool()
def spot_search_reference_docs(
    query: str,
    category: str | None = None,
    app: str | None = "shared",
    limit: int = 5,
) -> dict:
    """
    Full-text search over the spot-market knowledge base.

    Searches all ingested reference documents — PDFs, Excel files, policy
    documents, annual reports, research papers, and market rules.  Supports
    both English and Chinese (CJK) queries.

    Args:
        query:    Free-text search query (English or Chinese).
        category: Optional filter — market_rules | annual_report | policy_doc
                  | technical_spec | research_report | other.
        app:      Scope — 'shared' (default) searches documents visible to all
                  agents; 'strategist' also includes strategist-private docs.
        limit:    Max results to return (default 5, max 20).

    Returns:
        {
          "results": [
            {"doc_id": int, "file_name": str, "category": str,
             "page_no": int, "chunk_text": str, "rank": float}, ...
          ],
          "count": int
        }
    """
    return search_reference_docs(query=query, category=category, app=app, limit=limit)


@mcp.tool()
def spot_ingest_kb_document(
    s3_key: str | None = None,
    file_path: str | None = None,
    category: str | None = None,
    app: str = "shared",
) -> dict:
    """
    Add a reference document to the spot-market knowledge base.

    Supports all file types: PDF, Excel (.xlsx/.xls), PowerPoint (.pptx),
    Word (.docx), plain text, and images.  If the document is already in the
    knowledge base (same SHA-256 hash), returns the existing record without
    re-processing.

    Provide exactly one of s3_key or file_path.

    Args:
        s3_key:    S3 object key in the uploads bucket, e.g. "uploads/report.xlsx".
        file_path: Local or repo-relative path, e.g. "data/market-fundamentals/report.pdf".
        category:  Optional category override — market_rules | annual_report |
                   policy_doc | technical_spec | research_report | other.
        app:       Document scope — 'shared' (all agents, default) or 'strategist'.

    Returns:
        {
          "doc_id": int, "is_new": bool, "category": str,
          "filename": str, "status": "ingested"|"duplicate"|"error",
          "message": str
        }
    """
    return ingest_kb_document(
        s3_key=s3_key, file_path=file_path, category=category, app=app
    )


if __name__ == "__main__":
    mcp.run(transport="stdio")
