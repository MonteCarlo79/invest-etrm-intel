"""Main ingestion orchestrator for operating asset files.

Scans a directory (or processes a single file) and routes to
the appropriate parser based on filename match and file structure.
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path

import pandas as pd
from shared.agents.db import get_conn, execute_sql
from services.operating_assets.filename_mapper import resolve_asset


def ingest_file(file_path: str) -> dict:
    """Ingest a single file into the appropriate rm_ tables.

    Args:
        file_path: Absolute path to Excel file

    Returns:
        Dict with keys: asset_name, asset_type, parser, rows_written, errors
    """
    filename = os.path.basename(file_path)
    asset_info = resolve_asset(filename)

    if asset_info is None:
        return {"asset_name": None, "parser": None, "rows_written": 0,
                "errors": [f"No asset match for filename: {filename}"]}

    batch_id = str(uuid.uuid4())[:8]

    if asset_info["asset_type"] == "wind":
        from services.operating_assets.parsers.wind_farm import parse_wind_farm
        return parse_wind_farm(file_path, asset_info["asset_name"], batch_id)
    else:
        xl = pd.ExcelFile(file_path)
        sheets = xl.sheet_names

        results = {"asset_name": asset_info["asset_name"], "asset_type": "bess",
                   "parser": "bess", "rows_written": 0, "errors": []}

        if any("调度" in s or "计划" in s for s in sheets):
            from services.operating_assets.parsers.bess_dispatch import parse_bess_dispatch
            r = parse_bess_dispatch(xl, asset_info["asset_name"], batch_id)
            results["rows_written"] += r.get("rows_written", 0)
            results["errors"].extend(r.get("errors", []))

        if any("运营" in s or "统计" in s for s in sheets):
            from services.operating_assets.parsers.bess_daily import parse_bess_daily
            r = parse_bess_daily(xl, asset_info["asset_name"], batch_id)
            results["rows_written"] += r.get("rows_written", 0)
            results["errors"].extend(r.get("errors", []))

        return results


def scan_and_ingest(directory: str) -> list[dict]:
    """Scan directory for new/modified Excel files and ingest each."""
    results = []
    path = Path(directory)
    for f in path.glob("**/*.xlsx"):
        if f.name.startswith("~$"):
            continue
        result = ingest_file(str(f))
        results.append(result)
    return results
