"""Trader nomination ingestion (申报策略 folder tree → rm_nominations).

Layout survey (2026-08-28):
  01-苏右, 02-杭锦旗, 03-四子王, 05-乌海, 06-乌拉特:
      日期 | 时刻 | 预计划功率 | 正式申报
  04-谷山梁:
      日期 | 时刻 | 预策略D-2功率（MW） | 正式策略D-1功率（MW）
  07-巴盟:
      日期 | 时刻 | 预申报策略（MW） | 实际申报策略（MW）

Intervals are 5-min. Positive MW = discharge, negative = charge.
Header-driven column detection — no per-station hardcoding.
"""
from __future__ import annotations

import re
from typing import Any

import openpyxl

# 申报策略 station folder → rm_assets.name
NOMINATION_FOLDER_TO_ASSET = {
    "01-苏右": "景蓝乌尔图",
    "02-杭锦旗": "悦杭独贵",
    "03-四子王": "四子王旗",
    "04-谷山梁": "裕昭沙子坝",
    "05-乌海": "乌海康富",
    "06-乌拉特": "远景乌拉特",
    "07-巴盟": "景怡查干哈达",
}

# Header aliases → canonical field
_PLANNED_ALIASES = ("预计划功率", "预策略d-2功率", "预申报策略")
_NOMINATED_ALIASES = ("正式申报", "正式策略d-1功率", "实际申报策略")


def _norm(s: str) -> str:
    return re.sub(r"[\s（(].*$", "", (s or "").strip().lower())


def _match_alias(cell: str, aliases: tuple[str, ...]) -> bool:
    n = _norm(cell)
    return any(n.startswith(a) for a in aliases)


def _find_columns(header_row: list[Any]) -> dict[str, int] | None:
    """Map header cells to field indices. Returns None if the row isn't a nomination header."""
    cols: dict[str, int] = {}
    for i, cell in enumerate(header_row):
        text = str(cell or "").strip()
        if not text:
            continue
        n = _norm(text)
        if n == "日期":
            cols["date"] = i
        elif n == "时刻":
            cols["time"] = i
        elif "planned" not in cols and _match_alias(text, _PLANNED_ALIASES):
            cols["planned"] = i
        elif "nominated" not in cols and _match_alias(text, _NOMINATED_ALIASES):
            cols["nominated"] = i
    if {"date", "time", "nominated"} <= cols.keys():
        return cols
    return None


def _to_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        m = re.search(r"-?\d[\d,]*\.?\d*", str(v))
        return float(m.group(0).replace(",", "")) if m else None


def parse_nomination_sheet(ws) -> list[dict[str, Any]]:
    """Parse one nomination worksheet into interval dicts.

    Returns list of {interval_start, planned_mw, nominated_mw}; empty when the
    sheet has no nomination header.
    """
    rows = ws.iter_rows(values_only=True)
    cols = None
    header_row_idx = 0
    for idx, row in enumerate(rows):
        if idx > 5:
            break
        found = _find_columns(list(row))
        if found:
            cols = found
            header_row_idx = idx + 1
            break
    if cols is None:
        return []

    import pandas as pd

    items = []
    for row in ws.iter_rows(min_row=header_row_idx + 1, values_only=True):
        date_v = row[cols["date"]] if cols["date"] < len(row) else None
        time_v = row[cols["time"]] if cols["time"] < len(row) else None
        if date_v is None or time_v is None:
            continue
        try:
            ts = pd.Timestamp(f"{pd.to_datetime(date_v).date()} {time_v}", tz="Asia/Shanghai")
        except Exception:
            continue
        items.append({
            "interval_start": ts,
            "planned_mw": _to_float(row[cols["planned"]]) if "planned" in cols else None,
            "nominated_mw": _to_float(row[cols["nominated"]]),
        })
    return items


def parse_nomination_file(file_path: str) -> list[dict[str, Any]]:
    """Parse every nomination-looking sheet in one workbook."""
    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    items: list[dict[str, Any]] = []
    for ws in wb.worksheets:
        items.extend(parse_nomination_sheet(ws))
    wb.close()
    return items


def ingest_nominations(root: str, batch_id: str) -> dict[str, Any]:
    """Walk the 申报策略 tree and write nominations to rm_nominations.

    Returns a report dict: {assets: {asset_name: rows}, skipped: [...], errors: [...]}.
    """
    import os
    import uuid
    from shared.agents.db import get_conn

    report: dict[str, Any] = {"assets": {}, "skipped": [], "errors": []}
    with get_conn() as conn:
        with conn.cursor() as cur:
            for folder, asset_name in NOMINATION_FOLDER_TO_ASSET.items():
                cur.execute("SELECT id FROM marketdata.rm_assets WHERE name = %s", (asset_name,))
                row = cur.fetchone()
                if not row:
                    report["errors"].append(f"asset not found: {asset_name}")
                    continue
                asset_id = row[0]

                folder_path = os.path.join(root, folder)
                if not os.path.isdir(folder_path):
                    report["skipped"].append(f"missing folder: {folder}")
                    continue

                rows_written = 0
                for dirpath, _, files in os.walk(folder_path):
                    for fname in sorted(files):
                        if not fname.endswith(".xlsx") or fname.startswith("~$"):
                            continue
                        fpath = os.path.join(dirpath, fname)
                        try:
                            items = parse_nomination_file(fpath)
                        except Exception as e:
                            report["errors"].append(f"{fname}: {e}")
                            continue
                        if not items:
                            report["skipped"].append(f"{fname}: no nomination sheets")
                            continue
                        for it in items:
                            cur.execute("""
                                INSERT INTO marketdata.rm_nominations
                                    (asset_id, interval_start, planned_mw, nominated_mw,
                                     source_file, upload_batch_id)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (asset_id, interval_start) DO UPDATE SET
                                    planned_mw = EXCLUDED.planned_mw,
                                    nominated_mw = EXCLUDED.nominated_mw,
                                    source_file = EXCLUDED.source_file,
                                    upload_batch_id = EXCLUDED.upload_batch_id
                            """, (asset_id, it["interval_start"], it["planned_mw"],
                                  it["nominated_mw"], os.path.join(dirpath.replace(root, "").lstrip("/"), fname),
                                  batch_id))
                            rows_written += 1
                report["assets"][asset_name] = rows_written
                # Per-station commit: this network kills long transactions
                # (ERRORS.md bulk-load rule — a dead connection costs one chunk)
                conn.commit()
                print(f"[nominations] {asset_name}: {rows_written:,} rows committed", flush=True)
    return report
