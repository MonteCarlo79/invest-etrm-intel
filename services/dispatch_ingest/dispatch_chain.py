"""Dispatch chain ingestion (调度计划表 folder tree → rm_dispatch_chain).

Per-day sheets ("5.01".."5.31") plus an aggregate sheet (电力交易调度计划):
  时间 | SOC（%）| 交易员申报计划(MW) | 日前出清(MW) | 实时调度出清(MW) | 实际执行功率(MW)

The time cell colour encodes the restriction window:
  green (FF00B050) or no fill → NULL (both charge+discharge allowed)
  orange fill                 → 'charge_only'
  red fill (FFFF0000)         → 'discharge_only'
(colour semantics confirmed by user 2026-08-28)
"""
from __future__ import annotations

import re
from typing import Any

import openpyxl

# 调度计划表 station folder → rm_assets.name
DISPATCH_FOLDER_TO_ASSET = {
    "1.巴盟-景怡查干哈达": "景怡查干哈达",
    "2.谷山梁-裕昭沙子坝": "裕昭沙子坝",
    "3.杭锦旗-悦杭独贵": "悦杭独贵",
    "4.四子王旗-景通四益堂": "四子王旗",
    "5.苏右-景蓝乌尔图": "景蓝乌尔图",
    "6.乌拉特": "远景乌拉特",
}

_GREEN = {"FF00B050", "00B050", "92D050"}
_RED = {"FFFF0000", "FF0000"}
_ORANGE_LIKE = {"FFFFA500", "FFA500", "FFFFC000", "FFC000", "FFFFCC00", "FFCC00", "FFF4B084", "F4B084"}


def restriction_from_fill(fill) -> str | None:
    """Map a cell fill to a restriction value (see module docstring for semantics)."""
    if not fill or fill.fill_type != "solid" or not fill.start_color:
        return None
    rgb = (fill.start_color.rgb or "").upper()
    if rgb in _RED:
        return "discharge_only"
    if rgb in _ORANGE_LIKE:
        return "charge_only"
    return None  # green, blue-grey, anything else


def _norm_header(s: str) -> str:
    return re.sub(r"[\s（(].*$", "", (s or "").strip().lower())


def _find_columns(header_row: list[Any]) -> dict[str, int] | None:
    cols: dict[str, int] = {}
    for i, cell in enumerate(header_row):
        n = _norm_header(str(cell or ""))
        if n == "时间":
            cols["time"] = i
        elif n.startswith("soc") or n == "soc（%）":
            cols["soc"] = i
        elif n.startswith("交易员申报计划"):
            cols["nominated"] = i
        elif n.startswith("日前出清"):
            cols["da_cleared"] = i
        elif n.startswith("实时调度出清"):
            cols["rt_cleared"] = i
        elif n.startswith("实际执行功率"):
            cols["actual"] = i
    if {"time", "nominated", "da_cleared", "rt_cleared", "actual"} <= cols.keys():
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


def parse_dispatch_sheet(ws) -> list[dict[str, Any]]:
    """Parse one per-day dispatch sheet into interval dicts (with restriction from time-cell fill)."""
    header = None
    for idx, row in enumerate(ws.iter_rows(values_only=True)):
        if idx > 5:
            break
        found = _find_columns(list(row))
        if found:
            header = found
            break
    if header is None:
        return []

    import pandas as pd

    items = []
    for row in ws.iter_rows(min_row=2, max_col=max(header.values()) + 1):
        time_v = row[header["time"]].value
        if time_v is None:
            continue
        try:
            ts = pd.Timestamp(time_v)
            if ts.tz is None:
                ts = ts.tz_localize("Asia/Shanghai")
        except Exception:
            continue
        items.append({
            "interval_start": ts,
            "soc_pct": _to_float(row[header["soc"]].value) if "soc" in header else None,
            "nominated_mw": _to_float(row[header["nominated"]].value),
            "da_cleared_mw": _to_float(row[header["da_cleared"]].value),
            "rt_cleared_mw": _to_float(row[header["rt_cleared"]].value),
            "actual_mw": _to_float(row[header["actual"]].value),
            "restriction": restriction_from_fill(row[header["time"]].fill),
        })
    return items


def parse_dispatch_file(file_path: str) -> list[dict[str, Any]]:
    """Parse every dispatch-looking sheet in one workbook."""
    wb = openpyxl.load_workbook(file_path, data_only=True)
    items: list[dict[str, Any]] = []
    for ws in wb.worksheets:
        items.extend(parse_dispatch_sheet(ws))
    wb.close()
    return items


def ingest_dispatch_chain(root: str, batch_id: str) -> dict[str, Any]:
    """Walk the 调度计划表 tree and write to rm_dispatch_chain."""
    import os
    from shared.agents.db import get_conn

    report: dict[str, Any] = {"assets": {}, "skipped": [], "errors": []}
    with get_conn() as conn:
        with conn.cursor() as cur:
            for folder, asset_name in DISPATCH_FOLDER_TO_ASSET.items():
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
                            items = parse_dispatch_file(fpath)
                        except Exception as e:
                            report["errors"].append(f"{fname}: {e}")
                            continue
                        if not items:
                            report["skipped"].append(f"{fname}: no dispatch sheets")
                            continue
                        for it in items:
                            cur.execute("""
                                INSERT INTO marketdata.rm_dispatch_chain
                                    (asset_id, interval_start, soc_pct, nominated_mw,
                                     da_cleared_mw, rt_cleared_mw, actual_mw, restriction,
                                     source_file, upload_batch_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (asset_id, interval_start) DO UPDATE SET
                                    soc_pct = EXCLUDED.soc_pct,
                                    nominated_mw = EXCLUDED.nominated_mw,
                                    da_cleared_mw = EXCLUDED.da_cleared_mw,
                                    rt_cleared_mw = EXCLUDED.rt_cleared_mw,
                                    actual_mw = EXCLUDED.actual_mw,
                                    restriction = EXCLUDED.restriction,
                                    source_file = EXCLUDED.source_file,
                                    upload_batch_id = EXCLUDED.upload_batch_id
                            """, (asset_id, it["interval_start"], it["soc_pct"],
                                  it["nominated_mw"], it["da_cleared_mw"], it["rt_cleared_mw"],
                                  it["actual_mw"], it["restriction"],
                                  os.path.join(dirpath.replace(root, "").lstrip("/"), fname),
                                  batch_id))
                            rows_written += 1
                report["assets"][asset_name] = rows_written
                # Per-station commit: this network kills long transactions
                conn.commit()
                print(f"[dispatch_chain] {asset_name}: {rows_written:,} rows committed", flush=True)
    return report
