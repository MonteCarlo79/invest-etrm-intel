# -*- coding: utf-8 -*-
"""
services/document_intake/extract_mengxi_settlement.py

Extract BESS settlement data (上网电量) from Mengxi settlement PDFs (上网结算单).
These PDFs contain monthly discharge energy (MWh) for calculating compensation rates.

Usage:
    python extract_mengxi_settlement.py --file settlement.pdf --month 2025-07-01

Author: Matrix Agent
Created: 2026-03-26

NOTE: This module provides the scaffold for settlement extraction.
      Many settlement PDFs require OCR or manual review due to:
      - Image-based PDFs (scanned documents)
      - Complex table layouts
      - Inconsistent formats across months

      Files that cannot be reliably text-extracted should be marked
      as parse_pending and flagged for manual review.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

from sqlalchemy import create_engine, text

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

DB_DSN = os.getenv("DB_DSN") or os.getenv("PGURL")

# Asset alias mapping: folder/file name patterns -> asset_code
FOLDER_TO_ASSET = {
    "B-6": "suyou",
    "苏右": "suyou",
    "景蓝乌尔图": "suyou",
    "B-7": "wulate",
    "乌拉特": "wulate",
    "远景乌拉特": "wulate",
    "B-8": "wuhai",
    "乌海": "wuhai",
    "富景五虎山": "wuhai",
    "B-9": "hetao",
    "巴盟": "hetao",
    "景怡查干哈达": "hetao",
    "B-10": "wulanchabu",
    "景通红丰": "wulanchabu",
    "B-11": "siziwangqi",
    "四子王旗": "siziwangqi",
    "景通四益堂": "siziwangqi",
    "杭锦旗": "hangjinqi",
    "悦杭独贵": "hangjinqi",
    "谷山梁": "gushanliang",
    "裕昭沙子坝": "gushanliang",
}


def infer_asset_from_path(file_path: str) -> Optional[str]:
    """Infer asset_code from file path or filename."""
    path_str = str(file_path)
    for pattern, asset_code in FOLDER_TO_ASSET.items():
        if pattern in path_str:
            return asset_code
    return None


def infer_month_from_filename(filename: str) -> Optional[date]:
    """
    Infer settlement month from filename.
    Examples:
        7月 【B-6-上】苏右7月上网结算单.pdf -> 2025-07-01 (year inferred from context)
        远景乌拉特储能电站2025年7月上网结算单.pdf -> 2025-07-01
    """
    # Try YYYY年M月 pattern first
    pattern_full = r"(\d{4})年(\d{1,2})月"
    match = re.search(pattern_full, filename)
    if match:
        return date(int(match.group(1)), int(match.group(2)), 1)
    
    # Try M月 pattern (year needs to be inferred)
    pattern_month = r"(\d{1,2})月"
    match = re.search(pattern_month, filename)
    if match:
        # Default to 2025 if no year specified
        return date(2025, int(match.group(1)), 1)
    
    return None


def upsert_settlement_extracted(
    engine,
    asset_code: str,
    settlement_month: date,
    discharge_mwh: Optional[float],
    source_filename: str,
    parse_confidence: str = "high",
    parse_notes: Optional[str] = None,
    source_file_id: Optional[str] = None,
) -> None:
    """Upsert extracted settlement record into staging table."""
    with engine.begin() as conn:
        sql = text("""
            INSERT INTO staging.mengxi_settlement_extracted (
                source_file_id, source_filename, settlement_month,
                station_name_raw, asset_code, discharge_mwh,
                parse_confidence, parse_notes
            ) VALUES (
                :source_file_id, :source_filename, :settlement_month,
                :station_name_raw, :asset_code, :discharge_mwh,
                :parse_confidence, :parse_notes
            )
        """)
        conn.execute(sql, {
            "source_file_id": source_file_id,
            "source_filename": source_filename,
            "settlement_month": settlement_month,
            "station_name_raw": asset_code,  # Use asset_code as placeholder
            "asset_code": asset_code,
            "discharge_mwh": discharge_mwh,
            "parse_confidence": parse_confidence,
            "parse_notes": parse_notes,
        })


def register_pending_file(
    engine,
    file_path: str,
    asset_code: Optional[str],
    settlement_month: Optional[date],
    reason: str,
) -> None:
    """Register a file that needs manual review/OCR."""
    logger.warning("File pending manual review: %s - %s", file_path, reason)
    
    if not asset_code or not settlement_month:
        return
    
    upsert_settlement_extracted(
        engine=engine,
        asset_code=asset_code,
        settlement_month=settlement_month,
        discharge_mwh=None,
        source_filename=Path(file_path).name,
        parse_confidence="pending",
        parse_notes=f"Needs manual review: {reason}",
    )


def main():
    parser = argparse.ArgumentParser(description="Extract Mengxi settlement data")
    parser.add_argument("--file", help="Path to settlement PDF")
    parser.add_argument("--month", help="Settlement month (YYYY-MM-DD)")
    parser.add_argument("--scan-dir", help="Scan directory for settlement files")
    args = parser.parse_args()

    if not DB_DSN:
        raise RuntimeError("DB_DSN or PGURL environment variable required")

    engine = create_engine(DB_DSN, pool_pre_ping=True)

    if args.scan_dir:
        scan_path = Path(args.scan_dir)
        for pdf_file in scan_path.rglob("*上网结算*.pdf"):
asset_code = infer_asset_from_path(str(pdf_file))
            month = infer_month_from_filename(pdf_file.name)
            
            if asset_code and month:
                register_pending_file(
                    engine=engine,
                    file_path=str(pdf_file),
                    asset_code=asset_code,
                    settlement_month=month,
                    reason="PDF extraction not implemented - requires OCR/manual review",
                )
        print("Scan complete. Files registered as pending.")


if __name__ == "__main__":
    main()
