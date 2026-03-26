# -*- coding: utf-8 -*-
"""
services/document_intake/register_document.py

Settlement/compensation document registration service.
Uploads raw files to S3 and registers metadata in Postgres.

Usage:
    python register_document.py --file settlement.pdf --type settlement_pdf --province Mengxi --month 2025-07-01

Env vars required:
    S3_BUCKET, DB_DSN (or PGURL)
    AWS credentials (via env or IAM role)
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
from datetime import date
from pathlib import Path
from typing import Optional
from uuid import uuid4

import boto3
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_RAW_DOCUMENTS_PREFIX", "raw-documents")
DB_DSN = os.getenv("DB_DSN") or os.getenv("PGURL")

VALID_DOCUMENT_TYPES = [
    "settlement_pdf",
    "compensation_file",
    "dispatch_report",
    "grid_notice",
    "policy_document",
    "operational_report",
]


def _db_engine():
    if not DB_DSN:
        raise RuntimeError("DB_DSN or PGURL environment variable required")
    return create_engine(DB_DSN, pool_pre_ping=True)


def _s3_client():
    return boto3.client("s3")


def _compute_md5(file_path: Path) -> str:
    """Compute MD5 hash of file."""
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


def _infer_settlement_month(filename: str) -> Optional[date]:
    """
    Try to infer settlement month from filename.
    Examples:
        2025年7月储能容量补偿费用统计表.pdf -> 2025-07-01
        2026年2月储能容量补偿费用统计表.pdf -> 2026-02-01
    """
    # Pattern: YYYY年M月 or YYYY年MM月
    pattern = r"(\d{4})年(\d{1,2})月"
    match = re.search(pattern, filename)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        return date(year, month, 1)
    return None


def _infer_province(filename: str) -> Optional[str]:
    """Infer province from filename if present."""
    provinces = ["蒙西", "Mengxi", "内蒙", "蒙东", "安徽", "Anhui", "山东", "Shandong"]
    for p in provinces:
        if p in filename:
            # Normalize to English key
            if p in ["蒙西", "内蒙", "Mengxi"]:
                return "Mengxi"
            elif p in ["安徽", "Anhui"]:
                return "Anhui"
            elif p in ["山东", "Shandong"]:
                return "Shandong"
    return None


def register_document(
    file_path: Path,
    document_type: str,
    province: Optional[str] = None,
    settlement_month: Optional[date] = None,
    asset_code: Optional[str] = None,
    uploaded_by: Optional[str] = None,
) -> str:
    """
    Upload file to S3 and register in Postgres.
    
    Returns:
        file_id (UUID string)
    """
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET environment variable required")
    
    if document_type not in VALID_DOCUMENT_TYPES:
        raise ValueError(f"Invalid document_type: {document_type}. Valid: {VALID_DOCUMENT_TYPES}")
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Compute metadata
    file_id = str(uuid4())
    original_filename = file_path.name
    file_extension = file_path.suffix.lower()
    file_size = file_path.stat().st_size
    content_md5 = _compute_md5(file_path)
    
    # Infer missing metadata from filename
    if settlement_month is None:
        settlement_month = _infer_settlement_month(original_filename)
    if province is None:
        province = _infer_province(original_filename)
    
    # Build S3 key
    # Pattern: raw-documents/{document_type}/{province}/{YYYY-MM}/{file_id}_{filename}
    s3_key_parts = [S3_PREFIX, document_type]
    if province:
        s3_key_parts.append(province.lower())
    if settlement_month:
        s3_key_parts.append(settlement_month.strftime("%Y-%m"))
    s3_key_parts.append(f"{file_id}_{original_filename}")
    s3_key = "/".join(s3_key_parts)
    
    # MIME type
    mime_map = {
        ".pdf": "application/pdf",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
        ".csv": "text/csv",
        ".zip": "application/zip",
    }
    mime_type = mime_map.get(file_extension, "application/octet-stream")
    
    # Upload to S3
    logger.info("Uploading %s to s3://%s/%s", original_filename, S3_BUCKET, s3_key)
    s3 = _s3_client()
    s3.upload_file(
        str(file_path),
        S3_BUCKET,
        s3_key,
        ExtraArgs={"ContentType": mime_type}
    )
    
    # Register in Postgres
    engine = _db_engine()
    insert_sql = text("""
        INSERT INTO raw_data.file_registry (
            file_id, s3_bucket, s3_key,
            original_filename, file_extension, file_size_bytes, content_md5, mime_type,
            document_type, province, asset_code, settlement_month,
            upload_status, uploaded_by
        ) VALUES (
            :file_id::uuid, :s3_bucket, :s3_key,
            :original_filename, :file_extension, :file_size_bytes, :content_md5, :mime_type,
            :document_type, :province, :asset_code, :settlement_month,
            'uploaded', :uploaded_by
        )
        ON CONFLICT (s3_bucket, s3_key) DO UPDATE SET
            upload_status = 'uploaded',
            updated_at = now()
        RETURNING file_id
    """)
    
    with engine.begin() as conn:
        result = conn.execute(insert_sql, {
            "file_id": file_id,
            "s3_bucket": S3_BUCKET,
            "s3_key": s3_key,
            "original_filename": original_filename,
            "file_extension": file_extension,
            "file_size_bytes": file_size,
            "content_md5": content_md5,
            "mime_type": mime_type,
            "document_type": document_type,
            "province": province,
            "asset_code": asset_code,
            "settlement_month": settlement_month,
            "uploaded_by": uploaded_by,
        })
        row = result.fetchone()
        file_id = str(row[0])
    
    logger.info("Registered document: file_id=%s, s3_key=%s", file_id, s3_key)
    return file_id


def main():
    parser = argparse.ArgumentParser(description="Register settlement/compensation documents")
    parser.add_argument("--file", required=True, help="Path to file to register")
    parser.add_argument("--type", required=True, choices=VALID_DOCUMENT_TYPES, help="Document type")
    parser.add_argument("--province", help="Province (e.g., Mengxi)")
    parser.add_argument("--month", help="Settlement month (YYYY-MM-DD)")
    parser.add_argument("--asset", help="Asset code if file is asset-specific")
    parser.add_argument("--user", help="Uploaded by (email or username)")
    
    args = parser.parse_args()
    
    settlement_month = None
    if args.month:
        from datetime import datetime
        settlement_month = datetime.strptime(args.month, "%Y-%m-%d").date()
    
    file_id = register_document(
        file_path=Path(args.file),
        document_type=args.type,
        province=args.province,
        settlement_month=settlement_month,
        asset_code=args.asset,
        uploaded_by=args.user,
    )
    
    print(f"Registered: file_id={file_id}")


if __name__ == "__main__":
    main()
