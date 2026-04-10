"""S3 raw landing helpers."""
from __future__ import annotations
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_RAW_PREFIX = os.environ.get("S3_RAW_PREFIX", "raw-landing")


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def upload_to_landing(local_path: Path, collector: str, source_date: str) -> str:
    if not S3_BUCKET:
        return ""
    key = f"{S3_RAW_PREFIX}/{collector}/{source_date}/{local_path.name}"
    metadata = {
        "collector": collector,
        "source_date": source_date,
        "sha256": file_hash(local_path),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    s3 = boto3.client("s3")
    s3.upload_file(
        str(local_path), S3_BUCKET, key,
        ExtraArgs={"Metadata": {k: str(v) for k, v in metadata.items()}}
    )
    return f"s3://{S3_BUCKET}/{key}"
