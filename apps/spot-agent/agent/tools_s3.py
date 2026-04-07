from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Optional


@dataclass(frozen=True)
class S3PdfObject:
    bucket: str
    key: str
    last_modified: object | None = None
    size: int | None = None
    etag: str | None = None

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"

    @property
    def name(self) -> str:
        return Path(self.key).name


def _get_s3_client(region: Optional[str] = None):
    try:
        import boto3
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "S3 source mode requires boto3 in the ingestion runtime. "
            "Install boto3 in the execution environment before using source_mode: s3."
        ) from exc

    kwargs = {}
    if region:
        kwargs["region_name"] = region
    return boto3.client("s3", **kwargs)


def list_pdf_objects(bucket: str, prefixes: List[str], region: Optional[str] = None) -> List[S3PdfObject]:
    if not bucket:
        raise ValueError("s3_bucket is required when source_mode is 's3'")
    if not prefixes:
        raise ValueError("s3_prefixes must contain at least one prefix when source_mode is 's3'")

    client = _get_s3_client(region=region)
    results: List[S3PdfObject] = []
    seen: set[tuple[str, str]] = set()

    for prefix in prefixes:
        token = None
        while True:
            kwargs = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                if not key.lower().endswith(".pdf"):
                    continue
                marker = (bucket, key)
                if marker in seen:
                    continue
                seen.add(marker)
                results.append(
                    S3PdfObject(
                        bucket=bucket,
                        key=key,
                        last_modified=obj.get("LastModified"),
                        size=obj.get("Size"),
                        etag=obj.get("ETag"),
                    )
                )
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")

    results.sort(key=lambda item: item.key)
    return results


@contextmanager
def stage_pdf_to_temp(bucket: str, key: str, region: Optional[str] = None) -> Iterator[str]:
    client = _get_s3_client(region=region)
    suffix = Path(key).suffix or ".pdf"
    fd, tmp_path = tempfile.mkstemp(prefix="spot-report-", suffix=suffix)
    os.close(fd)
    try:
        client.download_file(bucket, key, tmp_path)
        yield tmp_path
    finally:
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass
