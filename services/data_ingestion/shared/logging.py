"""Structured JSON logger with secret masking and metrics emit."""
from __future__ import annotations
import json
import logging
import os
import re
import sys
from typing import Any

_SECRET_PATTERNS = [
    re.compile(r'(?i)(password|secret|token|key)["\s:=]+\S+'),
]


class _SecretFilter(logging.Filter):
    def filter(self, record):
        record.msg = _redact(str(record.msg))
        return True


def _redact(s: str) -> str:
    for p in _SECRET_PATTERNS:
        s = p.sub(lambda m: m.group(0).split("=")[0] + "=***REDACTED***", s)
    return s


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            '{"ts":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":%(message)s}',
            datefmt="%Y-%m-%dT%H:%M:%SZ"
        ))
        logger.addHandler(handler)
        logger.addFilter(_SecretFilter())
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    return logger


def emit_metrics(logger: logging.Logger, table: str, rows: int,
                 min_date: Any = None, max_date: Any = None):
    logger.info(json.dumps({
        "event": "rows_written",
        "table": table,
        "rows": rows,
        "min_date": str(min_date) if min_date else None,
        "max_date": str(max_date) if max_date else None,
    }))
