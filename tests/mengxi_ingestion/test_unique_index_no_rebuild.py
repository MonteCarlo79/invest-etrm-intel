"""Unique-index setup must never DROP + rebuild on every file load.

2026-11-11: the 04:01 CST reconcile task called DROP INDEX + non-concurrent
CREATE UNIQUE INDEX per sheet per file. On md_id_cleared_energy (36M rows) each
rebuild held an AccessExclusive lock ~9 min; the 07:05 CST 蒙西 BESS daily
report's MAX(data_date) timed out 3×120s. Both loaders (vendored reconcile
pipeline + in-app upload path) must create the index only if missing.
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]
                      / "bess-marketdata-ingestion" / "providers" / "mengxi"))

import load_excel_to_marketdata as vendored  # noqa: E402
from services.mengxi_ingestion import loader as app_loader  # noqa: E402


class _Recorder:
    def __init__(self):
        self.sql = []

    def begin(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt, *a, **k):
        self.sql.append(str(stmt))


KEYS = ["data_date", "datetime", "plant_name", "dispatch_unit_name"]


def _assert_idempotent(statements):
    joined = " ".join(statements).upper()
    assert "DROP INDEX" not in joined
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in joined


def test_vendored_loader_does_not_rebuild_index():
    rec = _Recorder()
    vendored.create_unique_index(rec, "marketdata", "md_id_cleared_energy", KEYS)
    _assert_idempotent(rec.sql)


def test_app_loader_does_not_rebuild_index():
    rec = _Recorder()
    app_loader._create_unique_index(rec, "marketdata", "md_id_cleared_energy", KEYS)
    _assert_idempotent(rec.sql)
