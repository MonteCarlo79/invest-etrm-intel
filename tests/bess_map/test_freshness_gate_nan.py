# tests/bess_map/test_freshness_gate_nan.py
"""Freshness gate must not treat NaN-realized rows as done (fossilization fix,
2026-10-05). Rows with realized NaN/NULL are stale and must be recomputed."""
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "services" / "bess_map"))
from run_capture_pipeline import get_last_capture_day  # noqa: E402


def _engine_returning(max_date):
    eng = MagicMock()
    ctx = eng.connect.return_value.__enter__.return_value
    ctx.execute.return_value.fetchone.return_value = (max_date,)
    return eng, ctx


def test_sql_excludes_nan_and_null_realized():
    eng, ctx = _engine_returning(None)
    get_last_capture_day(eng, "marketdata", "山西", "m", 4.0, 1.0, 0.85)
    sql = ctx.execute.call_args.args[0].text
    assert "realized_profit_per_mwh_day IS NOT NULL" in sql
    assert "realized_profit_per_mwh_day::text != 'NaN'" in sql


def test_all_nan_rows_returns_none():
    """A province whose rows are all NaN reads as never-run → full recompute."""
    eng, _ = _engine_returning(None)
    assert get_last_capture_day(eng, "marketdata", "福建", "m", 4.0, 1.0, 0.85) is None
