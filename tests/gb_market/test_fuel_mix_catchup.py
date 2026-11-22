"""Fuel-mix catch-up window (no network/DB).

Regression: NESO's Historic Generation Mix dataset publishes weeks in arrears,
but the nightly job only ever asked for yesterday -> 0 rows every night.
"""
import importlib.util
from datetime import date
from pathlib import Path

_PATH = Path(__file__).resolve().parents[2] / "apps" / "gb-market" / "fuel_mix_ingest.py"
_spec = importlib.util.spec_from_file_location("fuel_mix_ingest", _PATH)
fm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fm)


class _Cur:
    def __init__(self, last):
        self._last = last

    def execute(self, sql, params=None):
        pass

    def fetchone(self):
        return (self._last,)


class _Conn:
    def __init__(self, last):
        self._last = last

    def cursor(self):
        return _Cur(self._last)


def _run(monkeypatch, last_stored, neso_latest):
    calls = []
    monkeypatch.setattr(fm, "_neso_latest_date", lambda: neso_latest)
    monkeypatch.setattr(
        fm, "ingest_fuel_mix_range", lambda s, e, conn: calls.append((s, e)) or 48
    )
    n = fm.ingest_fuel_mix_catchup(_Conn(last_stored))
    return n, calls


def test_catchup_spans_last_stored_day_to_neso_latest(monkeypatch):
    n, calls = _run(monkeypatch, date(2026, 10, 20), date(2026, 11, 4))
    # last stored day is re-pulled (it may have been partial when ingested)
    assert calls == [(date(2026, 10, 20), date(2026, 11, 4))]
    assert n == 48


def test_catchup_empty_table_uses_default_lookback(monkeypatch):
    _, calls = _run(monkeypatch, None, date(2026, 11, 4))
    assert calls == [(date(2026, 10, 5), date(2026, 11, 4))]


def test_catchup_noop_when_neso_unreachable(monkeypatch):
    n, calls = _run(monkeypatch, date(2026, 10, 20), None)
    assert (n, calls) == (0, [])


def test_catchup_noop_when_db_ahead_of_neso(monkeypatch):
    n, calls = _run(monkeypatch, date(2026, 11, 10), date(2026, 11, 4))
    assert (n, calls) == (0, [])


def test_naive_neso_timestamp_is_utc_regardless_of_host_tz(monkeypatch):
    import time
    monkeypatch.setenv("TZ", "Asia/Singapore")
    time.tzset()
    try:
        assert fm._datetime_to_settlement_period("2026-11-03T00:00:00") == (date(2026, 11, 3), 1)
        assert fm._datetime_to_settlement_period("2026-11-03T23:30:00") == (date(2026, 11, 3), 48)
    finally:
        monkeypatch.delenv("TZ")
        time.tzset()
