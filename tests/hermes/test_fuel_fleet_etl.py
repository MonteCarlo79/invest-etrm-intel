# tests/hermes/test_fuel_fleet_etl.py
from unittest.mock import MagicMock, patch
import services.hermes.fuel_fleet_etl as etl


def _conn_mock():
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn, cur


_SEG = [{"fuel": "coal", "capacity_mw": 40000, "heat_rate_kj_kwh": 8200,
         "vom_yuan_mwh": 12, "label": "coal_main"}]


def test_upsert_inserts_valid_row(monkeypatch):
    conn, cur = _conn_mock()
    cur.fetchall.return_value = []  # no existing confirmed row
    monkeypatch.setattr(etl.psycopg2, "connect", lambda *a, **k: conn)
    out = etl.upsert_fuel_fleet_rows(
        [{"province": "山东", "effective_date": "2026-09-01",
          "coal_price_yuan_t": 850.0, "gas_price_yuan_m3": 3.1,
          "fleet_segments": _SEG}],
        "postgresql://x", "test")
    assert out["upserted"] == 1 and out["errors"] == []
    assert any("province_fuel_fleet" in str(c.args[0]) for c in cur.execute.call_args_list)


def test_upsert_rejects_bad_rows(monkeypatch):
    conn, cur = _conn_mock()
    monkeypatch.setattr(etl.psycopg2, "connect", lambda *a, **k: conn)
    out = etl.upsert_fuel_fleet_rows(
        [{"province": "", "effective_date": "2026-09-01", "fleet_segments": _SEG},
         {"province": "山东", "effective_date": "not-a-date", "fleet_segments": _SEG},
         {"province": "山西", "effective_date": "2026-09-01", "fleet_segments": []}],
        "postgresql://x", "test")
    assert out["upserted"] == 0 and len(out["errors"]) == 3


def test_conflict_flagged_on_large_diff(monkeypatch):
    conn, cur = _conn_mock()
    # existing confirmed row id=7 with coal 800; new row coal 1000 (>5% diff)
    cur.fetchall.return_value = [(7, 800.0)]
    monkeypatch.setattr(etl.psycopg2, "connect", lambda *a, **k: conn)
    out = etl.upsert_fuel_fleet_rows(
        [{"province": "山东", "effective_date": "2026-09-01",
          "coal_price_yuan_t": 1000.0, "gas_price_yuan_m3": None,
          "fleet_segments": _SEG}],
        "postgresql://x", "test")
    assert out["conflicts"] == 1
