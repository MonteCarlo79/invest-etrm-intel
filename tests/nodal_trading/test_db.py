# tests/nodal_trading/test_db.py
from datetime import date
from services.nodal_forecast import db

class _Cur:
    def __init__(self, rows=()): self._rows = list(rows); self.calls = []
    def execute(self, sql, params=None): self.calls.append((sql, params))
    def executemany(self, sql, seq): self.calls.append((sql, list(seq)))
    def fetchall(self): return self._rows
class _Conn:
    def __init__(self, rows=()): self.cur = _Cur(rows); self.committed = 0
    def cursor(self): return self.cur
    def commit(self): self.committed += 1

def test_ddl_has_three_new_tables():
    ddl = " ".join(db.DDL)
    assert "nodal_fc_grid_daily" in ddl
    assert "nodal_node_registry" in ddl
    assert "nodal_asset_registry" in ddl
    assert "nodal_strategy_daily" in ddl

def test_upsert_grid_forecast_roundtrip_sql():
    conn = _Conn()
    n = db.upsert_grid_forecast(conn, [dict(province="蒙西", target_date=date(2026,9,23),
        price_hat=321.5, model_version="v1")])
    assert n == 1
    assert "ON CONFLICT (province, target_date, model_version) DO UPDATE" in conn.cur.calls[0][0]

def test_fallback_flagged_when_table_empty(monkeypatch):
    conn = _Conn(rows=[])
    monkeypatch.setattr(db, "_lingfeng_rt_daily", lambda conn, prov: [
        (date(2026,9,22), 300.0), (date(2026,9,23), 310.0)])
    df = db.get_grid_forecast(conn, date(2026, 9, 24), province="蒙西")
    assert not df.empty
    assert set(df["model_version"]) == {"lingfeng_rt_fallback"}
