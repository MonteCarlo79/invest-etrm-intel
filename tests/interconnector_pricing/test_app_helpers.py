# tests/interconnector_pricing/test_app_helpers.py
class _Cur:
    def __init__(self, rows): self._rows = rows
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None): self.sql, self.params = sql, params
    def fetchall(self): return self._rows

class _Conn:
    def __init__(self, rows): self._cur = _Cur(rows)
    def cursor(self): return self._cur


def test_load_rt_hourly_rows_and_sql():
    from libs.interconnector_pricing.db_reads import load_rt_hourly
    rows = [("2026-01-01 10:00", 401.2), ("2026-01-01 11:00", 402.3)]
    conn = _Conn(rows)
    out = load_rt_hourly(conn, "江苏", months=12)
    assert out == rows
    assert "marketdata.spot_prices_hourly" in conn.cursor().sql


def test_load_trade_ttm_converts_mwh_to_100gwh():
    from libs.interconnector_pricing.db_reads import load_trade_ttm
    conn = _Conn([("宝合直流", 2_000_000.0)])   # 2e6 MWh = 20 亿kWh
    assert load_trade_ttm(conn) == {"宝合直流": 20.0}
