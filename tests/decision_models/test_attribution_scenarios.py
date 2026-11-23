"""run_daily_attribution reads what the LP batch actually writes (2026-11-23:
every attribution row had all-NULL P&L — legacy column + legacy names)."""
from datetime import date

import services.monitoring.run_daily_attribution as a

D = date(2026, 11, 18)


class _Conn:
    def __init__(self, rows): self.rows, self.sql = rows, None
    def __enter__(self): return self
    def __exit__(self, *x): return False
    def execute(self, sql, params=None):
        self.sql = str(sql)
        return self
    def fetchall(self): return self.rows


class _Engine:
    def __init__(self, rows): self.conn = _Conn(rows)
    def begin(self): return self.conn


def test_loader_reads_total_pnl_column():
    eng = _Engine([("suyou", "perfect_foresight_hourly", 311147.33)])
    out = a._load_scenario_pnl(eng, D)
    assert "COALESCE(total_pnl, total_revenue_yuan)" in eng.conn.sql
    assert out == {"suyou": {"perfect_foresight_hourly": 311147.33}}


def test_batch_scenarios_fill_the_ladder(monkeypatch):
    rows = [("suyou", "cleared_actual", 88637.0),
            ("suyou", "nominated_dispatch", 217762.0),
            ("suyou", "perfect_foresight_hourly", 311147.0),   # unrestricted PF
            ("suyou", "trading_cleared", 64301.0),             # no rung: ignored
            ("suyou", "perfect_foresight_grid_feasible", 290000.0)]  # zone-constrained
    written = []
    monkeypatch.setattr(a, "_upsert_attribution", lambda e, r: written.extend(r))
    monkeypatch.setattr(a, "_verify_post_write_identity", lambda *x: None)
    assert a.run_for_date(D, engine=_Engine(rows)) == 1
    r = written[0]
    assert r["pf_unrestricted_pnl"] == 311147.0 and r["pf_grid_feasible_pnl"] == 290000.0
    assert r["grid_restriction_loss"] == 311147.0 - 290000.0   # cost of TSO windows
    assert r["nominated_pnl"] == 217762.0 and r["cleared_actual_pnl"] == 88637.0
    assert r["execution_clearing_loss"] == 217762.0 - 88637.0
    assert r["realisation_gap_vs_pf_grid"] == 290000.0 - 88637.0
    # rungs with no scenario stay None — never a fabricated 0
    assert r["tt_strategy_pnl"] is None and r["nomination_loss"] is None


def test_null_scenario_never_blanks_a_filled_rung(monkeypatch):
    rows = [("suyou", "perfect_foresight_grid_feasible", 290000.0),
            ("suyou", "perfect_foresight_unrestricted", None)]   # legacy NULL row
    written = []
    monkeypatch.setattr(a, "_upsert_attribution", lambda e, r: written.extend(r))
    monkeypatch.setattr(a, "_verify_post_write_identity", lambda *x: None)
    a.run_for_date(D, engine=_Engine(rows))
    assert written[0]["pf_grid_feasible_pnl"] == 290000.0
    assert written[0]["pf_unrestricted_pnl"] is None
