import importlib.util
from pathlib import Path

from tests.market_maker.test_refresh import AS_OF, engine  # noqa: F401  (fixture reuse)

_SPEC = importlib.util.spec_from_file_location(
    "verify_snapshot", Path(__file__).resolve().parents[2] / "scripts/market_maker/verify_snapshot.py")
vs = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(vs)


def test_snapshot_recompute_matches(engine, monkeypatch, capsys):
    monkeypatch.setattr(vs, "_engine", lambda: engine)
    rc = vs.main(["--as-of", AS_OF.isoformat()])
    out = capsys.readouterr().out
    assert rc == 0
    assert "MAX_ABS_DIFF 0.0" in out
    assert "total_net_mwh" in out


def test_snapshot_recompute_with_var(engine, monkeypatch, capsys):
    from datetime import datetime, timedelta

    from sqlalchemy import text
    rows = [{"p": p, "t": datetime(2025, 6, 1) + timedelta(days=d), "v": 300 + (d * 7 % 23) + d * 0.1}
            for p in ("河北南网", "蒙西") for d in range(500)]
    with engine.begin() as c:
        c.execute(text("INSERT INTO marketdata.spot_prices_hourly VALUES (:p,:t,:v,NULL)"), rows)
    monkeypatch.setattr(vs, "_engine", lambda: engine)
    rc = vs.main(["--as-of", AS_OF.isoformat()])
    out = capsys.readouterr().out
    assert rc == 0 and "MAX_ABS_DIFF 0.0" in out
    assert "var_historical_95_1d: db=None" not in out
