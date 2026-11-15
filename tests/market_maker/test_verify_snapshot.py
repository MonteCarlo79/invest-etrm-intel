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


def test_diff_nan_none_semantics():
    nan = float("nan")
    assert vs._diff(None, None) == 0.0
    assert vs._diff(nan, nan) == 0.0
    assert vs._diff(1.0, 1.5) == 0.5
    for a, b in ((nan, 1.0), (1.0, nan), (None, 1.0), (1.0, None), (None, nan), (nan, None)):
        assert vs._diff(a, b) == float("inf")


def test_snapshot_recompute_with_marked_mtm(engine, monkeypatch, capsys):
    from sqlalchemy import text
    with engine.begin() as c:
        for prov in ("河北南网", "冀南", "蒙西"):
            for m in ("2026-12-01", "2027-01-01"):
                for blk in ("base", "peak", "valley", "flat"):
                    c.execute(text("INSERT INTO marketdata.rm_forward_curves (province, product,"
                                   " curve_date, delivery_date, price_cny_kwh, source, tou_block)"
                                   " VALUES (:p,'reference','2026-11-10',:m,0.45,'manual',:b)"),
                              {"p": prov, "m": m, "b": blk})
    monkeypatch.setattr(vs, "_engine", lambda: engine)
    rc = vs.main(["--as-of", AS_OF.isoformat()])
    out = capsys.readouterr().out
    mtm = [l for l in out.splitlines() if l.startswith("total_mtm_cny:")][0]
    assert "db=None" not in mtm and "snapshot=None" not in mtm
    assert rc == 0 and "MAX_ABS_DIFF 0.0" in out
