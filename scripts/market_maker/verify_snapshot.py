"""Snapshot recompute check: risk numbers must be reproducible from frozen files.

Computes net MWh, total MtM and VaR (95%, 1d) from the DB, freezes positions + curve +
per-province scenario series to /tmp/mm_snapshot_<as_of>/, recomputes from the files only,
prints each pair and `MAX_ABS_DIFF <value>`; exit 1 if > 1e-6.

    python scripts/market_maker/verify_snapshot.py --as-of 2026-11-15
"""
from __future__ import annotations

import argparse
import math
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from libs.curves.store import load_curve, load_spot_hourly  # noqa: E402
from libs.risk.exposure import exposure  # noqa: E402
from libs.risk.var import monthly_price_scenarios, portfolio_var  # noqa: E402
from services.market_maker.positions import assemble_positions, forward_only  # noqa: E402

TOL = 1e-6
SCENARIO_HISTORY_DAYS = 730


def _engine():
    from services.market_maker.refresh import _engine as eng
    return eng()


def _metrics(pos: pd.DataFrame, curve: pd.DataFrame, scen: dict, as_of: date) -> dict:
    exp = exposure(pos, curve)
    fwd = forward_only(exp, as_of)
    deltas = fwd.groupby("province")["net_mwh"].sum().to_dict() if not fwd.empty else {}
    var = portfolio_var(deltas, scen, confidence=0.95, horizon_days=1)
    return {
        "total_net_mwh": float(exp["net_mwh"].sum()) if not exp.empty else 0.0,
        "total_mtm_cny": None if exp.empty or exp["mtm_cny"].dropna().empty
        else float(exp["mtm_cny"].sum(min_count=1)),
        "var_historical_95_1d": var["historical"],
        "var_parametric_95_1d": var["parametric"],
    }


def _diff(a, b) -> float:
    a_na = a is None or math.isnan(a)
    b_na = b is None or math.isnan(b)
    if a_na or b_na:
        return 0.0 if (a is None) == (b is None) and a_na and b_na else math.inf
    return abs(a - b)


def _write(df: pd.DataFrame, path: Path) -> Path:
    try:
        df.to_parquet(path.with_suffix(".parquet"))
        return path.with_suffix(".parquet")
    except ImportError:
        df.to_csv(path.with_suffix(".csv"), index=False)
        return path.with_suffix(".csv")


def _read(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)


def _find(d: Path, stem: str) -> Path:
    return next(p for p in (d / f"{stem}.parquet", d / f"{stem}.csv") if p.exists())


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    a = ap.parse_args(argv)
    as_of = a.as_of
    engine = _engine()

    pos = assemble_positions(engine, None, as_of)
    curve = load_curve(engine, as_of=as_of)
    exp = forward_only(exposure(pos, curve), as_of)
    deltas = exp.groupby("province")["net_mwh"].sum().to_dict() if not exp.empty else {}
    scen = {}
    for p, d in deltas.items():
        if d != 0:
            hourly = load_spot_hourly(engine, p, as_of - timedelta(days=SCENARIO_HISTORY_DAYS), as_of)
            s = monthly_price_scenarios(hourly) if not hourly.empty else pd.Series(dtype=float)
            if not s.empty:
                scen[p] = s
    db = _metrics(pos, curve, scen, as_of)

    snap = Path(f"/tmp/mm_snapshot_{as_of.isoformat()}")
    snap.mkdir(parents=True, exist_ok=True)
    for old in snap.iterdir():
        old.unlink()
    _write(pos, snap / "positions")
    _write(curve, snap / "curve")
    names = {}
    for i, (p, s) in enumerate(sorted(scen.items())):
        names[f"scenario_{i}"] = p
        _write(pd.DataFrame({"province": p, "date": s.index.astype(str), "value": s.values}),
               snap / f"scenario_{i}")
    pos2, curve2 = _read(_find(snap, "positions")), _read(_find(snap, "curve"))
    for col in ("delivery_month", "start_date", "end_date", "delivery_date"):
        for df in (pos2, curve2):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date
    scen2 = {}
    # VaR depends only on scenario values and order, not on the index dtype.
    for stem in names:
        df = _read(_find(snap, stem))
        scen2[str(df["province"].iloc[0])] = pd.Series(df["value"].to_numpy(),
                                                       index=pd.Index(df["date"]))
    fz = _metrics(pos2, curve2, scen2, as_of)

    max_diff = 0.0
    for k in db:
        d = _diff(db[k], fz[k])
        max_diff = max(max_diff, d)
        print(f"{k}: db={db[k]} snapshot={fz[k]} diff={d}")
    print(f"snapshot: {snap}")
    print(f"MAX_ABS_DIFF {max_diff}")
    return 1 if max_diff > TOL else 0


if __name__ == "__main__":
    raise SystemExit(main())
