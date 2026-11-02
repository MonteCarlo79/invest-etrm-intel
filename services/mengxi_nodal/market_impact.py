"""Per-station market-impact coefficient (λ) estimation — 蒙西 book (plan task 13).

Economic model: when the station discharges Q MW at its node, its clearing
price moves to p − λ_d·Q; when it charges Q MW, to p + λ_c·Q (λ in CNY/MWh
per MW, expected ≥ 0). The nodal PF engine consumes the same rule
(services.bess_map.optimisation_engine.compute_dispatch_from_15min_prices,
impact_lambda=...).

Estimator (v1): for the station node n at 15-min timestamp t,

    price_dev[t] = node_price[n,t] − fleet_mean_price[t]        (same day+slot)
    price_dev[t] = Σ_h α_h·1[hour(t)=h] + c·net_mw[t] + ε[t]    (OLS per regime)

with net_mw = actual_dispatch_mw from ops_bess_dispatch_15min (positive =
discharge). The regression is fit separately on discharge slots (net > 0)
and charge slots (net < 0) — the price response sign flips with the regime.
Reported λ = −c, so a positive λ means "discharge depresses the node price /
charging lifts it". The cross-sectional fleet demeaning removes market-wide
shocks; the hour dummies remove the diurnal pattern.

IDENTIFICATION CAVEAT — read before using λ: dispatch responds to price
(reverse causality — the station tends to discharge when the price is high),
so v1 λ is the REDUCED-FORM magnitude, NOT the causal supply-shift
coefficient. When the dispatch-follows-price channel dominates, c comes out
positive and λ negative: the honest reading is "no usable impact signal" and
the PF consumer (nodal_pf_daily) falls back to price-taker for that station.
When λ > 0 it is the right magnitude for internal planning ("if we discharge
X MW harder, our clearing price moves about λ·X") — do not read it as a
structural elasticity.

Frames (hard invariant): md_mengxi_nodal_price_96_filled.metric_time and
ops_bess_dispatch_15min.interval_start are both CST-NAIVE — naive CST window
literals as plain bound parameters (PG coerces the unknown-typed parameter
to TIMESTAMP, the same pattern as nodal_pf_daily; the same SQL also runs on
the SQLite test harness, where CAST(:s AS timestamp) would take NUMERIC
affinity and silently compare wrong). No '+08' suffix and no AT TIME ZONE
rendering anywhere.

Node choice: the station's first registry price node with filled-table rows
in the window (own node preferred — registry order), the same resolution
rule as book_forecast. Stations with no node candidates at all (wuhai, the
wind books) skip loudly. Fleet mean: AVG(avg_node_price) over ALL filled-
table nodes at the same metric_time — the station's own node is included;
at the 蒙西 node count its 1/N weight attenuates λ negligibly and the
exclusion join is not worth it.
"""
from __future__ import annotations

from datetime import date, datetime

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

from libs.trading_book.registry import get_station
from services.mengxi_nodal.book_forecast import _candidate_nodes, _history_window

_LOOKBACK_DAYS = 90
_MIN_REGIME_OBS = 30  # below this a regime's λ is not identified -> None

_SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS reports"

_DDL = """
CREATE TABLE IF NOT EXISTS reports.station_market_impact (
    asset_code       TEXT        NOT NULL,
    estimated_at     TIMESTAMPTZ NOT NULL,
    lambda_discharge NUMERIC,
    lambda_charge    NUMERIC,
    n_obs            INTEGER     NOT NULL,
    r2               NUMERIC,
    se_discharge     NUMERIC,
    se_charge        NUMERIC,
    lookback_days    INTEGER     NOT NULL,
    PRIMARY KEY (asset_code, estimated_at)
)
"""

_UPSERT = text("""
    INSERT INTO reports.station_market_impact
        (asset_code, estimated_at, lambda_discharge, lambda_charge,
         n_obs, r2, se_discharge, se_charge, lookback_days)
    VALUES (:asset_code, :estimated_at, :lambda_discharge, :lambda_charge,
            :n_obs, :r2, :se_discharge, :se_charge, :lookback_days)
    ON CONFLICT (asset_code, estimated_at)
    DO UPDATE SET lambda_discharge = EXCLUDED.lambda_discharge,
                  lambda_charge    = EXCLUDED.lambda_charge,
                  n_obs            = EXCLUDED.n_obs,
                  r2               = EXCLUDED.r2,
                  se_discharge     = EXCLUDED.se_discharge,
                  se_charge        = EXCLUDED.se_charge,
                  lookback_days    = EXCLUDED.lookback_days
""")

_NODE_PROBE_SQL = """
    SELECT DISTINCT node_name
    FROM marketdata.md_mengxi_nodal_price_96_filled
    WHERE node_name IN :names
      AND metric_time >= :s
      AND metric_time <  :e
"""

# Node price + cross-sectional fleet mean in one round trip.
_PRICE_SQL = """
    SELECT n.metric_time   AS metric_time,
           n.avg_node_price AS node_price,
           f.fleet_price   AS fleet_price
    FROM marketdata.md_mengxi_nodal_price_96_filled n
    JOIN (
        SELECT metric_time, AVG(avg_node_price) AS fleet_price
        FROM marketdata.md_mengxi_nodal_price_96_filled
        WHERE metric_time >= :s AND metric_time < :e
        GROUP BY metric_time
    ) f ON f.metric_time = n.metric_time
    WHERE n.node_name = :node
      AND n.metric_time >= :s
      AND n.metric_time <  :e
    ORDER BY n.metric_time
"""

_DISPATCH_SQL = """
    SELECT interval_start, actual_dispatch_mw
    FROM marketdata.ops_bess_dispatch_15min
    WHERE asset_code = :ac
      AND interval_start >= :s
      AND interval_start <  :e
    ORDER BY interval_start
"""

# PG-and-SQLite-portable "latest per asset" (no DISTINCT ON).
_LATEST_SQL = """
    SELECT m.asset_code, m.lambda_discharge, m.lambda_charge, m.estimated_at
    FROM reports.station_market_impact m
    JOIN (
        SELECT asset_code, MAX(estimated_at) AS mx
        FROM reports.station_market_impact
        GROUP BY asset_code
    ) t ON t.asset_code = m.asset_code AND t.mx = m.estimated_at
"""


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        if engine.dialect.name == "postgresql":
            conn.execute(text(_SCHEMA_DDL))
        conn.execute(text(_DDL))


def _resolve_node(engine, candidates: list[str], s: str, e: str) -> str | None:
    """First candidate (registry priority order — own node first) with any
    filled-table rows in the window; None when none of them have data."""
    if not candidates:
        return None
    q = text(_NODE_PROBE_SQL).bindparams(bindparam("names", expanding=True))
    with engine.connect() as conn:
        present = {r[0] for r in conn.execute(q, {"names": candidates, "s": s, "e": e})}
    return next((n for n in candidates if n in present), None)


def _ols_impact(price_dev, net_mw, hour) -> tuple[float, float, float, int] | None:
    """One regime's OLS: price_dev ~ hour dummies (saturated, no intercept)
    + net_mw. Returns (c, se_c, r2, n) with c the raw coefficient on net_mw,
    or None when the regime is unidentified (too few observations or no
    dispatch variance)."""
    y = np.asarray(price_dev, dtype=float)
    x = np.asarray(net_mw, dtype=float)
    h = np.asarray(hour, dtype=int)
    ok = np.isfinite(y) & np.isfinite(x) & (h >= 0) & (h <= 23)
    y, x, h = y[ok], x[ok], h[ok]
    n = int(y.size)
    if n < _MIN_REGIME_OBS or float(np.var(x)) <= 0.0:
        return None
    H = np.zeros((n, 24))
    H[np.arange(n), h] = 1.0
    X = np.column_stack([H[:, H.sum(axis=0) > 0], x])  # drop empty-hour dummies
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    sse = float(resid @ resid)
    sst = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - sse / sst if sst > 0 else 0.0
    dof = max(n - X.shape[1], 1)
    cov = (sse / dof) * np.linalg.pinv(X.T @ X)
    se = float(np.sqrt(max(float(cov[-1, -1]), 0.0)))
    return float(beta[-1]), se, r2, n


def estimate_from_frames(price_df: pd.DataFrame, dispatch_df: pd.DataFrame) -> dict:
    """Fit the per-regime impact OLS on already-loaded frames.

    price_df:    metric_time, node_price, fleet_price (same day+slot basis).
    dispatch_df: interval_start, actual_dispatch_mw (positive = discharge).

    Returns {n_obs, per_regime: {charge, discharge}, se_charge,
    se_discharge, r2_charge, r2_discharge, n_charge, n_discharge}; each λ is
    −(raw coefficient), so positive = discharge depresses the node price.
    Regimes that fail the identification gate stay None. Idle slots
    (net == 0) carry no impact and are excluded from both regimes.
    """
    p = price_df.copy()
    p["metric_time"] = pd.to_datetime(p["metric_time"])
    d = dispatch_df.copy()
    d["interval_start"] = pd.to_datetime(d["interval_start"])
    df = p.merge(d, left_on="metric_time", right_on="interval_start", how="inner")
    df["price_dev"] = df["node_price"].astype(float) - df["fleet_price"].astype(float)
    df["net_mw"] = df["actual_dispatch_mw"].astype(float)
    df["hour"] = df["metric_time"].dt.hour

    out: dict = {"n_obs": 0,
                 "per_regime": {"charge": None, "discharge": None},
                 "se_discharge": None, "se_charge": None,
                 "r2_discharge": None, "r2_charge": None,
                 "n_discharge": 0, "n_charge": 0}
    for regime, mask in (("discharge", df["net_mw"] > 0),
                         ("charge", df["net_mw"] < 0)):
        g = df[mask]
        fit = _ols_impact(g["price_dev"], g["net_mw"], g["hour"])
        if fit is None:
            continue
        c, se, r2, n = fit
        out[f"n_{regime}"] = n
        out["per_regime"][regime] = -c
        out[f"se_{regime}"] = se
        out[f"r2_{regime}"] = r2
    out["n_obs"] = out["n_discharge"] + out["n_charge"]
    return out


def estimate_lambda(engine, asset_code: str, lookback_days: int = _LOOKBACK_DAYS,
                    as_of: date | None = None) -> dict:
    """Estimate one station's market-impact λ over the trailing
    lookback_days ending at as_of (default today; window [as_of−lookback,
    as_of), naive CST).

    Returns {lambda_cny_per_mwh_per_mw (headline = discharge regime), n_obs,
    r2, se, per_regime: {charge, discharge}, se_/r2_/n_ per regime,
    asset_code, node, as_of, lookback_days, reason}. Loud-skip contract:
    missing node / prices / dispatch, or both regimes unidentified, returns
    lambda_cny_per_mwh_per_mw = None with a human-readable reason (and a
    WARNING on stdout for the hard failures). Unknown asset_code -> KeyError
    (registry).
    """
    station = get_station(asset_code)
    end = as_of or date.today()
    s, e = _history_window(end, lookback_days)
    out: dict = {"asset_code": asset_code, "node": None, "as_of": end.isoformat(),
                 "lookback_days": lookback_days,
                 "lambda_cny_per_mwh_per_mw": None,
                 "n_obs": 0, "r2": None, "se": None,
                 "per_regime": {"charge": None, "discharge": None},
                 "se_discharge": None, "se_charge": None,
                 "r2_discharge": None, "r2_charge": None,
                 "n_discharge": 0, "n_charge": 0, "reason": None}

    def skip(reason: str) -> dict:
        print(f"WARNING market_impact: {reason}", flush=True)
        return {**out, "reason": reason}

    node = _resolve_node(engine, _candidate_nodes(station), s, e)
    if node is None:
        return skip(f"{asset_code}: no registry price node with "
                    f"filled-table rows in window [{s}, {e})")
    out["node"] = node

    price_df = pd.read_sql(text(_PRICE_SQL), engine,
                           params={"node": node, "s": s, "e": e})
    if price_df.empty:
        return skip(f"{asset_code}: node {node} has no filled-table prices "
                    f"in window [{s}, {e})")

    disp_df = pd.read_sql(text(_DISPATCH_SQL), engine,
                          params={"ac": asset_code, "s": s, "e": e})
    if disp_df.empty:
        return skip(f"{asset_code}: no ops_bess_dispatch_15min rows "
                    f"in window [{s}, {e})")

    out.update(estimate_from_frames(price_df, disp_df))
    lam_d = out["per_regime"]["discharge"]
    out["lambda_cny_per_mwh_per_mw"] = lam_d
    out["r2"] = out["r2_discharge"]
    out["se"] = out["se_discharge"]
    if lam_d is None and out["per_regime"]["charge"] is None:
        return skip(f"{asset_code}: both regimes unidentified "
                    f"(n_discharge={out['n_discharge']}, "
                    f"n_charge={out['n_charge']}, min {_MIN_REGIME_OBS})")
    if lam_d is None:
        out["reason"] = (f"{asset_code}: discharge regime unidentified "
                         f"(n={out['n_discharge']}, min {_MIN_REGIME_OBS}); "
                         f"charge λ available")
    return out


def persist_lambda(engine, asset_code: str, lookback_days: int = _LOOKBACK_DAYS,
                   as_of: date | None = None,
                   estimated_at: datetime | None = None) -> dict:
    """Estimate λ for one station and upsert it into
    reports.station_market_impact (PK asset_code + estimated_at).

    Nothing is written when no regime identified — the loud-skip reason is
    in the returned dict, the latest-read keeps any previous estimate, and
    stations with no estimate at all stay price-taker in nodal_pf_daily.
    estimated_at defaults to now; pass it explicitly for idempotent re-runs.
    """
    est = estimate_lambda(engine, asset_code, lookback_days=lookback_days,
                          as_of=as_of)
    if est["per_regime"]["discharge"] is None and est["per_regime"]["charge"] is None:
        return est
    ensure_table(engine)
    with engine.begin() as conn:
        conn.execute(_UPSERT, {
            "asset_code": asset_code,
            "estimated_at": estimated_at or datetime.now(),
            "lambda_discharge": est["per_regime"]["discharge"],
            "lambda_charge": est["per_regime"]["charge"],
            "n_obs": est["n_obs"],
            "r2": est["r2_discharge"],
            "se_discharge": est["se_discharge"],
            "se_charge": est["se_charge"],
            "lookback_days": lookback_days,
        })
    return est


def get_latest_lambdas(engine) -> dict[str, dict]:
    """Latest persisted estimate per asset_code ->
    {lambda_discharge, lambda_charge, estimated_at} (λs as floats, None when
    the regime was not identified). Creates the table when absent (IF NOT
    EXISTS) so batch readers (nodal_pf_daily) survive a never-estimated
    deployment."""
    ensure_table(engine)
    df = pd.read_sql(text(_LATEST_SQL), engine)
    out: dict[str, dict] = {}
    for row in df.itertuples():
        out[row.asset_code] = {
            "lambda_discharge": (None if row.lambda_discharge is None
                                 else float(row.lambda_discharge)),
            "lambda_charge": (None if row.lambda_charge is None
                              else float(row.lambda_charge)),
            "estimated_at": row.estimated_at,
        }
    return out
