"""Per-station market-impact coefficient (λ) estimation — 蒙西 book (plan task 13).

Economic model: when the station discharges Q MW at its node, its clearing
price moves to p − λ_d·Q; when it charges Q MW, to p + λ_c·Q (λ in CNY/MWh
per MW, expected ≥ 0). The nodal PF engine consumes the same rule
(services.bess_map.optimisation_engine.compute_dispatch_from_15min_prices,
impact_lambda=...).

Estimator (v2 — instrumented): for the station node n at 15-min timestamp t,

    price_dev[t] = node_price[n,t] − fleet_mean_price[t]        (same day+slot)
    stage 1:     net_mw[t]    = Σ_h γ_h·1[hour(t)=h] + π·nominated_mw[t] + u[t]
    stage 2:     price_dev[t] = Σ_h α_h·1[hour(t)=h] + c·net_mw_hat[t] + ε[t]

per regime (discharge slots net > 0, charge slots net < 0). net_mw =
actual_dispatch_mw from md_id_cleared_energy (positive = discharge). The
instrument nominated_mw is the operator's D-1 申报计划
(marketdata.bess_nomination_96, from the WeCom 调度计划表 workbooks):
decided before RT trading, it shifts dispatch without responding to the
contemporaneous RT price deviation — breaking the reverse causality that
made the v1 OLS (price_dev ~ net_mw) unusable on the discharge side.
Reported λ = −c, so a positive λ means "discharge depresses the node price /
charging lifts it". The cross-sectional fleet demeaning removes market-wide
shocks; the hour dummies (both stages) remove the diurnal pattern.

Weak-instrument gate: a regime's IV estimate is reported only when the
first-stage partial F on nominated_mw ≥ 10 (Stock–Yogo floor); otherwise
the regime is honestly unidentified and the PF consumer (nodal_pf_daily)
stays price-taker for that station. The v1 OLS coefficients are still
computed and persisted as diagnostics (lambda_*_ols columns) — never as
the headline, since discharge-side OLS carries the reverse-causality bias
(dispatch follows price → positive c → negative λ). IV λ > 0 is the
planning magnitude: "if we discharge X MW harder, our clearing price moves
about λ·X".

Frames (hard invariant): md_mengxi_nodal_price_96_filled.metric_time and
md_id_cleared_energy.datetime are both CST-NAIVE — naive CST window
literals as plain bound parameters (PG coerces the unknown-typed parameter
to TIMESTAMP, the same pattern as nodal_pf_daily; the same SQL also runs on
the SQLite test harness, where CAST(:s AS timestamp) would take NUMERIC
affinity and silently compare wrong). No '+08' suffix and no AT TIME ZONE
rendering anywhere. bess_nomination_96 stores (data_date, slot) — the
joinable timestamp is rebuilt in pandas (nomination_curves.get_nominations),
never in SQL.

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
from services.mengxi_nodal.nomination_curves import (
    ensure_table as _ensure_nomination_table,
    get_nominations,
)

_LOOKBACK_DAYS = 90
_MIN_REGIME_OBS = 30   # below this a regime's λ is not identified -> None
_MIN_FIRST_STAGE_F = 10.0  # Stock–Yogo weak-instrument floor on the 1st stage

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
    method               TEXT,
    lambda_discharge_ols NUMERIC,
    lambda_charge_ols    NUMERIC,
    f_discharge          NUMERIC,
    f_charge             NUMERIC,
    PRIMARY KEY (asset_code, estimated_at)
)
"""

# Idempotent column adds for tables created by v1 (PG only — SQLite test
# harnesses always create the full v2 shape above).
_ALTER_COLUMNS = (
    "ALTER TABLE reports.station_market_impact ADD COLUMN IF NOT EXISTS method TEXT",
    "ALTER TABLE reports.station_market_impact ADD COLUMN IF NOT EXISTS lambda_discharge_ols NUMERIC",
    "ALTER TABLE reports.station_market_impact ADD COLUMN IF NOT EXISTS lambda_charge_ols NUMERIC",
    "ALTER TABLE reports.station_market_impact ADD COLUMN IF NOT EXISTS f_discharge NUMERIC",
    "ALTER TABLE reports.station_market_impact ADD COLUMN IF NOT EXISTS f_charge NUMERIC",
)

_UPSERT = text("""
    INSERT INTO reports.station_market_impact
        (asset_code, estimated_at, lambda_discharge, lambda_charge,
         n_obs, r2, se_discharge, se_charge, lookback_days,
         method, lambda_discharge_ols, lambda_charge_ols,
         f_discharge, f_charge)
    VALUES (:asset_code, :estimated_at, :lambda_discharge, :lambda_charge,
            :n_obs, :r2, :se_discharge, :se_charge, :lookback_days,
            :method, :lambda_discharge_ols, :lambda_charge_ols,
            :f_discharge, :f_charge)
    ON CONFLICT (asset_code, estimated_at)
    DO UPDATE SET lambda_discharge = EXCLUDED.lambda_discharge,
                  lambda_charge    = EXCLUDED.lambda_charge,
                  n_obs            = EXCLUDED.n_obs,
                  r2               = EXCLUDED.r2,
                  se_discharge     = EXCLUDED.se_discharge,
                  se_charge        = EXCLUDED.se_charge,
                  lookback_days    = EXCLUDED.lookback_days,
                  method               = EXCLUDED.method,
                  lambda_discharge_ols = EXCLUDED.lambda_discharge_ols,
                  lambda_charge_ols    = EXCLUDED.lambda_charge_ols,
                  f_discharge          = EXCLUDED.f_discharge,
                  f_charge             = EXCLUDED.f_charge
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

# Dispatch source: md_id_cleared_energy (market-cleared dispatch — current,
# all stations; ops_bess_dispatch_15min stopped 2026-04).
# UNIT CONTRACT (verified 2026-11-05 against nomination files + fleet-total
# sheet + settlement bills): the exchange's per-plant 中标电量 is the interval
# AVERAGE POWER in MW — cleared_energy_mwh is MW despite the name; negative =
# charging. Use it as MW directly (no ×4). True interval energy = value × 0.25.
_DISPATCH_SQL = """
    SELECT datetime AS interval_start, cleared_energy_mwh AS actual_dispatch_mw
    FROM marketdata.md_id_cleared_energy
    WHERE plant_name LIKE :pn
      AND datetime >= :s
      AND datetime < :e
    ORDER BY datetime
"""

# PG-and-SQLite-portable "latest per asset" (no DISTINCT ON).
_LATEST_SQL = """
    SELECT m.asset_code, m.lambda_discharge, m.lambda_charge, m.estimated_at,
           m.method
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
            for stmt in _ALTER_COLUMNS:
                conn.execute(text(stmt))
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


def _hour_dummies(h: np.ndarray) -> np.ndarray:
    """Saturated hour dummies (no intercept), empty hours dropped."""
    n = h.size
    H = np.zeros((n, 24))
    H[np.arange(n), h] = 1.0
    return H[:, H.sum(axis=0) > 0]


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
    H = _hour_dummies(h)
    X = np.column_stack([H, x])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    sse = float(resid @ resid)
    sst = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - sse / sst if sst > 0 else 0.0
    dof = max(n - X.shape[1], 1)
    cov = (sse / dof) * np.linalg.pinv(X.T @ X)
    se = float(np.sqrt(max(float(cov[-1, -1]), 0.0)))
    return float(beta[-1]), se, r2, n


def _iv_impact(price_dev, net_mw, hour, instr) -> tuple[float, float, float, int, float] | None:
    """One regime's 2SLS: net_mw instrumented by nominated_mw (the D-1
    operator nomination), hour dummies in both stages.

    Returns (c_iv, se_c, r2, n, first_stage_f) with c_iv the IV coefficient
    on net_mw, or None when the regime is unidentified: too few obs, no
    dispatch/instrument variance, or a weak first stage (partial F on the
    instrument < _MIN_FIRST_STAGE_F, Stock–Yogo floor). r2 uses residuals
    against ACTUAL net_mw (can be low/negative — diagnostic only)."""
    y = np.asarray(price_dev, dtype=float)
    x = np.asarray(net_mw, dtype=float)
    h = np.asarray(hour, dtype=int)
    z = np.asarray(instr, dtype=float)
    ok = (np.isfinite(y) & np.isfinite(x) & np.isfinite(z)
          & (h >= 0) & (h <= 23))
    y, x, h, z = y[ok], x[ok], h[ok], z[ok]
    n = int(y.size)
    if n < _MIN_REGIME_OBS or float(np.var(x)) <= 0.0 or float(np.var(z)) <= 0.0:
        return None
    H = _hour_dummies(h)

    # First stage: x ~ H + z. Partial F on z vs the restricted x ~ H fit.
    Zu = np.column_stack([H, z])
    gamma, *_ = np.linalg.lstsq(Zu, x, rcond=None)
    x_hat = Zu @ gamma
    sse_u = float((x - x_hat) @ (x - x_hat))
    g_r, *_ = np.linalg.lstsq(H, x, rcond=None)
    resid_r = x - H @ g_r
    sse_r = float(resid_r @ resid_r)
    dof_u = max(n - Zu.shape[1], 1)
    f_stat = ((sse_r - sse_u) / 1.0) / (sse_u / dof_u) if sse_u > 0 else np.inf
    if not np.isfinite(f_stat) or f_stat < _MIN_FIRST_STAGE_F:
        return None

    # Second stage: y ~ H + x_hat; SEs from residuals on ACTUAL x.
    Xh = np.column_stack([H, x_hat])
    beta, *_ = np.linalg.lstsq(Xh, y, rcond=None)
    Xa = np.column_stack([H, x])
    resid = y - Xa @ beta
    sse = float(resid @ resid)
    sst = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - sse / sst if sst > 0 else 0.0
    dof = max(n - Xh.shape[1], 1)
    cov = (sse / dof) * np.linalg.pinv(Xh.T @ Xh)
    se = float(np.sqrt(max(float(cov[-1, -1]), 0.0)))
    return float(beta[-1]), se, r2, n, float(f_stat)


def estimate_from_frames(price_df: pd.DataFrame, dispatch_df: pd.DataFrame,
                         nomination_df: pd.DataFrame | None = None) -> dict:
    """Fit the per-regime impact estimators on already-loaded frames.

    price_df:      metric_time, node_price, fleet_price (same day+slot basis).
    dispatch_df:   interval_start, actual_dispatch_mw (positive = discharge).
    nomination_df: interval_start, nominated_mw — optional; when given and
    the instrument passes the weak-instrument gate, the per-regime IV
    estimate (2SLS) becomes the headline λ.

    Returns {n_obs, per_regime: {charge, discharge} (OLS), per_regime_iv,
    method_{charge,discharge}, se_/r2_/n_ per regime (OLS), iv se/f/n per
    regime}. Each λ is −(raw coefficient), so positive = discharge depresses
    the node price. Regimes that fail identification stay None. Idle slots
    (net == 0) carry no impact and are excluded from both regimes.
    """
    p = price_df.copy()
    p["metric_time"] = pd.to_datetime(p["metric_time"])
    d = dispatch_df.copy()
    d["interval_start"] = pd.to_datetime(d["interval_start"])
    df = p.merge(d, left_on="metric_time", right_on="interval_start", how="inner")
    if nomination_df is not None and not nomination_df.empty:
        n_df = nomination_df.copy()
        n_df["interval_start"] = pd.to_datetime(n_df["interval_start"])
        df = df.merge(n_df, on="interval_start", how="left")
    else:
        df["nominated_mw"] = np.nan
    df["price_dev"] = df["node_price"].astype(float) - df["fleet_price"].astype(float)
    df["net_mw"] = df["actual_dispatch_mw"].astype(float)
    df["hour"] = df["metric_time"].dt.hour

    out: dict = {"n_obs": 0,
                 "per_regime": {"charge": None, "discharge": None},
                 "per_regime_iv": {"charge": None, "discharge": None},
                 "method_charge": None, "method_discharge": None,
                 "se_discharge": None, "se_charge": None,
                 "r2_discharge": None, "r2_charge": None,
                 "n_discharge": 0, "n_charge": 0,
                 "se_iv_discharge": None, "se_iv_charge": None,
                 "r2_iv_discharge": None, "r2_iv_charge": None,
                 "f_discharge": None, "f_charge": None,
                 "n_iv_discharge": 0, "n_iv_charge": 0}
    for regime, mask in (("discharge", df["net_mw"] > 0),
                         ("charge", df["net_mw"] < 0)):
        g = df[mask]
        fit = _ols_impact(g["price_dev"], g["net_mw"], g["hour"])
        if fit is not None:
            c, se, r2, n = fit
            out[f"n_{regime}"] = n
            out["per_regime"][regime] = -c
            out[f"se_{regime}"] = se
            out[f"r2_{regime}"] = r2
        iv = _iv_impact(g["price_dev"], g["net_mw"], g["hour"],
                        g["nominated_mw"])
        if iv is not None:
            c_iv, se_iv, r2_iv, n_iv, f_iv = iv
            out["per_regime_iv"][regime] = -c_iv
            out[f"se_iv_{regime}"] = se_iv
            out[f"r2_iv_{regime}"] = r2_iv
            out[f"f_{regime}"] = f_iv
            out[f"n_iv_{regime}"] = n_iv
            out[f"method_{regime}"] = "iv_nomination"
    out["n_obs"] = out["n_discharge"] + out["n_charge"]
    return out


def estimate_lambda(engine, asset_code: str, lookback_days: int = _LOOKBACK_DAYS,
                    as_of: date | None = None) -> dict:
    """Estimate one station's market-impact λ over the trailing
    lookback_days ending at as_of (default today; window [as_of−lookback,
    as_of), naive CST).

    The headline lambda_cny_per_mwh_per_mw is the discharge-regime IV
    estimate (nomination-instrumented 2SLS); it is None when the instrument
    is absent or weak — the honest "no usable signal" state, with the
    reduced-form OLS kept in per_regime as a diagnostic only.

    Returns {lambda_cny_per_mwh_per_mw, n_obs, r2, se, per_regime (OLS),
    per_regime_iv, method_/f_/se_iv_/n_iv_ per regime, asset_code, node,
    as_of, lookback_days, reason}. Loud-skip contract: missing node /
    prices / dispatch returns a human-readable reason (and a WARNING on
    stdout for the hard failures). Unknown asset_code -> KeyError
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
                 "per_regime_iv": {"charge": None, "discharge": None},
                 "method_charge": None, "method_discharge": None,
                 "se_discharge": None, "se_charge": None,
                 "r2_discharge": None, "r2_charge": None,
                 "n_discharge": 0, "n_charge": 0,
                 "se_iv_discharge": None, "se_iv_charge": None,
                 "f_discharge": None, "f_charge": None,
                 "n_iv_discharge": 0, "n_iv_charge": 0, "reason": None}

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
                          params={"pn": f"%{station.plant_name}%", "s": s, "e": e})
    if disp_df.empty:
        return skip(f"{asset_code}: no cleared-dispatch rows for "
                    f"{station.plant_name} in window [{s}, {e})")

    _ensure_nomination_table(engine)
    nom_df = get_nominations(engine, asset_code,
                             date.fromisoformat(s[:10]), end)

    out.update(estimate_from_frames(price_df, disp_df, nom_df))
    lam_d = out["per_regime_iv"]["discharge"]
    out["lambda_cny_per_mwh_per_mw"] = lam_d
    out["r2"] = out["r2_discharge"]   # OLS r2 kept as the fit diagnostic
    out["se"] = out["se_iv_discharge"]
    if lam_d is None and out["per_regime_iv"]["charge"] is None:
        ols_note = ("OLS diagnostics only (reverse-causality biased): "
                    f"discharge {out['per_regime']['discharge']}, "
                    f"charge {out['per_regime']['charge']}")
        if out["n_iv_discharge"] == 0 and out["n_iv_charge"] == 0:
            return skip(f"{asset_code}: no usable nomination instrument in "
                        f"window (n_iv=0 both regimes); {ols_note}")
        return skip(f"{asset_code}: weak nomination instrument both regimes "
                    f"(f_discharge={out['f_discharge']}, "
                    f"f_charge={out['f_charge']}, floor "
                    f"{_MIN_FIRST_STAGE_F}); {ols_note}")
    if lam_d is None:
        out["reason"] = (f"{asset_code}: discharge regime IV unidentified "
                         f"(f={out['f_discharge']}, floor "
                         f"{_MIN_FIRST_STAGE_F}); charge IV available")
    return out


def persist_lambda(engine, asset_code: str, lookback_days: int = _LOOKBACK_DAYS,
                   as_of: date | None = None,
                   estimated_at: datetime | None = None) -> dict:
    """Estimate λ for one station and upsert it into
    reports.station_market_impact (PK asset_code + estimated_at).

    lambda_discharge/lambda_charge carry the IV estimates (None when
    unidentified — PF stays price-taker); the OLS reduced forms and
    first-stage F stats ride along as diagnostics. Nothing is written when
    no estimate of any kind identified — the loud-skip reason is in the
    returned dict, the latest-read keeps any previous estimate.
    estimated_at defaults to now; pass it explicitly for idempotent re-runs.
    """
    est = estimate_lambda(engine, asset_code, lookback_days=lookback_days,
                          as_of=as_of)
    any_estimate = any(est[k] is not None
                       for k in ("se_discharge", "se_charge",
                                 "se_iv_discharge", "se_iv_charge"))
    if not any_estimate:
        return est
    method = "+".join(sorted({m for m in (est["method_discharge"],
                                          est["method_charge"]) if m})) or None
    ensure_table(engine)
    with engine.begin() as conn:
        conn.execute(_UPSERT, {
            "asset_code": asset_code,
            "estimated_at": estimated_at or datetime.now(),
            "lambda_discharge": est["per_regime_iv"]["discharge"],
            "lambda_charge": est["per_regime_iv"]["charge"],
            "n_obs": est["n_obs"],
            "r2": est["r2_discharge"],
            "se_discharge": est["se_iv_discharge"],
            "se_charge": est["se_iv_charge"],
            "lookback_days": lookback_days,
            "method": method,
            "lambda_discharge_ols": est["per_regime"]["discharge"],
            "lambda_charge_ols": est["per_regime"]["charge"],
            "f_discharge": est["f_discharge"],
            "f_charge": est["f_charge"],
        })
    return est


def get_latest_lambdas(engine) -> dict[str, dict]:
    """Latest persisted estimate per asset_code ->
    {lambda_discharge, lambda_charge, estimated_at, method} (λs as floats,
    None when the regime was not identified). Creates the table when absent
    (IF NOT EXISTS) so batch readers (nodal_pf_daily) survive a
    never-estimated deployment."""
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
            "method": row.method,
        }
    return out
