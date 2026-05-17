"""GB BESS nightly pricing batch.

Computes three models for each BESS asset in the top-50 wholesale leaderboard
and writes results to intl_market.gb_pricing_results:

  1. Options value  — Kirk/Margrabe spread call strip (£/MW, annual horizon)
  2. PF actual DA   — perfect-foresight dispatch P&L on actual EPEX DA prices
  3. PF forecast DA — perfect-foresight dispatch P&L on OLS+fundamentals price forecast

Scheduled at 04:30 SGT (after market-data ingestion at 03:00).
Can also be triggered manually from the Data Management tab.

GB-specific parameters:
  - Settlement periods: 48 × 30-min per day (SP 1 = 00:00–00:30)
  - Peak: EPEX standard peakload product — Mon–Fri 08:00–20:00 (SP 17–40 inclusive)
  - Offpeak: Mon–Fri 00:00–08:00 and 20:00–24:00 (SP 1–16, 41–48); weekends are offpeak-only
  - The pre-computed daily_peakload / daily_offpeak columns from gb_epex_da_hh are used
    directly for Kirk calibration (daily_peakload is NULL on weekends/holidays, which are
    automatically excluded from the peak series).
  - Currency: £/MWh (models are currency-agnostic)
"""
from __future__ import annotations

import json
import logging
import math
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS intl_market.gb_pricing_results (
    settlement_date          DATE    NOT NULL,
    asset_name               TEXT    NOT NULL,
    power_mw                 NUMERIC,
    duration_h               NUMERIC,
    options_value_gbp_per_mw NUMERIC,
    pf_actual_da_pnl_gbp     NUMERIC,
    pf_forecast_da_pnl_gbp   NUMERIC,
    pf_actual_dispatch_48    JSONB,
    pf_forecast_dispatch_48  JSONB,
    actual_epex_48           JSONB,
    forecast_epex_48         JSONB,
    computed_at              TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (settlement_date, asset_name)
);
"""

# EPEX standard peakload: Mon–Fri 08:00–20:00 = SP 17–40 inclusive (1-indexed)
# Used as a fallback when daily_peakload column is unavailable.
_PEAK_SPS = set(range(17, 41))


# ---------------------------------------------------------------------------
# Kirk/Margrabe spread call pricer (no external dependencies)
# ---------------------------------------------------------------------------

def _norm_cdf(x: float) -> float:
    return math.erfc(-x / math.sqrt(2)) / 2.0


def _margrabe_call(F1: float, F2_eff: float, sigma_s: float, T: float) -> float:
    if T <= 0.0 or sigma_s <= 0.0 or F2_eff <= 0.0:
        return max(F1 - F2_eff, 0.0)
    sq_T = math.sqrt(T)
    d1 = (math.log(F1 / F2_eff) + 0.5 * sigma_s ** 2 * T) / (sigma_s * sq_T)
    d2 = d1 - sigma_s * sq_T
    return F1 * _norm_cdf(d1) - F2_eff * _norm_cdf(d2)


def _ann_vol(series: pd.Series) -> float:
    s = series[series > 0].dropna()
    if len(s) < 5:
        return 0.35
    lr = np.log(s.values[1:] / s.values[:-1])
    return float(lr.std() * math.sqrt(252)) if len(lr) >= 5 else 0.35


def compute_options_value(
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
    epex_history_df: pd.DataFrame,
    n_days_remaining: int = 365,
    corr: float = 0.85,
    om_cost_gbp_per_mwh: float = 0.0,
) -> float:
    """Kirk/Margrabe spread call strip value (£) for a GB BESS asset.

    Calibrated from EPEX DA price history. Uses the pre-computed daily_peakload /
    daily_offpeak columns when available (EPEX standard: Mon–Fri 08:00–20:00 peak,
    weekends excluded from the peak series). Falls back to SP 17–40 grouping.
    Returns total strip value £ for the asset (not per MW).
    """
    if epex_history_df.empty or len(epex_history_df) < 48:
        return 0.0

    # Prefer pre-computed EPEX product columns (NULL on weekends → auto-excluded)
    if "daily_peakload" in epex_history_df.columns and "daily_offpeak" in epex_history_df.columns:
        daily = (
            epex_history_df
            .groupby("delivery_date")
            .agg(peak=("daily_peakload", "first"), offpeak=("daily_offpeak", "first"))
            .dropna(subset=["peak", "offpeak"])  # drops weekends / holidays
        )
    else:
        # Fallback: classify SPs — weekday filter not applied, so slightly less accurate
        df = epex_history_df.copy()
        df["is_peak"] = df["settlement_period"].isin(_PEAK_SPS)
        raw = (
            df.groupby(["delivery_date", "is_peak"])["price"]
            .mean()
            .unstack("is_peak")
            .dropna()
        )
        if True not in raw.columns or False not in raw.columns:
            return 0.0
        daily = raw.rename(columns={True: "peak", False: "offpeak"})

    if "peak" not in daily.columns or "offpeak" not in daily.columns or len(daily) < 5:
        return 0.0

    peak_forward = float(daily["peak"].mean())
    offpeak_forward = float(daily["offpeak"].mean())
    peak_vol = _ann_vol(daily["peak"])
    offpeak_vol = _ann_vol(daily["offpeak"])

    # Kirk substitution: F2_eff = offpeak_forward / eta + om_cost
    eta = roundtrip_eff
    F2_eff = offpeak_forward / eta + om_cost_gbp_per_mwh

    sigma_s = math.sqrt(
        max(0, peak_vol ** 2 - 2 * corr * peak_vol * offpeak_vol + offpeak_vol ** 2)
    )

    # q_max = MWh dischargeable per day = power_mw * duration_h
    q_max_mwh = power_mw * duration_h

    # Strip: sum of daily Margrabe calls over n_days_remaining
    strip_value = 0.0
    for i in range(1, n_days_remaining + 1):
        T = i / 252.0
        c = _margrabe_call(peak_forward, F2_eff, sigma_s, T)
        strip_value += c

    return strip_value * q_max_mwh  # £ total


# ---------------------------------------------------------------------------
# Perfect-foresight dispatch LP
# ---------------------------------------------------------------------------

def _pf_dispatch_48(
    prices_48: list[float],
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
) -> tuple[float, list[float]]:
    """Exact PF dispatch LP for 48 half-hourly SPs via scipy.linprog (HiGHS).

    Variables: [c_0..c_47, d_0..d_47, s_0..s_47]
      c_t = charge rate (MW from grid)
      d_t = discharge rate (MW to grid)
      s_t = SOC (MWh) at end of SP t
    Returns (pnl_gbp, dispatch_grid_mw list 48 values).
    """
    try:
        from scipy.optimize import linprog
    except ImportError:
        logger.warning("scipy not available; using greedy PF dispatch")
        return _pf_dispatch_greedy(prices_48, power_mw, duration_h, roundtrip_eff)

    n = 48
    dt = 0.5  # h per SP
    E = power_mw * duration_h
    eta = roundtrip_eff

    # Objective: min Σ p*c*dt - Σ p*d*dt  (maximise profit)
    c_obj = (
        [p * dt for p in prices_48]   # charge cost
        + [-p * dt for p in prices_48]  # discharge revenue (neg = maximise)
        + [0.0] * n                    # SOC — no direct cost
    )

    # SOC dynamics: s_t = s_{t-1} + eta*c_t*dt - d_t*dt
    # → eta*c_t*dt - d_t*dt - s_t + s_{t-1} = 0  (s_{-1} = 0)
    A_eq = np.zeros((n, 3 * n))
    b_eq = np.zeros(n)
    for t in range(n):
        A_eq[t, t] = eta * dt          # c_t
        A_eq[t, n + t] = -dt           # d_t (discharge depletes SOC)
        A_eq[t, 2 * n + t] = -1.0     # -s_t
        if t > 0:
            A_eq[t, 2 * n + t - 1] = 1.0  # +s_{t-1}
        # RHS = 0

    bounds = [(0, power_mw)] * n + [(0, power_mw)] * n + [(0, E)] * n

    res = linprog(c_obj, A_eq=A_eq, b_eq=b_eq, bounds=bounds, method="highs")
    if res.status != 0:
        logger.debug("PF LP did not converge (status %d); using greedy", res.status)
        return _pf_dispatch_greedy(prices_48, power_mw, duration_h, roundtrip_eff)

    c_vals = res.x[:n]
    d_vals = res.x[n:2 * n]
    dispatch = [float(d - c) for d, c in zip(d_vals, c_vals)]
    pnl = sum(p * dt * disp for p, disp in zip(prices_48, dispatch))
    return float(pnl), dispatch


def _pf_dispatch_greedy(
    prices_48: list[float],
    power_mw: float,
    duration_h: float,
    roundtrip_eff: float,
) -> tuple[float, list[float]]:
    """Greedy PF fallback: charge at cheapest SPs, discharge at priciest."""
    n = 48
    dt = 0.5
    E = power_mw * duration_h
    max_e_per_sp = power_mw * dt

    sorted_low = sorted(range(n), key=lambda i: prices_48[i])
    sorted_high = sorted(range(n), key=lambda i: -prices_48[i])

    dispatch = [0.0] * n
    charge_budget = E
    for i in sorted_low:
        if charge_budget <= 0:
            break
        e = min(max_e_per_sp, charge_budget)
        dispatch[i] -= e / dt  # negative = charging
        charge_budget -= e

    total_charged = sum(-d * dt for d in dispatch if d < 0)
    discharge_budget = total_charged * roundtrip_eff
    for i in sorted_high:
        if discharge_budget <= 0:
            break
        if dispatch[i] < 0:
            continue
        e = min(max_e_per_sp, discharge_budget)
        dispatch[i] += e / dt
        discharge_budget -= e

    pnl = sum(p * dt * d for p, d in zip(prices_48, dispatch))
    return float(pnl), dispatch


# ---------------------------------------------------------------------------
# OLS + fundamentals price forecast
# ---------------------------------------------------------------------------

def _build_features(
    prices_48: list[float],
    fuel_mix_row: pd.Series | None,
    sp_index: int,
) -> list[float]:
    """Feature vector for one SP: [price, sin, cos, wind, solar, gas, nuclear, imports, demand, 1]."""
    sp_norm = 2 * math.pi * sp_index / 48
    feats = [
        prices_48[sp_index] if prices_48 else 0.0,
        math.sin(sp_norm),
        math.cos(sp_norm),
    ]
    if fuel_mix_row is not None:
        feats += [
            float(fuel_mix_row.get("wind_mw") or 0.0),
            float(fuel_mix_row.get("solar_mw") or 0.0),
            float(fuel_mix_row.get("gas_mw") or 0.0),
            float(fuel_mix_row.get("nuclear_mw") or 0.0),
            float(fuel_mix_row.get("imports_mw") or 0.0),
            float(fuel_mix_row.get("demand_mw") or 0.0),
        ]
    else:
        feats += [0.0] * 6
    feats.append(1.0)  # intercept
    return feats


def compute_ols_forecast(
    target_date: date,
    conn,
    lookback_days: int = 60,
) -> list[float] | None:
    """OLS forecast of 48 EPEX DA prices for target_date.

    Features per SP: [lag_epex_price, sin(2π*sp/48), cos(2π*sp/48),
                      wind_mw, solar_mw, gas_mw, nuclear_mw, imports_mw, demand_mw, 1]
    Trains on last lookback_days of EPEX DA + fuel mix data.
    Returns list[float] of 48 forecasted prices, or None if insufficient data.
    """
    cutoff = target_date - timedelta(days=lookback_days)

    # Load training EPEX DA prices
    try:
        epex_df = pd.read_sql(
            "SELECT delivery_date, settlement_period, price "
            "FROM intl_market.gb_epex_da_hh "
            "WHERE delivery_date BETWEEN %s AND %s "
            "ORDER BY delivery_date, settlement_period",
            conn,
            params=(cutoff.isoformat(), (target_date - timedelta(days=1)).isoformat()),
        )
    except Exception as exc:
        logger.warning("OLS forecast: EPEX query failed: %s", exc)
        return None

    if epex_df.empty or len(epex_df["delivery_date"].unique()) < 10:
        logger.info("OLS forecast: insufficient EPEX data (<%d days)", 10)
        return None

    # Load fuel mix for training period
    try:
        fm_df = pd.read_sql(
            "SELECT settlement_date, settlement_period, "
            "wind_mw, solar_mw, gas_mw, nuclear_mw, imports_mw, demand_mw "
            "FROM intl_market.gb_fuel_mix "
            "WHERE settlement_date BETWEEN %s AND %s",
            conn,
            params=(cutoff.isoformat(), (target_date - timedelta(days=1)).isoformat()),
        )
        fm_df["settlement_date"] = pd.to_datetime(fm_df["settlement_date"]).dt.date
    except Exception:
        fm_df = pd.DataFrame()

    # Load fuel mix for target_date (to use as features in forecast)
    try:
        fm_target = pd.read_sql(
            "SELECT settlement_period, wind_mw, solar_mw, gas_mw, nuclear_mw, imports_mw, demand_mw "
            "FROM intl_market.gb_fuel_mix WHERE settlement_date = %s",
            conn,
            params=(target_date.isoformat(),),
        )
    except Exception:
        fm_target = pd.DataFrame()

    # Build training data per SP
    epex_df["delivery_date"] = pd.to_datetime(epex_df["delivery_date"]).dt.date
    epex_pivot = epex_df.pivot(index="delivery_date", columns="settlement_period", values="price")

    dates = sorted(epex_pivot.index)
    if len(dates) < 2:
        return None

    # For each day d (predicting day d prices using day d-1 as lag), build feature matrix
    X_rows: list[list[float]] = []
    y_rows: list[list[float]] = []

    for i in range(1, len(dates)):
        d_prev = dates[i - 1]
        d_cur  = dates[i]
        if d_cur not in epex_pivot.index or d_prev not in epex_pivot.index:
            continue

        prices_prev_48 = [float(epex_pivot.loc[d_prev].get(sp, 0.0) or 0.0) for sp in range(1, 49)]
        prices_cur_48  = [float(epex_pivot.loc[d_cur].get(sp, 0.0) or 0.0) for sp in range(1, 49)]

        row_feats: list[float] = []
        for sp in range(48):
            fm_row = None
            if not fm_df.empty:
                mask = (fm_df["settlement_date"] == d_cur) & (fm_df["settlement_period"] == sp + 1)
                if mask.any():
                    fm_row = fm_df[mask].iloc[0]
            row_feats.extend(_build_features(prices_prev_48, fm_row, sp))

        X_rows.append(row_feats)
        y_rows.append(prices_cur_48)

    if len(X_rows) < 10:
        return None

    X = np.array(X_rows, dtype=float)
    Y = np.array(y_rows, dtype=float)  # shape: (n_days, 48)

    # Fit OLS per SP (each SP gets its own linear model)
    n_feat_per_sp = len(_build_features([], None, 0))  # features per SP

    # Last day's prices as lag
    last_date = dates[-1]
    prices_lag_48 = [float(epex_pivot.loc[last_date].get(sp, 0.0) or 0.0) for sp in range(1, 49)]

    forecast = []
    for sp in range(48):
        col_start = sp * n_feat_per_sp
        col_end   = col_start + n_feat_per_sp
        X_sp = X[:, col_start:col_end]

        fm_row = None
        if not fm_target.empty:
            mask = fm_target["settlement_period"] == sp + 1
            if mask.any():
                fm_row = fm_target[mask].iloc[0]

        x_pred = np.array(_build_features(prices_lag_48, fm_row, sp))

        try:
            coef, _, _, _ = np.linalg.lstsq(X_sp, Y[:, sp], rcond=None)
            predicted = float(np.dot(x_pred, coef))
            forecast.append(max(0.0, predicted))  # clamp negative prices to 0 for safety
        except Exception:
            forecast.append(prices_lag_48[sp])  # fallback: use lag price

    return forecast


# ---------------------------------------------------------------------------
# Asset specs helpers
# ---------------------------------------------------------------------------

def _get_top_wholesale_assets(conn, ref_start: str, ref_end: str, top_n: int = 50) -> pd.DataFrame:
    """Top N BESS assets by wholesale revenue in date range, with rated_power and energy_capacity."""
    try:
        return pd.read_sql(
            "SELECT lb.asset, "
            "  AVG(lb.rated_power) AS power_mw, "
            "  AVG(ba_e.value::NUMERIC) AS energy_capacity_mwh "
            "FROM intl_market.gb_bess_leaderboard lb "
            "LEFT JOIN intl_market.gb_bess_assets ba_e "
            "  ON ba_e.asset = lb.asset AND ba_e.history_table = 'energy_capacity' "
            "WHERE lb.settlement_date BETWEEN %s AND %s AND lb.market = 'wholesale' "
            "GROUP BY lb.asset "
            "ORDER BY SUM(lb.revenue) DESC "
            "LIMIT %s",
            conn,
            params=(ref_start, ref_end, top_n),
        )
    except Exception as exc:
        logger.warning("Could not fetch top wholesale assets: %s", exc)
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Main batch runner
# ---------------------------------------------------------------------------

def _ensure_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(_CREATE_TABLE_SQL)


def run_pricing_batch(batch_date: date | None, conn) -> dict[str, Any]:
    """Compute pricing models for top-50 wholesale BESS assets.

    Returns a status dict: {"processed": n, "errors": [...], "date": str}.
    """
    _ensure_table(conn)

    if batch_date is None:
        batch_date = date.today() - timedelta(days=1)

    ref_start = (batch_date - timedelta(days=90)).isoformat()
    ref_end   = batch_date.isoformat()

    assets_df = _get_top_wholesale_assets(conn, ref_start, ref_end)
    if assets_df.empty:
        logger.info("Pricing batch: no assets found for %s", batch_date)
        return {"processed": 0, "errors": [], "date": str(batch_date)}

    # Load EPEX DA prices for calibration (60-day history)
    epex_cutoff = (batch_date - timedelta(days=60)).isoformat()
    try:
        epex_hist = pd.read_sql(
            "SELECT delivery_date, settlement_period, price, "
            "  daily_peakload, daily_offpeak "
            "FROM intl_market.gb_epex_da_hh "
            "WHERE delivery_date BETWEEN %s AND %s "
            "ORDER BY delivery_date, settlement_period",
            conn,
            params=(epex_cutoff, batch_date.isoformat()),
        )
    except Exception as exc:
        logger.error("Pricing batch: EPEX history query failed: %s", exc)
        return {"processed": 0, "errors": [str(exc)], "date": str(batch_date)}

    # Target day's EPEX DA prices (48 SPs)
    target_epex_48: list[float] = []
    if not epex_hist.empty:
        day_prices = epex_hist[pd.to_datetime(epex_hist["delivery_date"]).dt.date == batch_date]
        if not day_prices.empty:
            sp_map = dict(zip(day_prices["settlement_period"], day_prices["price"]))
            target_epex_48 = [float(sp_map.get(sp, 0.0) or 0.0) for sp in range(1, 49)]

    if len(target_epex_48) < 48:
        logger.info("Pricing batch: no EPEX DA prices for %s — filling zeros", batch_date)
        target_epex_48 = [0.0] * 48

    # OLS forecast
    forecast_48 = compute_ols_forecast(batch_date, conn) or target_epex_48

    processed = 0
    errors: list[str] = []

    for _, asset_row in assets_df.iterrows():
        asset_name = str(asset_row["asset"])
        power_mw   = float(asset_row.get("power_mw") or 100.0)
        e_cap      = float(asset_row.get("energy_capacity_mwh") or power_mw * 2.0)
        duration_h = e_cap / power_mw if power_mw > 0 else 2.0
        eff        = 0.85  # standard roundtrip efficiency

        try:
            # 1. Options value
            options_val = compute_options_value(
                power_mw=power_mw,
                duration_h=duration_h,
                roundtrip_eff=eff,
                epex_history_df=epex_hist,
                n_days_remaining=365,
            )
            options_per_mw = options_val / power_mw if power_mw > 0 else 0.0

            # 2. PF dispatch on actual DA prices
            pf_actual_pnl, pf_actual_dispatch = _pf_dispatch_48(
                target_epex_48, power_mw, duration_h, eff
            )

            # 3. PF dispatch on OLS forecast prices
            pf_forecast_pnl, pf_forecast_dispatch = _pf_dispatch_48(
                forecast_48, power_mw, duration_h, eff
            )

            # Write to DB
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO intl_market.gb_pricing_results
                    (settlement_date, asset_name, power_mw, duration_h,
                     options_value_gbp_per_mw, pf_actual_da_pnl_gbp,
                     pf_forecast_da_pnl_gbp, pf_actual_dispatch_48,
                     pf_forecast_dispatch_48, actual_epex_48, forecast_epex_48)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (settlement_date, asset_name) DO UPDATE SET
                    power_mw                 = EXCLUDED.power_mw,
                    duration_h               = EXCLUDED.duration_h,
                    options_value_gbp_per_mw = EXCLUDED.options_value_gbp_per_mw,
                    pf_actual_da_pnl_gbp     = EXCLUDED.pf_actual_da_pnl_gbp,
                    pf_forecast_da_pnl_gbp   = EXCLUDED.pf_forecast_da_pnl_gbp,
                    pf_actual_dispatch_48    = EXCLUDED.pf_actual_dispatch_48,
                    pf_forecast_dispatch_48  = EXCLUDED.pf_forecast_dispatch_48,
                    actual_epex_48           = EXCLUDED.actual_epex_48,
                    forecast_epex_48         = EXCLUDED.forecast_epex_48,
                    computed_at              = NOW()
                """,
                (
                    batch_date.isoformat(),
                    asset_name,
                    round(power_mw, 2),
                    round(duration_h, 2),
                    round(options_per_mw, 2),
                    round(pf_actual_pnl, 2),
                    round(pf_forecast_pnl, 2),
                    json.dumps([round(v, 4) for v in pf_actual_dispatch]),
                    json.dumps([round(v, 4) for v in pf_forecast_dispatch]),
                    json.dumps([round(v, 4) for v in target_epex_48]),
                    json.dumps([round(v, 4) for v in forecast_48]),
                ),
            )
            processed += 1

        except Exception as exc:
            logger.error("Pricing batch error for %s: %s", asset_name, exc, exc_info=True)
            errors.append(f"{asset_name}: {exc}")

    logger.info("Pricing batch complete: %d/%d assets, %d errors, date=%s",
                processed, len(assets_df), len(errors), batch_date)
    return {"processed": processed, "errors": errors, "date": str(batch_date)}
