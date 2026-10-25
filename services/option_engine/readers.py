# services/option_engine/readers.py
"""DB readers for the option-value engine: prices, policy levers, capture.

This module does I/O (unlike libs/option_value) but ONLY via the SQLAlchemy
engine passed in by the caller — no module-level engine creation. The one
exception is ``read_price_volume_corr``, which wraps
``services.deal_engine.price_data.fetch_price_wind_correlation``; that
function manages its own engine internally (PGURL env var) — its existing
contract, kept per the task brief ("wrap it; don't reimplement").

Unit conventions (libs/option_value contracts):
- prices are ¥/MWh internally everywhere;
- capacity_stack converters return ¥/MWh_installed/yr → PolicyLevers per-kW
  fields get × duration_h / 1000 (a 1 kW unit with duration D carries
  D/1000 MWh of installed energy);
- ``discharge_comp_yuan_kwh`` and ``sysopfee_yuan_kwh`` stay RATES (¥/kWh) —
  the volume is applied later by ``value_stack.apply_policy`` (spec §11.8).
Every conversion is logged into ``PolicyLevers.notes``.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import text

from libs.option_value.contracts import PolicyLevers
from services.bess_map.capacity_stack import (
    DISCHARGE_MODE_RATE_THRESHOLD,
    STACKABLE_STATUSES,
    capacity_payment_per_mwh_yr,
    fr_component,
    fr_pool_skipped,
    load_ancillary_annual,
    load_cap_comp_latest,
    load_capacity_rows,
    load_fr_pool,
    load_sysopfee_latest,
)
from services.deal_engine.price_data import fetch_price_wind_correlation

# Sub-provincial regions without a regular data-update mechanism — excluded
# from screens: stale coverage makes them non-comparable. Mirror of
# apps/bess-map/app.py:994-997 (_RANKING_EXCLUDE); re-declared here because
# importing from the Streamlit app is not allowed. 甘肃西河 is the source-data
# spelling of the same entity as 甘肃河西.
_RANKING_EXCLUDE = {"海南礼记", "海南那悦", "豫南", "豫中东", "豫北", "豫西",
                    "甘肃河西", "甘肃西河"}

# Default capture-pipeline model ("champion"): theoretical_profit rows are
# duplicated across models in bess_capture_daily, so single-model reads must
# filter to one — ols_rt_time_v1 is the platform default (CLAUDE.md).
_CHAMPION_MODEL = "ols_rt_time_v1"

_PRICE_COLS = ("rt_price", "da_price")


# ---------------------------------------------------------------------------
# Pure transforms (unit-tested without a DB)
# ---------------------------------------------------------------------------

def _normalize_units(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Scale legacy ¥/kWh columns to ¥/MWh, per (day, column).

    Granularity and signature are load-bearing (T8 real-data gate):
    spot_prices_hourly carries MIXED-unit windows — multi-source ingestion
    flips scale across date ranges (the 山东 2025-10→2026-10 window
    alternates ¥/kWh and ¥/MWh segments ~18 times) — so a whole-column
    median either rescales the already-¥/MWh days to 1000× (measured on
    that window: clip bounds [-90000, 1383400] ¥/MWh, V_tot 254k ¥/kW) or
    leaves the ¥/kWh days 1000× too small. A day is ¥/kWh when
    max(ABS(values)) < 5 — empirically bimodal with a wide empty gap on
    all five gate provinces (kWh-side day max ≤ 0.26, MWh-side day min ≥
    50; nothing in [2, 50)). MAX, not median: half-placeholder days
    (山东 2026-09-24: 13 zeros + 11 genuine 400-512 ¥/MWh rows) have a
    zero median but are ¥/MWh; ABS because 山东 has genuinely
    all-negative ¥/MWh days (2026-01-01..11, day median −80 ¥/MWh) that a
    signed statistic would misread as ¥/kWh. All-zero placeholder days
    scale harmlessly (0×1000=0). Falls back to the whole-column rule
    (max-abs < 5) when no datetime column is present (unit tests).

    Returns the (copied) DataFrame and per-column metadata:
    ``{"rt_price": {"scaled": bool, "count": int, "days_scaled": int|None,
    "days_total": int|None}, ...}`` where count is the number of non-NaN
    values multiplied (0 when not scaled); days_* are None in whole-column
    mode.
    """
    out = df.copy()
    meta: dict[str, dict] = {}
    day_key = None
    if "datetime" in out.columns:
        day_key = pd.to_datetime(out["datetime"]).dt.date.to_numpy()
    for col in _PRICE_COLS:
        if col not in out.columns:
            continue
        s = pd.to_numeric(out[col], errors="coerce")
        if day_key is None:
            valid = s.dropna()
            scaled = bool(len(valid) > 0 and valid.abs().max() < 5.0)
            if scaled:
                out[col] = s * 1000.0
            else:
                out[col] = s
            meta[col] = {"scaled": scaled,
                         "count": int(len(valid)) if scaled else 0,
                         "days_scaled": None, "days_total": None}
            continue
        abs_max = s.abs().groupby(day_key).transform("max")
        scale_day = (abs_max < 5.0).to_numpy()  # all-NaN day → NaN → False
        out[col] = s.to_numpy(dtype=float) * np.where(scale_day, 1000.0, 1.0)
        day_frame = pd.DataFrame({"day": day_key, "scale": scale_day})
        meta[col] = {
            "scaled": bool(scale_day.any()),
            "count": int((s.notna().to_numpy() & scale_day).sum()),
            "days_scaled": int(day_frame.groupby("day")["scale"].first().sum()),
            "days_total": int(day_frame["day"].nunique()),
        }
    return out, meta


def _rt_da_fallback(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Per-hour RT → DA fallback: rt == 0 or NaN → da of the same hour.

    This is the per-hour fallback that ``fetch_price_history`` lacks (it only
    switches columns when the ENTIRE column is zero); mirrors
    ``run_capture_pipeline.py``'s ``rt.replace(0, nan).fillna(da)`` EXACTLY,
    including exact-zero comparison. GENUINE negative RT prices (山东 has
    real negative-price hours — paid-to-charge value) are kept, not masked:
    the capture pipeline that produces V_hist keeps them, and the engine must
    match or the calibration ratio is biased upward in exactly the
    negative-price provinces. Hours where both legs are missing stay NaN —
    curves drops those days downstream; do NOT ffill here. Returns (df copy,
    {"filled": n_substituted, "missing_both": n, "negatives_kept": n}).
    """
    out = df.copy()
    rt = pd.to_numeric(out["rt_price"], errors="coerce")
    da = pd.to_numeric(out["da_price"], errors="coerce")
    mask = rt.isna() | (rt == 0)
    substitutable = mask & da.notna()
    out["rt_price"] = rt.where(~mask, da)
    out["da_price"] = da
    meta = {
        "filled": int(substitutable.sum()),
        "missing_both": int((mask & da.isna()).sum()),
        "negatives_kept": int((rt < 0).sum()),
    }
    return out, meta


# ---------------------------------------------------------------------------
# Price series
# ---------------------------------------------------------------------------

def read_price_series(eng, province: str, window_end: date,
                      months: int = 12, return_meta: bool = False):
    """Hourly RT ¥/MWh series for ``province`` over the history window.

    Window semantics match ``curves.build_expected_curve``: window_start is
    the first day of the calendar month ``months`` before ``window_end``; the
    SQL upper bound is ``datetime <= window_end`` (per the brief's pinned
    query), so the anchor day contributes only its 00:00 row and is dropped
    downstream as an incomplete day.

    Read-time hygiene: unit normalization (per (day, column): a day whose
    max ABS price < 5 is ¥/kWh → ×1000 — whole-window statistics misread
    the mixed-unit segments multi-source ingestion produces) then per-hour
    RT=0/NaN → DA fallback (exact-zero only — genuine negative RT prices
    are kept, matching the capture pipeline that produces V_hist). Hours
    with both legs missing stay NaN (no ffill — curves drops incomplete
    days).

    Returns a Series indexed by naive datetime, or an empty Series when the
    province has no rows in the window. With ``return_meta=True`` returns
    ``(series, meta)`` where meta carries the read-time masking counts::

        {"units":    {col: {"scaled": bool, "count": int}, ...},
         "fallback": {"filled": int, "missing_both": int,
                      "negatives_kept": int}}

    (batch writes these into mc_params; default False keeps the pinned
    signature and call pattern).
    """
    window_start = (
        (pd.Timestamp(window_end) - pd.DateOffset(months=months))
        .replace(day=1)
        .date()
    )
    sql = text("""
        SELECT datetime, rt_price, da_price
        FROM marketdata.spot_prices_hourly
        WHERE province = :province AND datetime >= :start AND datetime <= :end
        ORDER BY datetime
    """)
    with eng.connect() as conn:
        df = pd.read_sql(sql, conn, params={
            "province": province, "start": window_start, "end": window_end,
        })
    if df.empty:
        s = pd.Series(np.array([], dtype=float),
                      index=pd.DatetimeIndex([], name="datetime"),
                      name="rt_price")
        meta = {"units": {},
                "fallback": {"filled": 0, "missing_both": 0,
                             "negatives_kept": 0}}
        return (s, meta) if return_meta else s
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime")
    df, units_meta = _normalize_units(df)
    df, fb_meta = _rt_da_fallback(df)
    s = df.set_index("datetime")["rt_price"]
    if s.index.tz is not None:
        s.index = s.index.tz_localize(None)
    meta = {"units": units_meta, "fallback": fb_meta}
    return (s, meta) if return_meta else s


# ---------------------------------------------------------------------------
# Policy levers
# ---------------------------------------------------------------------------

def _load_installed_bess_mw(eng) -> dict[str, float]:
    """province → latest non-null installed BESS power (MW).

    Same query as apps/bess-map's ``_load_installed_bess`` (app.py:1036);
    re-declared here because importing from the Streamlit app is not allowed.
    Needed by ``fr_component`` (both the mileage-actuals and pool-share FR
    paths divide by the provincial fleet).
    """
    sql = text("""
        SELECT DISTINCT ON (province) province, bess_mw
        FROM marketdata.province_installed_monthly
        WHERE bess_mw IS NOT NULL
        ORDER BY province, year_month DESC
    """)
    with eng.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return {p: float(v) for p, v in rows}


def read_policy_levers(eng, province: str, asset_type: str,
                       duration_h: float = 2.0) -> PolicyLevers:
    """Map the capacity_stack loaders into a ``PolicyLevers`` for one province.

    Unit handoff (ruling): capacity_price / cap_comp / fr are ¥/kW/yr
    (capacity_stack converters return ¥/MWh_installed/yr → × duration_h/1000);
    discharge_comp and sysopfee stay ¥/kWh RATES — the volume is applied by
    ``value_stack.apply_policy``. Every conversion is logged into ``notes``.

    ``duration_h`` is needed for the per-kW conversion of capacity-type
    levers (pro-rating under a base-hours policy is duration-dependent). It
    defaults to 2.0 so the brief's ``(eng, province, asset_type)`` call
    pattern stays valid; batch passes the asset's actual duration. For
    wind/solar the capacity levers are not applicable (value_stack
    ``_APPLICABLE``) and are zeroed without touching the converters (which
    raise on duration_h <= 0; wind/solar snapshots carry duration_h = 0).

    Missing rows → 0.0 with a ``notes`` entry. The ranking tab's 165/6h
    fallback default is deliberately NOT applied here: the option engine
    values confirmed policy only.
    """
    notes: list[str] = []
    levers = PolicyLevers()

    # 系统运行费: per-kWh rate on charging energy — applies to all asset
    # types (bess: on charge; wind/solar: on generation — apply_policy).
    # NOTE: province_sysopfee_monthly lives in the PUBLIC schema (the loader
    # deliberately has no marketdata. prefix).
    sysop = load_sysopfee_latest(eng)
    if province in sysop:
        levers.sysopfee_yuan_kwh = float(sysop[province])
        notes.append(
            f"sysopfee rate {levers.sysopfee_yuan_kwh} ¥/kWh "
            f"(public.province_sysopfee_monthly latest); stays a rate — "
            f"volume applied by apply_policy"
        )
    else:
        notes.append("sysopfee: no row → 0.0")

    if asset_type != "bess":
        notes.append(
            f"{asset_type}: capacity/cap_comp/FR/discharge_comp not "
            f"applicable → 0.0; mechanism_premium & green_cert have no DB "
            f"source in v1 (set via scenario overrides)"
        )
        levers.notes = "; ".join(notes)
        return levers

    # ── BESS capacity stack ────────────────────────────────────────────────
    cap_comp = load_cap_comp_latest(eng)      # PRIMARY capacity-rate source
    rows = load_capacity_rows(eng)            # coefficient + status layer
    coef_by_prov = {r[0]: float(r[3]) for r in rows}
    status_by_prov = {r[0]: str(r[4]) for r in rows}

    if province in cap_comp:
        rate, base_h = cap_comp[province]
        if rate < DISCHARGE_MODE_RATE_THRESHOLD:
            # 放电量补偿: 元/kWh discharged (蒙西/蒙东 0.28, 山东 0.0705) —
            # stays a rate; apply_policy multiplies by discharge volume.
            levers.discharge_comp_yuan_kwh = float(rate)
            notes.append(
                f"cap_comp {rate} < {DISCHARGE_MODE_RATE_THRESHOLD} → "
                f"per-kWh discharge-compensation rate (volume applied by "
                f"apply_policy)"
            )
        else:
            coef = coef_by_prov.get(province, 1.0)
            per_mwh = capacity_payment_per_mwh_yr(rate, base_h, coef, duration_h)
            levers.cap_comp_yuan_kw_yr = per_mwh * duration_h / 1000.0
            notes.append(
                f"cap_comp rate {rate} ¥/kW/yr on {base_h}h base × coef "
                f"{coef} → {per_mwh:.4f} ¥/MWh-installed/yr → "
                f"×{duration_h}/1000 → {levers.cap_comp_yuan_kw_yr:.4f} ¥/kW/yr"
            )
        # cap_comp is the PRIMARY source (loader docstring + app precedence):
        # a province_capacity_price row is superseded, never stacked on top.
        if status_by_prov.get(province) in STACKABLE_STATUSES:
            notes.append(
                "capacity_price row superseded by cap_comp (primary source) "
                "→ capacity_price_yuan_kw_yr = 0.0"
            )
    elif status_by_prov.get(province) in STACKABLE_STATUSES:
        _r = next(r for r in rows if r[0] == province)
        prate, pbase, pcoef, pstatus = float(_r[1]), float(_r[2]), float(_r[3]), str(_r[4])
        per_mwh = capacity_payment_per_mwh_yr(prate, pbase, pcoef, duration_h)
        levers.capacity_price_yuan_kw_yr = per_mwh * duration_h / 1000.0
        notes.append(
            f"capacity_price ({pstatus}) rate {prate} ¥/kW/yr on "
            f"{pbase}h base × coef {pcoef} → {per_mwh:.4f} "
            f"¥/MWh-installed/yr → ×{duration_h}/1000 → "
            f"{levers.capacity_price_yuan_kw_yr:.4f} ¥/kW/yr"
        )
    else:
        notes.append(
            "capacity price/cap comp: no confirmed row → 0.0 (ranking-tab "
            "165/6h fallback deliberately NOT applied — option engine values "
            "confirmed policy only)"
        )

    # ── 调频 FR: mileage actuals > pool-share > 0 (fr_component precedence,
    # incl. its 500 MW fleet floor) ─────────────────────────────────────────
    anc = load_ancillary_annual(eng)
    pool = load_fr_pool(eng)
    bess_mw = _load_installed_bess_mw(eng).get(province, 0.0)
    fr_per_mwh = fr_component(
        province, duration_h, anc.get(province), pool.get(province), bess_mw
    )
    levers.fr_yuan_kw_yr = fr_per_mwh * duration_h / 1000.0
    if fr_per_mwh > 0:
        src = "mileage actuals" if anc.get(province) else "pool-share"
        notes.append(
            f"FR ({src}) {fr_per_mwh:.4f} ¥/MWh-installed/yr → "
            f"×{duration_h}/1000 → {levers.fr_yuan_kw_yr:.4f} ¥/kW/yr"
        )
    elif fr_pool_skipped(bess_mw, pool.get(province)):
        notes.append(
            f"FR pool exists but fleet {bess_mw:.0f} MW < 500 MW floor → "
            f"pool-share suppressed → 0.0"
        )
    else:
        notes.append("FR: no mileage actuals and no pool → 0.0")

    levers.notes = "; ".join(notes)
    return levers


# ---------------------------------------------------------------------------
# Capture rate + historical perfect-foresight benchmark (bess_capture_daily)
# ---------------------------------------------------------------------------

def read_capture_rate(eng, province: str, duration_h: float,
                      model: str = _CHAMPION_MODEL, days: int = 90
                      ) -> float | None:
    """Aggregate capture Σrealized/Σtheoretical over the trailing ``days``.

    Follows the bess-map aggregate-capture convention (MEMORY.md): a
    NaN-realized day (forecast hole) counts as 0 P&L in the numerator;
    NaN-theoretical days are excluded from the denominator — legacy rows in
    the table carry literal 'NaN' doubles, hence the NULLIF guards. None when
    no rows or the denominator sums to 0.
    """
    sql = text("""
        SELECT SUM(COALESCE(NULLIF(realized_profit, 'NaN'::double precision), 0.0))
             / NULLIF(SUM(NULLIF(theoretical_profit, 'NaN'::double precision)), 0.0)
        FROM marketdata.bess_capture_daily
        WHERE province = :province AND model = :model AND duration_h = :duration_h
          AND date >= CURRENT_DATE - CAST(:days AS integer)
    """)
    with eng.connect() as conn:
        val = conn.execute(sql, {
            "province": province, "model": model,
            "duration_h": float(duration_h), "days": int(days),
        }).scalar()
    return float(val) if val is not None else None


def read_hist_pf_per_kw_yr(eng, province: str, duration_h: float,
                           window_start: date, window_end: date
                           ) -> float | None:
    """V_hist: historical perfect-foresight LP benchmark, ¥/kW/yr.

    ``AVG(theoretical_profit_per_mwh_day) × 365 × duration_h / 1000`` over
    [window_start, window_end], champion-model rows only (theoretical rows
    are duplicated across models — filter to one, spec line 48).

    Unit note: theoretical_profit_per_mwh_day is ¥ per MWh of installed
    energy per day (pipeline divides by power_mw × duration_h); the per-kW
    basis needs × duration_h/1000 (a 1 kW unit carries duration_h/1000 MWh) —
    same convention as the capacity-lever conversion, and required for the
    calibration ratio against v_tot_yuan_kw to be dimensionally meaningful.
    """
    sql = text("""
        SELECT AVG(NULLIF(theoretical_profit_per_mwh_day, 'NaN'::double precision))
        FROM marketdata.bess_capture_daily
        WHERE province = :province AND model = :model AND duration_h = :duration_h
          AND date >= :start AND date <= :end
    """)
    with eng.connect() as conn:
        val = conn.execute(sql, {
            "province": province, "model": _CHAMPION_MODEL,
            "duration_h": float(duration_h),
            "start": window_start, "end": window_end,
        }).scalar()
    if val is None:
        return None
    return float(val) * 365.0 * float(duration_h) / 1000.0


# ---------------------------------------------------------------------------
# Renewable CF profile + price-volume correlation
# ---------------------------------------------------------------------------

def read_cf_profile(eng, province: str, kind: str) -> np.ndarray | None:
    """(12, 24) capacity-factor profile from spot_fundamentals_hourly.

    Month-hour means of ``wind_mw``/``solar_mw``; row index is ``month % 12``
    (row 0 = December — the curves/renewable_models convention). Normalized
    by the GLOBAL max of the month-hour means so both the daily and the
    seasonal shape are preserved (per-hour-column normalization would force
    every hour to peak at 1.0 and erase the seasonal amplitude). Missing
    cells are filled with the hour's mean over the months present; hours
    never observed at all become 0.0.

    None when coverage is < 6 distinct calendar months, or when the series is
    degenerate (max ≤ 0 / no positive output anywhere).
    """
    col = {"wind": "wind_mw", "solar": "solar_mw"}.get(kind)
    if col is None:
        raise ValueError(f"kind must be 'wind' or 'solar', got {kind!r}")
    sql = text(f"""
        SELECT datetime, {col} AS mw
        FROM marketdata.spot_fundamentals_hourly
        WHERE province = :province AND {col} IS NOT NULL
        ORDER BY datetime
    """)
    with eng.connect() as conn:
        df = pd.read_sql(sql, conn, params={"province": province})
    if df.empty:
        return None
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    df["mw"] = pd.to_numeric(df["mw"], errors="coerce")
    df = df.dropna(subset=["mw"])
    if df.empty:
        return None
    if df["datetime"].dt.to_period("M").nunique() < 6:
        return None

    df["month"] = df["datetime"].dt.month
    df["hour"] = df["datetime"].dt.hour
    means = df.groupby(["month", "hour"])["mw"].mean()

    prof = np.full((12, 24), np.nan)
    for (m, h), v in means.items():
        prof[int(m) % 12, int(h)] = float(v)

    # Fill cells with no observation: the hour's mean over present months;
    # an hour never observed anywhere falls back to 0.0.
    hour_mean = np.nanmean(prof, axis=0)
    hour_mean = np.where(np.isnan(hour_mean), 0.0, hour_mean)
    rows, cols = np.where(np.isnan(prof))
    prof[rows, cols] = hour_mean[cols]

    mx = float(np.max(prof))
    if not np.isfinite(mx) or mx <= 0.0:
        return None
    return prof / mx


def read_price_volume_corr(eng, province: str, kind: str) -> float | None:
    """Pearson corr(monthly avg price, monthly wind CF) for one province.

    Wraps ``fetch_price_wind_correlation`` (which computes all provinces in
    one pass and manages its own engine internally). Wind only:
    staging.exchange_excel_metrics carries wind generation/capacity but no
    solar dataset — returns None for solar; the call site defaults to 0.0
    with a note. None also when the province is absent (e.g. < 6 overlapping
    months upstream).
    """
    if kind != "wind":
        return None
    df = fetch_price_wind_correlation()
    if df is None or df.empty:
        return None
    hit = df.loc[df["province"] == province, "correlation"]
    if hit.empty:
        return None
    return float(hit.iloc[0])


# ---------------------------------------------------------------------------
# Province list
# ---------------------------------------------------------------------------

def list_provinces(eng) -> list[str]:
    """Distinct provinces in spot_prices_hourly, minus _RANKING_EXCLUDE."""
    sql = text(
        "SELECT DISTINCT province FROM marketdata.spot_prices_hourly ORDER BY 1"
    )
    with eng.connect() as conn:
        provs = [r[0] for r in conn.execute(sql).fetchall()]
    return [p for p in provs if p and p not in _RANKING_EXCLUDE]
