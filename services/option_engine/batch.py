# services/option_engine/batch.py
"""Batch runner for the option-value engine (spec §5.8) — the ONLY writer
into ``marketdata.option_value_snapshot`` (spec §5.7).

Full chain per (province, asset_type, duration_h): readers → expected curve
→ PCA v2 fit → simulate (``n_days = min(365, curve.n_days)`` — the ¥/kW/yr
basis pin; ``compute_value_stack``'s prefix truncation keeps the intrinsic
same-horizon) → asset model from ``ASSET_MODELS`` →
``compute_value_stack`` + ``compute_greeks`` → snapshot upsert.

``mc_params`` carries the reproducibility record (spec §5.7): model, seed,
n_paths, n_components, clip bounds, data coverage, lever notes, read-time
masking counts (units + RT→DA fallback, incl. negatives kept), parametric
fallback notes, and the git sha.

Renewable params come from ``read_cf_profile`` / ``read_price_volume_corr``;
when either is None (or the correlation reader raises ValueError — it manages
its own PGURL engine) the run falls back to a documented SCREEN-GRADE
parametric shape (solar: bell peaking at hour 13; wind: flat 0.35; corr 0.0)
with a note in ``mc_params["parametric_fallbacks"]``.

I/O only via the caller's SQLAlchemy engine — no module-level engine.
"""
from __future__ import annotations

import dataclasses
import subprocess
from datetime import date

import numpy as np

from libs.option_value.asset_models import ASSET_MODELS
from libs.option_value.contracts import (
    BESSParams,
    PCASimConfig,
    RenewableParams,
)
from libs.option_value.curves import build_expected_curve
from libs.option_value.pca_simulator import fit_pca_v2, simulate_pca_v2
from libs.option_value.value_stack import compute_greeks, compute_value_stack
from services.option_engine import readers, snapshot

_HISTORY_MONTHS = 12
_N_COMPONENTS = 4

# Screen grid (spec §5.5 matrix): BESS at two durations + wind/solar.
# duration_h = 0.0 is the wind/solar convention (snapshot DDL comment).
_SCREEN_GRID = (("bess", 2.0), ("bess", 4.0), ("wind", 0.0), ("solar", 0.0))


def _git_sha() -> str:
    """Short HEAD sha for mc_params (spec §5.7); "unknown" outside a repo."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


def _parametric_cf_profile(kind: str) -> np.ndarray:
    """Screen-grade parametric CF profile, shape (12, 24), identical months.

    solar: sinusoidal daylight bell, zero before hour 7 / after hour 19,
    peaking 0.85 at hour 13. wind: flat 0.35 everywhere. Used ONLY when
    ``read_cf_profile`` finds no usable fundamentals coverage — always paired
    with a ``parametric_fallbacks`` note in mc_params.
    """
    if kind == "solar":
        h = np.arange(24, dtype=float)
        row = np.clip(0.85 * np.sin(np.pi * (h - 7.0) / 12.0), 0.0, None)
    else:
        row = np.full(24, 0.35)
    return np.tile(row, (12, 1))


def _build_params(eng, province: str, asset_type: str, duration_h: float,
                  fallback_notes: list[str]):
    """Asset params for ASSET_MODELS[asset_type]; renewable fallbacks noted."""
    if asset_type == "bess":
        return BESSParams(power_mw=1.0, duration_h=float(duration_h))

    cf_profile = readers.read_cf_profile(eng, province, asset_type)
    if cf_profile is None:
        cf_profile = _parametric_cf_profile(asset_type)
        shape = ("bell peaking hour 13, peak 0.85" if asset_type == "solar"
                 else "flat 0.35")
        fallback_notes.append(
            f"cf_profile: no usable spot_fundamentals_hourly coverage for "
            f"{province}/{asset_type} → parametric screen-grade fallback "
            f"({shape})"
        )
    try:
        corr = readers.read_price_volume_corr(eng, province, asset_type)
    except ValueError as e:
        corr = None
        fallback_notes.append(
            f"price_volume_corr: reader raised ValueError ({e}) → 0.0"
        )
    if corr is None:
        corr = 0.0
        fallback_notes.append(
            f"price_volume_corr: no DB estimate for {province}/{asset_type} "
            f"→ 0.0 (screen-grade)"
        )
    return RenewableParams(
        capacity_mw=1.0,
        cf_profile=np.asarray(cf_profile, dtype=float),
        price_volume_corr=float(corr),
        asset_type=asset_type,
    )


def run_one(eng, province: str, asset_type: str, duration_h: float,
            n_paths: int = 1000, seed: int = 42) -> dict:
    """Full engine chain for one (province, asset_type, duration_h) cell.

    Returns the snapshot row dict (also upserted). On insufficient price
    history returns ``{"skipped": province, "reason": "insufficient_history"}``
    and writes nothing — checked BEFORE any other reader call so a stubbed /
    dead engine cannot be touched on the skip path (Review Focus #2).
    """
    asof = date.today()  # asof_date = the run date
    res = readers.read_price_series(eng, province, asof,
                                    months=_HISTORY_MONTHS, return_meta=True)
    # Stubs may answer a bare Series even with return_meta=True — treat the
    # masking metadata as unavailable rather than forcing the pinned pattern.
    series, meta = res if isinstance(res, tuple) else (res, None)

    curve = build_expected_curve(series, asof, months=_HISTORY_MONTHS)
    curve = dataclasses.replace(curve, province=province)
    if curve.insufficient_history:
        return {"skipped": province, "reason": "insufficient_history"}
    # A calendar month with no complete day leaves a NaN month-hour row;
    # simulating on it would propagate NaN through every path (reachable on
    # feed holes like 福建's). Skip and write nothing.
    missing_rows = np.isnan(curve.month_hour_mean).any(axis=1)
    if missing_rows.any():
        months = sorted(12 if r == 0 else int(r)
                        for r in np.where(missing_rows)[0])
        return {"skipped": province, "reason": f"missing_months:{months}"}

    levers = readers.read_policy_levers(eng, province, asset_type,
                                        duration_h=duration_h)
    # The capture haircut is a BESS concept (bess_capture_daily has no
    # wind/solar rows); renewable snapshots carry capture_adj = None.
    capture_rate = (
        readers.read_capture_rate(eng, province, duration_h)
        if asset_type == "bess"
        else None
    )

    cfg = PCASimConfig(n_components=_N_COMPONENTS, n_paths=int(n_paths),
                       seed=int(seed))
    sim_params = fit_pca_v2(series, curve, cfg)
    # ¥/kW/yr basis pin: simulate at most one year of days even when the
    # curve window runs longer; compute_value_stack's prefix truncation
    # keeps the intrinsic same-horizon.
    sim_days = min(365, curve.n_days)
    paths = simulate_pca_v2(sim_params, curve, sim_days, cfg)

    fallback_notes: list[str] = []
    params = _build_params(eng, province, asset_type, duration_h,
                           fallback_notes)
    model = ASSET_MODELS[asset_type](params)

    stack = compute_value_stack(model, curve, paths, levers, capture_rate)
    greeks = compute_greeks(model, curve, sim_params, cfg, levers,
                            capture_rate, series)

    mc_params = {
        "model": "pca_v2",
        "innovations": cfg.innovations,
        "seed": int(seed),
        "n_paths": int(n_paths),
        "n_components": cfg.n_components,
        "n_days": curve.n_days,
        "sim_days": sim_days,
        "coverage_months": curve.coverage_months,
        "history_months": _HISTORY_MONTHS,
        "clip_floor_yuan_mwh": curve.floor,
        "clip_cap_yuan_mwh": curve.cap,
        # Read-time masking counts (ruling): None when the reader was
        # stubbed / meta unavailable.
        "units": (meta or {}).get("units"),
        "fallback": (meta or {}).get("fallback"),
        "capture_rate": capture_rate,
        "capture_model": (readers._CHAMPION_MODEL
                          if asset_type == "bess" else None),
        "lever_notes": levers.notes,
        "parametric_fallbacks": fallback_notes,
        "git_sha": _git_sha(),
    }

    row = {
        "province": province,
        "asset_type": asset_type,
        "duration_h": float(duration_h),
        "asof_date": asof,
        "window_start": curve.window_start,
        "window_end": curve.window_end,
        "intrinsic_yuan_kw": stack.intrinsic_yuan_kw,
        "extrinsic_yuan_kw": stack.extrinsic_yuan_kw,
        "total_yuan_kw": stack.total_yuan_kw,
        "capture_adj_yuan_kw": stack.capture_adj_yuan_kw,
        "policy_net_yuan_kw": stack.policy_net_yuan_kw,
        "extrinsic_p10": stack.extrinsic_p10,
        "extrinsic_p90": stack.extrinsic_p90,
        "mc_mean_volume_mwh_per_kw": stack.mean_volume_mwh_per_kw,
        "policy_json": dataclasses.asdict(levers),
        "greeks_json": greeks,
        "mc_params": mc_params,
    }
    snapshot.write_snapshot(eng, row)
    return row


def run_screen(eng, provinces: list[str] | None = None,
               n_paths: int = 200, seed: int = 42) -> list[dict]:
    """provinces × [(bess,2h), (bess,4h), (wind,0), (solar,0)] sweep.

    Robustness (ruling): per-(province, asset) exceptions — including
    ValueError from the correlation reader and any DB/DP failure — are caught,
    recorded as ``{"skipped": key, "reason": str(e)}`` and the sweep
    continues; one bad province must never kill the screen. run_one's
    insufficient-history skip is re-keyed to the full cell key.
    """
    if provinces is None:
        provinces = readers.list_provinces(eng)
    out: list[dict] = []
    for prov in provinces:
        for asset_type, dur in _SCREEN_GRID:
            key = f"{prov}:{asset_type}:{dur}"
            try:
                row = run_one(eng, prov, asset_type, dur,
                              n_paths=n_paths, seed=seed)
            except Exception as e:
                out.append({"skipped": key, "reason": str(e)})
                continue
            if "skipped" in row:
                out.append({"skipped": key, "reason": row["reason"]})
            else:
                out.append(row)
    return out
