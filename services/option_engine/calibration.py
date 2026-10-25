# services/option_engine/calibration.py
"""Calibration report: engine value vs the historical perfect-foresight LP
benchmark V_hist (``bess_capture_daily``), per province (spec §8).

``flag_ok = 0.7 <= ratio_energy <= 1.4`` where
``ratio_energy = (intrinsic + extrinsic) / v_hist`` — the spec §8 gate is the
engine's ENERGY economics against an energy-only benchmark. The
policy-inclusive ratio (``ratio_with_policy = v_tot / v_hist``) is kept as an
informational column only: V_hist carries no policy stack, so flagging on it
structurally flags exactly the big-policy provinces (measured on the T8 gate:
蒙西 1.94/2.15 and 山西 1.47/1.44 flagged while every energy-only ratio sat
in 0.82-1.24 — review fix round 1).

V_hist exists only for BESS — the capture pipeline is BESS-only — so
wind/solar rows carry NA ``v_hist_yuan_kw`` / ratios / ``flag_ok`` (not a
failure: nothing to calibrate against).

Reads latest snapshots per (province, asset_type, duration_h); never writes.
I/O only via the caller's SQLAlchemy engine — no module-level engine.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from services.option_engine import readers

RATIO_LO, RATIO_HI = 0.7, 1.4  # spec §8 acceptance band (on ratio_energy)

_COLUMNS = ["province", "asset_type", "duration_h", "v_tot_yuan_kw",
            "v_hist_yuan_kw", "ratio_energy", "flag_ok", "ratio_with_policy",
            "rt_days_scaled", "rt_days_total", "da_days_scaled",
            "fallback_filled", "fallback_missing_both",
            "fallback_negatives_kept"]


def latest_snapshots(eng, provinces: list[str]) -> list[dict]:
    """Latest snapshot row per (province, asset_type, duration_h), any asset.

    Fetched with one DISTINCT ON query (not per-key ``read_latest`` loops) so
    the report and the CLI's masking-count printout share one round-trip.
    """
    if not provinces:
        return []
    sql = text("""
        SELECT DISTINCT ON (province, asset_type, duration_h)
               province, asset_type, duration_h, asof_date,
               window_start, window_end,
               intrinsic_yuan_kw, extrinsic_yuan_kw, total_yuan_kw,
               capture_adj_yuan_kw, mc_params
        FROM marketdata.option_value_snapshot
        WHERE province = ANY(:provs)
        ORDER BY province, asset_type, duration_h, asof_date DESC
    """)
    with eng.connect() as conn:
        rows = conn.execute(sql, {"provs": list(provinces)}).mappings().all()
    return [dict(r) for r in rows]


def calibration_report(eng, provinces: list[str]) -> pd.DataFrame:
    """Energy-only and policy-inclusive ratios per latest snapshot row.

    V_hist is ``read_hist_pf_per_kw_yr`` over the SNAPSHOT's own window
    ([window_start, window_end]) so both legs cover the same period.
    ``flag_ok`` rides ``ratio_energy`` (intrinsic + extrinsic over V_hist);
    ``ratio_with_policy`` (V_tot over V_hist) is informational. Masking
    counts come from each row's mc_params (rt/da days rescaled, RT→DA
    fallback filled / missing_both / negatives_kept).
    """
    recs = []
    for s in latest_snapshots(eng, provinces):
        v_tot = s["total_yuan_kw"]
        intr = s["intrinsic_yuan_kw"]
        extr = s["extrinsic_yuan_kw"]
        v_hist = None
        if s["asset_type"] == "bess":
            v_hist = readers.read_hist_pf_per_kw_yr(
                eng, s["province"], float(s["duration_h"]),
                s["window_start"], s["window_end"],
            )
        energy = (intr + extr) if (intr is not None and extr is not None) else None
        ratio_energy = (energy / v_hist
                        if (energy is not None and v_hist) else None)
        ratio_pol = (float(v_tot) / v_hist
                     if (v_tot is not None and v_hist) else None)
        mc = s.get("mc_params") or {}
        units = mc.get("units") or {}
        rt_u, da_u = units.get("rt_price") or {}, units.get("da_price") or {}
        fb = mc.get("fallback") or {}
        recs.append({
            "province": s["province"],
            "asset_type": s["asset_type"],
            "duration_h": float(s["duration_h"]),
            "v_tot_yuan_kw": None if v_tot is None else float(v_tot),
            "v_hist_yuan_kw": v_hist,
            "ratio_energy": ratio_energy,
            "flag_ok": (None if ratio_energy is None
                        else bool(RATIO_LO <= ratio_energy <= RATIO_HI)),
            "ratio_with_policy": ratio_pol,
            "rt_days_scaled": rt_u.get("days_scaled"),
            "rt_days_total": rt_u.get("days_total"),
            "da_days_scaled": da_u.get("days_scaled"),
            "fallback_filled": fb.get("filled"),
            "fallback_missing_both": fb.get("missing_both"),
            "fallback_negatives_kept": fb.get("negatives_kept"),
        })
    df = pd.DataFrame(recs, columns=_COLUMNS)
    for col in ("v_tot_yuan_kw", "v_hist_yuan_kw", "ratio_energy",
                "ratio_with_policy"):
        df[col] = df[col].astype("Float64")  # nullable: NaN, never nan-mixed
    df["flag_ok"] = df["flag_ok"].astype("boolean")
    for col in ("rt_days_scaled", "rt_days_total", "da_days_scaled",
                "fallback_filled", "fallback_missing_both",
                "fallback_negatives_kept"):
        df[col] = df[col].astype("Int64")
    return df


def masking_summary(eng, provinces: list[str]) -> list[dict]:
    """Per-snapshot read-time masking counts from ``mc_params`` (ruling 2).

    One dict per latest snapshot row: province, asset_type, duration_h plus
    flattened ``units`` (per-column scaled/count) and ``fallback`` (filled /
    missing_both / negatives_kept) counts. None when the row predates the
    metadata or the reader was stubbed.
    """
    out = []
    for s in latest_snapshots(eng, provinces):
        mc = s.get("mc_params") or {}
        fb = mc.get("fallback") or {}
        units = mc.get("units") or {}
        out.append({
            "province": s["province"],
            "asset_type": s["asset_type"],
            "duration_h": float(s["duration_h"]),
            "asof_date": s["asof_date"],
            "units": units if units else None,
            "filled": fb.get("filled"),
            "missing_both": fb.get("missing_both"),
            "negatives_kept": fb.get("negatives_kept"),
        })
    return out
