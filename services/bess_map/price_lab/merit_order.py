"""Merit-order stack model — pure computation, no DB/I/O.

Variable cost (¥/MWh):
  coal: kg/kWh  = heat_rate_kj_kwh / COAL_LHV_KJ_KG
        vc = (coal_price_yuan_t / 1000) × kg/kWh × 1000 + VOM
  gas:  m³/kWh  = heat_rate_kj_kwh / GAS_LHV_KJ_M3
        vc = gas_price_yuan_m3 × m³/kWh × 1000 + VOM
"""
from __future__ import annotations

import math

COAL_LHV_KJ_KG = 29307.0   # ~7000 kcal/kg standard coal equivalent
GAS_LHV_KJ_M3 = 38931.0    # ~9300 kcal/m³


def _vc_yuan_mwh(fuel: str, fuel_price: float, heat_rate_kj_kwh: float, vom: float) -> float:
    if fuel == "coal":
        kg_per_kwh = heat_rate_kj_kwh / COAL_LHV_KJ_KG
        return fuel_price / 1000.0 * kg_per_kwh * 1000.0 + vom
    if fuel == "gas":
        m3_per_kwh = heat_rate_kj_kwh / GAS_LHV_KJ_M3
        return fuel_price * m3_per_kwh * 1000.0 + vom
    raise ValueError(f"unknown fuel {fuel!r}")


def build_stack(fleet_segments, coal_price_yuan_t, gas_price_yuan_m3,
                import_blocks=None, renewable_mw=0.0):
    stack = []
    if renewable_mw and renewable_mw > 0:
        stack.append({"label": "renewable", "vc_yuan_mwh": 0.0,
                      "capacity_mw": float(renewable_mw), "is_import": False})
    for s in fleet_segments:
        price = coal_price_yuan_t if s["fuel"] == "coal" else gas_price_yuan_m3
        stack.append({"label": s.get("label", s["fuel"]),
                      "vc_yuan_mwh": _vc_yuan_mwh(s["fuel"], price,
                                                  float(s["heat_rate_kj_kwh"]),
                                                  float(s["vom_yuan_mwh"])),
                      "capacity_mw": float(s["capacity_mw"]),
                      "is_import": False})
    for b in import_blocks or []:
        stack.append({"label": b["label"], "vc_yuan_mwh": float(b["price_yuan_mwh"]),
                      "capacity_mw": float(b["capacity_mw"]), "is_import": True})
    stack.sort(key=lambda s: s["vc_yuan_mwh"])
    return stack


def marginal_price(stack, residual_demand_mw):
    remaining = float(residual_demand_mw)
    if remaining < 0:
        return 0.0
    for seg in stack:
        if remaining <= seg["capacity_mw"]:
            return seg["vc_yuan_mwh"]
        remaining -= seg["capacity_mw"]
    return math.nan


# ── Scarcity markup calibration ─────────────────────────────────────────────

def fit_markup(observed, structural, tightness, n_bins=5):
    """Median observed/structural ratio per tightness bin, floored at 1.0,
    enforced monotone non-decreasing (isotonic via cummax)."""
    import numpy as np
    obs = np.asarray(observed, dtype=float)
    st = np.asarray(structural, dtype=float)
    ti = np.asarray(tightness, dtype=float)
    mask = np.isfinite(obs) & np.isfinite(st) & np.isfinite(ti) & (st > 0)
    if mask.sum() < 20:
        return [(0.0, 1.0)]
    obs, st, ti = obs[mask], st[mask], ti[mask]
    edges = np.quantile(ti, np.linspace(0, 1, n_bins + 1))
    edges[0], edges[-1] = -np.inf, np.inf
    pts = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        m = (ti >= lo) & (ti < hi)
        ratio = np.median(obs[m] / st[m]) if m.sum() else 1.0
        # representative tightness: mean of bin members keeps xs finite and
        # increasing even though edge bins have ±inf bounds
        if m.sum():
            t_rep = float(ti[m].mean())
        elif np.isfinite(lo + hi):
            t_rep = float((lo + hi) / 2)
        else:
            t_rep = float(ti.mean())
        pts.append((t_rep, max(1.0, float(ratio))))
    # monotone enforcement + keep representative tightness values
    out, best = [], 1.0
    for t, mk in pts:
        best = max(best, mk)
        out.append((t, best))
    return out


def apply_markup(vc, tightness, curve):
    if not curve:
        return vc
    xs = [p[0] for p in curve]
    ys = [p[1] for p in curve]
    t = min(max(tightness, xs[0]), xs[-1])
    for i in range(len(xs) - 1):
        if xs[i] <= t <= xs[i + 1]:
            if xs[i + 1] == xs[i]:
                return vc * ys[i]
            frac = (t - xs[i]) / (xs[i + 1] - xs[i])
            return vc * (ys[i] + frac * (ys[i + 1] - ys[i]))
    return vc * ys[-1]
