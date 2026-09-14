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
