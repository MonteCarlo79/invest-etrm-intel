"""Investment-standard BESS parameters (from assets/BESS/investment-standard-param.png).

Year-indexed standard table (Year 0–15): capacity is OVERRIDDEN by each asset's
actual capacity (Asset Config / rm_assets.capacity_mw); DOD, SOH, and RTE are
selected by the asset's age in years since commissioning (per user 2026-09-02).
"""
from __future__ import annotations

import datetime as dt

STANDARD_DOD = 0.90
STANDARD_SOH = [1.00, 0.95, 0.92, 0.91, 0.89, 0.87, 0.86, 0.84,
                0.83, 0.82, 0.81, 0.79, 0.78, 0.77, 0.76, 0.75]  # Year 0..15
STANDARD_RTE = [0.88, 0.88, 0.87, 0.87, 0.87, 0.87, 0.87, 0.87,
                0.87, 0.87, 0.87, 0.86, 0.86, 0.86, 0.86, 0.86]  # Year 0..15


def age_years(commission_date, as_of) -> int:
    """Whole years since commissioning at as_of, clamped to [0, 15].

    commission_date / as_of: datetime.date, str, or pandas Timestamp.
    None or unparseable commission_date → 0 (newest params).
    """
    if commission_date is None or (isinstance(commission_date, float) and commission_date != commission_date):
        return 0
    if isinstance(commission_date, str):
        commission_date = dt.date.fromisoformat(commission_date[:10])
    if isinstance(as_of, str):
        as_of = dt.date.fromisoformat(as_of[:10])
    # Normalize pandas Timestamps (and datetime.datetime) to plain dates
    if hasattr(commission_date, "date") and callable(getattr(commission_date, "date")):
        commission_date = commission_date.date()
    if hasattr(as_of, "date") and callable(getattr(as_of, "date")):
        as_of = as_of.date()
    years = (as_of - commission_date).days // 365
    return max(0, min(15, years))


def params_for_asset(capacity_mw: float, duration_h: float, commission_date, as_of) -> dict:
    """Standard parameters for one asset at a given date.

    Returns dict with: age, power_mw (asset capacity), duration_h,
    roundtrip_eff (year-table RTE by age), energy_cap_mwh
    (= capacity × duration × DOD × SOH[age]).
    """
    age = age_years(commission_date, as_of)
    return {
        "age": age,
        "power_mw": float(capacity_mw),
        "duration_h": float(duration_h),
        "roundtrip_eff": STANDARD_RTE[age],
        "dod": STANDARD_DOD,
        "soh": STANDARD_SOH[age],
        "energy_cap_mwh": float(capacity_mw) * float(duration_h) * STANDARD_DOD * STANDARD_SOH[age],
    }
