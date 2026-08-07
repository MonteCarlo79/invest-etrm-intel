# apps/bess-map/irr_helpers.py
"""Pure-math IRR helpers — no streamlit imports, fully testable."""
from __future__ import annotations
from typing import Optional
import pandas as pd


def _compute_irr(cashflows: list) -> Optional[float]:
    """Newton-Raphson IRR. Returns None if no solution found."""
    if not cashflows or cashflows[0] >= 0:
        return None
    r = 0.1
    for _ in range(300):
        npv  = sum(cf / (1 + r) ** t for t, cf in enumerate(cashflows))
        dnpv = sum(-t * cf / (1 + r) ** (t + 1) for t, cf in enumerate(cashflows))
        if abs(dnpv) < 1e-12:
            return None  # at inflection point, not a root
        r -= npv / dnpv
        if r <= -1:
            return None
        if abs(npv) < 1e-6:
            break
    else:
        return None  # did not converge in 300 iterations
    return r if -1 < r < 10 else None


def _compute_npv(cashflows: list, rate: float = 0.08) -> float:
    return sum(cf / (1 + rate) ** t for t, cf in enumerate(cashflows))


def build_cashflows(
    theo_per_mwh_day: float,
    capture_rate: float,
    duration_h: float,
    capex_per_kwh: float,
    rte: float,
    om_per_kw_yr: float,
    degradation: float,
    equity_pct: float,
    loan_rate: float,
    loan_tenure: int,
    project_life: int,
    power_mw: float = 1.0,
    sysopfee_per_mwh_day: float = 0.0,
    cap_comp_per_mwh_day: float = 0.0,
    fr_per_mwh_day: float = 0.0,
    subsidy_per_mwh: float = 0.0,
) -> tuple[list, dict]:
    """Returns (cashflows_list, annual_breakdown_dict) — normalised to 1 MW / N-hour plant.

    All *_per_mwh_day args are in ¥/MWh of installed energy capacity per day.
    sysopfee_per_mwh_day is negative (cost). cap_comp and fr are positive (revenue).
    subsidy_per_mwh is a legacy ¥/MWh-of-discharge catch-all, kept for back-compat.
    """
    e_cap = power_mw * duration_h          # MWh energy capacity
    capex = capex_per_kwh * e_cap * 1000   # ¥ total
    equity_capex = capex * equity_pct
    debt = capex * (1 - equity_pct)
    ann_debt = (
        debt * loan_rate / (1 - (1 + loan_rate) ** (-loan_tenure))
        if debt > 0 and loan_rate > 0
        else (debt / loan_tenure if loan_tenure > 0 else 0)
    )
    om_annual = om_per_kw_yr * power_mw

    # ~1 full cycle/day; discharge MWh ≈ e_cap × RTE
    daily_discharge = e_cap * rte

    base_spot_daily    = theo_per_mwh_day * capture_rate * e_cap
    base_cap_comp_daily = cap_comp_per_mwh_day * e_cap
    base_fr_daily      = fr_per_mwh_day * e_cap
    base_sysopfee_daily = sysopfee_per_mwh_day * e_cap   # negative
    base_subsidy_daily = subsidy_per_mwh * daily_discharge

    cfs = [-equity_capex]
    breakdown = {}
    for yr in range(1, project_life + 1):
        deg = (1 - degradation) ** (yr - 1)
        spot     = base_spot_daily     * 365 * deg
        cap_comp = base_cap_comp_daily * 365 * deg
        fr       = base_fr_daily       * 365 * deg
        sysopfee = base_sysopfee_daily * 365 * deg
        subsidy  = base_subsidy_daily  * 365 * deg
        ds       = ann_debt if yr <= loan_tenure else 0.0
        net      = spot + cap_comp + fr + sysopfee + subsidy - om_annual - ds
        cfs.append(net)
        breakdown[yr] = {
            "spot":     spot,
            "cap_comp": cap_comp,
            "fr":       fr,
            "sysopfee": sysopfee,
            "subsidy":  subsidy,
            "om":       om_annual,
            "debt_svc": ds,
            "net":      net,
        }

    # Scale to per-MWh installed capacity for readability
    scale = 1.0 / e_cap if e_cap > 0 else 1.0
    bd_scaled = {
        yr: {k: v * scale for k, v in row.items()}
        for yr, row in breakdown.items()
    }
    return cfs, bd_scaled


def _irr_defaults_for_province(
    province: str,
    duration_h: float,
    sof_df: pd.DataFrame,
    cc_df: pd.DataFrame,
    fr_df: pd.DataFrame,
    rte: float = 0.85,
    fr_util_pct: float = 0.30,
) -> dict:
    """Compute per-day ¥/MWh revenue/cost defaults from province market data.

    Returns dict with keys:
      sysopfee_day   float  ¥/MWh/day (negative — cost)
      cap_comp_day   float  ¥/MWh/day (positive — revenue)
      fr_day         float  ¥/MWh/day (positive — revenue)
      sysopfee_src   str    human-readable source label
      cap_comp_src   str
      fr_src         str
    """
    result: dict = {
        "sysopfee_day": 0.0,
        "cap_comp_day": 0.0,
        "fr_day":       0.0,
        "sysopfee_src": "无数据",
        "cap_comp_src": "无数据",
        "fr_src":       "无数据",
    }

    # ── 系统运行费: 2026 year-to-date average; fallback = latest ≤12 months ───
    if not sof_df.empty and province in sof_df["province"].values:
        prov_sof = sof_df[sof_df["province"] == province].sort_values(
            "year_month", ascending=False
        )
        sof_cur = prov_sof[prov_sof["year_month"].dt.year == 2026]
        if not sof_cur.empty:
            avg_fee = float(sof_cur["fee_yuan_kwh"].mean())
            result["sysopfee_src"] = f"2026均值 ({len(sof_cur)}个月)"
        elif not prov_sof.empty:
            recent = prov_sof.head(12)
            avg_fee = float(recent["fee_yuan_kwh"].mean())
            result["sysopfee_src"] = (
                f"无2026数据，取至{recent['year_month'].max():%Y-%m}均值"
            )
        else:
            avg_fee = None
        if avg_fee is not None:
            # fee(¥/kWh) × 1000(kWh/MWh capacity) / RTE / 365 days
            result["sysopfee_day"] = -(avg_fee * 1000.0 / rte / 365.0)

    # ── 容量补偿: most recent row with standard ¥/kW value ───────────────────
    if not cc_df.empty and province in cc_df["province"].values:
        prov_cc = cc_df[cc_df["province"] == province].copy()
        # Skip rows where notes indicate a ¥/kWh volume-based model (e.g. 蒙东, 山东)
        if "notes" in prov_cc.columns:
            prov_cc = prov_cc[
                ~prov_cc["notes"].fillna("").str.contains("kWh", case=False)
            ]
        prov_cc = prov_cc.dropna(subset=["cap_comp_yuan_kw"]).sort_values(
            "effective_date", ascending=False
        )
        if not prov_cc.empty:
            row = prov_cc.iloc[0]
            cap_val = float(row["cap_comp_yuan_kw"])
            # ¥/kW/year ÷ duration_h ÷ 365 = ¥/MWh installed/day
            result["cap_comp_day"] = cap_val / duration_h / 365.0
            result["cap_comp_src"] = f"最新数据 {str(row['effective_date'])[:7]}"
        else:
            # Province has cap_comp rows but all are non-standard
            result["cap_comp_src"] = "无标准数据（非¥/kW模式）"

    # ── 调频: most recent price × utilisation × 8760 / duration_h / 365 ──────
    if not fr_df.empty and province in fr_df["province"].values:
        prov_fr = (
            fr_df[fr_df["province"] == province]
            .dropna(subset=["fr_price_yuan_kw_h"])
            .sort_values("effective_date", ascending=False)
        )
        if not prov_fr.empty:
            row = prov_fr.iloc[0]
            fr_val = float(row["fr_price_yuan_kw_h"])
            # ¥/kW·h × util_pct × 8760 h/yr / duration_h / 365 = ¥/MWh installed/day
            result["fr_day"] = fr_val * fr_util_pct * 8760.0 / duration_h / 365.0
            result["fr_src"] = (
                f"最新 {str(row['effective_date'])[:7]} × {fr_util_pct*100:.0f}%"
            )

    return result


def _build_extra_rev_map(
    sof_df: pd.DataFrame,
    cc_df: pd.DataFrame,
    fr_df: pd.DataFrame,
    duration_h: float,
    selected_items: list,
    fr_util_pct: float = 0.30,
    rte: float = 0.85,
) -> dict:
    """Return {province: net annual ¥/MWh adjustment} for geo map payback overlay.

    selected_items is a subset of ["sysopfee", "cap_comp", "fr"].
    Result units match rank_df annual_theo/annual_real (¥/MWh installed/year).
    """
    if not selected_items:
        return {}

    all_provinces: set = set()
    for df in (sof_df, cc_df, fr_df):
        if not df.empty and "province" in df.columns:
            all_provinces.update(df["province"].unique())

    result = {}
    for prov in all_provinces:
        defaults = _irr_defaults_for_province(
            prov, duration_h, sof_df, cc_df, fr_df, rte, fr_util_pct
        )
        adj = 0.0
        if "sysopfee" in selected_items:
            adj += defaults["sysopfee_day"] * 365.0
        if "cap_comp" in selected_items:
            adj += defaults["cap_comp_day"] * 365.0
        if "fr" in selected_items:
            adj += defaults["fr_day"] * 365.0
        if adj != 0.0:
            result[prov] = adj
    return result
