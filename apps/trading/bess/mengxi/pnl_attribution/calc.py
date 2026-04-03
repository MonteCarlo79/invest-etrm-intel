# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 12:46:43 2026

@author: dipeng.chen
"""

# apps/trading/bess/mengxi/pnl_attribution/calc.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional
import numpy as np
import pandas as pd

DEFAULT_COMPENSATION_YUAN_PER_MWH = 350.0


def month_start(ts: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    return pd.Timestamp(year=ts.year, month=ts.month, day=1)


def get_monthly_compensation_rate(
    compensation_df: pd.DataFrame,
    coverage_df: pd.DataFrame | None,
    asset_code: str,
    trade_date: pd.Timestamp,
    default_rate: float = DEFAULT_COMPENSATION_YUAN_PER_MWH,
) -> tuple[float, bool, str | None]:
    """
    compensation_df expected columns:
    - asset_code
    - effective_month
    - compensation_yuan_per_mwh
    """
    effective_month = month_start(trade_date)

    blocked_reason = None
    if coverage_df is not None and not coverage_df.empty:
        cdf = coverage_df.copy()
        cdf["effective_month"] = pd.to_datetime(cdf["effective_month"], errors="coerce").dt.normalize()
        coverage_hit = cdf[
            (cdf["asset_code"] == asset_code)
            & (cdf["effective_month"] == effective_month)
            & (cdf["discharge_known"] == True)
            & (cdf["compensation_known"] == False)
        ]
        if not coverage_hit.empty:
            blocked_reason = "discharge_known_compensation_missing"

    if compensation_df is None or compensation_df.empty:
        if blocked_reason:
            return np.nan, True, blocked_reason
        return float(default_rate), False, None

    df = compensation_df.copy()
    df["effective_month"] = pd.to_datetime(df["effective_month"], errors="coerce").dt.normalize()

    hit = df[
        (df["asset_code"] == asset_code)
        & (df["effective_month"] == effective_month)
    ]

    if hit.empty:
        if blocked_reason:
            return np.nan, True, blocked_reason
        return float(default_rate), False, None

    val = pd.to_numeric(hit["compensation_yuan_per_mwh"], errors="coerce").dropna()
    if val.empty:
        if blocked_reason:
            return np.nan, True, blocked_reason
        return float(default_rate), False, None

    return float(val.iloc[0]), False, None

SCENARIOS = [
    "perfect_foresight_unrestricted",
    "perfect_foresight_grid_feasible",
    "cleared_actual",
    "nominated_dispatch",
    "tt_forecast_optimal",
    "tt_strategy",
]

ASSET_ALIAS_MAP = {
    "suyou": {
        "dispatch_unit_name_cn": "景蓝乌尔图储能电站",
        "short_name_cn": "苏右储能",
        "display_name_cn": "苏右",
        "tt_asset_name_en": "SuYou",
        "market_key": "Mengxi_SuYou",
        "city_cn": "锡林郭勒",
        "province": "Mengxi",
    },
    "wulate": {
        "dispatch_unit_name_cn": "远景乌拉特储能电站",
        "short_name_cn": "乌拉特中期储能",
        "display_name_cn": "乌拉特",
        "tt_asset_name_en": "WuLaTe",
        "market_key": "Mengxi_WuLaTe",
        "city_cn": "巴彦淖尔",
        "province": "Mengxi",
    },
    "wuhai": {
        "dispatch_unit_name_cn": "富景五虎山储能电站",
        "short_name_cn": "乌海储能",
        "display_name_cn": "乌海",
        "tt_asset_name_en": "WuHai",
        "market_key": "Mengxi_WuHai",
        "city_cn": "乌海",
        "province": "Mengxi",
    },
    "wulanchabu": {
        "dispatch_unit_name_cn": "景通红丰储能电站",
        "short_name_cn": "乌兰察布储能",
        "display_name_cn": "乌兰察布",
        "tt_asset_name_en": "WuLanChaBu",
        "market_key": "Mengxi_WuLanChaBu",
        "city_cn": "乌兰察布",
        "province": "Mengxi",
    },
    "hetao": {
        "dispatch_unit_name_cn": "景怡查干哈达储能电站",
        "short_name_cn": "河套储能",
        "display_name_cn": "河套",
        "tt_asset_name_en": None,
        "market_key": None,
        "city_cn": "巴彦淖尔",
        "province": "Mengxi",
    },
    "hangjinqi": {
        "dispatch_unit_name_cn": "悦杭独贵储能电站",
        "short_name_cn": "杭锦旗储能",
        "display_name_cn": "杭锦旗",
        "tt_asset_name_en": None,
        "market_key": None,
        "city_cn": "鄂尔多斯",
        "province": "Mengxi",
    },
    "siziwangqi": {
        "dispatch_unit_name_cn": "景通四益堂储能电站",
        "short_name_cn": "四子王旗储能",
        "display_name_cn": "四子王旗",
        "tt_asset_name_en": None,
        "market_key": None,
        "city_cn": "乌兰察布",
        "province": "Mengxi",
    },
    "gushanliang": {
        "dispatch_unit_name_cn": "裕昭沙子坝储能电站",
        "short_name_cn": "谷山梁储能",
        "display_name_cn": "谷山梁",
        "tt_asset_name_en": None,
        "market_key": None,
        "city_cn": "鄂尔多斯",
        "province": "Mengxi",
    },
}

# Use normalized rows in DB later; keep a local fallback for v1.
DEFAULT_SCENARIO_AVAILABILITY = {
    "suyou": {
        "perfect_foresight_unrestricted": True,
        "perfect_foresight_grid_feasible": True,
        "cleared_actual": True,
        "nominated_dispatch": True,
        "tt_forecast_optimal": True,
        "tt_strategy": True,
    },
    "wulate": {
        "perfect_foresight_unrestricted": True,
        "perfect_foresight_grid_feasible": True,
        "cleared_actual": True,
        "nominated_dispatch": True,
        "tt_forecast_optimal": True,
        "tt_strategy": True,
    },
    "wuhai": {
        "perfect_foresight_unrestricted": True,
        "perfect_foresight_grid_feasible": True,
        "cleared_actual": True,
        "nominated_dispatch": False,
        "tt_forecast_optimal": True,
        "tt_strategy": False,
    },
    "wulanchabu": {
        "perfect_foresight_unrestricted": True,
        "perfect_foresight_grid_feasible": True,
        "cleared_actual": True,
        "nominated_dispatch": False,
        "tt_forecast_optimal": True,
        "tt_strategy": False,
    },
    "hetao": {
        "perfect_foresight_unrestricted": True,
        "perfect_foresight_grid_feasible": True,
        "cleared_actual": True,
        "nominated_dispatch": False,
        "tt_forecast_optimal": False,
        "tt_strategy": False,
    },
    "hangjinqi": {
        "perfect_foresight_unrestricted": True,
        "perfect_foresight_grid_feasible": True,
        "cleared_actual": True,
        "nominated_dispatch": False,
        "tt_forecast_optimal": False,
        "tt_strategy": False,
    },
    "siziwangqi": {
        "perfect_foresight_unrestricted": True,
        "perfect_foresight_grid_feasible": True,
        "cleared_actual": True,
        "nominated_dispatch": False,
        "tt_forecast_optimal": False,
        "tt_strategy": False,
    },
    "gushanliang": {
        "perfect_foresight_unrestricted": True,
        "perfect_foresight_grid_feasible": True,
        "cleared_actual": True,
        "nominated_dispatch": False,
        "tt_forecast_optimal": False,
        "tt_strategy": False,
    },
}


def canonicalize_asset_name(value: str | None) -> Optional[str]:
    if not value:
        return None

    v = str(value).strip().lower()
    for asset_code, meta in ASSET_ALIAS_MAP.items():
        candidates = {
            asset_code,
            (meta.get("dispatch_unit_name_cn") or "").lower(),
            (meta.get("short_name_cn") or "").lower(),
            (meta.get("display_name_cn") or "").lower(),
            (meta.get("tt_asset_name_en") or "").lower(),
            (meta.get("market_key") or "").lower(),
        }
        if v in candidates:
            return asset_code
    return None


def asset_alias_df() -> pd.DataFrame:
    rows = []
    for asset_code, meta in ASSET_ALIAS_MAP.items():
        for alias_type, alias_value in meta.items():
            if alias_value:
                rows.append(
                    {
                        "asset_code": asset_code,
                        "alias_type": alias_type,
                        "alias_value": alias_value,
                        "province": meta.get("province"),
                        "city_cn": meta.get("city_cn"),
                    }
                )
    return pd.DataFrame(rows)


def scenario_availability_df() -> pd.DataFrame:
    rows = []
    for asset_code, mapping in DEFAULT_SCENARIO_AVAILABILITY.items():
        for scenario_name, available_flag in mapping.items():
            rows.append(
                {
                    "asset_code": asset_code,
                    "scenario_name": scenario_name,
                    "available_flag": bool(available_flag),
                }
            )
    return pd.DataFrame(rows)


def interval_hours_from_series(df: pd.DataFrame, fallback: float = 0.25) -> float:
    if df is None or df.empty or "time" not in df.columns:
        return fallback
    ts = pd.to_datetime(df["time"], errors="coerce").dropna().sort_values()
    if len(ts) < 2:
        return fallback
    delta = (ts.iloc[1] - ts.iloc[0]).total_seconds() / 3600.0
    if delta <= 0:
        return fallback
    return float(delta)


def prepare_time_series(df: pd.DataFrame, value_col: str, output_col: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["time", output_col])
    out = df.copy()
    out["time"] = pd.to_datetime(out["time"], errors="coerce")
    out[output_col] = pd.to_numeric(out[value_col], errors="coerce")
    out = out.dropna(subset=["time"]).sort_values("time")
    return out[["time", output_col]]


def merge_dispatch_and_price(dispatch_df: pd.DataFrame, actual_price_df: pd.DataFrame) -> pd.DataFrame:
    d = prepare_time_series(dispatch_df, "dispatch_mw", "dispatch_mw")
    p = prepare_time_series(actual_price_df, "price", "settlement_price")
    merged = d.merge(p, on="time", how="left")
    merged["settlement_price"] = merged["settlement_price"].astype(float)
    merged["dispatch_mw"] = merged["dispatch_mw"].astype(float)
    return merged.dropna(subset=["dispatch_mw"])


def compute_scenario_pnl(
    dispatch_df: pd.DataFrame,
    actual_price_df: pd.DataFrame,
    compensation_yuan_per_mwh: float,
    interval_hours: float | None = None,
) -> dict:
    merged = merge_dispatch_and_price(dispatch_df, actual_price_df)
    if merged.empty:
        return {
            "market_revenue_yuan": np.nan,
            "subsidy_revenue_yuan": np.nan,
            "total_revenue_yuan": np.nan,
            "discharge_mwh": np.nan,
            "charge_mwh": np.nan,
            "avg_daily_cycles": np.nan,
            "interval_hours": np.nan,
        }

    ih = interval_hours or interval_hours_from_series(merged)
    merged["dispatch_mwh"] = merged["dispatch_mw"] * ih
    merged["market_revenue_yuan"] = merged["dispatch_mwh"] * merged["settlement_price"]
    merged["discharge_mwh"] = merged["dispatch_mwh"].clip(lower=0.0)
    merged["charge_mwh"] = (-merged["dispatch_mwh"]).clip(lower=0.0)
    merged["subsidy_revenue_yuan"] = merged["discharge_mwh"] * compensation_yuan_per_mwh
    market_revenue = float(merged["market_revenue_yuan"].sum())
    subsidy_revenue = float(merged["subsidy_revenue_yuan"].sum())
    discharge_mwh = float(merged["discharge_mwh"].sum())
    charge_mwh = float(merged["charge_mwh"].sum())

    # Simple proxy: one cycle = discharged MWh / rated volume MWh.
    # Exact normalization should be done downstream with asset_master volume.
    avg_daily_cycles = np.nan

    return {
    "compensation_yuan_per_mwh": compensation_yuan_per_mwh,
    "market_revenue_yuan": market_revenue,
    "subsidy_revenue_yuan": subsidy_revenue,
    "total_revenue_yuan": market_revenue + subsidy_revenue,
    "discharge_mwh": discharge_mwh,
    "charge_mwh": charge_mwh,
    "avg_daily_cycles": avg_daily_cycles,
    "interval_hours": ih,
}


def build_daily_scenario_rows(
    trade_date: pd.Timestamp,
    asset_code: str,
    actual_price_df: pd.DataFrame,
    scenario_dispatch_map: Dict[str, pd.DataFrame],
    availability_map: Dict[str, bool],
    compensation_df: pd.DataFrame | None = None,
    compensation_coverage_df: pd.DataFrame | None = None,
    default_compensation_yuan_per_mwh: float = DEFAULT_COMPENSATION_YUAN_PER_MWH,
) -> pd.DataFrame:
    comp_rate, compensation_blocked, block_reason = get_monthly_compensation_rate(
        compensation_df=compensation_df,
        coverage_df=compensation_coverage_df,
        asset_code=asset_code,
        trade_date=trade_date,
        default_rate=default_compensation_yuan_per_mwh,
    )
    rows = []
    for scenario_name in SCENARIOS:
        available = bool(availability_map.get(scenario_name, False))
        if not available:
            rows.append(
                {
                    "trade_date": trade_date,
                    "asset_code": asset_code,
                    "scenario_name": scenario_name,
                    "scenario_available": False,
                    "market_revenue_yuan": np.nan,
                    "subsidy_revenue_yuan": np.nan,
                    "total_revenue_yuan": np.nan,
                    "discharge_mwh": np.nan,
                    "charge_mwh": np.nan,
                    "avg_daily_cycles": np.nan,
                    "compensation_yuan_per_mwh": comp_rate,
                    "compensation_blocked": compensation_blocked,
                    "compensation_block_reason": block_reason,
                }
            )
            continue

        dispatch_df = scenario_dispatch_map.get(scenario_name)
        if dispatch_df is None or dispatch_df.empty:
            rows.append(
                {
                    "trade_date": trade_date,
                    "asset_code": asset_code,
                    "scenario_name": scenario_name,
                    "scenario_available": False,
                    "market_revenue_yuan": np.nan,
                    "subsidy_revenue_yuan": np.nan,
                    "total_revenue_yuan": np.nan,
                    "discharge_mwh": np.nan,
                    "charge_mwh": np.nan,
                    "avg_daily_cycles": np.nan,
                    "compensation_yuan_per_mwh": comp_rate,
                    "compensation_blocked": compensation_blocked,
                    "compensation_block_reason": block_reason,
                }
            )
            continue

        metrics = compute_scenario_pnl(
            dispatch_df=dispatch_df,
            actual_price_df=actual_price_df,
            compensation_yuan_per_mwh=comp_rate,
        )
        rows.append(
            {
                "trade_date": trade_date,
                "asset_code": asset_code,
                "scenario_name": scenario_name,
                "scenario_available": True,
                "compensation_blocked": compensation_blocked,
                "compensation_block_reason": block_reason,
                **metrics,
            }
        )

    return pd.DataFrame(rows)


def build_daily_attribution_row(scenario_rows: pd.DataFrame) -> pd.DataFrame:
    def val(name: str) -> float | None:
        hit = scenario_rows.loc[scenario_rows["scenario_name"] == name, "total_revenue_yuan"]
        if hit.empty:
            return np.nan
        return float(hit.iloc[0]) if pd.notna(hit.iloc[0]) else np.nan

    asset_code = scenario_rows["asset_code"].iloc[0]
    trade_date = pd.to_datetime(scenario_rows["trade_date"].iloc[0])

    pf = val("perfect_foresight_unrestricted")
    pf_grid = val("perfect_foresight_grid_feasible")
    cleared = val("cleared_actual")
    nominated = val("nominated_dispatch")
    tt_fc = val("tt_forecast_optimal")
    tt_strat = val("tt_strategy")

    row = {
        "trade_date": trade_date,
        "asset_code": asset_code,
        "pf_unrestricted_pnl": pf,
        "pf_grid_feasible_pnl": pf_grid,
        "cleared_actual_pnl": cleared,
        "nominated_pnl": nominated,
        "tt_forecast_optimal_pnl": tt_fc,
        "tt_strategy_pnl": tt_strat,
        "grid_restriction_loss": pf - pf_grid if pd.notna(pf) and pd.notna(pf_grid) else np.nan,
        "forecast_error_loss": pf_grid - tt_fc if pd.notna(pf_grid) and pd.notna(tt_fc) else np.nan,
        "strategy_error_loss": tt_fc - tt_strat if pd.notna(tt_fc) and pd.notna(tt_strat) else np.nan,
        "nomination_loss": tt_strat - nominated if pd.notna(tt_strat) and pd.notna(nominated) else np.nan,
        "execution_clearing_loss": nominated - cleared if pd.notna(nominated) and pd.notna(cleared) else np.nan,
        "realisation_gap_vs_pf": pf - cleared if pd.notna(pf) and pd.notna(cleared) else np.nan,
        "realisation_gap_vs_pf_grid": pf_grid - cleared if pd.notna(pf_grid) and pd.notna(cleared) else np.nan,
    }
    return pd.DataFrame([row])
