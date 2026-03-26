import pandas as pd
from shared.agents.db import run_query


def load_top_provinces(limit: int = 10) -> pd.DataFrame:
    sql = """
    select province,
           irr_total,
           payback_years_total,
           irr_arbitrage,
           cap_payment_irr,
           ancillary_irr
    from bess_province_return_snapshot
    where as_of_date = (select max(as_of_date) from bess_province_return_snapshot)
    order by irr_total desc nulls last
    limit %s
    """
    return run_query(sql, params=[limit])


def load_spread_ts(days: int = 180) -> pd.DataFrame:
    sql = """
    select date, province, spread_cny_per_mwh
    from bess_theoretical_spread_ts
    where date >= current_date - (%s || ' days')::interval
    order by date, province
    """
    return run_query(sql, params=[days])


def load_capacity_mix() -> pd.DataFrame:
    sql = """
    select province, solar_mw, wind_mw, thermal_mw
    from nodal_capacity_mix_snapshot
    """
    return run_query(sql)


def load_mengxi_rank(days: int = 30) -> pd.DataFrame:
    sql = """
    select date, site, profit_cny, rank
    from mengxi_profitability_daily
    where date >= current_date - (%s || ' days')::interval
    order by date, rank
    """
    return run_query(sql, params=[days])


def compute_rank_delta(mx_df: pd.DataFrame) -> pd.DataFrame:
    if mx_df.empty:
        return mx_df

    mx_df = mx_df.copy()
    mx_df["date"] = pd.to_datetime(mx_df["date"])
    latest_date = mx_df["date"].max()
    first_date = latest_date - pd.Timedelta(days=29)

    latest = mx_df[mx_df["date"] == latest_date][["site", "rank"]]
    past = mx_df[mx_df["date"] == first_date][["site", "rank"]]

    merged = latest.merge(past, on="site", suffixes=("_latest", "_past"))
    merged["rank_delta"] = merged["rank_past"] - merged["rank_latest"]
    return merged.sort_values("rank_latest")


def compute_spread_stats(ts_df: pd.DataFrame) -> pd.DataFrame:
    if ts_df.empty:
        return ts_df

    out = (
        ts_df.groupby("province")["spread_cny_per_mwh"]
        .agg(["mean", "std", "min", "max"])
        .reset_index()
        .rename(
            columns={
                "mean": "spread_mean",
                "std": "spread_std",
                "min": "spread_min",
                "max": "spread_max",
            }
        )
    )
    return out.sort_values("spread_mean", ascending=False)


def compute_capacity_bias(cap_df: pd.DataFrame) -> pd.DataFrame:
    if cap_df.empty:
        return cap_df

    out = cap_df.copy()
    out["total_mw"] = out[["solar_mw", "wind_mw", "thermal_mw"]].fillna(0).sum(axis=1)
    out["total_mw"] = out["total_mw"].replace(0, 1)
    out["solar_ratio"] = out["solar_mw"] / out["total_mw"]
    out["wind_ratio"] = out["wind_mw"] / out["total_mw"]
    out["thermal_ratio"] = out["thermal_mw"] / out["total_mw"]
    out["structural_spread_bias"] = (
        out["solar_ratio"] * 1.0
        - out["wind_ratio"] * 0.6
        - out["thermal_ratio"] * 0.3
    )
    return out.sort_values("structural_spread_bias", ascending=False)


def build_strategy_summary() -> dict:
    provinces = load_top_provinces(limit=10)
    spreads = load_spread_ts(days=180)
    spread_stats = compute_spread_stats(spreads)
    cap = compute_capacity_bias(load_capacity_mix())
    mengxi = load_mengxi_rank(days=30)
    rank_delta = compute_rank_delta(mengxi)

    return {
        "top_provinces": provinces,
        "spread_stats": spread_stats,
        "capacity_bias": cap,
        "mengxi_rank_delta": rank_delta,
    }


def simple_strategy_memo(user_prompt: str, summary: dict) -> str:
    top = summary["top_provinces"]
    spread = summary["spread_stats"]
    cap = summary["capacity_bias"]

    top_txt = "No province data"
    if not top.empty:
        top_txt = ", ".join(top["province"].head(3).astype(str).tolist())

    spread_txt = "No spread data"
    if not spread.empty:
        best_spread = spread.iloc[0]
        spread_txt = (
            f"Highest average spread province: {best_spread['province']} "
            f"({best_spread['spread_mean']:.2f} CNY/MWh)"
        )

    cap_txt = "No structural capacity data"
    if not cap.empty:
        best_bias = cap.iloc[0]
        cap_txt = (
            f"Highest structural spread bias: {best_bias['province']} "
            f"({best_bias['structural_spread_bias']:.3f})"
        )

    return f"""
Strategy Agent v2

User request:
{user_prompt}

Key observations:
1. Top current province shortlist: {top_txt}
2. {spread_txt}
3. {cap_txt}

Suggested next step:
Convert the top-ranking provinces into a candidate portfolio and stress-test them in Portfolio & Risk Agent.
""".strip()