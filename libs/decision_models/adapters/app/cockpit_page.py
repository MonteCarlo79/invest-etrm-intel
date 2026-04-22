"""
libs/decision_models/adapters/app/cockpit_page.py

BESS Power Options Trading Cockpit — Streamlit page.

Treats each BESS asset as a strip of N daily spread call options (Kirk/Margrabe).
Surfaces: strip value, intrinsic/time value, Greeks, moneyness, and realization overlay.

Call render_cockpit_page() from the host app inside a tab or page block.

DB queries follow Pattern A (read persisted monitoring snapshots, no on-demand dispatch).
All DB functions are cached with @st.cache_data TTLs.

Run standalone (dev only):
    PGURL=postgresql://... streamlit run libs/decision_models/adapters/app/cockpit_page.py
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Asset physical specs (default fleet config, Inner Mongolia BESS fleet)
# ---------------------------------------------------------------------------

# Source: data/assets/资产清单2026.xlsx (updated 2026-04-21)
# 苏右=suyou, 杭锦旗=hangjinqi, 四子王旗=siziwangqi, 谷山梁=gushanliang,
# 景怡查干哈达储能电站=bameng, 乌拉特中旗=wulate, 乌海=wuhai, 乌兰察布=wulanchabu
# subsidy_yuan_per_mwh: 度电补贴, reduces effective strike K_eff = om_cost - subsidy
_ASSET_SPECS: Dict[str, Dict[str, float]] = {
    "suyou":       {"power_mw": 100.0,  "duration_h": 4.0, "roundtrip_eff": 0.85, "subsidy_yuan_per_mwh": 350.0},
    "hangjinqi":   {"power_mw": 100.0,  "duration_h": 4.0, "roundtrip_eff": 0.85, "subsidy_yuan_per_mwh": 350.0},
    "siziwangqi":  {"power_mw": 100.0,  "duration_h": 4.0, "roundtrip_eff": 0.85, "subsidy_yuan_per_mwh": 350.0},
    "gushanliang": {"power_mw": 500.0,  "duration_h": 4.0, "roundtrip_eff": 0.85, "subsidy_yuan_per_mwh": 350.0},
    "bameng":      {"power_mw": 1000.0, "duration_h": 4.0, "roundtrip_eff": 0.85, "subsidy_yuan_per_mwh": 350.0},
    "wulate":      {"power_mw": 100.0,  "duration_h": 4.0, "roundtrip_eff": 0.85, "subsidy_yuan_per_mwh": 350.0},
    "wuhai":       {"power_mw": 100.0,  "duration_h": 4.0, "roundtrip_eff": 0.85, "subsidy_yuan_per_mwh": 350.0},
    "wulanchabu":  {"power_mw": 3.35,   "duration_h": 2.0, "roundtrip_eff": 0.85, "subsidy_yuan_per_mwh": 0.0},
}

# Inner Mongolia price regime — derived from 60-day average RT clearing prices (2026-Q1/Q2):
#   Solar offpeak  08:00–16:00  avg  ~70 ¥/MWh  (solar generation suppresses prices)
#   Peak           00:00–08:00
#                  17:00–24:00  avg ~250 ¥/MWh  (morning ramp + evening demand)
#
# 15-min slot index (0-based from midnight) = hour × 4 + minute / 15
# Offpeak slots 32–63 correspond to 08:00 (inclusive) → 16:00 (exclusive)
_OFFPEAK_SLOTS = set(range(32, 64))  # 08:00–16:00

_STATUS_COLOR = {
    "NORMAL": "#2ecc71",
    "WARN": "#f39c12",
    "ALERT": "#e67e22",
    "CRITICAL": "#e74c3c",
    "DATA_ABSENT": "#95a5a6",
    "INDETERMINATE": "#bdc3c7",
}
_FRAGILITY_COLOR = {
    "LOW": "#2ecc71",
    "MEDIUM": "#f39c12",
    "HIGH": "#e67e22",
    "CRITICAL": "#e74c3c",
}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_engine():
    """Return a SQLAlchemy engine from PGURL env var."""
    from sqlalchemy import create_engine
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        st.error("PGURL environment variable is not set.")
        st.stop()
    return create_engine(url, pool_pre_ping=True)


@st.cache_resource
def _engine():
    return _get_engine()


@st.cache_data(ttl=300, show_spinner=False)
def _load_realization_status(snapshot_date: Optional[str] = None, lookback_days: int = 30) -> pd.DataFrame:
    """Latest realization status snapshot for all assets."""
    from sqlalchemy import text
    date_clause = (
        "snapshot_date = :snap_date"
        if snapshot_date
        else "snapshot_date = (SELECT MAX(snapshot_date) FROM monitoring.asset_realization_status)"
    )
    sql = text(f"""
        SELECT
            asset_code, snapshot_date, lookback_days, days_in_window,
            avg_cleared_actual_pnl, avg_pf_grid_feasible_pnl, realization_ratio,
            avg_grid_restriction_loss, avg_forecast_error_loss, avg_strategy_error_loss,
            avg_nomination_loss, avg_execution_clearing_loss,
            dominant_loss_bucket, status_level, narrative
        FROM monitoring.asset_realization_status
        WHERE {date_clause}
          AND lookback_days = :lookback_days
        ORDER BY
            CASE status_level
                WHEN 'CRITICAL'      THEN 1
                WHEN 'ALERT'         THEN 2
                WHEN 'WARN'          THEN 3
                WHEN 'DATA_ABSENT'   THEN 4
                WHEN 'INDETERMINATE' THEN 5
                WHEN 'NORMAL'        THEN 6
                ELSE 7
            END,
            realization_ratio ASC NULLS LAST
    """)
    params: Dict[str, Any] = {"lookback_days": lookback_days}
    if snapshot_date:
        params["snap_date"] = date.fromisoformat(snapshot_date)
    try:
        return pd.read_sql(sql, _engine(), params=params)
    except Exception as exc:
        st.warning(f"Realization status unavailable: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _load_fragility_status(snapshot_date: Optional[str] = None) -> pd.DataFrame:
    """Latest fragility status snapshot for all assets."""
    from sqlalchemy import text
    date_clause = (
        "snapshot_date = :snap_date"
        if snapshot_date
        else "snapshot_date = (SELECT MAX(snapshot_date) FROM monitoring.asset_fragility_status)"
    )
    sql = text(f"""
        SELECT
            asset_code, snapshot_date,
            realization_score, trend_score, composite_score, fragility_level,
            realization_ratio, realization_status_level, days_in_window,
            recent_ratio, prior_ratio, ratio_delta,
            dominant_factor, narrative
        FROM monitoring.asset_fragility_status
        WHERE {date_clause}
        ORDER BY composite_score DESC
    """)
    params: Dict[str, Any] = {}
    if snapshot_date:
        params["snap_date"] = date.fromisoformat(snapshot_date)
    try:
        return pd.read_sql(sql, _engine(), params=params)
    except Exception as exc:
        st.warning(f"Fragility status unavailable: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def _load_price_vols(vol_window_days: int = 60) -> pd.DataFrame:
    """
    Compute avg peak/offpeak forwards and annualised daily-spread vol
    for each asset from hist_mengxi_provincerealtimeclearprice_15min.

    Returns one row per asset with: peak_forward_yuan, offpeak_forward_yuan,
    peak_vol, offpeak_vol (annualised from daily log-returns over vol_window).

    Note: all assets in this dashboard share the same provincial clearing price
    (Inner Mongolia is a single-price area at the province level). Per-asset
    differences come from physical specs only.
    """
    from sqlalchemy import text
    import math

    cutoff = date.today() - timedelta(days=vol_window_days)
    sql = text("""
        SELECT
            time::date AS trade_date,
            CASE
                WHEN EXTRACT(hour FROM time) * 4 + EXTRACT(minute FROM time) / 15
                     BETWEEN 32 AND 63
                THEN 'offpeak'   -- 08:00–16:00: solar generation, depressed prices
                ELSE 'peak'      -- 00:00–08:00 + 17:00–24:00: no solar, demand-driven
            END AS period,
            AVG(price) AS avg_price
        FROM public.hist_mengxi_provincerealtimeclearprice_15min
        WHERE time >= :cutoff
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    try:
        df = pd.read_sql(sql, _engine(), params={"cutoff": cutoff}, parse_dates=["trade_date"])
    except Exception as exc:
        st.warning(f"Price data unavailable: {exc}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    pivot = df.pivot(index="trade_date", columns="period", values="avg_price").dropna()
    if pivot.empty or "peak" not in pivot.columns or "offpeak" not in pivot.columns:
        return pd.DataFrame()

    peak_fwd = float(pivot["peak"].mean())
    offpeak_fwd = float(pivot["offpeak"].mean())

    # Annualised vol from daily log-returns (drop non-positive prices before log)
    def _annualised_vol(series: pd.Series) -> float:
        s = series[series > 0].dropna()
        if len(s) < 5:
            return 0.30  # fallback default
        lr = s.apply(math.log).diff().dropna()
        if len(lr) < 5:
            return 0.30
        return float(lr.std() * math.sqrt(252))

    peak_vol = _annualised_vol(pivot["peak"])
    offpeak_vol = _annualised_vol(pivot["offpeak"])

    return pd.DataFrame([{
        "peak_forward_yuan": peak_fwd,
        "offpeak_forward_yuan": offpeak_fwd,
        "peak_vol": peak_vol,
        "offpeak_vol": offpeak_vol,
        "vol_window_days": vol_window_days,
        "n_price_days": len(pivot),
    }])


@st.cache_data(ttl=300, show_spinner=False)
def _load_attribution_history(asset_code: str, lookback_days: int = 30) -> pd.DataFrame:
    """30-day attribution waterfall history for a single asset."""
    from sqlalchemy import text
    cutoff = date.today() - timedelta(days=lookback_days)
    sql = text("""
        SELECT
            trade_date,
            pf_grid_feasible_pnl,
            cleared_actual_pnl,
            grid_restriction_loss,
            forecast_error_loss,
            strategy_error_loss,
            nomination_loss,
            execution_clearing_loss
        FROM reports.bess_asset_daily_attribution
        WHERE asset_code = :asset_code
          AND trade_date >= :cutoff
        ORDER BY trade_date DESC
    """)
    try:
        return pd.read_sql(sql, _engine(), params={"asset_code": asset_code, "cutoff": cutoff},
                           parse_dates=["trade_date"])
    except Exception as exc:
        st.warning(f"Attribution data unavailable for {asset_code}: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _load_realization_history(asset_code: str, lookback_days: int = 90) -> pd.DataFrame:
    """Historical realization ratio time-series for detail panel."""
    from sqlalchemy import text
    cutoff = date.today() - timedelta(days=lookback_days)
    sql = text("""
        SELECT snapshot_date, realization_ratio, composite_score, fragility_level, status_level
        FROM monitoring.asset_realization_status r
        LEFT JOIN monitoring.asset_fragility_status f USING (asset_code, snapshot_date)
        WHERE r.asset_code = :asset_code
          AND r.snapshot_date >= :cutoff
          AND r.lookback_days = 30
        ORDER BY snapshot_date
    """)
    try:
        return pd.read_sql(sql, _engine(), params={"asset_code": asset_code, "cutoff": cutoff},
                           parse_dates=["snapshot_date"])
    except Exception as exc:
        st.warning(f"Realization history unavailable for {asset_code}: {exc}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Kirk/Margrabe strip pricing (delegates to model)
# ---------------------------------------------------------------------------

def _price_strip(
    asset_code: str,
    peak_forward_yuan: float,
    offpeak_forward_yuan: float,
    peak_vol: float,
    offpeak_vol: float,
    peak_offpeak_corr: float,
    n_days_remaining: int,
    om_cost_yuan_per_mwh: float,
    spec: Dict[str, float],
) -> Dict[str, Any]:
    """
    Price a single asset as a spread call strip, returning both total and
    market-only values so callers can split market vs subsidy contribution.

    Prices the strip twice:
      1. market_only: om_cost_yuan_per_mwh = om_cost (no subsidy)
      2. total:       om_cost_yuan_per_mwh = om_cost - subsidy  (K_eff = K - subsidy)

    Returns the total result dict augmented with:
      market_strip_value_yuan: strip value without subsidy
      subsidy_value_yuan:      total - market (subsidy contribution)
      subsidy_yuan_per_mwh:    per-asset subsidy rate
    """
    import libs.decision_models.bess_spread_call_strip  # ensure registered
    from libs.decision_models.runners.local import run

    subsidy = spec.get("subsidy_yuan_per_mwh", 0.0)
    base = {
        "asset_code": asset_code,
        "as_of_date": str(date.today()),
        "n_days_remaining": n_days_remaining,
        "peak_forward_yuan": peak_forward_yuan,
        "offpeak_forward_yuan": offpeak_forward_yuan,
        "peak_vol": peak_vol,
        "offpeak_vol": offpeak_vol,
        "peak_offpeak_corr": peak_offpeak_corr,
        "roundtrip_eff": spec["roundtrip_eff"],
        "power_mw": spec["power_mw"],
        "duration_h": spec["duration_h"],
    }

    market_result = run("bess_spread_call_strip", {**base, "om_cost_yuan_per_mwh": om_cost_yuan_per_mwh})
    total_result  = run("bess_spread_call_strip", {**base, "om_cost_yuan_per_mwh": om_cost_yuan_per_mwh - subsidy})

    total_result["market_strip_value_yuan"] = market_result["strip_value_yuan"]
    total_result["subsidy_value_yuan"]       = total_result["strip_value_yuan"] - market_result["strip_value_yuan"]
    total_result["subsidy_yuan_per_mwh"]     = subsidy
    return total_result


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------

def _moneyness_color(pct: float) -> str:
    if pct > 10:
        return "#2ecc71"   # green — ITM
    if pct > -5:
        return "#f39c12"   # yellow — near-the-money
    return "#e74c3c"       # red — OTM


def _fmt_yuan(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"¥{v/1_000_000:.2f}M"
    if abs(v) >= 1_000:
        return f"¥{v/1_000:.1f}K"
    return f"¥{v:.0f}"


# ---------------------------------------------------------------------------
# Main render function
# ---------------------------------------------------------------------------

def render_cockpit_page() -> None:
    """Render the BESS Options Trading Cockpit within a Streamlit app."""

    # ------------------------------------------------------------------
    # Sidebar — cockpit controls
    # ------------------------------------------------------------------
    with st.sidebar:
        st.markdown("---")
        st.subheader("Options Cockpit")

        n_days = st.slider(
            "Strip horizon (calendar days)",
            min_value=30, max_value=365, value=365, step=1,
            help="Number of calendar days priced into the strip. 365 = full calendar year. "
                 "Strip value = sum of daily spread call options over this horizon.",
        )
        vol_window = st.selectbox("Vol window (days)", [30, 60, 90], index=1)
        om_cost = st.number_input("O&M cost (¥/MWh)", min_value=0.0, max_value=200.0,
                                  value=0.0, step=5.0)
        corr = st.slider("Peak/offpeak correlation", 0.50, 1.00, 0.85, step=0.01)

        with st.expander("Asset spec overrides", expanded=False):
            asset_specs = {}
            for ac, defaults in _ASSET_SPECS.items():
                st.markdown(f"**{ac}**")
                c1, c2, c3 = st.columns(3)
                power = c1.number_input("MW", min_value=1.0, max_value=2000.0,
                                        value=defaults["power_mw"], step=10.0,
                                        key=f"pwr_{ac}")
                dur = c2.number_input("h", min_value=0.5, max_value=8.0,
                                      value=defaults["duration_h"], step=0.5,
                                      key=f"dur_{ac}")
                eff = c3.number_input("η", min_value=0.50, max_value=1.00,
                                      value=defaults["roundtrip_eff"], step=0.01,
                                      key=f"eff_{ac}")
                asset_specs[ac] = {
                    "power_mw": power,
                    "duration_h": dur,
                    "roundtrip_eff": eff,
                    "subsidy_yuan_per_mwh": defaults["subsidy_yuan_per_mwh"],
                }

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------
    st.header("BESS Options Cockpit")
    horizon_label = f"{n_days}d (~{n_days/365:.1f}yr)"
    st.caption(
        "BESS fleet treated as spread call strips · Kirk/Margrabe pricing · "
        f"Horizon: {horizon_label} · Vol window: {vol_window}d · Corr: {corr:.2f} · "
        f"O&M: ¥{om_cost:.0f}/MWh  |  "
        "Strip value = sum of daily spread call options over the horizon (not annualised)"
    )

    # ------------------------------------------------------------------
    # Load market data + monitoring snapshots
    # ------------------------------------------------------------------
    with st.spinner("Loading market data and monitoring snapshots..."):
        price_df = _load_price_vols(vol_window_days=vol_window)
        real_df = _load_realization_status()
        frag_df = _load_fragility_status()

    if price_df.empty:
        st.warning("No market price data available. Ensure DB connection and price history table is populated.")
        return

    peak_fwd = float(price_df["peak_forward_yuan"].iloc[0])
    offpeak_fwd = float(price_df["offpeak_forward_yuan"].iloc[0])
    peak_vol_val = float(price_df["peak_vol"].iloc[0])
    offpeak_vol_val = float(price_df["offpeak_vol"].iloc[0])
    n_price_days = int(price_df["n_price_days"].iloc[0])

    st.caption(
        f"Market inputs ({n_price_days}d avg): "
        f"Peak fwd ¥{peak_fwd:.1f}/MWh · Offpeak fwd ¥{offpeak_fwd:.1f}/MWh · "
        f"Peak vol {peak_vol_val:.1%} · Offpeak vol {offpeak_vol_val:.1%}"
    )

    # ------------------------------------------------------------------
    # Price all 8 assets
    # ------------------------------------------------------------------
    strip_results: List[Dict[str, Any]] = []
    for ac in _ASSET_SPECS:
        spec = asset_specs.get(ac, _ASSET_SPECS[ac])
        result = _price_strip(
            asset_code=ac,
            peak_forward_yuan=peak_fwd,
            offpeak_forward_yuan=offpeak_fwd,
            peak_vol=peak_vol_val,
            offpeak_vol=offpeak_vol_val,
            peak_offpeak_corr=corr,
            n_days_remaining=n_days,
            om_cost_yuan_per_mwh=om_cost,
            spec=spec,
        )
        strip_results.append(result)

    strip_df = pd.DataFrame(strip_results)

    # Merge realization status
    if not real_df.empty:
        real_merge = real_df[["asset_code", "realization_ratio", "status_level"]].copy()
        strip_df = strip_df.merge(real_merge, on="asset_code", how="left")
    else:
        strip_df["realization_ratio"] = None
        strip_df["status_level"] = "DATA_ABSENT"

    if not frag_df.empty:
        frag_merge = frag_df[["asset_code", "fragility_level", "composite_score"]].copy()
        strip_df = strip_df.merge(frag_merge, on="asset_code", how="left")
    else:
        strip_df["fragility_level"] = "UNKNOWN"
        strip_df["composite_score"] = None

    # ------------------------------------------------------------------
    # Row 1: Portfolio metrics
    # ------------------------------------------------------------------
    portfolio_value  = strip_df["strip_value_yuan"].sum()
    portfolio_market = strip_df["market_strip_value_yuan"].sum()
    portfolio_subsidy = strip_df["subsidy_value_yuan"].sum()
    avg_realization = (
        strip_df["realization_ratio"].dropna().mean()
        if "realization_ratio" in strip_df.columns else None
    )
    alert_count = (
        strip_df["status_level"].isin(["ALERT", "CRITICAL"]).sum()
        if "status_level" in strip_df.columns else 0
    )
    avg_moneyness = strip_df["moneyness_pct"].mean()

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Portfolio Total Value", _fmt_yuan(portfolio_value))
    m2.metric("Market Value", _fmt_yuan(portfolio_market),
              help="Spread call value without subsidy")
    m3.metric("Subsidy Value", _fmt_yuan(portfolio_subsidy),
              help="Incremental value from 度电补贴")
    m4.metric("Fleet Avg Realization",
              f"{avg_realization:.1%}" if avg_realization is not None else "N/A")
    m5.metric("ALERT/CRITICAL Assets", int(alert_count))

    st.markdown("---")

    # ------------------------------------------------------------------
    # Row 2: Strip valuation table
    # ------------------------------------------------------------------
    st.subheader("Spread Call Strip Valuations")

    display_cols = {
        "asset_code": "Asset",
        "strip_value_yuan": "Total Value (¥)",
        "market_strip_value_yuan": "Market Value (¥)",
        "subsidy_value_yuan": "Subsidy Value (¥)",
        "subsidy_yuan_per_mwh": "Subsidy (¥/MWh)",
        "per_day_value_yuan": "Per-Day Total (¥)",
        "net_spread_forward": "Net Spread Fwd (¥/MWh)",
        "moneyness_pct": "Moneyness (%)",
        "intrinsic_value_yuan": "Intrinsic (¥)",
        "time_value_yuan": "Time Value (¥)",
        "delta_yuan_per_yuan": "Delta (¥/¥/MWh)",
        "vega_yuan_per_vol_point": "Vega (¥/vol pt)",
        "theta_yuan_per_day": "Theta (¥/day)",
    }

    table_df = strip_df[list(display_cols.keys())].copy()
    table_df = table_df.rename(columns=display_cols)

    def _style_row(row):
        mono = row["Moneyness (%)"]
        return ["background-color: rgba(46,204,113,0.12)" if mono > 10
                else "background-color: rgba(243,156,18,0.12)" if mono > -5
                else "background-color: rgba(231,76,60,0.12)"] * len(row)

    float_fmts = {
        "Total Value (¥)": "{:,.0f}",
        "Market Value (¥)": "{:,.0f}",
        "Subsidy Value (¥)": "{:,.0f}",
        "Subsidy (¥/MWh)": "{:.0f}",
        "Per-Day Total (¥)": "{:,.0f}",
        "Net Spread Fwd (¥/MWh)": "{:.1f}",
        "Moneyness (%)": "{:.1f}",
        "Intrinsic (¥)": "{:,.0f}",
        "Time Value (¥)": "{:,.0f}",
        "Delta (¥/¥/MWh)": "{:,.0f}",
        "Vega (¥/vol pt)": "{:,.0f}",
        "Theta (¥/day)": "{:,.0f}",
    }

    styled = table_df.style.apply(_style_row, axis=1).format(float_fmts)
    st.dataframe(styled, width="stretch", hide_index=True)

    st.markdown("---")

    # ------------------------------------------------------------------
    # Row 3: Realization overlay + Fragility panel
    # ------------------------------------------------------------------
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("Realization vs Option Value")
        fig_scatter = go.Figure()

        for _, row in strip_df.iterrows():
            ratio = row.get("realization_ratio") if pd.notna(row.get("realization_ratio")) else None
            frag = row.get("fragility_level", "UNKNOWN")
            color = _FRAGILITY_COLOR.get(str(frag), "#95a5a6")

            fig_scatter.add_trace(go.Scatter(
                x=[ratio],
                y=[row["strip_value_yuan"]],
                mode="markers+text",
                marker=dict(
                    size=max(12, row["q_max_mwh_per_day"] * 0.8),
                    color=color,
                    opacity=0.85,
                    line=dict(color="white", width=1),
                ),
                text=[row["asset_code"]],
                textposition="top center",
                textfont=dict(size=11),
                name=row["asset_code"],
                showlegend=False,
                hovertemplate=(
                    f"<b>{row['asset_code']}</b><br>"
                    f"Realization: %{{x:.1%}}<br>"
                    f"Strip value: ¥%{{y:,.0f}}<br>"
                    f"Fragility: {frag}<extra></extra>"
                ),
            ))

        # Reference lines
        fig_scatter.add_vline(x=0.70, line_dash="dash", line_color="#2ecc71",
                               annotation_text="NORMAL (70%)", annotation_position="top right")
        fig_scatter.add_vline(x=0.50, line_dash="dash", line_color="#f39c12",
                               annotation_text="WARN (50%)", annotation_position="bottom right")

        fig_scatter.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=30, b=50),
            xaxis=dict(title="Realization Ratio", tickformat=".0%", range=[0, 1.05]),
            yaxis=dict(title="Strip Value (¥)"),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(fig_scatter, width="stretch", key="cockpit_scatter")

    with col_right:
        st.subheader("Fragility Status")
        if frag_df.empty:
            st.info("No fragility data available.")
        else:
            for _, frow in frag_df.iterrows():
                level = str(frow.get("fragility_level", "UNKNOWN"))
                color = _FRAGILITY_COLOR.get(level, "#95a5a6")
                score = frow.get("composite_score", 0.0)
                score_str = f"{score:.2f}" if pd.notna(score) else "—"
                st.markdown(
                    f'<div style="background:{color}20; border-left:4px solid {color}; '
                    f'padding:6px 10px; margin-bottom:6px; border-radius:3px;">'
                    f'<b>{frow["asset_code"]}</b> &nbsp;'
                    f'<span style="color:{color}; font-weight:bold;">{level}</span> '
                    f'<span style="color:#666; font-size:0.85em;">score={score_str}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # ------------------------------------------------------------------
    # Row 4: Asset detail panel
    # ------------------------------------------------------------------
    st.subheader("Asset Detail")
    all_assets = list(_ASSET_SPECS.keys())
    selected_asset = st.selectbox("Select asset", all_assets, key="cockpit_asset_detail")

    asset_strip = strip_df[strip_df["asset_code"] == selected_asset]
    if not asset_strip.empty:
        row = asset_strip.iloc[0]
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Strip Value", _fmt_yuan(row["strip_value_yuan"]))
        mc2.metric("Intrinsic / Time", f"{_fmt_yuan(row['intrinsic_value_yuan'])} / {_fmt_yuan(row['time_value_yuan'])}")
        mc3.metric("Moneyness", f"{row['moneyness_pct']:.1f}%")
        mc4.metric("q_max MWh/day", f"{row['q_max_mwh_per_day']:.0f}")

    detail_left, detail_right = st.columns(2)

    with detail_left:
        st.markdown("**Attribution Waterfall (30d avg)**")
        attr_df = _load_attribution_history(selected_asset, lookback_days=30)
        if attr_df.empty:
            st.info("No attribution data available.")
        else:
            avg_attr = attr_df.mean(numeric_only=True)
            loss_buckets = [
                ("Grid Restriction", float(avg_attr.get("grid_restriction_loss", 0))),
                ("Forecast Error", float(avg_attr.get("forecast_error_loss", 0))),
                ("Strategy Error", float(avg_attr.get("strategy_error_loss", 0))),
                ("Nomination", float(avg_attr.get("nomination_loss", 0))),
                ("Exec/Clearing", float(avg_attr.get("execution_clearing_loss", 0))),
            ]
            labels = [b[0] for b in loss_buckets]
            values = [b[1] for b in loss_buckets]
            colors = ["#e74c3c" if v > 0 else "#2ecc71" for v in values]

            fig_attr = go.Figure(go.Bar(
                x=values,
                y=labels,
                orientation="h",
                marker_color=colors,
                hovertemplate="%{y}: ¥%{x:,.0f}<extra></extra>",
            ))

            # Reference: per-day strip value
            if not asset_strip.empty:
                per_day = float(asset_strip.iloc[0]["per_day_value_yuan"])
                fig_attr.add_vline(x=per_day, line_dash="dot", line_color="#3498db",
                                   annotation_text=f"Option value/day (¥{per_day:,.0f})",
                                   annotation_position="top right")

            fig_attr.update_layout(
                height=280,
                margin=dict(l=10, r=10, t=20, b=20),
                xaxis=dict(title="¥", tickformat=",.0f"),
                plot_bgcolor="white",
                paper_bgcolor="white",
            )
            st.plotly_chart(fig_attr, width="stretch", key="cockpit_attr")

    with detail_right:
        st.markdown("**Realization Ratio History (90d)**")
        hist_df = _load_realization_history(selected_asset, lookback_days=90)
        if hist_df.empty:
            st.info("No realization history available.")
        else:
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Scatter(
                x=hist_df["snapshot_date"],
                y=hist_df["realization_ratio"],
                mode="lines+markers",
                name="Realization ratio",
                line=dict(color="#3498db", width=2),
                marker=dict(size=4),
                yaxis="y",
            ))
            if "composite_score" in hist_df.columns and hist_df["composite_score"].notna().any():
                fig_hist.add_trace(go.Scatter(
                    x=hist_df["snapshot_date"],
                    y=hist_df["composite_score"],
                    mode="lines",
                    name="Fragility score",
                    line=dict(color="#e67e22", width=1.5, dash="dash"),
                    yaxis="y2",
                ))
            fig_hist.add_hline(y=0.70, line_dash="dash", line_color="#2ecc71",
                                annotation_text="NORMAL", yref="y")
            fig_hist.add_hline(y=0.50, line_dash="dash", line_color="#f39c12",
                                annotation_text="WARN", yref="y")
            fig_hist.update_layout(
                height=280,
                margin=dict(l=10, r=10, t=20, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0),
                xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
                yaxis=dict(title="Realization Ratio", tickformat=".0%", range=[0, 1.1]),
                yaxis2=dict(title="Fragility Score", overlaying="y", side="right",
                            range=[0, 1.1], showgrid=False),
                plot_bgcolor="white",
                paper_bgcolor="white",
            )
            st.plotly_chart(fig_hist, width="stretch", key="cockpit_hist")

    st.markdown("---")

    # ------------------------------------------------------------------
    # Row 5: Narrative alerts
    # ------------------------------------------------------------------
    st.subheader("Active Alerts")

    # Realization alerts (non-NORMAL assets)
    if not real_df.empty:
        alert_real = real_df[real_df["status_level"].isin(["ALERT", "CRITICAL", "WARN"])]
        if not alert_real.empty:
            st.markdown("**Realization Narratives**")
            for _, arow in alert_real.iterrows():
                color = _STATUS_COLOR.get(str(arow["status_level"]), "#95a5a6")
                st.markdown(
                    f'<div style="background:{color}20; border-left:4px solid {color}; '
                    f'padding:8px 12px; margin-bottom:8px; border-radius:3px;">'
                    f'<b>[{arow["status_level"]}] {arow["asset_code"]}</b> — '
                    f'{arow.get("narrative", "")}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # Fragility alerts (HIGH/CRITICAL)
    if not frag_df.empty:
        alert_frag = frag_df[frag_df["fragility_level"].isin(["HIGH", "CRITICAL"])]
        if not alert_frag.empty:
            st.markdown("**Fragility Narratives**")
            for _, frow in alert_frag.iterrows():
                color = _FRAGILITY_COLOR.get(str(frow["fragility_level"]), "#95a5a6")
                st.markdown(
                    f'<div style="background:{color}20; border-left:4px solid {color}; '
                    f'padding:8px 12px; margin-bottom:8px; border-radius:3px;">'
                    f'<b>[{frow["fragility_level"]}] {frow["asset_code"]}</b> — '
                    f'{frow.get("narrative", "")}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    if (real_df.empty or not real_df["status_level"].isin(["ALERT", "CRITICAL", "WARN"]).any()) \
            and (frag_df.empty or not frag_df["fragility_level"].isin(["HIGH", "CRITICAL"]).any()):
        st.success("No active WARN/ALERT/CRITICAL conditions. Fleet is operating normally.")


# ---------------------------------------------------------------------------
# Standalone dev entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    st.set_page_config(page_title="BESS Options Cockpit (dev)", layout="wide")
    render_cockpit_page()
