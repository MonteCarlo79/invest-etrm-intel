"""
libs/decision_models/adapters/app/dispatch_pnl_page.py

Streamlit page: Mengxi BESS Dispatch & P&L Waterfall.

Hero view for the 5-step P&L attribution chain:
  PF Unrestricted → PF Grid-Feasible → Forecast Optimal
  → Nomination → Trading Cleared → Actual Cleared

Data sources:
  - Pre-computed P&L:   reports.bess_asset_daily_attribution
  - Trading cleared:    marketdata.md_id_cleared_energy
  - Ops dispatch:       marketdata.ops_bess_dispatch_15min  (nominated + actual)
  - RT nodal price:     canon.nodal_rt_price_15min
  - PF/Forecast series: libs.decision_models.workflows.daily_strategy_report

No on-demand LP computation — all P&L values come from pre-computed DB tables.
The dispatch chart series load from DB; PF/Forecast series are fetched from the
existing workflow payload (may be slow if not yet cached).

Drop into any Streamlit app:
    from libs.decision_models.adapters.app.dispatch_pnl_page import render_dispatch_pnl_page
    render_dispatch_pnl_page(asset_code, trade_date)
"""
from __future__ import annotations

import datetime
import os
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_IM_ASSET_CODES = ["suyou", "hangjinqi", "siziwangqi", "gushanliang"]
_IM_ASSET_DISPLAY = {
    "suyou":       "SuYou (景蓝乌尔图)",
    "hangjinqi":   "HangJinQi (悦杭独贵)",
    "siziwangqi":  "SiZiWangQi (景通四益堂储)",
    "gushanliang": "GuShanLiang (裕昭沙子坝)",
}

# Waterfall step definitions: (key, display_label, color)
_WATERFALL_STEPS = [
    ("pf_unrestricted_pnl",   "PF Unrestricted",  "#2ecc71"),
    ("pf_grid_feasible_pnl",  "PF Grid-Feasible", "#27ae60"),
    ("tt_forecast_optimal_pnl","Forecast Optimal", "#3498db"),
    ("nominated_pnl",          "Nomination",       "#e67e22"),
    ("trading_cleared_pnl",    "Trading Cleared",  "#e74c3c"),
    ("cleared_actual_pnl",     "Actual Cleared",   "#c0392b"),
]

# Loss steps: (label, from_key, to_key, color)
_LOSS_STEPS = [
    ("Grid Restriction",  "pf_unrestricted_pnl",    "pf_grid_feasible_pnl",   "#e74c3c"),
    ("Forecast Error",    "pf_grid_feasible_pnl",   "tt_forecast_optimal_pnl","#e67e22"),
    ("Nomination Gap",    "tt_forecast_optimal_pnl","nominated_pnl",           "#f39c12"),
    ("Market Clearing",   "nominated_pnl",           "trading_cleared_pnl",    "#e67e22"),
    ("Execution",         "trading_cleared_pnl",     "cleared_actual_pnl",     "#c0392b"),
]


# ---------------------------------------------------------------------------
# DB engine
# ---------------------------------------------------------------------------

@st.cache_resource
def _engine():
    from sqlalchemy import create_engine
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        st.error("PGURL environment variable is not set.")
        st.stop()
    return create_engine(url, pool_pre_ping=True)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def _load_attribution_pnl(asset_code: str, trade_date: datetime.date) -> Dict[str, Any]:
    """Load pre-computed P&L values from reports.bess_asset_daily_attribution."""
    from sqlalchemy import text
    sql = text("""
        SELECT
            pf_unrestricted_pnl,
            pf_grid_feasible_pnl,
            tt_forecast_optimal_pnl,
            nominated_pnl,
            cleared_actual_pnl
        FROM reports.bess_asset_daily_attribution
        WHERE asset_code = :asset
          AND trade_date = :dt
        LIMIT 1
    """)
    try:
        df = pd.read_sql(sql, _engine(), params={"asset": asset_code, "dt": trade_date})
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as exc:
        st.warning(f"Attribution table unavailable: {exc}")
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def _load_trading_cleared_pnl(asset_code: str, trade_date: datetime.date) -> Optional[float]:
    """
    Compute trading cleared P&L from marketdata.md_id_cleared_energy.
    P&L = SUM(cleared_energy_mwh_15min * cleared_price) for the day.
    """
    from sqlalchemy import text
    sql = text("""
        SELECT COALESCE(SUM(cleared_energy_mwh_15min * cleared_price), 0) AS trading_cleared_pnl
        FROM marketdata.md_id_cleared_energy
        WHERE asset_code = :asset
          AND time::date = :dt
    """)
    try:
        df = pd.read_sql(sql, _engine(), params={"asset": asset_code, "dt": trade_date})
        if df.empty:
            return None
        val = df.iloc[0]["trading_cleared_pnl"]
        return float(val) if val is not None else None
    except Exception as exc:
        st.warning(f"md_id_cleared_energy unavailable: {exc}")
        return None


@st.cache_data(ttl=300, show_spinner=False)
def _load_ops_dispatch(asset_code: str, trade_date: datetime.date) -> pd.DataFrame:
    """
    Load nominated and actual dispatch + nodal price from marketdata.ops_bess_dispatch_15min.
    Returns: DataFrame with columns [time, nominated_mw, actual_mw, nodal_price]
    """
    from sqlalchemy import text
    sql = text("""
        SELECT
            interval_start AS time,
            nominated_dispatch_mw  AS nominated_mw,
            actual_dispatch_mw     AS actual_mw,
            nodal_price_excel      AS nodal_price
        FROM marketdata.ops_bess_dispatch_15min
        WHERE asset_code = :asset
          AND data_date = :dt
        ORDER BY interval_start
    """)
    try:
        df = pd.read_sql(sql, _engine(), params={"asset": asset_code, "dt": trade_date},
                         parse_dates=["time"])
        return df
    except Exception as exc:
        st.warning(f"ops_bess_dispatch_15min unavailable: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _load_trading_cleared_dispatch(asset_code: str, trade_date: datetime.date) -> pd.DataFrame:
    """
    Load trading cleared energy series from marketdata.md_id_cleared_energy.
    Returns: DataFrame with columns [time, cleared_mwh, cleared_price]
    """
    from sqlalchemy import text
    sql = text("""
        SELECT
            time,
            cleared_energy_mwh_15min  AS cleared_mwh,
            cleared_price
        FROM marketdata.md_id_cleared_energy
        WHERE asset_code = :asset
          AND time::date = :dt
        ORDER BY time
    """)
    try:
        df = pd.read_sql(sql, _engine(), params={"asset": asset_code, "dt": trade_date},
                         parse_dates=["time"])
        return df
    except Exception as exc:
        st.warning(f"md_id_cleared_energy dispatch series unavailable: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _load_nodal_price(asset_code: str, trade_date: datetime.date) -> pd.DataFrame:
    """Load 15-min nodal RT prices from canon.nodal_rt_price_15min."""
    from sqlalchemy import text
    sql = text("""
        SELECT time, price
        FROM canon.nodal_rt_price_15min
        WHERE asset_code = :asset
          AND time::date = :dt
        ORDER BY time
    """)
    try:
        df = pd.read_sql(sql, _engine(), params={"asset": asset_code, "dt": trade_date},
                         parse_dates=["time"])
        return df
    except Exception as exc:
        st.warning(f"nodal_rt_price_15min unavailable: {exc}")
        return pd.DataFrame()


def _load_pf_forecast_dispatch(asset_code: str, date_str: str) -> Dict[str, Any]:
    """
    Load PF and Forecast Optimal dispatch series by running the existing workflow.
    Returns dispatch_chart_data dict from render_bess_strategy_dashboard_payload.
    This may be slow (LP computation) — caller should wrap in st.spinner.
    """
    try:
        from libs.decision_models.workflows.daily_strategy_report import (
            run_bess_daily_strategy_analysis,
            render_bess_strategy_dashboard_payload,
        )
        analysis = run_bess_daily_strategy_analysis(
            asset_code, date_str, use_ops_dispatch=True,
        )
        payload = render_bess_strategy_dashboard_payload(
            asset_code, date_str, analysis=analysis,
        )
        return payload.get("dispatch_chart_data", {})
    except Exception as exc:
        st.warning(f"PF/Forecast dispatch series unavailable: {exc}")
        return {}


# ---------------------------------------------------------------------------
# Chart builders
# ---------------------------------------------------------------------------

def _build_waterfall_chart(pnl: Dict[str, Any]) -> "go.Figure":
    """
    Plotly absolute-value waterfall chart showing the P&L cascade.
    Uses go.Bar with a connector to show the step-down from PF to Actual.
    """
    import plotly.graph_objects as go

    labels = []
    values = []
    colors = []
    for key, label, color in _WATERFALL_STEPS:
        v = pnl.get(key)
        if v is not None:
            labels.append(label)
            values.append(float(v))
            colors.append(color)

    if not labels:
        fig = go.Figure()
        fig.add_annotation(text="No P&L data", xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False, font=dict(size=14))
        fig.update_layout(height=300, plot_bgcolor="white", paper_bgcolor="white")
        return fig

    # Build Plotly waterfall trace — show each level as absolute value
    # with connector lines between steps
    measures = ["absolute"] * len(labels)
    y_vals = values

    fig = go.Figure(go.Waterfall(
        orientation="v",
        measure=measures,
        x=labels,
        y=y_vals,
        connector=dict(line=dict(color="rgb(63,63,63)", width=1, dash="solid")),
        decreasing=dict(marker=dict(color="#e74c3c")),
        increasing=dict(marker=dict(color="#2ecc71")),
        totals=dict(marker=dict(color="#3498db")),
        text=[f"¥{v:,.0f}" for v in y_vals],
        textposition="outside",
        hovertemplate="%{x}<br>¥%{y:,.0f}<extra></extra>",
    ))

    # Overlay with individual colored bars for clarity
    fig.data = []  # reset — use a grouped bar instead for clearer coloring
    fig.add_trace(go.Bar(
        x=labels,
        y=y_vals,
        marker_color=colors,
        text=[f"¥{v:,.0f}" for v in y_vals],
        textposition="outside",
        hovertemplate="%{x}<br>¥%{y:,.0f}<extra></extra>",
        name="P&L (¥)",
    ))

    # Add connector lines between consecutive bars
    for i in range(len(values) - 1):
        v_curr = values[i]
        fig.add_shape(
            type="line",
            x0=i + 0.4, x1=i + 0.6,
            y0=v_curr, y1=v_curr,
            line=dict(color="#555", width=1, dash="dot"),
        )

    y_min = min(values) * 0.9 if values else 0
    y_max = max(values) * 1.15 if values else 1

    fig.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=40, b=20),
        yaxis=dict(
            title="P&L (¥)",
            tickformat=",.0f",
            range=[max(0, y_min), y_max],
            showgrid=True,
            gridcolor="#f0f0f0",
        ),
        xaxis=dict(showgrid=False),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
        bargap=0.3,
    )
    return fig


def _build_dispatch_chart(
    ops_df: pd.DataFrame,
    cleared_df: pd.DataFrame,
    price_df: pd.DataFrame,
    chart_data: Dict[str, Any],
) -> "go.Figure":
    """
    2-subplot dispatch chart:
      Row 1: all 5 dispatch series (MWh/15min)
      Row 2: RT nodal price (CNY/MWh)
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        row_heights=[0.65, 0.35],
        vertical_spacing=0.06,
        subplot_titles=("Dispatch (MWh per 15-min interval)", "RT Nodal Price (CNY/MWh)"),
    )

    def _strip_tz(idx):
        if idx.tz is not None:
            return pd.DatetimeIndex(
                [t.replace(tzinfo=None) for t in idx.tz_convert("Asia/Shanghai")]
            )
        return idx

    bar_width_ms = 14 * 60 * 1000

    # ── Actual Cleared Dispatch — solid bar (row 1) ─────────────────────────
    if not ops_df.empty and "actual_mw" in ops_df.columns:
        _idx = _strip_tz(pd.to_datetime(ops_df["time"]))
        # Convert MW to MWh/15min (÷4)
        _vals = ops_df["actual_mw"].fillna(0) / 4
        fig.add_trace(go.Bar(
            x=_idx,
            y=_vals,
            name="Actual Cleared",
            marker=dict(color="#c0392b", opacity=0.40),
            width=bar_width_ms,
            hovertemplate="%{y:.3f} MWh<extra>Actual Cleared</extra>",
        ), row=1, col=1)

    # ── Trading Cleared (md_id_cleared_energy) — bar overlay (row 1) ────────
    if not cleared_df.empty and "cleared_mwh" in cleared_df.columns:
        _idx = _strip_tz(pd.to_datetime(cleared_df["time"]))
        fig.add_trace(go.Bar(
            x=_idx,
            y=cleared_df["cleared_mwh"].fillna(0),
            name="Trading Cleared",
            marker=dict(color="#e74c3c", opacity=0.35),
            width=bar_width_ms,
            hovertemplate="%{y:.3f} MWh<extra>Trading Cleared</extra>",
        ), row=1, col=1)

    # ── Step lines for Nominated, PF, Forecast Opt (row 1) ──────────────────
    step_series = []

    if not ops_df.empty and "nominated_mw" in ops_df.columns:
        _idx = _strip_tz(pd.to_datetime(ops_df["time"]))
        _vals = ops_df["nominated_mw"].fillna(0) / 4
        step_series.append(("Nominated (申报)", _idx, _vals, "#4C72B0", "solid"))

    # PF and Forecast Opt from workflow payload
    if chart_data:
        pf_ts = chart_data.get("pf_timestamps", [])
        pf_vals = chart_data.get("pf_dispatch_mwh", [])
        if pf_ts and pf_vals:
            _idx = _strip_tz(pd.to_datetime(pf_ts))
            step_series.append(("PF Dispatch", _idx, pf_vals, "#2ca02c", "dash"))

        fc_ts = chart_data.get("forecast_timestamps", [])
        fc_vals = chart_data.get("forecast_dispatch_mwh", [])
        if fc_ts and fc_vals:
            _idx = _strip_tz(pd.to_datetime(fc_ts))
            step_series.append(("Forecast Optimal", _idx, fc_vals, "#E69F00", "dashdot"))

    for label, idx, vals, color, dash in step_series:
        fig.add_trace(go.Scatter(
            x=idx,
            y=vals,
            mode="lines",
            name=label,
            line=dict(color=color, dash=dash, width=1.8, shape="hv"),
            hovertemplate="%{y:.3f} MWh<extra>" + label + "</extra>",
        ), row=1, col=1)

    # ── RT Nodal Price (row 2) ───────────────────────────────────────────────
    if not price_df.empty:
        _idx = _strip_tz(pd.to_datetime(price_df["time"]))
        fig.add_trace(go.Scatter(
            x=_idx,
            y=price_df["price"],
            mode="lines",
            name="RT Price",
            line=dict(color="#9467BD", width=1.5),
            hovertemplate="%{y:,.0f} CNY/MWh<extra>RT Price</extra>",
        ), row=2, col=1)

    fig.update_layout(
        height=520,
        hovermode="x unified",
        barmode="overlay",
        legend=dict(orientation="h", yanchor="bottom", y=1.04, xanchor="left", x=0),
        margin=dict(l=60, r=20, t=70, b=40),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    fig.update_yaxes(title_text="MWh / 15-min", row=1, col=1)
    fig.update_yaxes(title_text="CNY/MWh", tickfont=dict(color="#9467BD"),
                     title_font=dict(color="#9467BD"), row=2, col=1)
    fig.update_xaxes(title_text="Time (CST)", row=2, col=1)

    return fig


# ---------------------------------------------------------------------------
# Main render function
# ---------------------------------------------------------------------------

def render_dispatch_pnl_page(asset_code: str, trade_date: datetime.date) -> None:
    """
    Render the Dispatch & P&L Waterfall page for a single asset + date.

    Parameters
    ----------
    asset_code : str
        Asset code, e.g. 'suyou', 'hangjinqi', 'siziwangqi', 'gushanliang'
    trade_date : datetime.date
        The trading date to analyse.
    """
    display = _IM_ASSET_DISPLAY.get(asset_code, asset_code)
    date_str = str(trade_date)

    st.header(f"Dispatch & P&L Waterfall — {display}")
    st.caption(
        f"Trading date: **{date_str}** · "
        "P&L chain: PF Unrestricted → PF Grid-Feasible → Forecast Optimal → Nomination → "
        "Trading Cleared (market) → Actual Cleared (physical)"
    )

    # ── Load pre-computed attribution P&L ────────────────────────────────────
    with st.spinner("Loading P&L attribution data…"):
        attr_pnl = _load_attribution_pnl(asset_code, trade_date)
        trading_cleared_pnl = _load_trading_cleared_pnl(asset_code, trade_date)

    # Merge trading_cleared_pnl into the attr dict
    pnl = dict(attr_pnl)
    if trading_cleared_pnl is not None:
        pnl["trading_cleared_pnl"] = trading_cleared_pnl

    attribution_available = bool(attr_pnl)
    if not attribution_available:
        st.info(
            f"No pre-computed attribution found for **{display}** on **{date_str}**. "
            "Run `python -m services.monitoring.run_daily_attribution` to populate "
            "`reports.bess_asset_daily_attribution`. "
            "Dispatch chart is still shown below from available series."
        )

    # ── P&L Metric Cards ─────────────────────────────────────────────────────
    if attribution_available or trading_cleared_pnl is not None:
        st.subheader("P&L by Stage")
        col_labels = ["PF Unrestricted", "Forecast Optimal", "Nomination",
                      "Trading Cleared", "Actual Cleared"]
        col_keys   = ["pf_unrestricted_pnl", "tt_forecast_optimal_pnl", "nominated_pnl",
                      "trading_cleared_pnl", "cleared_actual_pnl"]
        cols = st.columns(len(col_labels))
        for col, label, key in zip(cols, col_labels, col_keys):
            v = pnl.get(key)
            col.metric(label, f"¥{v:,.0f}" if v is not None else "—")

        # Loss metrics row
        st.subheader("Value Lost at Each Step")
        loss_cols = st.columns(len(_LOSS_STEPS))
        for col, (loss_label, from_key, to_key, _) in zip(loss_cols, _LOSS_STEPS):
            v_from = pnl.get(from_key)
            v_to   = pnl.get(to_key)
            if v_from is not None and v_to is not None:
                loss = v_from - v_to
                col.metric(loss_label, f"¥{loss:,.0f}", delta=f"{loss/v_from:.1%}" if v_from else None,
                           delta_color="inverse")
            else:
                col.metric(loss_label, "—")

        st.markdown("---")

    # ── Waterfall Chart ───────────────────────────────────────────────────────
    if pnl:
        st.subheader("P&L Waterfall")
        fig_wf = _build_waterfall_chart(pnl)
        st.plotly_chart(fig_wf, use_container_width=True, key="dispatch_pnl_waterfall")
        st.markdown("---")

    # ── Load dispatch series ──────────────────────────────────────────────────
    with st.spinner("Loading dispatch series…"):
        ops_df       = _load_ops_dispatch(asset_code, trade_date)
        cleared_df   = _load_trading_cleared_dispatch(asset_code, trade_date)
        price_df     = _load_nodal_price(asset_code, trade_date)

    # PF + Forecast Opt dispatch (may be slow — run with expander to keep page responsive)
    chart_data: Dict[str, Any] = {}
    _has_pf_in_daily_ops = (
        "id_cleared_timestamps" in chart_data or
        "pf_timestamps" in chart_data
    )
    with st.expander("Load PF & Forecast dispatch series (runs LP, ~10–30s)", expanded=False):
        if st.button("Compute PF & Forecast Optimal dispatch", key="btn_pf_dispatch"):
            with st.spinner("Running LP for PF & Forecast Optimal dispatch…"):
                chart_data = _load_pf_forecast_dispatch(asset_code, date_str)
                st.session_state["_dispatch_pnl_chart_data"] = chart_data
                st.success("Done.")

    # Restore from session state if previously computed
    chart_data = st.session_state.get("_dispatch_pnl_chart_data", {})

    # ── Dispatch Chart ────────────────────────────────────────────────────────
    st.subheader("Dispatch Chart")
    has_any_dispatch = (
        not ops_df.empty or not cleared_df.empty or not price_df.empty or bool(chart_data)
    )

    if has_any_dispatch:
        fig_dispatch = _build_dispatch_chart(ops_df, cleared_df, price_df, chart_data)
        st.plotly_chart(fig_dispatch, use_container_width=True, key="dispatch_pnl_dispatch_chart")
        st.caption(
            "Bars: Actual Cleared (dark red, semi-transparent), Trading Cleared (light red, overlay). "
            "Step lines: Nominated=blue solid, PF=green dashed, Forecast Opt=orange dash-dot. "
            "Positive = discharge, negative = charge. "
            "PF/Forecast series require LP computation (expand panel above)."
        )
    else:
        st.info(
            f"No dispatch data found for **{display}** on **{date_str}**. "
            "Check that the ops ingestion pipeline has processed this date "
            "(`marketdata.ops_bess_dispatch_15min`)."
        )

    # ── Data availability summary ─────────────────────────────────────────────
    with st.expander("Data availability", expanded=False):
        rows = [
            ("reports.bess_asset_daily_attribution", "✓" if attribution_available else "✗ empty"),
            ("md_id_cleared_energy (P&L)", "✓" if trading_cleared_pnl is not None else "✗ empty"),
            ("ops_bess_dispatch_15min (nominated + actual)", f"✓ {len(ops_df)} intervals" if not ops_df.empty else "✗ empty"),
            ("md_id_cleared_energy (dispatch series)", f"✓ {len(cleared_df)} intervals" if not cleared_df.empty else "✗ empty"),
            ("canon.nodal_rt_price_15min", f"✓ {len(price_df)} intervals" if not price_df.empty else "✗ empty"),
            ("PF / Forecast Opt dispatch", "✓ loaded" if chart_data else "not loaded (click above)"),
        ]
        for source, status in rows:
            color = "green" if status.startswith("✓") else "red"
            st.markdown(
                f'<span style="color:{color};">{status}</span> `{source}`',
                unsafe_allow_html=True,
            )
