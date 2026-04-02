# -*- coding: utf-8 -*-
"""
Strategy Diagnostics — Inner Mongolia BESS
==========================================
Streamlit multipage entry (pages/strategy_diagnostics.py).

Tabs (v1)
---------
1. Leaderboard                – ranked table         | evidence: observed
2. Envision vs Top Performers – metric comparison     | evidence: observed
3. Gap Decomposition          – additive breakdown    | evidence: proxy-based
4. Strategy Inference & Notes – heuristic labels      | evidence: heuristic inference

Container imports
-----------------
PYTHONPATH=/apps. `services/bess_inner_mongolia` copied to
/apps/services/bess_inner_mongolia via the one COPY line added to the Dockerfile.
Python 3.11 namespace packages — no services/__init__.py needed.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# path guard — mirrors app.py
_APP_ROOT = Path(__file__).resolve().parent.parent  # /apps
for _p in (str(_APP_ROOT), "/apps"):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

from auth.rbac import require_role
from services.bess_inner_mongolia.queries import (
    load_available_date_ranges,
    load_clusters,
    load_results_flat,
)
from services.bess_inner_mongolia.peer_benchmark import (
    _PRIMARY_RANK_COL,
    compute_gap_decomposition,
    compute_envision_vs_top,
    compute_leaderboard,
)
from services.bess_inner_mongolia.strategy_diagnostics import (
    DISCLAIMER,
    EVIDENCE_OBSERVED,
    EVIDENCE_PROXY,
    EVIDENCE_HEURISTIC,
    build_strategy_table,
)

# ── auth + page config ────────────────────────────────────────────────────────
require_role(["Admin", "Trader", "Quant", "Analyst"])

st.set_page_config(
    page_title="IM BESS — Strategy Diagnostics",
    layout="wide",
    page_icon="🧭",
)
load_dotenv()

ENVISION_TAG = "远景"

# ── helpers ───────────────────────────────────────────────────────────────────
_EV_COLOR = {EVIDENCE_OBSERVED: "green", EVIDENCE_PROXY: "orange", EVIDENCE_HEURISTIC: "red"}
_EV_ICON  = {EVIDENCE_OBSERVED: "🟢",   EVIDENCE_PROXY: "🟡",    EVIDENCE_HEURISTIC: "🟠"}


def _ev(level: str) -> str:
    return f":{_EV_COLOR.get(level,'gray')}[{_EV_ICON.get(level,'⚪')} **{level}**]"


def _hi(row: pd.Series) -> list[str]:
    if row.get("is_envision", False):
        return ["background-color:#1a5c36;color:white;font-weight:bold"] * len(row)
    return [""] * len(row)


_FMT: dict[str, str] = {
    _PRIMARY_RANK_COL:                        "{:,.0f}",
    "arbitrage_per_installed_volume_per_day": "{:,.0f}",
    "total_profit_per_discharge_mwh":         "{:,.0f}",
    "estimated_cycles_per_day":               "{:.3f}",
    "efficiency":                             "{:.1%}",
    "irr":                                    "{:.1%}",
    "payback_years":                          "{:.1f}",
    "MW":                                     "{:,.0f}",
    "expected_total_profit_万元":             "{:,.2f}",
}

# ── DB ────────────────────────────────────────────────────────────────────────
@st.cache_resource
def _engine():
    pg = os.getenv("PGURL")
    if not pg:
        st.error("❌ PGURL not set."); st.stop()
    return create_engine(pg)


@st.cache_data(ttl=300)
def _ranges() -> list[tuple[str, str]]:
    return load_available_date_ranges(_engine())


@st.cache_data(ttl=300)
def _results(s: str, e: str) -> pd.DataFrame:
    return load_results_flat(_engine(), s, e)


@st.cache_data(ttl=300)
def _clusters(s: str, e: str) -> pd.DataFrame:
    return load_clusters(_engine(), s, e)


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("🧭 Strategy Diagnostics")
st.sidebar.caption("Inner Mongolia BESS · Competitive Intelligence")

with st.sidebar:
    available = _ranges()
    if not available:
        st.warning("No pipeline results in DB. Run the main pipeline first.")
        st.stop()

    labels = [f"{s}  →  {e}" for s, e in available]
    idx = st.selectbox("Result period", range(len(labels)),
                       format_func=lambda i: labels[i], index=0)
    start_date, end_date = available[idx]
    top_n = st.slider("Top-N peers", 3, 15, 5)

    st.markdown("---")
    st.markdown(
        "**Evidence key**\n\n"
        f"- {_ev(EVIDENCE_OBSERVED)}: pipeline output\n"
        f"- {_ev(EVIDENCE_PROXY)}: model approximation\n"
        f"- {_ev(EVIDENCE_HEURISTIC)}: rule-based label"
    )

# ══════════════════════════════════════════════════════════════════════════════
# DATA
# ══════════════════════════════════════════════════════════════════════════════
result_df  = _results(start_date, end_date)
cluster_df = _clusters(start_date, end_date)

if result_df.empty:
    st.warning(f"No results for **{start_date} → {end_date}**."); st.stop()

env_mask = result_df.get("owner", pd.Series(dtype=str)).str.contains(ENVISION_TAG, na=False)
env_df   = result_df[env_mask]

# ── KPI strip ─────────────────────────────────────────────────────────────────
st.title("Strategy Diagnostics")
st.caption(
    f"Period: **{start_date}** → **{end_date}** · "
    f"{len(result_df)} assets · clusters: {'yes' if not cluster_df.empty else 'none'}"
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("远景 assets",    len(env_df))
k2.metric("Total BESS assets", len(result_df))

if _PRIMARY_RANK_COL in result_df.columns and not env_df.empty:
    pct_rank = 1.0 - result_df[_PRIMARY_RANK_COL].rank(ascending=False, pct=True)[env_df.index].median()
    k3.metric("远景 median percentile", f"{pct_rank*100:.0f}th",
              help=f"Profit/MW·day | {_ev(EVIDENCE_OBSERVED)}")
    best_env = float(env_df[_PRIMARY_RANK_COL].max())
    best_all = float(result_df[_PRIMARY_RANK_COL].max())
    k4.metric("远景 best vs market best",
              f"{best_env/best_all*100:.0f}%" if best_all > 0 else "N/A",
              help=f"Profit/MW·day ratio | {_ev(EVIDENCE_OBSERVED)}")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# TABS — PLACEHOLDER (filled by edits below)
# ══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4 = st.tabs([
    "🏆 Leaderboard",
    "📊 Envision vs Top Performers",
    "🔍 Gap Decomposition",
    "🧭 Strategy Inference & Notes",
])

# ─── TAB 1 ───────────────────────────────────────────────────────────────────
with tab1:
    st.subheader("Leaderboard — all BESS assets")
    st.markdown(f"Evidence: {_ev(EVIDENCE_OBSERVED)} — direct from pipeline output.")

    lb = compute_leaderboard(result_df, envision_tag=ENVISION_TAG)
    _cols1 = ["overall_rank","plant_name","owner","is_envision",
               _PRIMARY_RANK_COL,"arbitrage_per_installed_volume_per_day",
               "total_profit_per_discharge_mwh","estimated_cycles_per_day",
               "efficiency","irr","payback_years","MW","expected_total_profit_万元"]
    lb_disp = lb[[c for c in _cols1 if c in lb.columns]].copy()
    st.dataframe(
        lb_disp.style
            .format({k: v for k, v in _FMT.items() if k in lb_disp.columns})
            .apply(_hi, axis=1),
        use_container_width=True,
    )

    if all(c in lb.columns for c in ["estimated_cycles_per_day", _PRIMARY_RANK_COL]):
        st.markdown(f"#### Cycles vs Profit/MW·day  {_ev(EVIDENCE_OBSERVED)}")
        fig1 = px.scatter(
            lb, x="estimated_cycles_per_day", y=_PRIMARY_RANK_COL,
            color="is_envision",
            color_discrete_map={True: "#28a745", False: "#6c757d"},
            hover_name="plant_name",
            hover_data={"owner": True, "efficiency": ":.1%", "irr": ":.1%"},
            labels={"estimated_cycles_per_day": "Cycles/day",
                    _PRIMARY_RANK_COL: "Profit/MW·day (¥)",
                    "is_envision": "Envision"},
            title="Cycles vs Profitability",
        )
        fig1.update_traces(marker=dict(size=9, opacity=0.85))
        st.plotly_chart(fig1, use_container_width=True)

# ─── TAB 2 ───────────────────────────────────────────────────────────────────
with tab2:
    st.subheader(f"Envision vs Top-{top_n} Peers")
    st.markdown(f"Evidence: {_ev(EVIDENCE_OBSERVED)} — all values direct from pipeline.")

    vs = compute_envision_vs_top(result_df, top_n=top_n, envision_tag=ENVISION_TAG)

    if vs["envision"].empty:
        st.warning("No Envision assets found.")
    elif vs["top_peers"].empty:
        st.warning("No non-Envision peers found.")
    else:
        comp = vs["comparison"]
        _chart_cols = [_PRIMARY_RANK_COL, "estimated_cycles_per_day", "efficiency", "irr"]
        cc = comp[comp["col"].isin(_chart_cols)].copy()
        if not cc.empty:
            fig2 = go.Figure([
                go.Bar(x=cc["metric"], y=cc["envision_mean"],  name="Envision — mean",      marker_color="#28a745"),
                go.Bar(x=cc["metric"], y=cc["top_peer_mean"],  name=f"Top-{top_n} — mean",  marker_color="#6c757d"),
                go.Bar(x=cc["metric"], y=cc["top_peer_best"],  name=f"Top-{top_n} — best",  marker_color="#343a40"),
            ])
            fig2.update_layout(barmode="group", title=f"Envision vs Top-{top_n} Peers",
                               legend=dict(orientation="h", y=-0.28))
            st.plotly_chart(fig2, use_container_width=True)
            st.caption("⚠️ Chart mixes units — use table below for per-metric values.")

        st.markdown(f"#### Metric comparison  {_ev(EVIDENCE_OBSERVED)}")
        _gfmt = {k: "{:,.3f}" for k in ["envision_mean","envision_best","top_peer_mean","top_peer_best"]}
        _gfmt["gap_mean_vs_mean"] = "{:+,.3f}"
        cd = comp.drop(columns=["col","evidence_level"], errors="ignore")

        def _gap_color(col: pd.Series) -> list[str]:
            if col.name != "gap_mean_vs_mean":
                return [""] * len(col)
            return ["color:#dc3545" if (isinstance(v, float) and v < 0)
                    else "color:#28a745" if (isinstance(v, float) and v > 0)
                    else "" for v in col]

        st.dataframe(
            cd.style.format({k: v for k, v in _gfmt.items() if k in cd.columns}).apply(_gap_color),
            use_container_width=True,
        )

        st.markdown(f"#### Top-{top_n} peers detail")
        _p_cols = ["plant_name","owner",_PRIMARY_RANK_COL,"estimated_cycles_per_day","efficiency","irr","MW"]
        pd_disp = vs["top_peers"][[c for c in _p_cols if c in vs["top_peers"].columns]]
        st.dataframe(pd_disp.style.format({k: v for k, v in _FMT.items() if k in pd_disp.columns}),
                     use_container_width=True)

# ─── TAB 3 ───────────────────────────────────────────────────────────────────
with tab3:
    st.subheader("Gap Decomposition — Envision vs Best Peer")
    st.markdown(f"Evidence: {_ev(EVIDENCE_PROXY)} — additive model, each component holds other factors fixed.")
    st.info(
        "**Model note** — 2h duration proxy used (conservative; affects magnitudes, not ordering). "
        "The 'Unexplained' residual absorbs model error and omitted factors.", icon="ℹ️")

    gap_df = compute_gap_decomposition(result_df, envision_tag=ENVISION_TAG)

    if gap_df.empty:
        st.warning("Need both Envision and non-Envision assets to compute decomposition.")
    else:
        sel = st.selectbox("Select Envision asset", sorted(gap_df["plant_name"].unique()))
        rg  = gap_df[gap_df["plant_name"] == sel].iloc[0]

        env_b  = float(rg.get("envision_profit_day",  0) or 0)
        peer_b = float(rg.get("best_peer_profit_day", 0) or 0)
        comps  = {
            "Utilisation gap":   float(rg.get("utilization_gap",   0) or 0),
            "Price-capture gap": float(rg.get("price_capture_gap", 0) or 0),
            "Efficiency gap":    float(rg.get("efficiency_gap",    0) or 0),
            "Unexplained":       float(rg.get("unexplained",       0) or 0),
        }
        vals_w = [env_b] + list(comps.values()) + [peer_b]
        fig3 = go.Figure(go.Waterfall(
            orientation="v",
            measure=["absolute"] + ["relative"] * len(comps) + ["total"],
            x=["Envision base"] + list(comps.keys()) + ["Best peer"],
            y=vals_w,
            text=[f"{v:+,.0f}" for v in vals_w],
            textposition="outside",
            connector={"line": {"color": "rgb(63,63,63)"}},
            increasing={"marker": {"color": "#dc3545"}},
            decreasing={"marker": {"color": "#28a745"}},
            totals={"marker": {"color": "#343a40"}},
        ))
        fig3.update_layout(
            title=f"{sel} vs {rg.get('best_peer_name','best peer')} — gap (¥/MW·day)",
            yaxis_title="Profit/MW·day (¥)", showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

        st.markdown("#### All Envision assets — gap summary")
        _gcols = ["plant_name","envision_profit_day","best_peer_profit_day",
                  "total_gap","utilization_gap","price_capture_gap","efficiency_gap","unexplained","best_peer_name"]
        gd = gap_df[[c for c in _gcols if c in gap_df.columns]].copy()
        _gf2 = {"envision_profit_day":"{:,.0f}","best_peer_profit_day":"{:,.0f}",
                "total_gap":"{:+,.0f}","utilization_gap":"{:+,.0f}",
                "price_capture_gap":"{:+,.0f}","efficiency_gap":"{:+,.0f}","unexplained":"{:+,.0f}"}
        st.dataframe(gd.style.format({k: v for k, v in _gf2.items() if k in gd.columns}),
                     use_container_width=True)
        st.caption(f"{rg.get('decomp_note','')} | Evidence: {_ev(EVIDENCE_PROXY)}")

# ─── TAB 4 ───────────────────────────────────────────────────────────────────
with tab4:
    st.subheader("Strategy Inference & Notes")
    st.error(DISCLAIMER, icon="⚠️")

    st.markdown(f"""
**Evidence levels on this tab**

| Badge | Meaning |
|---|---|
| {_ev(EVIDENCE_OBSERVED)} | Direct pipeline output (market-cleared data) |
| {_ev(EVIDENCE_PROXY)} | Derived via model step applied to observed data |
| {_ev(EVIDENCE_HEURISTIC)} | Rule-based label; competitors' actual strategies are **not known** |
""")
    st.markdown("---")

    strat_df = build_strategy_table(
        result_df,
        clusters=cluster_df if not cluster_df.empty else None,
        envision_tag=ENVISION_TAG,
    )

    if strat_df.empty:
        st.warning("No strategy data."); st.stop()

    env_only = st.checkbox("Show 远景 assets only", value=True)
    ds = strat_df[strat_df["is_envision"]] if env_only else strat_df

    if ds.empty:
        st.warning("No Envision assets in current result set."); st.stop()

    # Style distribution
    st.markdown(f"#### Style distribution  {_ev(EVIDENCE_HEURISTIC)}")
    vc = ds["strategy_style"].value_counts().reset_index()
    vc.columns = ["style", "count"]
    fig4 = px.bar(vc, x="style", y="count", color="style",
                  title="Inferred strategy style (heuristic)",
                  labels={"style": "Style (inferred)", "count": "# assets"})
    fig4.update_layout(showlegend=False, xaxis_tickangle=-15)
    st.plotly_chart(fig4, use_container_width=True)

    # Per-asset detail
    st.markdown("#### Per-asset detail")
    st.caption(
        f"Cycles/day, efficiency, profit/MWh: {_ev(EVIDENCE_OBSERVED)} · "
        f"Strategy style: {_ev(EVIDENCE_HEURISTIC)} · "
        f"Nodal context: {_ev(EVIDENCE_PROXY)}"
    )
    sel_a = st.selectbox("Select asset", sorted(ds["plant_name"].tolist()))
    ar = ds[ds["plant_name"] == sel_a].iloc[0] if not ds[ds["plant_name"] == sel_a].empty else None

    if ar is not None:
        cl, cr = st.columns([1, 2])
        with cl:
            st.markdown(f"**Observed** {_ev(EVIDENCE_OBSERVED)}")
            cyc = ar.get("cycles_per_day"); eff = ar.get("efficiency"); ppm = ar.get("profit_per_mwh")
            st.metric("Cycles/day",      f"{cyc:.3f}" if cyc is not None else "N/A")
            st.metric("Round-trip eff.", f"{eff:.1%}"  if eff is not None else "N/A")
            st.metric("Profit/MWh (¥)", f"{ppm:,.0f}" if ppm is not None else "N/A")
        with cr:
            st.markdown(f"**Strategy style** {_ev(EVIDENCE_HEURISTIC)}")
            st.markdown(f"### {ar['strategy_style']}")
            st.markdown(f"**Rationale:** {ar['rationale']}")
        st.markdown(f"**Nodal context** {_ev(EVIDENCE_PROXY)}")
        st.info(ar["nodal_context"], icon="🗺️")

    # Full table
    st.markdown("#### Strategy table")
    _sc = ["plant_name","owner","is_envision","cycles_per_day","efficiency","profit_per_mwh","strategy_style","evidence_level"]
    sd = ds[[c for c in _sc if c in ds.columns]].copy()
    _sf = {"cycles_per_day": "{:.3f}", "efficiency": "{:.1%}", "profit_per_mwh": "{:,.0f}"}
    st.dataframe(
        sd.style.format({k: v for k, v in _sf.items() if k in sd.columns}).apply(_hi, axis=1),
        use_container_width=True,
    )

    # Analyst notes (session only in v1)
    st.markdown("---")
    st.markdown("#### Analyst Notes")
    st.caption(f"Session-state only (not persisted to DB in v1). Period: {start_date} → {end_date}.")
    nk   = f"strategy_notes_{start_date}_{end_date}"
    prev = st.session_state.get(nk, "")
    new  = st.text_area("Notes (session only)", value=prev, height=150,
                         placeholder="e.g. Asset X: 2.1 cycles/day — likely co-located solar forcing discharge. Gap appears price-capture dominated.")
    if new != prev:
        st.session_state[nk] = new
        st.success("Saved to session.")
