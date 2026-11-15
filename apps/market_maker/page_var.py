"""VaR & Greeks page: net delta by month, portfolio VaR, BESS strip Greeks, spot stress."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from apps.market_maker.data import (
    DASH, SERIES, book_name_map, coverage_note, fmt_num, forward_only, get_curve, month_label,
    price_scenarios, selected_positions,
)
from libs.risk.bess_greeks import bess_monthly_greeks
from libs.risk.exposure import exposure
from libs.risk.var import portfolio_var


def _delta_section(exp: pd.DataFrame, names: dict) -> None:
    st.markdown("**Net delta by month** (MWh, buy +)")
    by_m = exp.groupby("month", sort=True)["net_mwh"].sum()
    fig = go.Figure(go.Bar(x=by_m.index, y=by_m.values, name="Portfolio", marker_color=SERIES[0],
                           marker_line=dict(color="#fcfcfb", width=2),
                           hovertemplate="%{x}<br>Net delta: %{y:,.0f} MWh<extra></extra>"))
    fig.update_layout(title="Portfolio net delta", height=300, margin=dict(l=10, r=10, t=50, b=10),
                      yaxis_title="MWh", plot_bgcolor="#fcfcfb")
    fig.update_yaxes(gridcolor="#e8e7e3", zerolinecolor="#a3a29c")
    st.plotly_chart(fig, width="stretch")
    per_book = exp.assign(book=exp["book_id"].map(lambda b: f"{b} · {names.get(int(b), '?')}")
                          ).pivot_table(index="month", columns="book", values="net_mwh", aggfunc="sum")
    per_book["Portfolio"] = per_book.sum(axis=1, min_count=1)
    st.dataframe(per_book.map(fmt_num), width="stretch")


def _var_section(engine, exp: pd.DataFrame, as_of) -> None:
    st.markdown("**Portfolio VaR** (historical / parametric, ¥)")
    deltas = exp.groupby("province")["net_mwh"].sum().to_dict()
    scen = {}
    for p, d in deltas.items():
        if d != 0:
            s = price_scenarios(engine, p, as_of)
            if not s.empty:
                scen[p] = s
    res = {(c, h): portfolio_var(deltas, scen, confidence=c, horizon_days=h)
           for c in (0.95, 0.99) for h in (1, 10)}
    cols = st.columns(4)
    for col, ((c, h), r) in zip(cols, res.items()):
        col.metric(f"{c:.0%} · {h}d historical", fmt_num(r["historical"]))
        col.caption(f"parametric {fmt_num(r['parametric'])} · n={r['n_scenarios']}")
    r0 = res[(0.95, 1)]
    if r0["historical"] is None and any(d != 0 for d in deltas.values()):
        st.caption(f"{DASH} = fewer than 20 aligned daily scenarios (have {r0['n_scenarios']}).")
    for p, mwh in r0["delta_missing_mwh"].items():
        st.warning(f"VaR excludes {mwh:,.0f} MWh in {p} (no price history)")


def _stress_section(exp: pd.DataFrame) -> None:
    st.markdown("**Spot stress on net delta**")
    pct = st.slider("Spot price shock (%)", -50, 50, 10, step=5, key="mm_stress_pct")
    marked = exp[exp["mark_cny_mwh"].notna()]
    pnl = float((marked["net_mwh"] * marked["mark_cny_mwh"]).sum()) * pct / 100.0
    st.metric(f"P&L for {pct:+d}% shock, ¥", fmt_num(pnl) if not marked.empty else DASH)
    unmarked = exp[exp["mark_cny_mwh"].isna()]
    if not unmarked.empty:
        st.caption(f"Excludes {unmarked['net_mwh'].sum():,.0f} net MWh with no curve mark "
                   f"(months: {', '.join(sorted(unmarked['month'].unique()))}).")


def _bess_section(sel: dict, curve: pd.DataFrame) -> None:
    st.markdown("**BESS Greeks** (Margrabe spread-call strip per month)")
    books = sel["books"]
    bess = books[(books["asset_type"] == "bess") & books["book_id"].isin(sel["book_ids"])
                 & books["province"].isin(sel["provinces"])]
    if bess.empty:
        st.info("No BESS asset books in the selection.")
        return
    sb = st.sidebar
    sb.subheader("BESS Greeks inputs")
    rte = sb.number_input("Round-trip efficiency", 0.5, 1.0, 0.85, 0.01, key="mm_rte")
    pv = sb.number_input("Peak vol", 0.05, 3.0, 0.6, 0.05, key="mm_peak_vol")
    ov = sb.number_input("Off-peak vol", 0.05, 3.0, 0.6, 0.05, key="mm_offpeak_vol")
    for b in bess.itertuples(index=False):
        if pd.isna(b.capacity_mw) or pd.isna(b.bess_duration_h):
            st.warning(f"{b.book_id} · {b.book_name}: capacity_mw / bess_duration_h missing.")
            continue
        g = bess_monthly_greeks(float(b.capacity_mw), float(b.bess_duration_h), rte, sel["months"],
                                curve, b.province, pv, ov, as_of=sel["as_of"])
        st.markdown(f"*{b.book_id} · {b.book_name}* — {b.capacity_mw:g} MW × "
                    f"{b.bess_duration_h:g} h, {b.province}")
        view = g.copy()
        view["delivery_month"] = view["delivery_month"].map(month_label)
        for c in ("peak_cny_mwh", "offpeak_cny_mwh"):
            view[c] = view[c].map(lambda v: fmt_num(v, 1))
        for c in ("value_cny", "delta_peak_mwh", "delta_offpeak_mwh", "vega_cny_per_volpt"):
            view[c] = view[c].map(fmt_num)
        st.dataframe(view, hide_index=True, width="stretch")
        if g["note"].astype(bool).any():
            notes = g.loc[g["note"].astype(bool)]
            st.caption("; ".join(f"{month_label(m)}: {n}"
                                 for m, n in zip(notes["delivery_month"], notes["note"])))


def render_var(engine, sel: dict) -> None:
    st.subheader("VaR & Greeks")
    curve = get_curve(engine, sel["as_of"])
    pos = selected_positions(engine, sel)
    if pos.empty:
        st.info("No positions for the selected books and provinces — no delta or VaR to show.")
    else:
        st.caption(coverage_note(sel["as_of"]))
        exp = forward_only(exposure(pos, curve), sel["as_of"])
        if exp.empty:
            st.info("No open positions in forward delivery months — no delta or VaR to show.")
            _bess_section(sel, curve)
            return
        exp["month"] = exp["delivery_month"].map(month_label)
        _delta_section(exp, book_name_map(sel))
        _var_section(engine, exp, sel["as_of"])
        _stress_section(exp)
    _bess_section(sel, curve)
