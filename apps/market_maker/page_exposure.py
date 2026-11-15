"""Exposure & MtM page: month x book net MWh, physical vs financial, hedge ratio, MtM."""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from apps.market_maker.data import (
    DASH, PHYS_FIN_COLORS, book_name_map, fmt_num, get_curve, month_label, selected_positions,
)
from libs.risk.exposure import exposure


def _layout(fig, title: str, ytitle: str):
    fig.update_layout(title=title, height=340, margin=dict(l=10, r=10, t=50, b=10),
                      yaxis_title=ytitle, hovermode="x unified", plot_bgcolor="#fcfcfb",
                      legend=dict(orientation="h", y=1.12, x=0))
    fig.update_yaxes(gridcolor="#e8e7e3", zerolinecolor="#a3a29c")
    fig.update_xaxes(showgrid=False)
    return fig


def render_exposure(engine, sel: dict) -> None:
    st.subheader("Exposure & MtM")
    pos = selected_positions(engine, sel)
    if pos.empty:
        st.info("No positions for the selected books and provinces — no exposure to show.")
        return
    exp = exposure(pos, get_curve(engine, sel["as_of"]))
    names = book_name_map(sel)
    exp["month"] = exp["delivery_month"].map(month_label)
    exp["book"] = exp["book_id"].map(lambda b: f"{b} · {names.get(int(b), '?')}")

    st.markdown("**Net MWh by month × book** (buy +, sell −)")
    mat = exp.pivot_table(index="month", columns="book", values="net_mwh", aggfunc="sum")
    st.dataframe(mat.map(fmt_num), width="stretch")

    by_m = exp.groupby("month", sort=True)[["physical_mwh", "financial_mwh"]].sum()
    c1, c2 = st.columns(2)
    fig = go.Figure()
    for col, name in (("physical_mwh", "Physical"), ("financial_mwh", "Financial")):
        fig.add_bar(x=by_m.index, y=by_m[col], name=name, marker_color=PHYS_FIN_COLORS[name],
                    marker_line=dict(color="#fcfcfb", width=2),
                    hovertemplate="%{x}<br>" + name + ": %{y:,.0f} MWh<extra></extra>")
    fig.update_layout(barmode="relative")
    c1.plotly_chart(_layout(fig, "Physical vs financial MWh", "MWh"), width="stretch")

    hr = pd.Series(np.where(by_m["physical_mwh"] != 0,
                            -by_m["financial_mwh"] / by_m["physical_mwh"].replace(0, np.nan), np.nan),
                   index=by_m.index)
    fig2 = go.Figure(go.Scatter(x=hr.index, y=hr.values, mode="lines+markers", name="Hedge ratio",
                                line=dict(color=PHYS_FIN_COLORS["Physical"], width=2),
                                marker=dict(size=8), connectgaps=False,
                                hovertemplate="%{x}<br>Hedge ratio: %{y:.0%}<extra></extra>"))
    fig2.update_yaxes(tickformat=".0%")
    c2.plotly_chart(_layout(fig2, "Hedge ratio (−financial / physical)", "ratio"),
                    width="stretch")
    if hr.isna().any():
        c2.caption("No physical volume (ratio undefined): " + ", ".join(hr.index[hr.isna()]))

    st.markdown("**Mark-to-market**")
    no_mark = sorted(exp.loc[exp["mark_cny_mwh"].isna(), "month"].unique())
    view = exp[["book", "province", "month", "tou_block", "physical_mwh", "financial_mwh",
                "net_mwh", "mark_cny_mwh", "mtm_cny"]].sort_values(["book", "month", "tou_block"])
    for c in ("physical_mwh", "financial_mwh", "net_mwh", "mtm_cny"):
        view[c] = view[c].map(fmt_num)
    view["mark_cny_mwh"] = view["mark_cny_mwh"].map(lambda v: fmt_num(v, 1))
    st.dataframe(view, hide_index=True, width="stretch")
    total = exp["mtm_cny"].sum(min_count=1)
    st.metric("Total MtM (marked rows), ¥", fmt_num(total))
    if no_mark:
        st.caption(f"{DASH} = no curve mark. Months without a mark (excluded from MtM): "
                   + ", ".join(no_mark))
