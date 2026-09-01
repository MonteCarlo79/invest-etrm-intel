"""Nodal Analysis tab — BESS zones, price zone explorer, congestion-day clusters.

Rendered inside apps/mengxi-dashboard/app.py as tab 11. Data sources:
  - services/mengxi_nodal.zones (mirror of knowledge/mengxi/bess_node_registry.md)
  - marketdata.md_id_cleared_energy (日内出清价)
  - marketdata.md_mengxi_nodal_price_96 (Fengxing 实时节点价 view)
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from services.mengxi_nodal.analysis import (
    cluster_day_prices as _na_cluster,
    detect_split_days as _na_splits,
)
from services.mengxi_nodal.data import (
    get_asset_price_vectors as _na_asset_vecs,
    get_day_node_matrix as _na_day_matrix,
    get_node_price_vectors as _na_node_vecs,
)
from services.mengxi_nodal.zones import (
    CURRENT_ASSETS as _NA_ASSETS,
    UPCOMING_ASSETS as _NA_UPCOMING,
    ZONES as _NA_ZONES,
)


def render(get_engine) -> None:
    st.title("Mengxi Nodal Analysis — BESS Zones & Price Clusters")
    st.caption(
        "Sources: bess_node_registry (knowledge/mengxi), marketdata.md_id_cleared_energy, "
        "marketdata.md_mengxi_nodal_price_96 (view over the multi-province nodal table)"
    )

    # ── Section 1: Zone Roster ───────────────────────────────────────────────
    st.header("1 · Zone Roster")
    _na_by_code = {a["asset_code"]: a for a in _NA_ASSETS}
    for _z in _NA_ZONES:
        with st.container(border=True):
            st.subheader(_z["zone"])
            _zc1, _zc2 = st.columns([1, 2])
            with _zc1:
                st.markdown("**Our assets**")
                if _z["our_assets"]:
                    for _c in _z["our_assets"]:
                        _a = _na_by_code[_c]
                        st.markdown(
                            f"- **{_a['plant_name']}** ({_a['asset_code']}"
                            + (f", {_a['capacity_mw']}MW" if _a["capacity_mw"] else "")
                            + f") — own node: `{_a['own_node'] or '— (settles at parent)'}`"
                        )
                else:
                    st.caption("—")
                if _z["sibling_bess"]:
                    st.markdown("**Sibling BESS**")
                    st.markdown(" · ".join(_z["sibling_bess"]))
            with _zc2:
                st.markdown("**Zone plants (wind/solar/thermal)**")
                st.markdown(" · ".join(_z["sibling_plants"]) if _z["sibling_plants"] else "—")
    with st.expander("Upcoming assets (simulation study)", expanded=False):
        for _u in _NA_UPCOMING:
            st.markdown(f"- **{_u['asset_code']}** — {_u['capacity']}, {_u['substation']} ({_u['conn_kv']}kV)")

    # ── Section 2: Price Zone Explorer ───────────────────────────────────────
    st.header("2 · Price Zone Explorer")
    _na_c1, _na_c2, _na_c3 = st.columns([2, 1, 1])
    with _na_c1:
        _na_asset = st.selectbox(
            "Asset",
            _NA_ASSETS,
            format_func=lambda a: f"{a['plant_name']} ({a['asset_code']})",
            key="na_asset",
        )
    with _na_c2:
        _na_start = st.date_input("From", value=date(2026, 8, 1), key="na_start")
    with _na_c3:
        _na_end = st.date_input("To", value=date(2026, 8, 30), key="na_end")

    @st.cache_data(ttl=300, show_spinner="Loading price series…")
    def _na_load_explorer(plant, parents, own, start, end):
        eng = get_engine()
        asset_vecs = _na_asset_vecs(eng, plant, start, end)
        nodes = list(parents) + ([own] if own else [])
        node_vecs = _na_node_vecs(eng, nodes, start, end)
        return asset_vecs, node_vecs

    if _na_start > _na_end:
        st.warning("Start date must be ≤ end date.")
    else:
        _na_av, _na_nv = _na_load_explorer(
            _na_asset["plant_name"], tuple(_na_asset["parent_nodes"]), _na_asset["own_node"],
            _na_start, _na_end,
        )
        if not _na_av:
            st.info(f"No cleared_price data for {_na_asset['plant_name']} in range.")
        else:
            # parent reference: mean of available parent nodes per slot

            def _na_parent_vec(day):
                mats = [v[day] for v in _na_nv.values() if day in v]
                if not mats:
                    return None
                return np.nanmean(np.stack(mats), axis=0)

            _na_parent = {}
            for _d in _na_av:
                _pv = _na_parent_vec(_d)
                if _pv is not None:
                    _na_parent[_d] = _pv

            # Reference for split detection: own meter node where it exists,
            # else parent bus (四子王旗 settles at 杜尔伯特站).
            _na_ref_name = "own meter node" if (_na_asset["own_node"] and _na_asset["own_node"] in _na_nv) else "parent bus (mean)"
            _na_ref = _na_nv[_na_asset["own_node"]] if _na_ref_name == "own meter node" else _na_parent
            _na_split_days = _na_splits(_na_av, _na_ref, tol=0.01, min_match=0.99)

            # long-form series for plotting
            _na_rows = []
            for _d, _v in sorted(_na_av.items()):
                _base = datetime(_d.year, _d.month, _d.day)
                for _i in range(96):
                    if not np.isnan(_v[_i]):
                        _na_rows.append((_base + timedelta(minutes=15 * _i), "asset cleared_price", _v[_i]))
            for _d, _v in sorted(_na_parent.items()):
                _base = datetime(_d.year, _d.month, _d.day)
                for _i in range(96):
                    if not np.isnan(_v[_i]):
                        _na_rows.append((_base + timedelta(minutes=15 * _i), "parent bus (mean)", _v[_i]))
            if _na_asset["own_node"] and _na_asset["own_node"] in _na_nv:
                for _d, _v in sorted(_na_nv[_na_asset["own_node"]].items()):
                    _base = datetime(_d.year, _d.month, _d.day)
                    for _i in range(96):
                        if not np.isnan(_v[_i]):
                            _na_rows.append((_base + timedelta(minutes=15 * _i), "own meter node", _v[_i]))

            _na_fig = go.Figure()
            _na_colors = {"asset cleared_price": "#d62728", "parent bus (mean)": "#1f77b4", "own meter node": "#2ca02c"}
            _na_pdf = pd.DataFrame(_na_rows, columns=["ts", "series", "price"])
            for _s, _g in _na_pdf.groupby("series"):
                _na_fig.add_trace(go.Scatter(
                    x=_g["ts"], y=_g["price"], name=_s, mode="lines",
                    line=dict(color=_na_colors[_s], width=1.2),
                ))
            for _sd in _na_split_days:
                _na_fig.add_vrect(
                    x0=datetime(_sd.year, _sd.month, _sd.day),
                    x1=datetime(_sd.year, _sd.month, _sd.day) + timedelta(days=1),
                    fillcolor="#d62728", opacity=0.12, line_width=0,
                )
            _na_fig.update_layout(
                height=420, margin=dict(l=40, r=20, t=30, b=40),
                yaxis_title="CNY/MWh", legend=dict(orientation="h", y=1.1),
            )
            st.plotly_chart(_na_fig, use_container_width=True)
            _na_m1, _na_m2 = st.columns(2)
            _na_m1.metric("days with asset data", len(_na_av))
            _na_m2.metric(f"days cleared ≠ {_na_ref_name} (>1% slots)", len(_na_split_days))
            st.caption(
                f"cleared_price = 日内出清价 (md_id_cleared_energy); node prices = 实时节点价 (Fengxing). "
                f"Block-level divergence between them is a product/data characteristic question, not necessarily congestion. "
                + ("Split days: " + ", ".join(str(d) for d in _na_split_days[:30]) if _na_split_days else "")
            )

    # ── Section 3: Congestion-Day Cluster View ───────────────────────────────
    st.header("3 · Congestion-Day Cluster View")
    _na_day = st.date_input("Day", value=date(2026, 8, 25), key="na_day")

    @st.cache_data(ttl=600, show_spinner="Loading day matrix & clustering…")
    def _na_load_clusters(day):
        eng = get_engine()
        return _na_day_matrix(eng, day)

    _na_matrix = _na_load_clusters(_na_day)
    if not _na_matrix:
        st.info(f"No Fengxing node data for {_na_day}.")
    else:
        _na_clusters = _na_cluster(_na_matrix, tol=0.01)
        _na_sizes = [len(c) for c in _na_clusters]
        _na_k1, _na_k2, _na_k3 = st.columns(3)
        _na_k1.metric("nodes", len(_na_matrix))
        _na_k2.metric("distinct price clusters", len(_na_clusters))
        _na_k3.metric("largest cluster", _na_sizes[0])

        _na_top = _na_sizes[:10]
        _na_fig2 = go.Figure(go.Bar(
            x=[f"#{i+1}" for i in range(len(_na_top))], y=_na_top,
            marker_color="#1f77b4",
        ))
        _na_fig2.update_layout(height=240, margin=dict(l=40, r=20, t=10, b=40),
                               yaxis_title="nodes in cluster", xaxis_title="cluster rank (by size)")
        st.plotly_chart(_na_fig2, use_container_width=True)

        # Cluster membership per asset — anchored on the asset's own meter node
        # (RT-consistent: same Fengxing source as the matrix). 四子王旗 anchors
        # on 杜尔伯特站/220kV.1M (no own node).
        st.markdown("**Our assets — cluster membership (RT, by own meter node)**")
        _na_tbl = []
        for _a in _NA_ASSETS:
            _anchor = _a["own_node"] or _a["parent_nodes"][0]
            _ci = next((i for i, c in enumerate(_na_clusters) if _anchor in c), None)
            if _ci is None:
                _na_tbl.append({"asset": _a["plant_name"], "cluster rank": "no data", "cluster size": "—", "BESS in same cluster": "—"})
                continue
            _members = _na_clusters[_ci]
            _bess_in = [n.split(".", 1)[-1] for n in _members if "储能" in n and n != _anchor][:8]
            _na_tbl.append({
                "asset": _a["plant_name"],
                "cluster rank": f"#{_ci + 1}",
                "cluster size": len(_members),
                "BESS in same cluster": " · ".join(_bess_in) if _bess_in else "—",
            })
        st.dataframe(pd.DataFrame(_na_tbl), use_container_width=True, hide_index=True)
