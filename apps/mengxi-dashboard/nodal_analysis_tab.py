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
    get_asset_interval_series as _na_intervals,
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

    def _na_fmt_siblings(entries):
        return " · ".join(f"{n}({mw:g}MW)" if mw is not None else n for n, mw in entries)

    def _na_zone_totals(z):
        bess_mw = sum(mw for _, mw in z["sibling_bess"] if mw) + sum(
            _na_by_code[c]["capacity_mw"] or 0 for c in z["our_assets"])
        plant_mw = sum(mw for _, mw in z["sibling_plants"] if mw)
        return bess_mw, plant_mw

    for _z in _NA_ZONES:
        with st.container(border=True):
            _bess_mw, _plant_mw = _na_zone_totals(_z)
            st.subheader(_z["zone"])
            if _z.get("transformers"):
                st.markdown(
                    f"**{_z['transformers']}** → N-1 firm ≈ **{_z['firm_mva']:,} MVA**  |  "
                    f"zone installed: BESS **{_bess_mw:,.0f} MW** · wind/solar/thermal **{_plant_mw:,.0f} MW**"
                )
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
                    st.markdown(_na_fmt_siblings(_z["sibling_bess"]))
            with _zc2:
                st.markdown("**Zone plants (wind/solar/thermal)**")
                st.markdown(_na_fmt_siblings(_z["sibling_plants"]) if _z["sibling_plants"] else "—")
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
        intervals = _na_intervals(eng, plant, start, end)
        return asset_vecs, node_vecs, intervals

    if _na_start > _na_end:
        st.warning("Start date must be ≤ end date.")
    else:
        _na_av, _na_nv, _na_iv = _na_load_explorer(
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
            _na_styles = {
                "asset cleared_price": dict(color="#d62728", width=1.4),
                "own meter node": dict(color="#2ca02c", width=2.6),
                "parent bus (mean)": dict(color="#1f77b4", width=1.2, dash="dash"),
            }
            _na_pdf = pd.DataFrame(_na_rows, columns=["ts", "series", "price"])
            for _s, _g in _na_pdf.groupby("series"):
                _na_fig.add_trace(go.Scatter(
                    x=_g["ts"], y=_g["price"], name=_s, mode="lines",
                    line=_na_styles[_s],
                ))
            # Charge/discharge shading from cleared energy (sign per interval,
            # consecutive same-sign slots merged into blocks).
            # cleared_energy_mwh < 0 = charging (grid → battery), > 0 = discharging.
            _na_blocks = []
            if not _na_iv.empty:
                _cur_s, _cur_start, _prev_ts = 0, None, None
                for _row in _na_iv.itertuples():
                    _e = _row.cleared_energy_mwh
                    _s = -1 if (_e is not None and _e < 0) else (1 if (_e is not None and _e > 0) else 0)
                    if _s != _cur_s:
                        if _cur_s != 0:
                            _na_blocks.append((_cur_start, _prev_ts + timedelta(minutes=15), _cur_s))
                        _cur_s, _cur_start = _s, _row.datetime
                    _prev_ts = _row.datetime
                if _cur_s != 0:
                    _na_blocks.append((_cur_start, _prev_ts + timedelta(minutes=15), _cur_s))
            for _x0, _x1, _s in _na_blocks:
                _na_fig.add_vrect(
                    x0=_x0, x1=_x1, line_width=0,
                    fillcolor="#d62728" if _s < 0 else "#2ca02c",
                    opacity=0.10,
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
                "Background: **red = charging** (cleared_energy < 0), **green = discharging** (> 0), unshaded = idle or missing intervals. "
                "Lines: **asset cleared_price** = 日内出清价 (md_id_cleared_energy); **own meter node** / **parent bus** = 实时节点价 (Fengxing RT) "
                "at the asset's meter node / its parent substation (mean of bus meters). "
                "Gaps = intervals with no cleared record for the asset; Fengxing RT lines continue through them. "
                + ("Days where cleared diverges from RT: " + ", ".join(str(d) for d in _na_split_days[:30]) if _na_split_days else "")
            )

    # ── Section 3: Congestion-Day Cluster View ───────────────────────────────
    st.header("3 · Congestion-Day Cluster View")
    st.caption(
        "How to read this: for the chosen day, every node's 96-interval RT price curve is compared; nodes with "
        "(rounded) identical curves are grouped into a **cluster** = a set of nodes that experienced the same price that day. "
        "**Many clusters** (e.g. 146) = the market was fragmented by congestion into many price zones; "
        "**few clusters** = near-uniform pricing. Bars: the 10 largest clusters by node count. "
        "Table: which cluster each of our assets sat in (anchored at its own meter node) and which other BESS shared it — "
        "those are the assets that saw the same prices as ours that day."
    )
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
