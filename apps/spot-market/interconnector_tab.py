# apps/spot-market/interconnector_tab.py
"""Interconnector tab (S1 topology + data upload). Rendered from app.py.

S2/S3/S4 sections are appended by Tasks 13-15 below the S1 section.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_echarts import Map, st_echarts

from services.interconnector import data as ic_data
from services.interconnector import ingest as ic_ingest
from services.interconnector import topology as ic_topo
from services.interconnector.registry import get_channels

_GEO_PATH = Path(__file__).resolve().parents[2] / "assets" / "geo" / "china_provinces.json"

# Click handler (Task 1 spike §5): _ts nonce is MANDATORY — chart_event is
# persistent component state, so without a per-event timestamp every unrelated
# rerun would re-process the last click. meta carries the full channel/flow
# dict for series (line) clicks — consumed by later tasks; geo clicks drive
# the province panel below.
_CLICK_JS = ("function(params) { return {name: params.name || null, "
             "componentType: params.componentType, seriesType: params.seriesType || null, "
             "meta: (params.data && params.data.meta) ? params.data.meta : null, "
             "_ts: Date.now()}; }")

_SUFFIXES = ("壮族自治区", "回族自治区", "维吾尔自治区", "特别行政区", "自治区", "省", "市")


def _short(name: str) -> str:
    """'山东省' → '山东', '内蒙古自治区' → '内蒙古' (registry send_prov/recv_prov form)."""
    for s in _SUFFIXES:
        name = name.replace(s, "")
    return name


@st.cache_data(ttl=3600)
def _geo() -> dict:
    return json.loads(_GEO_PATH.read_text())


# province centroids for flows view — loaded from GeoJSON + sub-province anchors
@st.cache_data(ttl=3600)
def _flow_coords() -> dict:
    coords = {}
    for f in _geo()["features"]:
        p = f["properties"]
        c = p.get("centroid") or p.get("center")
        if c:
            coords[_short(p["name"])] = c
    coords.update(ic_topo.FLOW_COORDS)
    return coords


_NUM_TRADE_COLS = ("vol_pre_mwh", "vol_post_mwh", "send_price", "land_price",
                   "jingrong_vol_mwh", "jingrong_price", "channel_fee")


def _f(v):
    """NUMERIC comes back from psycopg2 as Decimal — coerce to float/None so
    aggregations and echarts JSON serialization see plain floats."""
    return None if v is None or pd.isna(v) else float(v)


@st.cache_data(ttl=300)
def _trades(_conn) -> list[dict]:
    df = pd.read_sql("SELECT * FROM staging.interconnector_trades", _conn)
    for c in _NUM_TRADE_COLS:
        if c in df.columns:
            df[c] = df[c].map(_f)
    return df.to_dict("records")


@st.cache_data(ttl=300)
def _channels(_conn) -> list[dict]:
    """DB rows → registry-shaped dicts (send/recv/formal renamed, sc/rc rebuilt
    from lon/lat, numerics coerced to float)."""
    df = pd.read_sql("SELECT * FROM staging.interconnector_channels", _conn)
    if df.empty:
        return []
    return [dict(name=r["name"], formal=r["formal_name"],
                 send=r["send_station"], send_prov=r["send_prov"],
                 sc=[float(r["send_lon"]), float(r["send_lat"])],
                 recv=r["recv_station"], recv_prov=r["recv_prov"],
                 rc=[float(r["recv_lon"]), float(r["recv_lat"])],
                 kv=r["kv"], gw=float(r["gw"]), commissioned=r["commissioned"],
                 km=_f(r["km"]), category=r["category"], note=r["note"])
            for r in df.to_dict("records")]


def _clear_ic_caches() -> None:
    # Targeted clear — st.cache_data.clear() would nuke every price cache for
    # all sessions on what is a small staging-table change.
    _trades.clear()
    _channels.clear()


def _upload_section(conn) -> None:
    with st.expander("数据上传 (Data Upload)", expanded=False):
        f1 = st.file_uploader("华东跨省数据汇总.xlsx (2026)", type=["xlsx"], key="ic_up_hd")
        if f1 and st.button("导入华东汇总", key="ic_btn_hd"):
            fp = ic_ingest.file_fingerprint(f1.name, f1.size)
            if st.session_state.get("ic_fp_hd") == fp:
                st.info("同一文件已导入过（指纹一致），跳过。")
            else:
                rows = ic_ingest.parse_huadong(f1, year=2026)
                n = ic_ingest.replace_trades(conn, rows, fp)
                st.session_state["ic_fp_hd"] = fp
                _clear_ic_caches()
                st.success(f"已导入 {n} 笔交易（全量替换）。")
        f2 = st.file_uploader("跨区组织交易情况.xlsx (2025全年)", type=["xlsx"], key="ic_up_kj")
        label = st.text_input("快照标签 snapshot label", value="2025-full", key="ic_snap_label")
        if f2 and st.button("导入跨区组织", key="ic_btn_kj"):
            rows = []
            for sheet in ("中长期", "上海"):
                rows += ic_ingest.parse_mlt_snapshot(f2, sheet=sheet)
            n = ic_ingest.replace_snapshot(conn, label, rows)
            _clear_ic_caches()
            st.success(f"已导入 {n} 行到快照 {label}。")
        if st.button("重建通道注册表 (reseed channels)", key="ic_btn_seed"):
            n = ic_ingest.upsert_channels(conn, get_channels())
            _clear_ic_caches()
            st.success(f"通道注册表已重建：{n} 回。")


def _panel(selected: str | None, channels: list[dict], agg: dict) -> None:
    if not selected:
        st.caption("点击省份查看其连接的通道与容量；点击通道线查看明细。")
        top = sorted(channels, key=lambda c: -c["gw"])[:8]
        for c in top:
            a = agg.get(c["name"], {})
            st.markdown(f"**{c['kv']} {c['name']}** — {c['gw']:g} GW  ")
            st.caption(f"{c['send']}({c['send_prov']}) → {c['recv']}({c['recv_prov']}) · {c['category']} · "
                       f"2026成交 {a.get('vol_gwh', 0):,.0f} GWh"
                       + (f" · 落地 {a['land']:,.0f} 元/MWh" if a.get("land") else ""))
        return
    out = [c for c in channels if c["send_prov"] == selected]
    inc = [c for c in channels if c["recv_prov"] == selected]
    if st.button("返回 (reset)", key="ic_panel_reset"):
        st.session_state["ic_sel_prov"] = None
        st.rerun()
    st.markdown(f"**{selected}** — 送出 {len(out)} 回 · 受入 {len(inc)} 回 · "
                f"合计 {sum(c['gw'] for c in out + inc):.1f} GW")
    for c in out + inc:
        a = agg.get(c["name"], {})
        st.markdown(f"**{c['kv']} {c['name']}** — {c['gw']:g} GW · {c['category']}  ")
        st.caption(f"{c['send']} → {c['recv']} · {c['commissioned']}投运 · "
                   f"2026成交 {a.get('vol_gwh', 0):,.0f} GWh"
                   + (f" · 落地 {a['land']:,.0f} 元/MWh" if a.get("land") else ""))


def render(conn) -> None:
    st.title("跨区通道 Interconnector Trading")
    ic_ingest.ensure_tables(conn)
    _upload_section(conn)

    channels = _channels(conn)
    if not channels:                      # first run: seed from registry
        ic_ingest.upsert_channels(conn, get_channels())
        _clear_ic_caches()
        channels = _channels(conn)
    trades = _trades(conn)
    agg = ic_data.per_channel_agg(trades) if trades else {}

    st.header("1 · 通道拓扑 Channel Topology")
    view = st.radio("视图", ["物理通道", "交易流向"], horizontal=True, key="ic_view")
    col_map, col_panel = st.columns([2, 1])
    with col_map:
        if view == "物理通道":
            opts = ic_topo.physical_options(channels, agg, st.session_state.get("ic_sel_prov"))
        else:
            if not trades:
                st.info("尚未导入华东跨省数据汇总 — 请在上方数据上传区导入。")
                opts = ic_topo.physical_options(channels, agg, None)
            else:
                coords = _flow_coords()
                flows = ic_data.aggregate_flows(trades)
                # Defensive: anchors without map coords (e.g. 蒙西) would KeyError
                # inside flows_options — drop them and say so.
                dropped = sorted({a for f in flows for a in (f["send"], f["recv"])
                                  if a not in coords})
                flows = [f for f in flows if f["send"] in coords and f["recv"] in coords]
                if dropped:
                    st.caption(f"以下锚点缺地图坐标，流向图未包含：{', '.join(dropped)}")
                opts = ic_topo.flows_options(flows, coords)
        result = st_echarts(options=opts, map=Map("china", _geo()),
                            events={"click": _CLICK_JS}, height="640px", key="ic_map")
        evt = result.get("chart_event") if isinstance(result, dict) else None
        if evt and evt.get("_ts") and evt["_ts"] != st.session_state.get("ic_last_evt_ts"):
            st.session_state["ic_last_evt_ts"] = evt["_ts"]   # dedup persistent state
            if evt.get("componentType") == "geo" and evt.get("name"):
                prov = _short(evt["name"])
                if prov != st.session_state.get("ic_sel_prov"):
                    st.session_state["ic_sel_prov"] = prov
                    st.rerun()
    with col_panel:
        _panel(st.session_state.get("ic_sel_prov"), channels, agg)

    st.subheader("通道明细 (by capacity)")
    st.dataframe(pd.DataFrame([{
        "通道": c["name"], "送出": f"{c['send']} ({c['send_prov']})",
        "受入": f"{c['recv']} ({c['recv_prov']})", "类别": c["category"],
        "电压": c["kv"], "容量GW": c["gw"], "投运": c["commissioned"],
        "2026成交GWh": agg.get(c["name"], {}).get("vol_gwh", 0),
        "加权落地价": agg.get(c["name"], {}).get("land"),
        "备注": c["note"]} for c in sorted(channels, key=lambda x: -x["gw"])]),
        use_container_width=True, hide_index=True)

    st.header("2 · 通道裕度 Capacity & Balance of Year")
    st.caption("成交量口径：通道成交电量（华东跨省数据汇总）为交易代理，非调度口径物理潮流；"
               "物理容量 ≠ 可交易容量（ATC 需扣除配套优先/中长期/保供/安全约束）。")
    boy = ic_data.balance_of_year(channels, agg, date.today())
    st.dataframe(pd.DataFrame([{
        "通道": b["name"], "容量GW": b["gw"], "年能力GWh": b["capability_gwh"],
        "2026成交GWh": b["traded_gwh"], "利用率%": b["utilization_pct"],
        "剩余小时": b["remaining_hours"], "剩余能力GWh": b["remaining_capability_gwh"]}
        for b in boy]), use_container_width=True, hide_index=True)

    st.subheader("省间现货日报趋势 (A5)")
    dr = st.date_input("日期范围", value=(date(2026, 1, 1), date.today()), key="ic_a5_range")
    if isinstance(dr, tuple) and len(dr) == 2:
        rows = pd.read_sql(
            "SELECT report_date, direction, metric_type, price_yuan_kwh, total_vol_100gwh "
            "FROM staging.spot_interprov_flow WHERE report_date BETWEEN %s AND %s",
            conn, params=(dr[0], dr[1])).to_dict("records")
        trend = ic_data.daily_interprov_trend(rows)
        if trend.empty:
            st.info("所选时段无省间现货数据。")
        else:
            import plotly.express as px
            # 最高均价 (solid) / 最低均价 (dashed) are distinct bands per direction
            # — same split as the Inter-Provincial Flow tab, never blended. The
            # dash map is pinned: appearance-order assignment inverts on real data.
            bands = ic_data.price_bands(trend)
            fig = px.line(bands, x="report_date", y="price_yuan_kwh",
                          color="direction", line_dash="metric_type",
                          line_dash_map={"最高均价": "solid", "最低均价": "dash"},
                          labels={"report_date": "", "price_yuan_kwh": "均价 元/kWh",
                                  "direction": "", "metric_type": ""})
            st.plotly_chart(fig, use_container_width=True)
            vol = trend[trend["metric_type"] == "最高均价"]  # volume only on 最高均价 rows
            fig2 = px.bar(vol, x="report_date", y="total_vol_100gwh", color="direction",
                          labels={"report_date": "", "total_vol_100gwh": "总电量 亿kWh", "direction": ""})
            st.plotly_chart(fig2, use_container_width=True)

    # S3 (A4+A2) added by Task 14; S4 section added by Task 15 — appends below.
    st.header("3 · 中长期交易 MLT Patterns & 新能源中长期义务")
    snap_label = st.text_input("历史快照标签", value="2025-full", key="ic_snap_read")
    snap = pd.read_sql("SELECT * FROM staging.interconnector_mlt_snapshot WHERE snapshot_label = %s",
                       conn, params=(snap_label,)).to_dict("records")
    if snap:
        gp = ic_data.green_premium(snap)
        st.subheader("绿电溢价 (2025快照, 市场化绿电 vs 其他市场化)")
        st.dataframe(pd.DataFrame(gp), use_container_width=True, hide_index=True)
    else:
        st.info(f"快照 {snap_label} 无数据 — 请在数据上传区导入跨区组织交易情况。")

    st.subheader("A2 新能源中长期义务与出口信号")
    st.caption("义务为省级口径：省内中长期 + 外送中长期 均可履约；"
               "回收风险 = max(0, 要求 − 省内 − 外送) × |中长期价 − 现货价|。"
               "新能源电量为历史实际（远期月份=历史同期估计）；% 可在此修改并保存。")
    rules = ic_data.load_mlt_rules(
        Path(__file__).resolve().parents[2] / "knowledge" / "interconnectors" / "mlt_contract_requirements.md")
    overrides = ic_ingest.get_pct_overrides(conn)
    emm = pd.read_sql(
        "SELECT province, report_month, medium_longterm_volume_gwh, contract_avg_price_yuan_mwh,"
        " wind_volume_gwh, solar_volume_gwh FROM staging.exchange_monthly_metrics", conn)
    emm["report_month"] = pd.to_datetime(emm["report_month"]).dt.date
    senders = sorted({t["send_anchor"] for t in trades}) if trades else []
    rows = []
    for prov in senders:
        pct, src = ic_data.resolve_mlt_pct(prov, rules, overrides)
        # T12M windows: table has no ordering guarantee — sort before .tail(12).
        pe = emm[emm["province"] == prov].sort_values("report_month")
        renewable_gwh = float((pe["wind_volume_gwh"].fillna(0) + pe["solar_volume_gwh"].fillna(0)).tail(12).sum()) if not pe.empty else None
        exported_gwh = sum((t["vol_post_mwh"] or 0) for t in trades if t["send_anchor"] == prov) / 1000
        within_gwh = float(pe["medium_longterm_volume_gwh"].fillna(0).tail(12).sum()) if not pe.empty else None
        rows.append(dict(prov=prov, pct=pct, src=src, renewable_gwh=renewable_gwh,
                         within_gwh=within_gwh, exported_gwh=round(exported_gwh, 1),
                         required_gwh=round(renewable_gwh * pct / 100, 1) if renewable_gwh else None))
    for r in rows:
        c1, c2 = st.columns([3, 1])
        label = f"{r['prov']} — MLT% ({'默认80%' if r['src']=='default' else ('规则库' if r['src']=='rule' else '已覆盖')})"
        new_pct = c2.number_input(label, 0.0, 100.0, float(r["pct"]), 0.5,
                                  key=f"ic_pct_{r['prov']}", label_visibility="visible")
        if new_pct != r["pct"]:
            ic_ingest.set_pct_override(conn, r["prov"], new_pct)
            st.rerun()
        c1.write(f"可再生(T12M): {r['renewable_gwh'] or '无数据':>12} GWh · 要求: {r['required_gwh'] or '—'} GWh · "
                 f"省内中长期: {r['within_gwh'] or '—'} GWh · 外送: {r['exported_gwh']} GWh")
