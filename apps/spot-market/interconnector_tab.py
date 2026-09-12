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
from services.interconnector import model as ic_model
from services.interconnector import topology as ic_topo
from services.interconnector.registry import get_channels

_GEO_PATH = Path(__file__).resolve().parents[2] / "assets" / "geo" / "china_provinces.json"

# Click handler (Task 1 spike §5): _ts nonce is MANDATORY — chart_event is
# persistent component state, so without a per-event timestamp every unrelated
# rerun would re-process the last click. meta carries the full channel/flow
# dict for series (line) clicks — rendered as a detail card in the side panel;
# geo clicks drive the province panel below.
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
    # Series.map re-coerces None back to NaN (float64 inference), and on pandas 3.x
    # df.where(pd.notna(df), None) no longer converts NaN→None — astype(object) first
    # is the only form that preserves None through to_dict.
    return df.astype(object).where(pd.notna(df), None).to_dict("records")


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


@st.cache_data(ttl=300)
def _interprov_flow(_conn, start, end) -> list[dict]:
    # province_cn/province_share feed the A5 share table; price_chg_pct the
    # 环比 distribution — all parsed per-row by interprov_parser.
    return pd.read_sql(
        "SELECT report_date, direction, metric_type, price_yuan_kwh, total_vol_100gwh,"
        " province_cn, province_share, price_chg_pct"
        " FROM staging.spot_interprov_flow WHERE report_date BETWEEN %s AND %s",
        _conn, params=(start, end)).to_dict("records")


@st.cache_data(ttl=300)
def _exchange_monthly(_conn) -> pd.DataFrame:
    """S3 A2 inputs — numerics coerced here (NUMERIC→Decimal breaks .mean())."""
    df = pd.read_sql(
        "SELECT province, report_month, medium_longterm_volume_gwh, contract_avg_price_yuan_mwh,"
        " spot_avg_price_yuan_mwh, wind_volume_gwh, solar_volume_gwh"
        " FROM staging.exchange_monthly_metrics", _conn)
    df["report_month"] = pd.to_datetime(df["report_month"]).dt.date
    for c in ("medium_longterm_volume_gwh", "contract_avg_price_yuan_mwh",
              "spot_avg_price_yuan_mwh", "wind_volume_gwh", "solar_volume_gwh"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=300)
def _contract_prices(_conn) -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT province, report_month, contract_avg_price_yuan_mwh"
        " FROM staging.exchange_monthly_metrics", _conn)
    df["report_month"] = pd.to_datetime(df["report_month"])
    return df


@st.cache_data(ttl=300)
def _spot_monthly_avg(_conn) -> pd.DataFrame:
    # Unbounded GROUP BY over marketdata.spot_prices_hourly (the platform's
    # largest table) — must not run on every rerun; the 5-min cache bounds it.
    return pd.read_sql(
        "SELECT province, date_trunc('month', datetime)::date AS m,"
        " AVG(da_price) AS p FROM marketdata.spot_prices_hourly"
        " GROUP BY 1, 2", _conn)


@st.cache_data(ttl=300)
def _agreements(_conn) -> list[dict]:
    rows = ic_ingest.get_agreements(_conn)
    for r in rows:  # NUMERIC → Decimal breaks st.data_editor numeric editing
        r["annual_gwh"] = float(r["annual_gwh"]) if r["annual_gwh"] is not None else None
    return rows


def _clear_ic_caches() -> None:
    # Targeted clear — st.cache_data.clear() would nuke every price cache for
    # all sessions on what is a small staging-table change.
    _trades.clear()
    _channels.clear()
    _interprov_flow.clear()
    _exchange_monthly.clear()
    _contract_prices.clear()
    _spot_monthly_avg.clear()
    _agreements.clear()


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


def _line_card(m: dict) -> None:
    """Detail card for a clicked line: channel dict (physical view, has 'gw')
    or flow dict (flows view, has 'send'/'recv'). Same _panel styling."""
    if st.button("返回 (reset)", key="ic_line_reset"):
        st.session_state["ic_sel_line"] = None
        st.rerun()
    if "gw" in m:  # physical channel
        st.markdown(f"**{m['kv']} {m['name']}** — {m['gw']:g} GW · {m['category']}  ")
        st.caption(f"{m['send']}({m['send_prov']}) → {m['recv']}({m['recv_prov']}) · "
                   f"{m['commissioned']}投运")
        st.caption(f"2026成交 {m.get('vol_gwh') or 0:,.0f} GWh"
                   + (f" · 落地 {m['land']:,.0f} 元/MWh" if m.get("land") else ""))
        if m.get("note"):
            st.caption(m["note"])
    else:  # trade-flow arc
        st.markdown(f"**{m['send']} → {m['recv']}** — {m.get('vol_gwh') or 0:,.0f} GWh · "
                    f"{m.get('trades') or 0}笔  ")
        parts = [f"落地 {m['land']:,.0f}" if m.get("land") else None,
                 f"上网 {m['sendp']:,.0f}" if m.get("sendp") else None,
                 f"通道费 {m['fee']:,.0f}" if m.get("fee") else None]
        st.caption(" · ".join(p for p in parts if p) + " 元/MWh" if any(parts) else "价格无数据")
        if m.get("channels"):
            st.caption(f"通道: {', '.join(m['channels'])}")
        if m.get("send_raws"):
            st.caption(f"送出方: {', '.join(m['send_raws'])}")


def _panel(selected: str | None, line: dict | None, channels: list[dict], agg: dict) -> None:
    if line:
        _line_card(line["meta"])
        return
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
        st.session_state["ic_sel_line"] = None
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
    if ic_ingest.seed_agreements_if_empty(conn):   # one-time gov-agreement seed
        _clear_ic_caches()
    if ic_ingest.seed_mech_share_if_empty(conn):   # one-time mechanism-share seed
        _clear_ic_caches()
    trades = _trades(conn)
    agg = ic_data.per_channel_agg(trades) if trades else {}

    def _nd(v):
        # uniform str output: mixed float+str columns crash Arrow's
        # st.dataframe serializer (falls back noisily). String-formatted keeps
        # the explicit 无数据 marker AND serializes clean.
        return "无数据" if v is None else f"{v:,.1f}"

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
                # flows_options only colors receivers in RECV_COLORS — surface
                # any new receiver so an upload never silently drops flows.
                dropped_recv = sorted({f["recv"] for f in flows} - set(ic_topo.RECV_COLORS))
                if dropped_recv:
                    st.warning(f"以下受端无配色，流向图未显示：{', '.join(dropped_recv)}"
                               " — 请在 topology.RECV_COLORS 中补充。")
                # Defensive: anchors without map coords (e.g. 蒙西) would KeyError
                # inside flows_options — drop them and say so.
                dropped = sorted({a for f in flows for a in (f["send"], f["recv"])
                                  if a not in coords})
                flows = [f for f in flows if f["send"] in coords and f["recv"] in coords]
                if dropped:
                    st.caption(f"以下锚点缺地图坐标，流向图未包含：{', '.join(dropped)}")
                st.caption("受端配色仅覆盖 江苏/上海/浙江/福建。")
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
                    st.session_state["ic_sel_line"] = None
                    st.rerun()
            elif evt.get("seriesType") == "lines" and evt.get("meta"):
                sel = {"view": view, "meta": evt["meta"]}
                if sel != st.session_state.get("ic_sel_line"):
                    st.session_state["ic_sel_line"] = sel
                    st.rerun()
    with col_panel:
        line = st.session_state.get("ic_sel_line")
        if line and line.get("view") != view:   # stale selection from the other view
            line = None
        _panel(st.session_state.get("ic_sel_prov"), line, channels, agg)

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

    st.subheader("政府间协议电量 (Gov Agreements)")
    st.caption("政府间框架/优先计划年送电量（GWh/年），行可直接编辑/增删，保存后入库并持久化。"
               "「2026华东成交」列为该省对在本 tab 华东交易中的实际成交（仅覆盖江苏/上海/浙江/福建受端）。"
               "种子数据来自公开报道，备注「待核实」— 请以官方文件为准修正。")
    _agr = _agreements(conn)
    _agr_rows = [dict(r, **{"2026华东成交": round(sum(
        (t["vol_post_mwh"] or 0) for t in trades
        if t["send_anchor"] == r["send_prov"] and t["recv_province"] == r["recv_prov"]) / 1000, 1)})
        for r in _agr]
    edited = st.data_editor(
        pd.DataFrame(_agr_rows).rename(columns={
            "send_prov": "送出省", "recv_prov": "受入省", "annual_gwh": "协议年电量GWh",
            "period": "协议期", "channel_hint": "通道", "source": "来源",
            "note": "备注", "2026华东成交": "2026华东成交GWh"}),
        num_rows="dynamic", use_container_width=True, hide_index=True,
        key="ic_agr_editor")
    if st.button("保存协议电量", key="ic_agr_save"):
        renamed = edited.rename(columns={
            "送出省": "send_prov", "受入省": "recv_prov", "协议年电量GWh": "annual_gwh",
            "协议期": "period", "通道": "channel_hint", "来源": "source", "备注": "note"})
        keep = renamed[renamed["send_prov"].notna() & renamed["recv_prov"].notna()]
        n = ic_ingest.save_agreements(conn, keep[
            ["send_prov", "recv_prov", "annual_gwh", "period",
             "channel_hint", "source", "note"]].to_dict("records"))
        _clear_ic_caches()
        st.success(f"已保存 {n} 条协议。")
        st.rerun()

    st.subheader("通道基准交易量 (Benchmark: 2025 actual + 2026 YTD)")
    st.caption("2025 MLT = 跨区组织快照实测；2026 MLT = 华东汇总实测。"
               "现货分量按各通道 MLT 电量占比从全国省间现货总量分摊（假设，无通道级现货披露）。"
               "YoY = 2026 YTD × 12/9 年化 ÷ 2025 全年（MLT+现货分摊）。")
    _snap_label_bm = st.session_state.get("ic_snap_label", "2025-full")
    snap_bm = pd.read_sql("SELECT * FROM staging.interconnector_mlt_snapshot WHERE snapshot_label = %s",
                          conn, params=(_snap_label_bm,)).to_dict("records")
    spot_tot = pd.read_sql(
        "SELECT EXTRACT(year FROM report_date)::int AS y, SUM(total_vol_100gwh)*100 AS gwh"
        " FROM staging.spot_interprov_flow GROUP BY 1", conn)
    spot_by_year = {int(r["y"]): float(r["gwh"] or 0) for _, r in spot_tot.iterrows()}
    if snap_bm:
        bm = ic_data.benchmark_per_channel(snap_bm, trades,
                                           spot_by_year.get(2025, 0.0), spot_by_year.get(2026, 0.0))
        st.dataframe(pd.DataFrame([{
            "通道": b["channel"], "2025MLT(GWh)": f"{b['mlt_2025_gwh']:,.0f}",
            "2025现货分摊(GWh)": f"{b['spot_2025_gwh']:,.0f}",
            "2025合计(GWh)": f"{b['total_2025_gwh']:,.0f}",
            "2026MLT-YTD(GWh)": f"{b['mlt_2026_gwh']:,.0f}",
            "2026现货分摊(GWh)": f"{b['spot_2026_gwh']:,.0f}",
            "YoY(年化)": (f"{(b['mlt_2026_gwh']*12/9 + b['spot_2026_gwh']*12/9) / b['total_2025_gwh']:.2f}x"
                          if b["total_2025_gwh"] > 0 else "—")} for b in bm]),
            use_container_width=True, hide_index=True)
    else:
        st.info(f"快照 {_snap_label_bm} 无数据 — 基准表需 2025 跨区组织数据。")

    st.subheader("省间现货日报趋势 (A5)")
    dr = st.date_input("日期范围", value=(date(2026, 1, 1), date.today()), key="ic_a5_range")
    if isinstance(dr, tuple) and len(dr) == 2:
        rows = _interprov_flow(conn, dr[0], dr[1])
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

            share, dropped_provs = ic_data.province_share_table(rows)
            if share:
                st.caption("省份占比 — 所选区间日报披露占比的均值 (%)")
                st.dataframe(pd.DataFrame([{
                    "方向": s["direction"], "省份": s["province"],
                    "平均占比%": s["avg_share"], "披露天数": s["days"]} for s in share]),
                    use_container_width=True, hide_index=True)
                if dropped_provs:
                    st.caption(f"已剔除日报源文件中的异常省份名（源文件笔误，未并入任何省份）: {', '.join(dropped_provs)}")
            else:
                st.info("所选时段无省份占比披露。")
            dts = ic_data.day_type_split(trend)
            if dts:
                st.caption("日类型拆分 (最高均价口径：均价按电量加权，电量单位亿kWh)")
                st.dataframe(pd.DataFrame([{
                    "方向": d["direction"], "日类型": d["day_type"],
                    "均价(元/kWh)": d["avg_price"], "总电量(亿kWh)": d["total_vol_100gwh"],
                    "天数": d["days"]} for d in dts]),
                    use_container_width=True, hide_index=True)
            chg = pd.DataFrame([r for r in rows
                                if r.get("price_chg_pct") is not None
                                and r.get("metric_type") == "最高均价"])
            if not chg.empty:
                fig_c = px.histogram(chg, x="price_chg_pct", color="direction", nbins=40,
                                     barmode="overlay", opacity=0.7,
                                     labels={"price_chg_pct": "日环比 % (最高均价)", "direction": ""})
                st.plotly_chart(fig_c, use_container_width=True)

    # S3 (A4+A2) added by Task 14; S4 section added by Task 15 — appends below.
    st.header("3 · 中长期交易 MLT Patterns & 新能源中长期义务")
    snap_label = st.text_input("历史快照标签", value="2025-full", key="ic_snap_read")
    snap = pd.read_sql("SELECT * FROM staging.interconnector_mlt_snapshot WHERE snapshot_label = %s",
                       conn, params=(snap_label,)).to_dict("records")
    if snap:
        st.subheader("A4 交易结构对比 (2025快照 vs 2026)")
        st.caption("快照原始单位为亿kWh，下表统一换算为 GWh (×100)；2026交易为 MWh→GWh。")
        tt = ic_data.snapshot_price_by_trade_type(snap)
        st.caption("按交易类型 — 2025快照 (落地均价按电量加权)")
        st.dataframe(pd.DataFrame([{
            "交易类型": r["trade_type"], "电量GWh(2025快照)": r["vol_gwh"],
            "落地均价(元/MWh)": _nd(r["landing_price"])} for r in tt]),
            use_container_width=True, hide_index=True)
        c_a4a, c_a4b = st.columns(2)
        if trades:
            yoy_ch = ic_data.yoy_volume_compare(snap, trades, "channel")
            yoy_pair = ic_data.yoy_volume_compare(snap, trades, "pair")
            with c_a4a:
                st.caption("按通道 YoY (GWh) — 仅两侧均有的通道；2026为成交量口径，串联交易全额计入每回通道")
                if yoy_ch:
                    st.dataframe(pd.DataFrame([{
                        "通道": r["label"], "2025快照GWh": r["vol_2025_gwh"],
                        "2026GWh": r["vol_2026_gwh"]} for r in yoy_ch]),
                        use_container_width=True, hide_index=True)
                else:
                    st.info("快照与2026交易无共同通道。")
            with c_a4b:
                st.caption("按省对 YoY (GWh) — 仅两侧均有的省对")
                if yoy_pair:
                    st.dataframe(pd.DataFrame([{
                        "省对": r["label"], "2025快照GWh": r["vol_2025_gwh"],
                        "2026GWh": r["vol_2026_gwh"]} for r in yoy_pair]),
                        use_container_width=True, hide_index=True)
                else:
                    st.info("快照与2026交易无共同省对。")
        else:
            st.caption("2026交易未导入 — 仅显示2025快照结构 (GWh)。")
            with c_a4a:
                st.dataframe(pd.DataFrame([{
                    "通道": r["label"], "2025快照GWh": r["vol_gwh"]}
                    for r in ic_data.snapshot_volume_by(snap, "channel")]),
                    use_container_width=True, hide_index=True)
            with c_a4b:
                st.dataframe(pd.DataFrame([{
                    "省对": r["label"], "2025快照GWh": r["vol_gwh"]}
                    for r in ic_data.snapshot_volume_by(snap, "pair")]),
                    use_container_width=True, hide_index=True)
        gp = ic_data.green_premium(snap)
        st.subheader("绿电溢价 (2025快照, 市场化绿电 vs 其他市场化)")
        st.dataframe(pd.DataFrame(gp), use_container_width=True, hide_index=True)
    else:
        st.info(f"快照 {snap_label} 无数据 — 请在数据上传区导入跨区组织交易情况。")

    st.subheader("MLT 交易浏览器 (2026明细)")
    if not trades:
        st.info("尚未导入华东跨省数据汇总。")
    else:
        all_recv = sorted({t["recv_province"] for t in trades})
        all_send = sorted({t["send_anchor"] for t in trades})
        all_ch = sorted({c for t in trades
                         for c in (t["channel_1"], t["channel_2"], t["channel_3"]) if c})
        all_period = sorted({t["period_type"] for t in trades if t.get("period_type")})
        all_months = sorted({t["month_start"] for t in trades})
        f1, f2, f3 = st.columns(3)
        sel_recv = f1.multiselect("受入省", all_recv, default=all_recv, key="ic_mlt_recv")
        sel_send = f2.multiselect("送出方", all_send, default=all_send, key="ic_mlt_send")
        sel_ch = f3.multiselect("通道", all_ch, default=all_ch, key="ic_mlt_ch")
        f4, f5 = st.columns(2)
        sel_period = f4.multiselect("期间类型", all_period, default=all_period, key="ic_mlt_period")
        sel_months = f5.select_slider("月份范围", options=all_months,
                                      value=(all_months[0], all_months[-1]),
                                      format_func=lambda d: str(d)[:7], key="ic_mlt_months")
        ft = ic_data.filter_trades(trades, recv=sel_recv, send=sel_send, channels=sel_ch,
                                   months=sel_months, periods=sel_period)
        if not ft:
            st.info("筛选结果为空。")
        else:
            st.dataframe(pd.DataFrame([{
                "起始月": str(t["month_start"])[:7], "期间": t.get("period_type"),
                "送出": t["send_raw"], "受入": t["recv_province"],
                "通道": " / ".join(c for c in (t["channel_1"], t["channel_2"], t["channel_3"]) if c),
                "校核后电量MWh": t["vol_post_mwh"], "上网价(元/MWh)": t["send_price"],
                "落地价(元/MWh)": t["land_price"], "通道费(元/MWh)": t["channel_fee"]}
                for t in ft]), use_container_width=True, hide_index=True)
            import plotly.express as px
            pv = ic_data.monthly_volume_by_recv(ft)
            if not pv.empty:
                mpv = pv.reset_index().melt(id_vars="m", var_name="recv", value_name="gwh")
                st.plotly_chart(px.bar(mpv, x="m", y="gwh", color="recv", barmode="stack",
                                       labels={"m": "", "gwh": "月度电量 GWh", "recv": "受入省"}),
                                use_container_width=True)
            sdf = pd.DataFrame([t for t in ft
                                if t["send_price"] is not None and t["land_price"] is not None])
            if not sdf.empty:
                fig_s = px.scatter(sdf, x="send_price", y="land_price", size="vol_post_mwh",
                                   color="recv_province",
                                   labels={"send_price": "上网价 元/MWh", "land_price": "落地价 元/MWh",
                                           "recv_province": "受入省"})
                lo = float(min(sdf["send_price"].min(), sdf["land_price"].min()))
                hi = float(max(sdf["send_price"].max(), sdf["land_price"].max()))
                fig_s.add_shape(type="line", x0=lo, y0=lo, x1=hi, y1=hi,
                                line=dict(dash="dash", color="#9aa6b5"))
                fig_s.add_annotation(x=hi, y=hi, text="通道费", showarrow=False,
                                     xanchor="right", yanchor="bottom",
                                     font=dict(size=11, color="#5b6675"))
                st.plotly_chart(fig_s, use_container_width=True)
            jr = [t for t in ft if t.get("jingrong_price") is not None]
            if jr:
                st.caption("景融 vs 落地 (元/MWh)")
                st.dataframe(pd.DataFrame([{
                    "起始月": str(t["month_start"])[:7], "受入": t["recv_province"],
                    "送出": t["send_raw"],
                    "景融电量MWh": t["jingrong_vol_mwh"], "景融价": t["jingrong_price"],
                    "落地价": t["land_price"],
                    "景融-落地": (round(t["jingrong_price"] - t["land_price"], 1)
                                 if t["land_price"] is not None else None)} for t in jr]),
                    use_container_width=True, hide_index=True)
            else:
                st.info("筛选结果中无景融量价数据。")

    st.subheader("A2 新能源中长期义务与出口信号")
    st.caption("义务为省级口径：省内中长期 + 外送中长期 均可履约；"
               "回收风险 = max(0, 要求 − 省内 − 外送) × |中长期价 − 现货价|。"
               "新能源电量为历史实际（远期月份=历史同期估计）；% 可在此修改并保存。")
    rules = ic_data.load_mlt_rules(
        Path(__file__).resolve().parents[2] / "knowledge" / "interconnectors" / "mlt_contract_requirements.md")
    overrides = ic_ingest.get_pct_overrides(conn)
    emm = _exchange_monthly(conn)
    senders = sorted({t["send_anchor"] for t in trades}
                     | {r["send_prov"] for r in snap if not r["is_subtotal"]})
    flows = ic_data.aggregate_flows(trades) if trades else []
    mech_shares = ic_ingest.get_mech_shares(conn)
    cur_month = date.today().replace(day=1)
    rows = []
    for prov in senders:
        pct, src = ic_data.resolve_mlt_pct(prov, rules, overrides)
        mech = mech_shares.get(prov)
        # T12M windows: table has no ordering guarantee — sort before .tail(12).
        pe = emm[emm["province"] == prov].sort_values("report_month")
        ws = pe["wind_volume_gwh"].fillna(0) + pe["solar_volume_gwh"].fillna(0)
        monthly_map = {m: float(v) for m, v in zip(pe["report_month"], ws)}
        renewable_gwh = float(ws.tail(12).sum()) if not pe.empty else None
        market_gwh = (ic_model.market_renewable(renewable_gwh, mech)
                      if renewable_gwh is not None else None)
        within_gwh = float(pe["medium_longterm_volume_gwh"].fillna(0).tail(12).sum()) if not pe.empty else None
        mlt_price = pe["contract_avg_price_yuan_mwh"].tail(12).mean() if not pe.empty else None
        mlt_price = None if mlt_price is None or pd.isna(mlt_price) else float(mlt_price)
        spot_price = pe["spot_avg_price_yuan_mwh"].tail(12).mean() if not pe.empty else None
        spot_price = None if spot_price is None or pd.isna(spot_price) else float(spot_price)
        exported_gwh = round(sum((t["vol_post_mwh"] or 0) for t in trades
                                 if t["send_anchor"] == prov) / 1000, 1)
        # 要求 = 市场交易新能源电量(扣机制电量) × MLT%
        required_gwh = round(market_gwh * pct / 100, 1) if market_gwh is not None else None
        exposure = (ic_data.recycle_gap(required_gwh, within_gwh, exported_gwh, mlt_price, spot_price)
                    if required_gwh is not None and within_gwh is not None else None)
        fl = [f for f in flows if f["send"] == prov and f["sendp"] is not None and f["vol_gwh"]]
        sendp = (round(sum(f["sendp"] * f["vol_gwh"] for f in fl)
                       / sum(f["vol_gwh"] for f in fl), 1) if fl else None)
        premium = ic_data.speculation_premium(sendp, mlt_price)
        signal = ("无数据" if premium is None else "出口溢价 (export wins)" if premium > 0
                  else "省内溢价 (sell at home)" if premium < 0 else "持平")
        ya_est = ic_data.year_ago_estimate(monthly_map, cur_month) if monthly_map else None
        rows.append(dict(prov=prov, pct=pct, src=src, mech=mech, renewable_gwh=renewable_gwh,
                         market_gwh=market_gwh,
                         within_gwh=within_gwh, exported_gwh=exported_gwh,
                         required_gwh=required_gwh, mlt_price=mlt_price, spot_price=spot_price,
                         exposure=exposure, sendp=sendp, premium=premium, signal=signal,
                         ya_est=ya_est))

    st.dataframe(pd.DataFrame([{
        "省份": r["prov"], "MLT%": r["pct"],
        "机制占比%": (f"{r['mech']:.0f}" if r["mech"] is not None else "待填"),
        "可再生T12M(GWh)": _nd(r["renewable_gwh"]),
        "市场交易电量(GWh)": _nd(r["market_gwh"]),
        "要求(GWh)": _nd(r["required_gwh"]),
        "省内中长期T12M(GWh)": _nd(r["within_gwh"]), "外送(GWh)": _nd(r["exported_gwh"]),
        "中长期均价(元/MWh)": _nd(r["mlt_price"]), "现货均价(元/MWh)": _nd(r["spot_price"]),
        "回收敞口(千元)": _nd(r["exposure"]),
        "上网VWAP(元/MWh)": _nd(r["sendp"]), "投机溢价(元/MWh)": _nd(r["premium"]),
        "信号": r["signal"],
        "当月可再生估计·历史同期(GWh)": _nd(r["ya_est"])} for r in rows]),
        use_container_width=True, hide_index=True)
    for r in rows:
        c1, c2 = st.columns([3, 1])
        label = f"{r['prov']} — MLT% ({'默认80%' if r['src']=='default' else ('规则库' if r['src']=='rule' else '已覆盖')})"
        new_pct = c2.number_input(label, 0.0, 100.0, float(r["pct"]), 0.5,
                                  key=f"ic_pct_{r['prov']}", label_visibility="visible")
        if new_pct != r["pct"]:
            ic_ingest.set_pct_override(conn, r["prov"], new_pct)
            st.rerun()
        mech0 = float(r["mech"]) if r["mech"] is not None else 0.0
        new_mech = c2.number_input(f"{r['prov']} — 机制占比% (136号文, 0=未知/待填)",
                                   0.0, 100.0, mech0, 1.0,
                                   key=f"ic_mech_{r['prov']}", label_visibility="visible")
        if new_mech != mech0:
            ic_ingest.set_mech_share(conn, r["prov"], new_mech)
            st.rerun()
        c1.write(f"可再生(T12M): {r['renewable_gwh'] if r['renewable_gwh'] is not None else '无数据'} GWh · "
                 f"市场交易: {r['market_gwh'] if r['market_gwh'] is not None else '—'} GWh · "
                 f"要求: {r['required_gwh'] if r['required_gwh'] is not None else '—'} GWh · "
                 f"省内中长期: {r['within_gwh'] if r['within_gwh'] is not None else '—'} GWh · "
                 f"外送: {r['exported_gwh']} GWh")
    if trades:
        import plotly.express as px
        mx = ic_data.monthly_exported_by_sender(trades).reset_index().melt(
            id_vars="m", var_name="send", value_name="gwh")
        st.plotly_chart(px.line(mx, x="m", y="gwh", color="send",
                                labels={"m": "", "gwh": "外送电量 GWh", "send": ""}),
                        use_container_width=True)

    # S4 (A3) added by Task 15.
    st.header("4 · 价差回测 Month-Ahead Spread Backtest")
    st.caption("月前价代理 = 交易月前一月披露月报中的合约均价 (contract_avg_price，"
               "报告月 M−1 对应交割月 M)；交割月现货 = LingFeng 日前月度均价；"
               "落地价 = 该省对当月覆盖交易加权 (VWAP)。缺口月份显示 无数据，不插值。"
               "净价差 = 受端现货 − 送端现货 − 通道费；净价差仅在存在该省对交易时扣减通道费，"
               "无交易月份按零费显示。")
    if trades:
        pairs = sorted({(t["send_anchor"], t["recv_province"]) for t in trades})
        pair = st.selectbox("省对", pairs, format_func=lambda p: f"{p[0]} → {p[1]}",
                            key="ic_a3_pair")
        months = sorted({t["month_start"] for t in trades})
        msel = st.multiselect("交割月", months, default=months[-3:], key="ic_a3_months")
        # month-ahead proxy: contract_avg_price at report month M-1 → keyed to delivery month M
        emm2 = _contract_prices(conn)
        month_ahead = {}
        for _, r in emm2.iterrows():
            if pd.notna(r["contract_avg_price_yuan_mwh"]):
                dm = (r["report_month"] + pd.offsets.MonthBegin(1)).date()
                month_ahead[(r["province"], dm)] = float(r["contract_avg_price_yuan_mwh"])
        # spot_prices_hourly columns are province/datetime/da_price (app.py:866-903)
        spot = _spot_monthly_avg(conn)
        spot_monthly = {(r["province"], r["m"]): float(r["p"]) for _, r in spot.iterrows()
                        if pd.notna(r["p"])}
        bt = ic_data.backtest_rows([pair], msel, month_ahead, spot_monthly, trades)
        st.dataframe(pd.DataFrame([{
            "送出": r["send"], "受入": r["recv"], "交割月": r["month"],
            "送出月前价(元/MWh)": _nd(r["send_ahead"]), "送出现货均价(元/MWh)": _nd(r["send_spot"]),
            "受入月前价(元/MWh)": _nd(r["recv_ahead"]), "受入现货均价(元/MWh)": _nd(r["recv_spot"]),
            "落地价VWAP(元/MWh)": _nd(r["landing"]),
            "落地-受端现货(元/MWh)": _nd(r["premium_over_recv_spot"]),
            "净价差(元/MWh)": _nd(r["realized_spread"])} for r in bt]),
            use_container_width=True, hide_index=True)
        hits = [r for r in bt if r["premium_over_recv_spot"] is not None]
        if hits:
            beat = sum(1 for r in hits if r["premium_over_recv_spot"] < 0)
            st.metric("落地价低于受端现货的月份占比", f"{100*beat/len(hits):.0f}% ({beat}/{len(hits)})")
    else:
        st.info("尚未导入华东跨省数据汇总。")

    # ── S5: Forecast Model (stack + statistical overlay) ─────────────────────
    st.header("5 · 预测模型 Forecast Model")
    st.caption("确定性 merit-order 堆栈 + 价差回归参照。受入侧需求从落地成本最低的来源向上填充；"
               "送出侧可送电量 = 协议基线 + 义务缺口 ÷ 年内剩余月数 + 价差机会量"
               "（受通道剩余能力与历史月度峰值 ×1.2 约束）。下月月度口径；"
               "回归参照需 ≥4 个月历史点（月度省对成交量 ~ 当月价差）。")
    if not trades:
        st.info("尚未导入华东跨省数据汇总 — 无法建模。")
    else:
        import plotly.graph_objects as go
        from services.interconnector import model as ic_model

        flows_all = ic_data.aggregate_flows(trades)
        agr = _agreements(conn)
        spot = _spot_monthly_avg(conn)
        spot_monthly = {(r["province"], r["m"]): float(r["p"]) for _, r in spot.iterrows()
                        if pd.notna(r["p"])}
        latest_m = max((m for (_, m) in spot_monthly), default=None)

        receivers = sorted({t["recv_province"] for t in trades})
        c1, c2, c3 = st.columns(3)
        recv = c1.selectbox("受入省", receivers,
                            index=receivers.index("上海") if "上海" in receivers else 0,
                            key="ic_s5_recv")
        recv_flows = [f for f in flows_all if f["recv"] == recv]
        months_n = max((f["months"] for f in recv_flows), default=1) or 1
        demand0 = round(sum(f["vol_gwh"] for f in recv_flows) / months_n, 0)
        demand = c2.number_input("下月需求 GWh（默认可编辑）", 0.0, 50000.0,
                                 float(demand0), 50.0, key="ic_s5_demand")
        sens = c3.slider("价差敏感度（机会量弹性）", 0.0, 2.0, 1.0, 0.1, key="ic_s5_sens")

        # pair-level monthly volumes (regression points + historical max)
        pair_vol: dict[tuple, dict] = {}
        for t in trades:
            if t["recv_province"] != recv or not t["vol_post_mwh"]:
                continue
            k = (t["send_anchor"], t["month_start"])
            pair_vol[k] = pair_vol.get(k, 0.0) + t["vol_post_mwh"] / 1000

        # A2 gap per sender (same inputs as S3: renewable T12M, within, exported, pct)
        emm5 = _exchange_monthly(conn)
        emm5["report_month"] = pd.to_datetime(emm5["report_month"]).dt.date
        remaining_months = max(1, 12 - date.today().month + 1)

        def _gap_monthly(prov: str) -> float:
            pe = emm5[emm5["province"] == prov].sort_values("report_month")
            if pe.empty:
                return 0.0
            pct, _ = ic_data.resolve_mlt_pct(prov, rules, overrides)
            renewable = float((pe["wind_volume_gwh"].fillna(0) + pe["solar_volume_gwh"].fillna(0)).tail(12).sum())
            market_gwh = ic_model.market_renewable(renewable, mech_shares.get(prov))
            within = float(pe["medium_longterm_volume_gwh"].fillna(0).tail(12).sum())
            exported = sum((t["vol_post_mwh"] or 0) for t in trades if t["send_anchor"] == prov) / 1000
            gap = max(0.0, market_gwh * pct / 100 - within - exported)
            return gap / remaining_months

        sends = sorted({t["send_anchor"] for t in trades if t["recv_province"] == recv}
                       | {a["send_prov"] for a in agr if a["recv_prov"] == recv})
        candidates, overlay = [], []
        for s in sends:
            f = next((x for x in flows_all if x["send"] == s and x["recv"] == recv), None)
            sendp = f["sendp"] if f else None
            fee = f["fee"] if f else None
            agr_m = ic_model.monthly_agreement_volume(agr, s, recv)
            gap_m = _gap_monthly(s)
            local = spot_monthly.get((s, latest_m))
            spread = round(sendp - local, 1) if (sendp is not None and local is not None) else None
            monthly_vols = sorted(((m, v) for (sd, m), v in pair_vol.items() if sd == s))
            hist_max = max((v for _, v in monthly_vols), default=0.0)
            hist_avg = (sum(v for _, v in monthly_vols[-3:]) / len(monthly_vols[-3:])
                        if monthly_vols else 0.0)
            opp = ic_model.opportunistic_volume(spread, hist_max, sens)
            fc = ic_model.exporter_forecast(agr_m, hist_avg, gap_m, opp, None, hist_max or None)
            cost = ic_model.landed_cost(sendp, fee)
            candidates.append(dict(send=s, cost=cost, volume=fc["total"],
                                   drivers=fc, sendp=sendp, fee=fee, spread=spread))
            pts = [(spot_monthly[(recv, m)] - spot_monthly[(s, m)] - (fee or 0), v)
                   for (sd, m), v in pair_vol.items()
                   if sd == s and (recv, m) in spot_monthly and (s, m) in spot_monthly]
            cur_spread = (spot_monthly.get((recv, latest_m), 0) - (local or 0) - (fee or 0))
            reg = ic_model.spread_regression(pts)
            overlay.append(dict(send=s, n=reg.get("n", 0), r2=reg.get("r2"),
                                reg_fc=round(reg["forecast"](cur_spread), 1) if reg.get("ok") else None))

        result = ic_model.importer_stack(candidates, demand)
        recv_spot = spot_monthly.get((recv, latest_m))

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("预计流入", f"{result['inflow']:,.0f} GWh")
        m2.metric("边际落地成本", _nd(result["marginal_price"]) + (" 元/MWh" if result["marginal_price"] else ""))
        m3.metric("需求缺口", f"{result['unfilled']:,.0f} GWh")
        m4.metric(f"{recv}现货均价({latest_m}月)" if latest_m else f"{recv}现货",
                  _nd(recv_spot) + (" 元/MWh" if recv_spot else ""))

        # stack chart: allocated (solid) + unallocated (faded), cost-ordered, demand line
        fig = go.Figure()
        fig.add_bar(y=[a["send"] for a in result["stack"]],
                    x=[a["allocated"] for a in result["stack"]],
                    orientation="h", name="分配量", marker_color="#2563eb")
        fig.add_bar(y=[a["send"] for a in result["stack"]],
                    x=[a["available"] - a["allocated"] for a in result["stack"]],
                    orientation="h", name="未分配余量", marker_color="rgba(37,99,235,0.25)")
        fig.add_vline(x=demand, line_dash="dash", line_color="#e11d48",
                      annotation_text=f"需求 {demand:,.0f}")
        fig.update_layout(barmode="stack", height=60 + 44 * len(result["stack"]),
                          margin=dict(l=40, r=20, t=10, b=40),
                          xaxis_title="GWh/月", yaxis=dict(autorange="reversed"),
                          legend=dict(orientation="h", y=1.12))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("送出侧分解与回归参照")
        ov_by_send = {o["send"]: o for o in overlay}
        st.dataframe(pd.DataFrame([{
            "送出": c["send"],
            "落地成本(元/MWh)": _nd(c["cost"]),
            "上网VWAP": _nd(c["sendp"]), "通道费": _nd(c["fee"]),
            "与省内价差": _nd(c["spread"]),
            "协议GWh": f"{c['drivers']['agreement']:,.1f}",
            "历史基线GWh": f"{c['drivers']['hist_baseline']:,.1f}",
            "基线GWh": f"{c['drivers']['baseline']:,.1f}",
            "义务缺口GWh": f"{c['drivers']['obligation']:,.1f}",
            "机会量GWh": f"{c['drivers']['opportunistic']:,.1f}",
            "可送合计GWh": f"{c['drivers']['total']:,.1f}",
            "分配GWh": f"{next(a['allocated'] for a in result['stack'] if a['send']==c['send']):,.1f}",
            "回归预测GWh": _nd(ov_by_send[c["send"]]["reg_fc"]),
            "回归(n,r²)": f"({ov_by_send[c['send']]['n']}, {ov_by_send[c['send']]['r2'] if ov_by_send[c['send']]['r2'] is not None else '—'})"}
            for c in sorted(candidates, key=lambda x: (x["cost"] is None, x["cost"] or 0))]),
            use_container_width=True, hide_index=True)
        if result["marginal_price"] and recv_spot:
            diff = result["marginal_price"] - recv_spot
            st.caption(f"边际落地成本 vs {recv}现货: {diff:+.1f} 元/MWh — "
                       + ("受入仍优于本地现货。" if diff < 0 else "本地现货更优，堆栈上段缺乏经济性。"))
