"""Price Forecast tab — DB loaders (top) + Streamlit UI (bottom, Tasks 9-11).

Only this module touches the DB for the price-forecast feature. All loaders
take a SQLAlchemy engine; Streamlit caching is applied in the UI layer.
"""
from __future__ import annotations

import json
from datetime import date
import pandas as pd


def load_confirmed_fuel_fleet(eng, province: str):
    df = pd.read_sql(
        """SELECT coal_price_yuan_t, gas_price_yuan_m3, fleet_segments, effective_date
           FROM marketdata.province_fuel_fleet
           WHERE province = %(p)s AND status = 'confirmed'
           ORDER BY effective_date DESC, ingested_at DESC LIMIT 1""",
        eng, params={"p": province})
    if df.empty:
        return None
    r = df.iloc[0]
    segs = r["fleet_segments"]
    if isinstance(segs, str):
        segs = json.loads(segs)
    return {"coal_price_yuan_t": r["coal_price_yuan_t"],
            "gas_price_yuan_m3": r["gas_price_yuan_m3"],
            "fleet_segments": segs,
            "effective_date": r["effective_date"]}


def load_import_blocks(eng, recv_province: str) -> list[dict]:
    trades = pd.read_sql(
        """SELECT DISTINCT ON (channel_1) channel_1, land_price
           FROM staging.interconnector_trades
           WHERE recv_province = %(p)s AND channel_1 IS NOT NULL AND land_price IS NOT NULL
           ORDER BY channel_1, month_start DESC""",
        eng, params={"p": recv_province})
    if trades.empty:
        return []
    channels = pd.read_sql("SELECT name, gw FROM staging.interconnector_channels", eng)
    cap = dict(zip(channels["name"], channels["gw"]))
    blocks = []
    for _, r in trades.iterrows():
        gw = cap.get(r["channel_1"])
        blocks.append({"label": r["channel_1"],
                       "price_yuan_mwh": float(r["land_price"]),   # already ¥/MWh
                       "capacity_mw": float(gw) * 1000.0 if gw else 1000.0})
    return blocks


def load_fundamentals_d1(eng, province: str, start: date, end: date) -> pd.DataFrame:
    df = pd.read_sql(
        """SELECT datetime,
                  COALESCE(load_d1_mw, load_mw)                       AS load_d1_mw,
                  COALESCE(renewable_d1_mw, renewable_total_mw)       AS renewable_total_d1_mw,
                  COALESCE(bidding_space_d1_mw, bidding_space_mw)     AS bidding_space_d1_mw,
                  COALESCE(wind_d1_mw, wind_mw)                       AS wind_d1_mw,
                  COALESCE(solar_d1_mw, solar_mw)                     AS solar_d1_mw,
                  COALESCE(net_export_d1_mw, net_export_mw)           AS net_export_d1_mw
           FROM marketdata.spot_fundamentals_hourly
           WHERE province = %(p)s AND datetime BETWEEN %(s)s AND %(e)s
           ORDER BY datetime""",
        eng, params={"p": province, "s": start, "e": end})
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.set_index("datetime")


def load_rt_prices(eng, province: str, start: date, end: date) -> pd.DataFrame:
    df = pd.read_sql(
        """SELECT datetime, rt_price FROM marketdata.spot_prices_hourly
           WHERE province = %(p)s AND datetime BETWEEN %(s)s AND %(e)s
           ORDER BY datetime""",
        eng, params={"p": province, "s": start, "e": end})
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.set_index("datetime")


def compute_net_import_share(fund_df: pd.DataFrame) -> pd.Series:
    share = -fund_df["net_export_d1_mw"] / fund_df["load_d1_mw"]
    return share.clip(-1, 1).fillna(0.0)


def load_landing_price_latest(eng, recv_province: str):
    df = pd.read_sql(
        """SELECT SUM(vol_post_mwh * land_price) / NULLIF(SUM(vol_post_mwh), 0) AS wavg
           FROM staging.interconnector_trades
           WHERE recv_province = %(p)s AND land_price IS NOT NULL
             AND month_start = (SELECT MAX(month_start) FROM staging.interconnector_trades
                                WHERE recv_province = %(p)s)""",
        eng, params={"p": recv_province})
    if df.empty or df.iloc[0]["wavg"] is None:
        return None
    return float(df.iloc[0]["wavg"])   # land_price already stored in ¥/MWh


# ── Section ①: merit-order explorer ─────────────────────────────────────────

def compute_daily_stack_series(stack, residual_demand, markup_curve, total_capacity_mw):
    from services.bess_map.price_lab.merit_order import marginal_price, apply_markup
    out = {}
    for ts, dem in residual_demand.items():
        vc = marginal_price(stack, dem)
        if vc != vc:                      # nan — demand above stack
            out[ts] = float("nan")
            continue
        tightness = dem / total_capacity_mw if total_capacity_mw else 0.0
        out[ts] = apply_markup(vc, tightness, markup_curve)
    return pd.Series(out)


def render_merit_order_explorer(st, eng, provinces):
    """Section ①: stack chart + marginal price marker + what-if fuel inputs."""
    from services.bess_map.price_lab.merit_order import build_stack
    import plotly.graph_objects as go

    prov = st.selectbox("省份 / Province", provinces, key="pf_mo_prov")
    ff = load_confirmed_fuel_fleet(eng, prov)
    if ff is None:
        st.info("该省份暂无已确认的燃料/装机数据（Hermes 扫描结果为 draft，待确认）。")
        return
    c1, c2 = st.columns(2)
    coal = c1.number_input("煤价 (¥/t)", value=float(ff["coal_price_yuan_t"] or 850.0), key="pf_coal")
    gas = c2.number_input("气价 (¥/m³)", value=float(ff["gas_price_yuan_m3"] or 3.0), key="pf_gas")
    day = st.date_input("日期", key="pf_mo_day")

    fund = load_fundamentals_d1(eng, prov, day, day)
    if fund.empty:
        st.info("该日无电网预测数据。")
        return
    renewable_mw = float(fund["renewable_total_d1_mw"].mean())
    imports = load_import_blocks(eng, prov)
    stack = build_stack(ff["fleet_segments"], coal, gas,
                        import_blocks=imports, renewable_mw=renewable_mw)

    # step chart
    fig = go.Figure()
    x = 0.0
    for seg in stack:
        fig.add_trace(go.Bar(x=[seg["capacity_mw"]], y=[seg["vc_yuan_mwh"]],
                             base=x, name=seg["label"], width=1.0,
                             marker_color="#d62728" if seg["is_import"] else None))
        x += seg["capacity_mw"]
    fig.update_layout(barmode="stack", barnorm=None, xaxis_title="累计容量 (MW)",
                      yaxis_title="变动成本 (¥/MWh)", height=420, showlegend=True)
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"进口/通道块以红色显示；数据源：province_fuel_fleet "
               f"({ff['effective_date']}) + interconnector_trades 最新月落地价。")
