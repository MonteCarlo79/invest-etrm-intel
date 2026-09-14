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


# ── Section ②: PCA decomposition ────────────────────────────────────────────

def build_feature_frame(fund_df, net_import_share, landing_price):
    from services.bess_map.price_lab.pca_shapes import FEATURE_COLUMNS
    if fund_df.empty:
        # loaders return a RangeIndex frame when empty — index.dayofweek/month
        # below assume DatetimeIndex, so bail out first (Task-8 review note)
        return pd.DataFrame(columns=FEATURE_COLUMNS)
    ff = fund_df[["load_d1_mw", "renewable_total_d1_mw", "bidding_space_d1_mw",
                  "wind_d1_mw", "solar_d1_mw"]].copy()
    ff["net_import_share"] = net_import_share
    ff["landing_price"] = float(landing_price) if landing_price is not None else 0.0
    ff["dow"] = ff.index.dayofweek
    ff["month"] = ff.index.month
    return ff[FEATURE_COLUMNS]


def render_pca_section(st, eng, provinces):
    """Section ②: scree + component shapes + score-vs-driver history."""
    from services.bess_map.price_lab.pca_shapes import (
        build_deviation_matrix, compute_pca)
    import plotly.graph_objects as go

    prov = st.selectbox("省份 / Province", provinces, key="pf_pca_prov")
    days = st.slider("训练窗口 (天)", 90, 365, 365, key="pf_pca_days")
    end = pd.Timestamp.now().date()
    start = end - pd.Timedelta(days=days)
    prices = load_rt_prices(eng, prov, start, end)
    if prices.empty:
        st.info("无价格数据。")
        return
    mat = build_deviation_matrix(prices)
    if len(mat) < 30:
        st.info(f"完整日数据不足 ({len(mat)} 天 < 30)。")
        return
    n_pcs = st.slider("主成分个数", 2, 6, 4, key="pf_pca_k")
    out = compute_pca(mat, n_pcs=n_pcs)

    var = out["variance_explained"][:n_pcs]
    fig_scree = go.Figure(go.Bar(x=[f"PC{i+1}" for i in range(n_pcs)], y=var))
    fig_scree.update_layout(title="方差解释率 (%)", height=260)
    st.plotly_chart(fig_scree, use_container_width=True)

    fig_load = go.Figure()
    for i, vec in enumerate(out["loadings"]):
        fig_load.add_trace(go.Scatter(x=list(range(24)), y=vec, mode="lines", name=f"PC{i+1}"))
    fig_load.update_layout(title="主成分形状 (sum=24 归一)", height=320)
    st.plotly_chart(fig_load, use_container_width=True)
    st.caption("得分为原始特征向量投影；形状预测模型见 ③。")


# ── Section ③: forecast vs actual + backtest ────────────────────────────────

def smape(actual, pred):
    import numpy as np
    a, p = np.asarray(actual, float), np.asarray(pred, float)
    denom = (np.abs(a) + np.abs(p)) / 2.0
    mask = np.isfinite(a) & np.isfinite(p) & (denom > 0)
    if not mask.any():
        return float("nan")
    return float(np.mean(np.abs(a[mask] - p[mask]) / denom[mask]) * 100.0)


def run_hybrid_forecast(eng, province, target_date, train_days=365):
    """D-1 hybrid forecast for target_date: merit-order level + PCA shape."""
    import numpy as np
    from services.bess_map.price_lab.merit_order import build_stack, fit_markup, marginal_price, apply_markup
    from services.bess_map.price_lab.pca_shapes import (
        build_deviation_matrix, compute_pca, fit_score_models, predict_scores,
        reconstruct_shape)
    from services.bess_map.price_lab.hybrid import combine, clip_bounds

    train_start = target_date - pd.Timedelta(days=train_days)
    hist_start = target_date - pd.Timedelta(days=90)   # markup window
    prices = load_rt_prices(eng, province, train_start, target_date - pd.Timedelta(days=1))
    fund_train = load_fundamentals_d1(eng, province, train_start, target_date)
    ff = load_confirmed_fuel_fleet(eng, province)
    if prices.empty or fund_train.empty or ff is None:
        return pd.DataFrame()

    # --- level: merit order on target day, markup fit on last 90d
    coal = ff["coal_price_yuan_t"] or 850.0
    gas = ff["gas_price_yuan_m3"] or 3.0
    imports = load_import_blocks(eng, province)
    fund_target = fund_train[fund_train.index.date == target_date]
    if fund_target.empty:
        return pd.DataFrame()
    # residual demand below is already renewable-net, so the stack gets NO
    # renewable block (a zero-cost block would double-count it)
    stack = build_stack(ff["fleet_segments"], coal, gas, imports)
    total_cap = sum(s["capacity_mw"] for s in stack)

    hist = fund_train[fund_train.index < fund_target.index[0]].tail(90 * 24)
    if not hist.empty:
        vc_hist = np.array([marginal_price(build_stack(ff["fleet_segments"], coal, gas, imports),
                                           row["load_d1_mw"] - row["renewable_total_d1_mw"] + row["net_export_d1_mw"])
                            for _, row in hist.iterrows()])
        tight_hist = (hist["load_d1_mw"] - hist["renewable_total_d1_mw"] + hist["net_export_d1_mw"]) / total_cap
        actual_hist = prices.reindex(hist.index)["rt_price"]
        curve = fit_markup(actual_hist, pd.Series(vc_hist, index=hist.index), tight_hist)
    else:
        curve = [(0.0, 1.0)]

    level = []
    for ts, row in fund_target.iterrows():
        # net_export enters with PLUS: positive = outflow = added demand
        dem = row["load_d1_mw"] - row["renewable_total_d1_mw"] + row["net_export_d1_mw"]
        vc = marginal_price(stack, dem)
        level.append(apply_markup(vc, dem / total_cap, curve) if vc == vc else float("nan"))
    level = np.array(level)

    # --- shape: PCA on deviation matrix + ridge score forecast
    mat = build_deviation_matrix(prices)
    if len(mat) < 30:
        # degenerate PCA/ridge below the section-② floor (np.linalg.eigh on a
        # nan covariance when mat is empty; near-singular fits under ~30 days)
        return pd.DataFrame()
    out = compute_pca(mat, n_pcs=4)
    fund_hist = fund_train.loc[mat.index[0]:mat.index[-1]]
    if fund_hist.empty:
        # no fundamentals overlapping the PCA window — ridge fit would get 0
        # feature rows against n score rows (shape mismatch in np.linalg.solve)
        return pd.DataFrame()
    share_hist = compute_net_import_share(fund_hist)
    landing = load_landing_price_latest(eng, province)
    X_hist = build_feature_frame(fund_hist, share_hist, landing)
    # daily-aggregate features to match scores' daily rows
    X_daily = X_hist.groupby(X_hist.index.date).mean()
    common = min(len(X_daily), len(out["scores"]))
    w, mu, sd = fit_score_models(out["scores"][-common:], X_daily.iloc[-common:])
    share_t = compute_net_import_share(fund_target)
    X_t = build_feature_frame(fund_target, share_t, landing)
    X_t_daily = X_t.groupby(X_t.index.date).mean()
    score_pred = predict_scores(w, mu, sd, X_t_daily)[0]
    shape = reconstruct_shape(out["loadings"], score_pred, out["_raw_eigvecs"])

    lo, hi = clip_bounds(prices["rt_price"])
    fc = combine(level, shape, lo, hi)
    actual = load_rt_prices(eng, province, target_date, target_date)["rt_price"].reindex(fund_target.index)
    return pd.DataFrame({"level": level, "shape": shape, "forecast": fc,
                         "actual": actual.values}, index=fund_target.index)


def run_backtest(eng, province, n_days=90):
    import numpy as np
    end = pd.Timestamp.now().date() - pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=n_days)
    res = {"hybrid": [], "merit_only": [], "pca_only": [], "naive_lag1": []}
    prices = load_rt_prices(eng, province, start - pd.Timedelta(days=7), end)["rt_price"]
    for d in pd.date_range(start, end):
        r = run_hybrid_forecast(eng, province, d.date())
        if r.empty:
            continue
        a = r["actual"].values
        res["hybrid"].append((a, r["forecast"].values))
        res["merit_only"].append((a, r["level"].values))
        res["pca_only"].append((a, (np.nanmean(r["level"]) + r["shape"]).values))
        naive = prices.shift(24).reindex(r.index).values
        res["naive_lag1"].append((a, naive))
    out = {}
    for k, pairs in res.items():
        if not pairs:
            out[k] = (float("nan"), float("nan"))
            continue
        a = np.concatenate([p[0] for p in pairs])
        p = np.concatenate([p[1] for p in pairs])
        out[k] = (float(np.nanmean(np.abs(a - p))), smape(a, p))
    return out


def render_forecast_section(st, eng, provinces):
    """Section ③: D-1 hybrid forecast vs realized RT + 90-day rolling backtest."""
    import numpy as np
    import plotly.graph_objects as go

    prov = st.selectbox("省份 / Province", provinces, key="pf_fc_prov")
    day = st.date_input("预测日期", key="pf_fc_day")

    ff = load_confirmed_fuel_fleet(eng, prov)
    if ff is None:
        st.info("该省份暂无已确认的燃料/装机数据（Hermes 扫描结果为 draft，待确认）。")
        return

    r = run_hybrid_forecast(eng, prov, day)
    if r.empty:
        st.info("该日无电网预测数据（或训练窗口内完整日价格不足 30 天）。")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=r.index, y=r["level"], mode="lines",
                             name="Merit-order 水平"))
    fig.add_trace(go.Scatter(x=r.index, y=r["shape"] + float(np.nanmean(r["level"])),
                             mode="lines", name="PCA 形状 (+水平均值)"))
    fig.add_trace(go.Scatter(x=r.index, y=r["forecast"], mode="lines",
                             name="Hybrid 预测", line=dict(width=3)))
    fig.add_trace(go.Scatter(x=r.index, y=r["actual"], mode="lines",
                             name="实际 RT", line=dict(dash="dot")))
    fig.update_layout(xaxis_title="时刻", yaxis_title="价格 (¥/MWh)", height=420,
                      showlegend=True)
    st.plotly_chart(fig, use_container_width=True)

    @st.cache_data(ttl=3600)
    def _backtest(_eng, province):
        return run_backtest(_eng, province)

    st.caption("回测：过去 90 天滚动起点评估，逐日全量重拟合（首次运行约需数分钟，结果缓存 1 小时）。")
    if st.button("运行 90 天回测 / Run backtest", key="pf_bt_run"):
        with st.spinner("回测运行中…"):
            bt = _backtest(eng, prov)
        labels = {"hybrid": "Hybrid (merit+PCA)", "merit_only": "Merit-order only",
                  "pca_only": "PCA only", "naive_lag1": "Naive lag-1"}
        tbl = pd.DataFrame(
            [{"模型": labels[k], "MAE (¥/MWh)": round(v[0], 2), "sMAPE (%)": round(v[1], 2)}
             for k, v in bt.items()]).set_index("模型")
        st.dataframe(tbl, use_container_width=True)
