"""现货月报 tab — official monthly report (电力现货市场价格与运行月报) view.

Source tables (written by hermes via services/spot_ingest/monthly_report.py):
  public.spot_monthly_national  — one row per month
  public.spot_monthly_province  — one row per (month, 表2 province)

Section 1: national trend.  Section 2: province × month pivot.
Section 3: official-vs-LingFeng cross-check — the exchange's monthly average is
the authoritative anchor; marketdata.spot_prices_hourly (LingFeng) is aggregated
to the same month and compared. Note the official figure is volume-weighted
while the hourly aggregate is a simple mean, so a few % gap is structural;
only deviations above the threshold are flagged.
"""
from __future__ import annotations

from decimal import Decimal

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

DEV_THRESHOLD = 0.10  # |hourly/official − 1| above this is flagged
MIN_VALID_DAYS = 15   # fewer real (non-placeholder) days → coverage gap, not a price finding

_METRICS = {
    "rt_avg_price": "实时均价 (元/kWh)",
    "da_avg_price": "日前均价 (元/kWh)",
    "rt_mom_pct": "实时环比 (%)",
    "da_mom_pct": "日前环比 (%)",
    "mlt_avg_price": "中长期均价 (元/kWh)",
    "mlt_coverage_pct": "中长期覆盖率 (%)",
    "rt_volume_yi_kwh": "实时电量 (亿kWh)",
    "da_volume_yi_kwh": "日前电量 (亿kWh)",
    "mlt_volume_yi_kwh": "中长期电量 (亿kWh)",
}


def to_kwh(prices: pd.Series) -> pd.Series:
    """spot_prices_hourly is ¥/MWh in current pipelines (median > 5 ¥/kWh is
    physically impossible) — same rule as the forecast tab."""
    med = prices.median()
    return prices / 1000.0 if pd.notna(med) and med > 5 else prices


def cross_check(official: pd.DataFrame, hourly: pd.DataFrame,
                threshold: float = DEV_THRESHOLD,
                min_days: int = MIN_VALID_DAYS) -> pd.DataFrame:
    """official: province_cn, report_month, rt_avg_price, da_avg_price (¥/kWh).
    hourly: province, month, rt_mean, da_mean (already ¥/kWh), optional rt_days.
    Returns one row per official (province, month) with deviations and a flag;
    rows with < min_days valid days are low_cov and never flagged."""
    h = hourly.rename(columns={"province": "province_cn", "month": "report_month"})
    df = official.merge(h, on=["province_cn", "report_month"], how="left")
    for side in ("rt", "da"):
        off, ours = df[f"{side}_avg_price"], df[f"{side}_mean"]
        df[f"{side}_dev_pct"] = (ours / off - 1.0) * 100
    df["no_hourly"] = df[["rt_mean", "da_mean"]].isna().all(axis=1)
    days = df["rt_days"] if "rt_days" in df else pd.Series(min_days, index=df.index)
    df["low_cov"] = ~df["no_hourly"] & (days.fillna(0) < min_days)
    devs = df[["rt_dev_pct", "da_dev_pct"]].abs()
    df["flag"] = (devs > threshold * 100).any(axis=1) & ~df["low_cov"]
    return df


def _decimals_to_float(df: pd.DataFrame) -> None:
    """NUMERIC arrives as Decimal (object dtype). Coerce only columns that hold
    Decimals — text/date columns (report_month, source_file, new ones) are left alone."""
    for c in df.columns:
        vals = df[c].dropna()
        if len(vals) and vals.map(lambda v: isinstance(v, Decimal)).all():
            df[c] = pd.to_numeric(df[c], errors="coerce")


@st.cache_data(ttl=300)
def _load_official(_conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    nat = pd.read_sql("SELECT * FROM spot_monthly_national ORDER BY report_month", _conn)
    prov = pd.read_sql("SELECT * FROM spot_monthly_province ORDER BY report_month, province_cn", _conn)
    for df in (nat, prov):
        _decimals_to_float(df)
    return nat, prov


@st.cache_data(ttl=1800)
def _load_hourly_monthly(_conn, start: str, end_excl: str) -> pd.DataFrame:
    # Bounded to the report months — spot_prices_hourly is the largest table.
    # All-zero price days are LingFeng placeholders (pre-v7-guard history still
    # in the table — 浙江 2026-09 is 23/25 such days); they count as missing,
    # same definition as the lingfeng-ingest placeholder-zero-day guard.
    df = pd.read_sql(
        """
        WITH d AS (
            SELECT province, datetime::date AS d_day,
                   AVG(rt_price) AS rt, AVG(da_price) AS da,
                   bool_and(COALESCE(rt_price, 0) = 0) AS rt_zero,
                   bool_and(COALESCE(da_price, 0) = 0) AS da_zero
            FROM marketdata.spot_prices_hourly
            WHERE datetime >= %(s)s AND datetime < %(e)s
            GROUP BY 1, 2
        )
        SELECT province, date_trunc('month', d_day)::date AS month,
               AVG(rt) FILTER (WHERE NOT rt_zero) AS rt_mean,
               AVG(da) FILTER (WHERE NOT da_zero) AS da_mean,
               COUNT(*) FILTER (WHERE NOT rt_zero) AS rt_days,
               COUNT(*) FILTER (WHERE rt_zero) AS zero_days
        FROM d GROUP BY 1, 2
        """,
        _conn, params={"s": start, "e": end_excl})
    for c in ("rt_mean", "da_mean"):
        df[c] = to_kwh(pd.to_numeric(df[c], errors="coerce"))
    return df


def _render_national(nat: pd.DataFrame) -> None:
    st.subheader("全国汇总")
    x = nat["report_month"].astype(str)
    fig = go.Figure()
    fig.add_scatter(x=x, y=nat["rt_avg_price"], name="实时均价", mode="lines+markers")
    fig.add_scatter(x=x, y=nat["da_avg_price"], name="日前均价", mode="lines+markers")
    fig.add_scatter(x=x, y=nat["mlt_avg_price"], name="中长期均价", mode="lines+markers",
                    line={"dash": "dot"})
    fig.add_bar(x=x, y=nat["mlt_coverage_pct"], name="中长期覆盖率 (%)", yaxis="y2", opacity=0.3)
    fig.update_layout(height=360, yaxis_title="元/kWh",
                      yaxis2={"title": "%", "overlaying": "y", "side": "right"},
                      legend={"orientation": "h"}, margin={"t": 20})
    st.plotly_chart(fig, use_container_width=True)


def _render_pivot(prov: pd.DataFrame) -> None:
    st.subheader("省份 × 月份")
    metric = st.selectbox("指标", list(_METRICS), format_func=_METRICS.get, key="sm_metric")
    pv = prov.pivot_table(index="province_cn", columns="report_month", values=metric, aggfunc="first")
    pv.columns = [str(c)[:7] for c in pv.columns]
    if len(pv.columns):
        pv = pv.sort_values(pv.columns[-1], ascending=False)
    st.dataframe(pv.style.format("{:.3f}" if "price" in metric else "{:.2f}", na_rep="—"),
                 use_container_width=True)


def _render_cross_check(conn, prov: pd.DataFrame) -> None:
    st.subheader("官方口径交叉校验 · LingFeng 小时数据")
    st.caption(
        f"官方月均价为成交量加权，小时数据为算术平均，两者存在结构性差异；"
        f"偏差超过 ±{DEV_THRESHOLD:.0%} 标红。灰色 = LingFeng 无该省该月数据。"
        f"全天为 0 的 LingFeng 占位日按缺失处理；有效天数 < {MIN_VALID_DAYS} 标黄（覆盖不足，偏差不具统计意义）。"
    )
    months = sorted(prov["report_month"].unique())
    start = pd.Timestamp(months[0]).strftime("%Y-%m-%d")
    end_excl = (pd.Timestamp(months[-1]) + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    hourly = _load_hourly_monthly(conn, start, end_excl)
    off = prov[["province_cn", "report_month", "rt_avg_price", "da_avg_price"]].copy()
    hourly["month"] = pd.to_datetime(hourly["month"]).dt.date
    off["report_month"] = pd.to_datetime(off["report_month"]).dt.date
    cc = cross_check(off, hourly)

    month = st.selectbox("月份", sorted(cc["report_month"].unique(), reverse=True),
                         format_func=lambda d: str(d)[:7], key="sm_cc_month")
    view = cc[cc["report_month"] == month].sort_values("rt_dev_pct", key=lambda s: s.abs(),
                                                       ascending=False)
    n_flag, n_none = int(view["flag"].sum()), int(view["no_hourly"].sum())
    n_low = int(view["low_cov"].sum())
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("可比省份", len(view) - n_none - n_low)
    c2.metric("偏差超阈值", n_flag)
    c3.metric("覆盖不足", n_low)
    c4.metric("LingFeng 无数据", n_none)

    show = view[["province_cn", "rt_avg_price", "rt_mean", "rt_dev_pct",
                 "da_avg_price", "da_mean", "da_dev_pct", "rt_days", "zero_days",
                 "flag", "no_hourly", "low_cov"]].rename(columns={
        "province_cn": "省份", "rt_avg_price": "官方实时", "rt_mean": "LingFeng实时",
        "rt_dev_pct": "实时偏差%", "da_avg_price": "官方日前", "da_mean": "LingFeng日前",
        "da_dev_pct": "日前偏差%", "rt_days": "有效天数", "zero_days": "占位日"})

    def _row_style(r):
        if r["no_hourly"]:
            return ["color: #999"] * len(r)
        if r["low_cov"]:
            return ["background-color: #fff4d6"] * len(r)
        return ["background-color: #fde2e2" if r["flag"] else ""] * len(r)

    st.dataframe(
        show.style.apply(_row_style, axis=1)
            .format({c: "{:.3f}" for c in ("官方实时", "LingFeng实时", "官方日前", "LingFeng日前")}
                    | {"实时偏差%": "{:+.1f}", "日前偏差%": "{:+.1f}",
                       "有效天数": "{:.0f}", "占位日": "{:.0f}"}, na_rep="—")
            .hide(axis="columns", subset=["flag", "no_hourly", "low_cov"]),
        use_container_width=True, hide_index=True)


def render(conn) -> None:
    nat, prov = _load_official(conn)
    if prov.empty:
        st.info("暂无现货月报数据。在飞书把「电力现货市场价格与运行月报（YYYY年M月）.pdf」发给 Hermes 即可入库。")
        return
    st.caption(f"来源：电力现货市场价格与运行月报 · 已入库 {prov['report_month'].nunique()} 个月 · "
               f"最新 {str(prov['report_month'].max())[:7]}")
    if not nat.empty:
        _render_national(nat)
    _render_pivot(prov)
    _render_cross_check(conn, prov)
