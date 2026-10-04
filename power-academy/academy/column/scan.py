from datetime import date, timedelta

import pandas as pd
import sqlalchemy as sa

PRICE_SQL = ("select province, datetime, rt_price, da_price "
             "from marketdata.spot_prices_hourly where datetime >= :cut")


def load_prices(engine, days: int = 35) -> pd.DataFrame:
    cut = (pd.Timestamp(date.today() - timedelta(days=days))).tz_localize("UTC")
    with engine.connect() as c:
        return pd.read_sql(sa.text(PRICE_SQL), c, params={"cut": cut})


def scan_anomalies(df: pd.DataFrame, today: str, top: int = 5) -> list:
    today = pd.Timestamp(today)
    week_start = today - pd.Timedelta(days=7)
    prior_start = today - pd.Timedelta(days=35)
    out = []
    for prov, g in df.groupby("province"):
        g = g.dropna(subset=["rt_price"])
        week = g[g.datetime >= week_start]
        prior = g[(g.datetime >= prior_start) & (g.datetime < week_start)]
        if len(week) < 24 or len(prior) < 96:
            continue
        base_max = prior.rt_price.max()
        base_std = prior.rt_price.std() or 1e-9
        wk_max = week.rt_price.max()
        spike = (wk_max - base_max) / base_std
        wk_basis = (week.rt_price - week.da_price).abs().mean()
        pr_basis = (prior.rt_price - prior.da_price).abs().mean() or 1e-9
        basis_shift = wk_basis / pr_basis
        wk_vol = week.rt_price.std() / base_std
        metric, score, detail = max([
            ("rt_max_spike", spike,
             f"周实时最高 {wk_max:.3f} vs 前28天最高 {base_max:.3f}（{spike:+.1f}σ）"),
            ("da_rt_basis_shift", basis_shift,
             f"周均|RT-DA|基差 {wk_basis:.3f} vs 前28天 {pr_basis:.3f}（{basis_shift:.1f}x）"),
            ("volatility_shift", wk_vol,
             f"周实时波动率 {week.rt_price.std():.3f} vs 前28天 {base_std:.3f}（{wk_vol:.1f}x）")],
            key=lambda x: x[1])
        out.append({"province": prov, "metric": metric, "score": round(score, 2),
                    "detail": detail})
    return sorted(out, key=lambda x: -x["score"])[:top]


def render_scan_md(anomalies: list, today: str) -> str:
    lines = [f"# 周度价格异动扫描（截至 {today}）", ""]
    for a in anomalies:
        lines.append(f"- **{a['province']}** [{a['metric']}] {a['detail']}")
    return "\n".join(lines) + "\n"
