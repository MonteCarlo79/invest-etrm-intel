# services/interconnector/data.py
"""Aggregations for the interconnector tab. Pure functions over row dicts — no DB here."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

def aggregate_flows(trades: list[dict]) -> list[dict]:
    agg = {}
    for r in trades:
        k = (r["send_anchor"], r["recv_province"])
        a = agg.setdefault(k, dict(send=r["send_anchor"], recv=r["recv_province"],
                                   vol=0.0, lv=0.0, lvol=0.0, sv=0.0, svol=0.0,
                                   fv=0.0, fvol=0.0, n=0,
                                   channels=set(), send_raws=set(), months=set()))
        vol = r["vol_post_mwh"] or 0.0
        a["vol"] += vol
        if r["land_price"] is not None and vol:
            a["lv"] += r["land_price"] * vol; a["lvol"] += vol
        if r["send_price"] is not None and vol:
            a["sv"] += r["send_price"] * vol; a["svol"] += vol
        if r["channel_fee"] is not None and vol:
            a["fv"] += r["channel_fee"] * vol; a["fvol"] += vol
        a["n"] += 1
        a["channels"].update(c for c in (r["channel_1"], r["channel_2"], r["channel_3"]) if c)
        a["send_raws"].add(r["send_raw"]); a["months"].add(r["month_start"])
    out = []
    for a in agg.values():
        out.append(dict(send=a["send"], recv=a["recv"], vol_gwh=round(a["vol"]/1000, 3),
            land=round(a["lv"]/a["lvol"]) if a["lvol"] else None,
            sendp=round(a["sv"]/a["svol"]) if a["svol"] else None,
            fee=round(a["fv"]/a["fvol"]) if a["fvol"] else None, trades=a["n"],
            channels=sorted(a["channels"]), send_raws=sorted(a["send_raws"]), months=len(a["months"])))
    return sorted(out, key=lambda x: -x["vol_gwh"])

def per_channel_agg(trades: list[dict]) -> dict[str, dict]:
    """Full trade volume attributed to EVERY listed channel (series path wheeling)."""
    agg: dict[str, dict] = {}
    for r in trades:
        vol = r["vol_post_mwh"] or 0.0
        for ch in {c for c in (r["channel_1"], r["channel_2"], r["channel_3"]) if c}:
            a = agg.setdefault(ch, dict(vol=0.0, lv=0.0, lvol=0.0, n=0))
            a["vol"] += vol; a["n"] += 1
            if r["land_price"] is not None and vol:
                a["lv"] += r["land_price"] * vol; a["lvol"] += vol
    return {ch: dict(vol_gwh=round(a["vol"]/1000, 3),
                     land=round(a["lv"]/a["lvol"]) if a["lvol"] else None,
                     trades=a["n"]) for ch, a in agg.items()}

def monthly_volume_by_recv(trades: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame([{"m": r["month_start"], "recv": r["recv_province"],
                        "gwh": (r["vol_post_mwh"] or 0)/1000} for r in trades])
    if df.empty:
        return pd.DataFrame()
    return df.pivot_table(index="m", columns="recv", values="gwh", aggfunc="sum").fillna(0)

def balance_of_year(channels: list[dict], channel_agg: dict, as_of: date) -> list[dict]:
    """A1: capability vs YTD traded (traded-volume proxy — NOT physical flow)."""
    year_end = date(as_of.year, 12, 31)
    remaining_days = (year_end - as_of).days + 1
    remaining_hours = remaining_days * 24
    out = []
    for c in channels:
        cap = c["gw"] * 8760
        traded = channel_agg.get(c["name"], {}).get("vol_gwh", 0.0)
        out.append(dict(name=c["name"], gw=c["gw"], capability_gwh=round(cap, 1),
            traded_gwh=traded,
            utilization_pct=(100*traded/cap) if cap else 0.0,
            remaining_hours=remaining_hours,
            remaining_capability_gwh=round(c["gw"]*remaining_hours, 1)))
    return sorted(out, key=lambda x: -x["gw"])

def daily_interprov_trend(rows: list[dict]) -> pd.DataFrame:
    """A5: per report_date+direction+metric_type — avg spot price, total volume.
    metric_type stays in the key: 最高均价/最低均价 are distinct price bands
    (mirrors the spot-market Inter-Provincial Flow tab) — never blend them."""
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return (df.groupby(["report_date", "direction", "metric_type"], as_index=False)
              .agg(price_yuan_kwh=("price_yuan_kwh", "mean"),
                   total_vol_100gwh=("total_vol_100gwh", "sum"))
              .sort_values("report_date"))

_PRICE_BANDS = ("最高均价", "最低均价")

def price_bands(trend: pd.DataFrame) -> pd.DataFrame:
    """A5 price chart rows: only the two 均价 bands — 最高价/最高电量 rows also
    carry price_yuan_kwh but are extremes/volume, not average-price series."""
    return trend[trend["metric_type"].isin(_PRICE_BANDS)]

def resolve_mlt_pct(province: str, rules: dict[str, float], overrides: dict[str, float],
                    default: float = 80.0) -> tuple[float, str]:
    if province in overrides: return overrides[province], "override"
    if province in rules: return rules[province], "rule"
    return default, "default"

def load_mlt_rules(path) -> dict[str, float]:
    import re
    rules = {}
    p = Path(path)
    if not p.exists():
        return rules
    for line in p.read_text().splitlines():
        m = re.match(r"\s*-\s*([一-鿿]{2,4})[:：]\s*(\d+(?:\.\d+)?)\s*%", line)
        if m:
            rules[m.group(1)] = float(m.group(2))
    return rules

def year_ago_estimate(monthly: dict, month: date) -> float | None:
    """Forward-month estimate = year-ago same-month actual (历史同期). Never forecast."""
    try:
        prev = date(month.year - 1, month.month, 1)
    except ValueError:
        return None
    return monthly.get(prev)

def recycle_gap(required_gwh: float, within_gwh: float, exported_gwh: float,
                mlt_price: float | None, spot_price: float | None) -> float | None:
    if mlt_price is None or spot_price is None:
        return None
    shortfall = max(0.0, required_gwh - within_gwh - exported_gwh)
    return shortfall * abs(mlt_price - spot_price)

def _pair_vwap(trades, send, recv, price_key):
    num = den = 0.0
    for r in trades:
        if r["send_anchor"] == send and r["recv_province"] == recv and r[price_key] is not None and r["vol_post_mwh"]:
            num += r[price_key] * r["vol_post_mwh"]; den += r["vol_post_mwh"]
    return num/den if den else None

def backtest_rows(pairs, delivery_months, month_ahead, spot_monthly, trades):
    out = []
    for send, recv in pairs:
        fee = _pair_vwap(trades, send, recv, "channel_fee")
        for m in delivery_months:
            covering = [r for r in trades if r["send_anchor"] == send
                        and r["recv_province"] == recv
                        and r["month_start"] <= m <= r["month_end"]]
            land = _pair_vwap(covering, send, recv, "land_price") if covering else None
            ss, rs = spot_monthly.get((send, m)), spot_monthly.get((recv, m))
            out.append(dict(send=send, recv=recv, month=m,
                send_ahead=month_ahead.get((send, m)), send_spot=ss,
                recv_ahead=month_ahead.get((recv, m)), recv_spot=rs,
                landing=land,
                premium_over_recv_spot=(land - rs) if (land is not None and rs is not None) else None,
                realized_spread=(rs - ss - (fee or 0)) if (ss is not None and rs is not None) else None))
    return out

def green_premium(snapshot_rows: list[dict]) -> list[dict]:
    by: dict[tuple, dict] = {}
    for r in snapshot_rows:
        if r["is_subtotal"] or r["landing_price"] is None:
            continue
        k = (r["send_prov"], r["recv_prov"], r["channel"])
        e = by.setdefault(k, {})
        if r["trade_type"] == "省间绿电交易（市场化交易）": e["green"] = r["landing_price"]
        elif r["trade_type"] == "其他市场化交易": e["other"] = r["landing_price"]
    return [dict(send=k[0], recv=k[1], channel=k[2], green_price=v["green"],
                 other_price=v["other"], premium=round(v["green"]-v["other"], 2))
            for k, v in by.items() if "green" in v and "other" in v]

def speculation_premium(sendp: float | None, within_mlt: float | None) -> float | None:
    """A2 speculation signal: 上网VWAP − 省内中长期均价. None-safe."""
    if sendp is None or within_mlt is None:
        return None
    return sendp - within_mlt

def monthly_exported_by_sender(trades: list[dict]) -> pd.DataFrame:
    """Monthly exported GWh pivot: index month_start, columns send_anchor."""
    df = pd.DataFrame([{"m": r["month_start"], "send": r["send_anchor"],
                        "gwh": (r["vol_post_mwh"] or 0)/1000} for r in trades])
    if df.empty:
        return pd.DataFrame()
    return df.pivot_table(index="m", columns="send", values="gwh", aggfunc="sum").fillna(0)

def _num(v) -> float | None:
    """NUMERIC comes back from the DB as Decimal — coerce to float/None."""
    return None if v is None or pd.isna(v) else float(v)

# ── A4 pattern explorer (2025 snapshot vs 2026 trades) ───────────────────────
# Units: snapshot volume is 亿kWh (volume_100m_kwh) → ×100 = GWh;
# trades volume is MWh (vol_post_mwh) → /1000 = GWh.

def _snap_label(r: dict, dim: str) -> str | None:
    if dim == "trade_type":
        return r.get("trade_type") or None
    if dim == "channel":
        return r.get("channel") or None
    if dim == "pair":
        s, v = r.get("send_prov"), r.get("recv_prov")
        return f"{s}→{v}" if s and v else None
    raise ValueError(f"unknown dim {dim}")

def snapshot_volume_by(rows: list[dict], dim: str) -> list[dict]:
    """A4: snapshot volume grouped by dim ('trade_type'|'channel'|'pair').
    Subtotal rows excluded. Returns [{label, vol_gwh}] sorted desc."""
    agg: dict[str, float] = {}
    for r in rows:
        if r.get("is_subtotal"):
            continue
        label = _snap_label(r, dim)
        if not label:
            continue
        agg[label] = agg.get(label, 0.0) + (_num(r.get("volume_100m_kwh")) or 0.0) * 100
    return [dict(label=k, vol_gwh=round(v, 3)) for k, v in
            sorted(agg.items(), key=lambda kv: -kv[1])]

def trades_volume_by(trades: list[dict], dim: str) -> list[dict]:
    """A4: 2026 trade volume (GWh) by dim ('channel'|'pair'). Channel attribution
    counts full volume on EVERY listed channel (series path wheeling)."""
    agg: dict[str, float] = {}
    for r in trades:
        vol = (r.get("vol_post_mwh") or 0.0) / 1000
        if dim == "channel":
            labels = {c for c in (r.get("channel_1"), r.get("channel_2"), r.get("channel_3")) if c}
        elif dim == "pair":
            s, v = r.get("send_anchor"), r.get("recv_province")
            labels = {f"{s}→{v}"} if s and v else set()
        else:
            raise ValueError(f"unknown dim {dim}")
        for label in labels:
            agg[label] = agg.get(label, 0.0) + vol
    return [dict(label=k, vol_gwh=round(v, 3)) for k, v in
            sorted(agg.items(), key=lambda kv: -kv[1])]

def yoy_volume_compare(snapshot_rows: list[dict], trades: list[dict], dim: str) -> list[dict]:
    """A4 YoY: labels present in BOTH the 2025 snapshot and 2026 trades.
    Returns [{label, vol_2025_gwh, vol_2026_gwh}] sorted by 2025 desc."""
    s25 = {r["label"]: r["vol_gwh"] for r in snapshot_volume_by(snapshot_rows, dim)}
    s26 = {r["label"]: r["vol_gwh"] for r in trades_volume_by(trades, dim)}
    both = sorted(set(s25) & set(s26), key=lambda k: -s25[k])
    return [dict(label=k, vol_2025_gwh=s25[k], vol_2026_gwh=s26[k]) for k in both]

def snapshot_price_by_trade_type(rows: list[dict]) -> list[dict]:
    """A4: volume-weighted 落地均价 by trade type (subtotals excluded).
    vol_gwh counts ALL non-subtotal rows of the type; VWAP only rows with price.
    Returns [{trade_type, vol_gwh, landing_price}] sorted by vol_gwh desc."""
    agg: dict[str, dict] = {}
    for r in rows:
        if r.get("is_subtotal") or not r.get("trade_type"):
            continue
        a = agg.setdefault(r["trade_type"], dict(vol=0.0, pv=0.0, pvol=0.0))
        a["vol"] += (_num(r.get("volume_100m_kwh")) or 0.0) * 100
        p = _num(r.get("landing_price"))
        w = _num(r.get("volume_100m_kwh")) or 0.0
        if p is not None and w:
            a["pv"] += p * w; a["pvol"] += w
    return [dict(trade_type=k, vol_gwh=round(a["vol"], 1),
                 landing_price=round(a["pv"]/a["pvol"], 2) if a["pvol"] else None)
            for k, a in sorted(agg.items(), key=lambda kv: -kv[1]["vol"])]

# ── MLT explorer filter ──────────────────────────────────────────────────────

def filter_trades(trades: list[dict], recv=None, send=None, channels=None,
                  months=None, periods=None) -> list[dict]:
    """MLT explorer filter — each criterion None/empty → no constraint on that dim.
    channels matches ANY of channel_1/2/3; months=(lo, hi) keeps trades whose
    [month_start, month_end] span OVERLAPS the filter span. Order preserved."""
    out = []
    for r in trades:
        if recv and r.get("recv_province") not in recv:
            continue
        if send and r.get("send_anchor") not in send:
            continue
        if channels and not ({r.get("channel_1"), r.get("channel_2"), r.get("channel_3")}
                             - {None}) & set(channels):
            continue
        if periods and r.get("period_type") not in periods:
            continue
        if months:
            lo, hi = months
            end = r.get("month_end") or r.get("month_start")
            if r.get("month_start") is None or r["month_start"] > hi or end < lo:
                continue
        out.append(r)
    return out

# ── A5 sub-panels ────────────────────────────────────────────────────────────

def province_share_table(rows: list[dict]) -> list[dict]:
    """A5: avg reported province_share by (direction, province_cn) over the range.
    province_share is parsed per-row by interprov_parser ('浙江(35%)' → 35.0);
    rows without a share are excluded. Sorted direction asc, avg_share desc."""
    agg: dict[tuple, dict] = {}
    for r in rows:
        share, prov = _num(r.get("province_share")), r.get("province_cn")
        if share is None or not prov:
            continue
        k = (r.get("direction"), prov)
        a = agg.setdefault(k, dict(s=0.0, days=set()))
        a["s"] += share; a["days"].add(r.get("report_date"))
    return [dict(direction=k[0], province=k[1],
                 avg_share=round(a["s"]/len(a["days"]), 1), days=len(a["days"]))
            for k, a in sorted(agg.items(), key=lambda kv: (kv[0][0], -kv[1]["s"]/len(kv[1]["days"])))]

def day_type_split(trend: pd.DataFrame) -> list[dict]:
    """A5: weekday vs weekend split on 最高均价 rows (the volume-carrying band).
    Avg price is volume-weighted (simple mean if no volume); volume in 亿kWh."""
    if trend is None or trend.empty:
        return []
    df = trend[trend["metric_type"] == "最高均价"].copy()
    if df.empty:
        return []
    df["dow"] = pd.to_datetime(df["report_date"]).dt.dayofweek
    df["day_type"] = df["dow"].map(lambda d: "周末" if d >= 5 else "工作日")
    out = []
    for (direction, day_type), g in df.groupby(["direction", "day_type"]):
        g = g.copy()
        g["p"] = g["price_yuan_kwh"].map(_num)
        g["v"] = g["total_vol_100gwh"].map(_num)
        priced = g.dropna(subset=["p"])
        w = priced["v"].fillna(0)
        avg = (float((priced["p"]*w).sum()/w.sum()) if w.sum() > 0
               else (float(priced["p"].mean()) if not priced.empty else None))
        out.append(dict(direction=direction, day_type=day_type,
                        avg_price=round(avg, 4) if avg is not None else None,
                        total_vol_100gwh=round(float(g["v"].fillna(0).sum()), 2),
                        days=len(g)))
    order = {"工作日": 0, "周末": 1}
    return sorted(out, key=lambda r: (r["direction"], order.get(r["day_type"], 9)))
