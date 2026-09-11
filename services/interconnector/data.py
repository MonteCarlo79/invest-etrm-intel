# services/interconnector/data.py
"""Aggregations for the interconnector tab. Pure functions over row dicts — no DB here."""
from __future__ import annotations

from datetime import date

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

def resolve_mlt_pct(province: str, rules: dict[str, float], overrides: dict[str, float],
                    default: float = 80.0) -> tuple[float, str]:
    if province in overrides: return overrides[province], "override"
    if province in rules: return rules[province], "rule"
    return default, "default"

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
