# services/retail_risk/parsers/trades_zhejiang.py
"""浙江 trades: 景融浙江持仓 CSV -> TRADES_COLS frame + load frame.

CSV has per-channel volume/price pairs per hour. All positions are buy-side
(retailer procurement). 绿电 rows keep channel='annual', counterparty='绿电'.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_CHANNEL_COLS = {  # csv prefix -> (channel, instrument_type, counterparty)
    "yr_sb": ("annual", "bilateral", None),
    "yr_jj": ("annual", "forward", None),
    "yr_gp": ("annual", "forward", None),
    "month_sb": ("monthly_auction", "bilateral", None),
    "month_jj": ("monthly_auction", "forward", None),
    "month_gp": ("monthly_listed", "forward", None),
    "10days_jj": ("intramonth_match", "forward", None),
    "cm": ("intramonth_match", "forward", None),
    "green_total": ("annual", "forward", "绿电"),
}


def _find_csv(root: Path) -> Path | None:
    d = root / "浙江" / "交易记录"
    hits = sorted(d.glob("景融浙江持仓_*.csv"))
    return hits[-1] if hits else None


def parse_zhejiang_trades(root: str | Path) -> pd.DataFrame:
    root = Path(root)
    csv = _find_csv(root)
    if csv is None:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    df = pd.read_csv(csv, parse_dates=["start_time"])
    rows = []
    for r in df.itertuples(index=False):
        d = r.start_time.date()
        h = r.start_time.hour
        for prefix, (ch, inst, cp) in _CHANNEL_COLS.items():
            vol = getattr(r, f"position_{prefix}_volume", None)
            px = getattr(r, f"position_{prefix}_price", None)
            if vol is None or pd.isna(vol) or float(vol) == 0:
                continue
            rows.append([d, h, ch, inst, "buy", float(vol),
                         float(px) if pd.notna(px) else None,
                         cp, prefix, csv.name])
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def parse_zhejiang_load(root: str | Path) -> pd.DataFrame:
    """Forecast/actual load -> (delivery_date, hour, nominated_mwh, settled_mwh)."""
    root = Path(root)
    csv = _find_csv(root)
    if csv is None:
        return pd.DataFrame(columns=["delivery_date", "hour", "nominated_mwh", "settled_mwh"])
    df = pd.read_csv(csv, parse_dates=["start_time"])
    out = pd.DataFrame({
        "delivery_date": df["start_time"].dt.date,
        "hour": df["start_time"].dt.hour,
        "nominated_mwh": df["volume_pre"],
        "settled_mwh": df["volume_rt"],
    })
    return out.dropna(how="all", subset=["nominated_mwh", "settled_mwh"])
