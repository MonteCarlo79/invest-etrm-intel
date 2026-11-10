# services/retail_risk/parsers/trades_zhejiang.py
"""浙江 trades: 景融浙江持仓 CSV -> TRADES_COLS frame + load frame.

CSV has per-channel volume/price pairs per hour. All positions are buy-side
(retailer procurement). 绿电 rows keep channel='annual', counterparty='绿电'.

Months the CSV does not cover (it starts 2026-04) come from the trader's
浙江日滚动撮合汇总YYYYMM.xlsx, sheet *分日持仓量:
  - side table (cols 16-18: label, 量, 价) = monthly volume + price per channel,
    read until the '滚撮' row (rows after it are planning helpers);
    hourly shape from the '{m}月年度双边/竞价' grid when present, else spread
    evenly over the channel's days (上/中/下旬 = days 1-10/11-20/21-end) x 24h;
  - daily rows (col 3 日期, col 7 撮合成交量, col 10 撮合均价) = net 滚撮 per day,
    spread evenly over 24h; negative volume = net sell.
Cross-province lines (黑龙江/青海/华北/跨省...) keep the label as counterparty.
"""
from __future__ import annotations

import calendar
import datetime
import re
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


_XUN_DAYS = {"上旬": (1, 10), "中旬": (11, 20), "下旬": (21, 31)}
_SUMMARY_RE = re.compile(r"浙江日滚动撮合汇总(\d{4})(\d{2})\.xlsx$")


def _side_kind(label: str):
    """side-table label -> (channel, instrument, counterparty, day range | None, grid?)"""
    if label in ("年度双边", "年度竞价"):
        return "annual", "bilateral" if "双边" in label else "forward", None, None, True
    if label in ("月度竞价", "月度双边"):
        return "monthly_auction", "bilateral" if "双边" in label else "forward", None, None, False
    for xun, rng in _XUN_DAYS.items():
        if label.startswith(xun):
            return "intramonth_match", "forward", None, rng, False
    ch = ("intramonth_match" if "月内" in label else
          "monthly_auction" if "月度" in label else "annual")
    return ch, "forward", label, None, False


def _spread(rows, days, vol, ch, inst, direction, px, cp, term, fname):
    per = vol / (len(days) * 24)
    for d in days:
        for h in range(24):
            rows.append([d, h, ch, inst, direction, per, px, cp, term, fname])


def _parse_summary(path: Path, year: int, month: int) -> pd.DataFrame:
    xl = pd.ExcelFile(path)
    sheet = next((s for s in xl.sheet_names if s.endswith("分日持仓量")), None)
    if sheet is None:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    t = pd.read_excel(xl, sheet, header=None)
    ndays = calendar.monthrange(year, month)[1]
    all_days = [datetime.date(year, month, d) for d in range(1, ndays + 1)]
    rows: list = []
    for label, vol, px in t.iloc[1:, 16:19].itertuples(index=False):
        if not isinstance(label, str) or not label.strip():
            continue
        label = label.strip()
        if label.startswith("滚撮"):
            break
        if pd.isna(vol) or float(vol) == 0:
            continue
        ch, inst, cp, rng, grid = _side_kind(label)
        px = float(px) if pd.notna(px) else None
        gname = f"{month}月{label}"
        if grid and gname in xl.sheet_names:
            g = pd.read_excel(xl, gname, header=None)
            for r in g.iloc[1:].itertuples(index=False):
                if not isinstance(r[0], (pd.Timestamp, datetime.datetime)):
                    continue
                for h in range(24):
                    v = r[5 + h]
                    if pd.notna(v) and float(v) != 0:
                        rows.append([r[0].date(), h, ch, inst, "buy", float(v), px,
                                     cp, label, path.name])
            continue
        days = ([d for d in all_days if rng[0] <= d.day <= rng[1]] if rng else all_days)
        _spread(rows, days, float(vol), ch, inst, "buy", px, cp, label, path.name)
    for d, v, p in t.iloc[1:, [3, 7, 10]].itertuples(index=False):
        if not isinstance(d, (pd.Timestamp, datetime.datetime)) or pd.isna(v) or float(v) == 0:
            continue
        v = float(v)
        _spread(rows, [d.date()], abs(v), "intramonth_match", "forward",
                "buy" if v > 0 else "sell", float(p) if pd.notna(p) else None,
                None, "滚撮", path.name)
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def _summary_frames(root: Path, skip_months: set) -> list[pd.DataFrame]:
    out = []
    for p in sorted((root / "浙江" / "交易记录" / "滚动撮合").glob("浙江日滚动撮合汇总*.xlsx")):
        m = _SUMMARY_RE.search(p.name)
        if not m or (int(m.group(1)), int(m.group(2))) in skip_months:
            continue
        f = _parse_summary(p, int(m.group(1)), int(m.group(2)))
        if not f.empty:
            out.append(f)
    return out


def parse_zhejiang_trades(root: str | Path) -> pd.DataFrame:
    root = Path(root)
    csv = _find_csv(root)
    csv_frame = _parse_csv(csv) if csv is not None else pd.DataFrame(columns=schemas.TRADES_COLS)
    covered = {(d.year, d.month) for d in csv_frame["delivery_date"]}
    frames = [f for f in [csv_frame, *_summary_frames(root, covered)] if not f.empty]
    if not frames:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    return pd.concat(frames, ignore_index=True)


def _parse_csv(csv: Path) -> pd.DataFrame:
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
