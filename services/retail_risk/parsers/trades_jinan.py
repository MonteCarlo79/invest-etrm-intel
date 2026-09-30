# services/retail_risk/parsers/trades_jinan.py
"""冀南 trades: 中长期交易结果/**/我的交易结果 sheets -> TRADES_COLS frame.

Monthly files (月度竞价/年度双边/年度竞价) are per-day hour-block profiles applied to
every day of the delivery period (Review Focus #4). 日滚动交易 files carry the
delivery date in the filename.
"""
from __future__ import annotations

import calendar
import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_FILE_KIND = [
    ("日滚动交易", ("intramonth_match", "forward")),
    ("月度竞价", ("monthly_auction", "forward")),
    ("月度挂牌", ("monthly_listed", "forward")),
    ("年度双边", ("annual", "bilateral")),
    ("年度竞价", ("annual", "forward")),
]
_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月")
_DATE_RE = re.compile(r"\((\d{4}-\d{2}-\d{2})\)")


def _kind_for(fname: str):
    for key, ci in _FILE_KIND:
        if key in fname:
            return key, ci
    return None, None


def _delivery_dates(path: Path, year: int, month: int) -> list[pd.Timestamp]:
    """月度/年度 files -> all days of delivery period; 日滚动 -> [filename date]."""
    m = _DATE_RE.search(path.name)
    if m:
        return [pd.Timestamp(m.group(1))]
    if "年度" in str(path):
        start, end = pd.Timestamp(year, 1, 1), pd.Timestamp(year, 12, 31)
    else:
        ndays = calendar.monthrange(year, month)[1]
        start, end = pd.Timestamp(year, month, 1), pd.Timestamp(year, month, ndays)
    return list(pd.date_range(start, end, freq="D"))


def _parse_file(path: Path, year: int, month: int) -> pd.DataFrame:
    term, ci = _kind_for(path.name)
    if ci is None:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    try:
        df = pd.read_excel(path, sheet_name="我的交易结果")
    except ValueError:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    df = df.dropna(subset=["成交电量"])
    rows = []
    for r in df.itertuples(index=False):
        block = str(r[0])  # 分时段类型 first col
        m = re.match(r"(\d{2}):00-(\d{2}):00", block.replace(" ", ""))
        if not m:
            continue
        hour = int(m.group(1))
        direction = "buy" if "买" in str(r.买卖方向) else "sell"
        for d in _delivery_dates(path, year, month):
            rows.append([d.date(), hour, ci[0], ci[1], direction,
                         float(r.成交电量), float(r.成交均价) if pd.notna(r.成交均价) else None,
                         None, term, path.name])
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def parse_jinan_trades(root: str | Path) -> pd.DataFrame:
    base = Path(root) / "冀南" / "中长期交易结果"
    frames = []
    for path in sorted(base.rglob("*.xlsx")):
        m = _MONTH_RE.search(str(path))
        if m:
            frames.append(_parse_file(path, int(m.group(1)), int(m.group(2))))
        elif "年度" in str(path):
            frames.append(_parse_file(path, 2026, 1))
    if not frames:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    return pd.concat(frames, ignore_index=True)
