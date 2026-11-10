# services/retail_risk/parsers/trades_jinan.py
"""冀南 trades -> TRADES_COLS frame. Two exchange export formats:

A. 日滚动交易 files, sheet 我的交易结果: [分时段类型 '00:00-01:00', 交易单元, 买卖方向,
   成交电量, 成交均价] — single delivery day taken from the filename '(2026-03-28)'.
B. Everything else (年度双边/年度竞价 folders, 月度竞价*, 上/中/下旬竞价), sheet 交易结果:
   summary row, blank row, then header [购方名称, 售方名称, 时间段 'YYYY-MM-DD至YYYY-MM-DD',
   分时段编码 1-24, 成交电量, 成交电价, ...]. 成交电量 is the PERIOD total for that hour
   block -> spread evenly over the days of 时间段. 景融 as 购方 = buy, as 售方 = sell.
   Channel comes from the folder (年度/*) or the filename (月度/旬).
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_SELF = "景融"
_KINDS = [   # (match in path, term, channel, instrument)
    ("日滚动交易", "日滚动", "intramonth_match", "forward"),
    ("年度双边", "年度双边", "annual", "bilateral"),
    ("年度竞价", "年度竞价", "annual", "forward"),
    ("旬竞价", "旬竞价", "intramonth_match", "forward"),
    ("月度挂牌", "月度挂牌", "monthly_listed", "forward"),
    ("月度竞价", "月度竞价", "monthly_auction", "forward"),
]
_DATE_RE = re.compile(r"\((\d{4}-\d{2}-\d{2})\)")
_PERIOD_RE = re.compile(r"(\d{4}-\d{2}-\d{2})至(\d{4}-\d{2}-\d{2})")
_EMPTY = pd.DataFrame(columns=schemas.TRADES_COLS)


def _kind_for(path: Path):
    s = str(path)
    for key, term, ch, inst in _KINDS:
        if key in s:
            return term, ch, inst
    return None


def _parse_rolling(path: Path, term: str, ch: str, inst: str) -> pd.DataFrame:
    m = _DATE_RE.search(path.name)
    if not m:
        return _EMPTY
    day = pd.Timestamp(m.group(1)).date()
    df = pd.read_excel(path, sheet_name="我的交易结果").dropna(subset=["成交电量"])
    rows = []
    for r in df.itertuples(index=False):
        hm = re.match(r"(\d{2}):00-(\d{2}):00", str(r[0]).replace(" ", ""))
        if not hm:
            continue
        rows.append([day, int(hm.group(1)), ch, inst,
                     "buy" if "买" in str(r.买卖方向) else "sell",
                     float(r.成交电量), float(r.成交均价) if pd.notna(r.成交均价) else None,
                     None, term, path.name])
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def _parse_counterparty(path: Path, term: str, ch: str, inst: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="交易结果", header=2).dropna(subset=["成交电量"])
    rows = []
    for r in df.itertuples(index=False):
        pm = _PERIOD_RE.search(str(r.时间段))
        if not pm or pd.isna(r.分时段编码):
            continue
        vol = float(r.成交电量)
        if vol == 0:
            continue
        buy = _SELF in str(r.购方名称)
        if not buy and _SELF not in str(r.售方名称):
            continue                                     # not our trade
        days = pd.date_range(pm.group(1), pm.group(2), freq="D")
        hour = int(r.分时段编码) - 1
        px = float(r.成交电价) if pd.notna(r.成交电价) else None
        cp = str(r.售方名称 if buy else r.购方名称)
        for d in days:
            rows.append([d.date(), hour, ch, inst, "buy" if buy else "sell",
                         vol / len(days), px, cp, term, path.name])
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def _parse_file(path: Path) -> pd.DataFrame:
    kind = _kind_for(path)
    if kind is None:
        return _EMPTY
    sheets = pd.ExcelFile(path).sheet_names
    if "我的交易结果" in sheets:
        return _parse_rolling(path, *kind)
    if "交易结果" in sheets:
        return _parse_counterparty(path, *kind)
    return _EMPTY


def parse_jinan_trades(root: str | Path) -> pd.DataFrame:
    base = Path(root) / "冀南" / "中长期交易结果"
    frames = [f for f in (_parse_file(p) for p in sorted(base.rglob("*.xlsx"))) if not f.empty]
    if not frames:
        return _EMPTY.copy()
    return pd.concat(frames, ignore_index=True)
