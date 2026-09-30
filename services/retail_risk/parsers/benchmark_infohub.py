# services/retail_risk/parsers/benchmark_infohub.py
"""信息汇总 workbooks: 中长期价格 sheet right block -> market benchmarks.

Right block layout: a header row whose cells contain channel names (双边协商交易,
集中竞价交易, 挂牌交易, 月内集中竞价), each spanning two columns (净合约量, 加权均价),
plus a 月份 column. Located by anchors, never by fixed coordinates. Volumes are 亿度
(x10,000 -> MWh). Years inferred from a year marker left of 月份 when present, else
the file's by<date> stamp; rows labelled 合计 are excluded.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_CHANNELS = ["双边协商交易", "集中竞价交易", "挂牌交易", "月内集中竞价"]
_PROVINCE_RE = re.compile(r"^(.+?)电力市场信息汇总")
_YEAR_RE = re.compile(r"by(\d{4})\d{4}")


def infohub_files(root: str | Path) -> list[Path]:
    d = Path(root) / "台账" / "mtm"
    return sorted(d.glob("*电力市场信息汇总*.xlsx"))


def parse_infohub_benchmarks(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    pm = _PROVINCE_RE.match(path.name)
    if not pm:
        raise ValueError(f"Cannot parse province from {path.name}")
    province = pm.group(1)
    default_year = int(_YEAR_RE.search(path.name).group(1)) if _YEAR_RE.search(path.name) else 2026

    df = pd.read_excel(path, sheet_name="中长期价格", header=None)

    def _squash(v) -> str:
        return re.sub(r"\s+", "", str(v))

    # Anchor on the channel-header row: contains 月份 AND at least one channel name.
    # (Earlier blocks in the sheet also carry 加权均价 cells — anchoring on 加权均价
    # alone grabs the wrong block.)
    chan_idx = None
    for i in range(len(df)):
        row = [_squash(v) for v in df.iloc[i]]
        if "月份" in row and any(c in row for c in _CHANNELS):
            chan_idx = i
            break
    if chan_idx is None:
        return pd.DataFrame(columns=schemas.BENCH_COLS)

    chan_row = [_squash(v) for v in df.iloc[chan_idx]]
    month_col = chan_row.index("月份")
    chan_cols = []                       # (channel, vol_col, price_col)
    for j, v in enumerate(chan_row):
        if v in _CHANNELS:
            chan_cols.append((v, j, j + 1))

    rows = []
    current_year = default_year
    for i in range(chan_idx + 2, len(df)):   # +2: skip the 净合约量/加权均价 sub-header
        label = _squash(df.iloc[i, month_col])
        for j in range(month_col):       # year marker left of 月份 (sparse: forward-fill)
            yv = df.iloc[i, j]
            if pd.notna(yv) and re.fullmatch(r"20\d{2}(\.0)?", str(yv)):
                current_year = int(float(yv))
        if not label or label == "nan":
            continue                     # blank row inside the block
        m = re.match(r"(\d{1,2})月", label)
        if "合计" in label:
            continue
        if not m:
            break                        # next block's header (e.g. spot 出清均价) — stop
        mnum = int(m.group(1))
        for chan, vc, pc in chan_cols:
            px, vol = df.iloc[i, pc], df.iloc[i, vc]
            if pd.isna(px):
                continue
            rows.append([province, chan, f"{current_year}-{mnum:02d}-01", float(px),
                         float(vol) * 10000.0 if pd.notna(vol) else None,
                         "infohub", path.name])
    return pd.DataFrame(rows, columns=schemas.BENCH_COLS)
