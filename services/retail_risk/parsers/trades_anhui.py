# services/retail_risk/parsers/trades_anhui.py
"""安徽 trades: 滚搓市场成交结果 (hourly cleared vol/price) + 中长期合同批次 sheets."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_HOUR_RE = re.compile(r"段\d+:(\d{2}):00-(\d{2}):00")


def _parse_guncuo(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_excel(path, sheet_name="滚搓市场成交结果")
    except ValueError:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    hour_cols = [c for c in df.columns if re.fullmatch(r"\d{1,2}点", str(c))]
    rows = []
    i = 0
    while i < len(df) - 1:
        vol_row, px_row = df.iloc[i], df.iloc[i + 1]
        if str(vol_row.get("交易类型")) == "电量" and str(px_row.get("交易类型")) == "电价":
            delivery = pd.to_datetime(vol_row["标的日"]).date()
            for c in hour_cols:
                h = int(str(c).replace("点", "")) - 1       # 1点 = hour 0
                vol, px = vol_row[c], px_row[c]
                if pd.notna(vol) and float(vol) != 0:
                    rows.append([delivery, h, "intramonth_match", "forward", "buy",
                                 float(vol), float(px) if pd.notna(px) else None,
                                 None, "滚撮", path.name])
            i += 2
        else:
            i += 1
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def _parse_contracts(path: Path) -> pd.DataFrame:
    rows = []
    for sheet in pd.ExcelFile(path).sheet_names:
        df = pd.read_excel(path, sheet_name=sheet, header=None)
        for r in df.itertuples(index=False):
            try:
                date_range, seg = str(r[3]), str(r[4])
                m = _HOUR_RE.search(seg.replace(" ", ""))
                if not m:
                    continue
                delivery = pd.to_datetime("-".join(date_range.split("-")[:3])).date()
                rows.append([delivery, int(m.group(1)), "monthly_auction", "forward", "buy",
                             float(r[5]), float(r[6]) if pd.notna(r[6]) else None,
                             None, f"批次:{sheet}", path.name])
            except (ValueError, TypeError, IndexError):
                continue
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def parse_anhui_trades(root: str | Path) -> pd.DataFrame:
    d = Path(root) / "安徽" / "交易记录"
    frames = []
    for f in sorted(d.glob("*月-中长期交易.xlsx")):
        frames.append(_parse_guncuo(f))
    for f in sorted(d.glob("安徽中长期合同*.xlsx")):
        frames.append(_parse_contracts(f))
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    return pd.concat(frames, ignore_index=True)
