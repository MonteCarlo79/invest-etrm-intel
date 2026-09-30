# services/retail_risk/parsers/trades_shandong.py
"""山东 trades: 持仓明细 daily × channel -> hour-level TRADES_COLS via 分月分时比例.

Daily channel volumes are spread to 24 hours by the MTM workbook's 分月分时比例
(estimated rows: source_term gets ' (est)'). Prices come from the 月前中长期电价
sheet (same layout). 预估/实际用电量 -> load frame (nominated/settled).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_CHANNEL_COLS = {
    "绿电": ("annual", "forward", "绿电"),
    "年度双边": ("annual", "bilateral", None),
    "年度竞价": ("annual", "forward", None),
    "年度挂牌": ("annual", "forward", None),
    "月度双边": ("monthly_auction", "bilateral", None),
    "月度竞价": ("monthly_auction", "forward", None),
    "月度挂牌1": ("monthly_listed", "forward", None),
    "月度挂牌2": ("monthly_listed", "forward", None),
}


def _detail_files(root: Path) -> list[Path]:
    return sorted((root / "山东").glob("2026*/景融/*持仓明细.xlsx"))


def _spread_day(date, vols: dict, pxs: dict, ratio_df: pd.DataFrame) -> list:
    rows = []
    r = ratio_df[ratio_df.month == date.month]
    if r.empty:
        return rows
    for col, (ch, inst, cp) in _CHANNEL_COLS.items():
        vol = vols.get(col)
        if vol is None or pd.isna(vol) or float(vol) == 0:
            continue
        px = pxs.get(col)
        for rr in r.itertuples(index=False):
            rows.append([date, int(rr.hour), ch, inst, "buy",
                         float(vol) * float(rr.ratio),
                         float(px) if px is not None and pd.notna(px) else None,
                         cp, f"{col} (est)", None])
    return rows


def parse_shandong_trades(root: str | Path, ratio_df: pd.DataFrame) -> pd.DataFrame:
    root = Path(root)
    rows = []
    for f in _detail_files(root):
        vols = pd.read_excel(f, sheet_name="月前中长期持仓")
        try:
            pxs = pd.read_excel(f, sheet_name="月前中长期电价")
        except ValueError:
            pxs = pd.DataFrame()
        vols = vols.rename(columns={vols.columns[0]: "日期"})
        pxs = pxs.rename(columns={pxs.columns[0]: "日期"}) if not pxs.empty else pxs
        vols["日期"] = pd.to_datetime(vols["日期"]).dt.date
        if not pxs.empty:
            pxs["日期"] = pd.to_datetime(pxs["日期"]).dt.date
        for r in vols.itertuples(index=False):
            v = {c: getattr(r, c, None) for c in _CHANNEL_COLS}
            p = {}
            if not pxs.empty:
                prow = pxs[pxs["日期"] == r.日期]
                if not prow.empty:
                    p = {c: prow.iloc[0][c] for c in _CHANNEL_COLS if c in prow.columns}
            rows.extend(_spread_day(r.日期, v, p, ratio_df))
    out = pd.DataFrame(rows, columns=schemas.TRADES_COLS)
    if not out.empty:
        out["source_file"] = "持仓明细"
    return out


def parse_shandong_load(root: str | Path) -> pd.DataFrame:
    root = Path(root)
    frames = []
    for f in _detail_files(root):
        vols = pd.read_excel(f, sheet_name="月前中长期持仓")
        vols = vols.rename(columns={vols.columns[0]: "日期"})
        frames.append(pd.DataFrame({
            "delivery_date": pd.to_datetime(vols["日期"]).dt.date,
            "nominated_mwh": vols["预估用电量"],
            "settled_mwh": vols["实际用电量"],
        }))
    if not frames:
        return pd.DataFrame(columns=["delivery_date", "nominated_mwh", "settled_mwh"])
    return pd.concat(frames, ignore_index=True)
