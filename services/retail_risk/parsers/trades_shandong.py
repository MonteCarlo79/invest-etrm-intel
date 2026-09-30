# services/retail_risk/parsers/trades_shandong.py
"""山东 trades: 持仓明细 daily × channel -> hour-level TRADES_COLS via 分月分时比例.

Daily channel volumes are spread to 24 hours by the MTM workbook's 分月分时比例
(estimated rows: source_term gets ' (est)'). Prices come from the 月前中长期电价
sheet (same layout). 预估/实际用电量 -> load frame (nominated/settled).
"""
from __future__ import annotations

import re
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


def _curve_files(root: Path) -> list[Path]:
    return sorted((root / "山东").glob("2026*/景融/日用电曲线/*.xlsx"))


def _read_curve(path: Path) -> pd.Series:
    """One 日用电曲线 file -> hourly book load (MWh), summed over customers' 电量 rows."""
    df = pd.read_excel(path, sheet_name=0)
    df = df[df["类型"].astype(str).str.contains("电量", na=False)]
    hour_cols = [c for c in df.columns if re.fullmatch(r"\d{2}:\d{2}", str(c))]
    out = df[hour_cols].sum()
    out.index = [int(str(c).split(":")[0]) - 1 for c in hour_cols]   # 01:00 -> hour 0
    return out


def month_shapes_from_curves(root: str | Path) -> pd.DataFrame:
    """Actual hourly load shape per month from 日用电曲线 -> (month, hour, ratio),
    normalised to sum 1 per month. Empty when no curve files exist."""
    root = Path(root)
    frames = []
    for f in _curve_files(root):
        m = int(f.stem[4:6])
        s = _read_curve(f)
        frames.append(pd.DataFrame({"month": m, "hour": s.index, "mwh": s.values}))
    if not frames:
        return pd.DataFrame(columns=["month", "hour", "ratio"])
    all_h = pd.concat(frames, ignore_index=True)
    shape = all_h.groupby(["month", "hour"], as_index=False)["mwh"].sum()
    shape["ratio"] = shape["mwh"] / shape.groupby("month")["mwh"].transform("sum")
    return shape[["month", "hour", "ratio"]]


def day_shapes_from_curves(root: str | Path) -> pd.DataFrame:
    """Per-day hourly shape -> (delivery_date, hour, share), normalised per day."""
    root = Path(root)
    frames = []
    for f in _curve_files(root):
        d = pd.to_datetime(f.stem, format="%Y%m%d").date()
        s = _read_curve(f)
        total = s.sum()
        if total <= 0:
            continue
        frames.append(pd.DataFrame({"delivery_date": d, "hour": s.index,
                                    "share": (s / total).values}))
    if not frames:
        return pd.DataFrame(columns=["delivery_date", "hour", "share"])
    return pd.concat(frames, ignore_index=True)


def _spread_day(date, vols: dict, pxs: dict, day_shape: pd.DataFrame,
                month_shape: pd.DataFrame) -> list:
    """Spread one day's channel volumes to hours. Shape priority: the day's actual
    curve share, else the month-average ratio. Both normalised by construction."""
    shape = day_shape if not day_shape.empty else month_shape
    if shape.empty:
        return []
    share_col = "share" if "share" in shape.columns else "ratio"
    rows = []
    for col, (ch, inst, cp) in _CHANNEL_COLS.items():
        vol = vols.get(col)
        if vol is None or pd.isna(vol) or float(vol) == 0:
            continue
        px = pxs.get(col)
        for rr in shape.itertuples(index=False):
            rows.append([date, int(rr.hour), ch, inst, "buy",
                         float(vol) * float(getattr(rr, share_col)),
                         float(px) if px is not None and pd.notna(px) else None,
                         cp, f"{col} (est)", None])
    return rows


def parse_shandong_trades(root: str | Path, shapes: pd.DataFrame | None = None) -> pd.DataFrame:
    """shapes: output of day_shapes_from_curves (delivery_date, hour, share).
    Days without a curve fall back to the month-average shape."""
    root = Path(root)
    if shapes is None:
        shapes = day_shapes_from_curves(root)
    month_shapes = month_shapes_from_curves(root)
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
            day_shape = shapes[shapes.delivery_date == r.日期] if not shapes.empty else shapes
            m_shape = month_shapes[month_shapes.month == r.日期.month] if not month_shapes.empty else month_shapes
            rows.extend(_spread_day(r.日期, v, p, day_shape, m_shape))
    out = pd.DataFrame(rows, columns=schemas.TRADES_COLS)
    if not out.empty:
        out["source_file"] = "持仓明细"
    return out


def parse_shandong_load(root: str | Path) -> pd.DataFrame:
    """Hourly load: settled_mwh from 日用电曲线 (actual per-customer sums);
    nominated_mwh = 持仓明细 daily 预估用电量 spread by the day's shape (est)."""
    root = Path(root)
    day_shapes = day_shapes_from_curves(root)
    if day_shapes.empty:
        return pd.DataFrame(columns=["delivery_date", "hour", "nominated_mwh", "settled_mwh"])
    frames = []
    for f in _curve_files(root):
        d = pd.to_datetime(f.stem, format="%Y%m%d").date()
        s = _read_curve(f)
        frames.append(pd.DataFrame({"delivery_date": d, "hour": s.index,
                                    "settled_mwh": s.values}))
    settled = pd.concat(frames, ignore_index=True)
    nominated = []
    for f in _detail_files(root):
        vols = pd.read_excel(f, sheet_name="月前中长期持仓")
        vols = vols.rename(columns={vols.columns[0]: "日期"})
        vols["日期"] = pd.to_datetime(vols["日期"]).dt.date
        for r in vols.itertuples(index=False):
            shape = day_shapes[day_shapes.delivery_date == r.日期]
            if shape.empty or pd.isna(getattr(r, "预估用电量", None)):
                continue
            for rr in shape.itertuples(index=False):
                nominated.append([r.日期, int(rr.hour),
                                  float(r.预估用电量) * float(rr.share)])
    out = settled.merge(pd.DataFrame(nominated, columns=["delivery_date", "hour", "nominated_mwh"]),
                        on=["delivery_date", "hour"], how="outer")
    return out
