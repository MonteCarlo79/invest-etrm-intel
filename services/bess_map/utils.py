from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, List

import numpy as np
import pandas as pd
import re


PriceType = Literal["rt", "da"]


@dataclass(frozen=True)
class EfficiencyParams:
    """Map round-trip efficiency to StorageOpt multipliers."""
    roundtrip_eff: float

    @property
    def disch_eff(self) -> float:
        return float(np.sqrt(self.roundtrip_eff))

    @property
    def charge_eff(self) -> float:
        return float(1.0 / np.sqrt(self.roundtrip_eff))



def infer_province_from_filename(path: Path) -> str:
    """
    从文件名推断省份/区域名称：
    - 兼容  "4.山西.xlsx" -> 山西
    - 兼容  "1 蒙西.xlsx" -> 蒙西
    - 兼容  "01_甘肃.xlsx" -> 甘肃
    - 兼容  "2-广东.xlsx" -> 广东
    规则：优先提取文件名里第一段连续中文作为省份名。
    """
    stem = path.stem.strip()

    # 如果形如 "4.山西" 先去掉前缀序号段
    if "." in stem:
        tail = stem.split(".", 1)[1].strip()
        stem = tail or stem

    # 提取第一段连续中文
    m = re.search(r"[\u4e00-\u9fff]+", stem)
    if m:
        return m.group(0).strip()

    # fallback：去掉常见“序号+分隔符”
    stem2 = re.sub(r"^\s*\d+\s*[\.\-_、\s]*", "", stem).strip()
    return stem2 or stem



def parse_datetime_from_date_timepoint(date_series: pd.Series, timepoint_series: pd.Series) -> pd.DatetimeIndex:
    """日期 + 时点(HHMM int, end-of-interval) -> interval-start timestamp.

    说明：0015 表示 00:00-00:15 这段，因此需要整体回退 15 分钟。
    """
    d = pd.to_datetime(date_series)

    tp = pd.to_numeric(timepoint_series, errors="coerce").fillna(0).astype(int)
    hour = tp // 100
    minute = tp % 100
    dt = d + pd.to_timedelta(hour, unit="h") + pd.to_timedelta(minute, unit="m")

    # 关键：时点是“区间结束时刻”，整体回退 15 分钟得到区间起点
    dt = dt - pd.to_timedelta(15, unit="m")

    return pd.DatetimeIndex(dt)


def guess_price_columns(xlsx_path: str, province: str | None = None):
    """
    Auto-detect RT/DA price columns from the first sheet header.
    Priority:
      1) columns containing province name (if provided)
      2) columns containing key words
    """
    # 只读表头（速度快）
    df_head = pd.read_excel(xlsx_path, sheet_name=0, nrows=1)
    cols = [str(c).strip() for c in df_head.columns]

    def score(col: str, kind: str) -> int:
        s = 0
        c = col.replace(" ", "")
        # 省名加分
        if province and province in c:
            s += 50

        # 关键词加分
        if kind == "rt":
            # 实时 / 实时价格 / RT
            if "实时" in c:
                s += 20
            if "实时价格" in c:
                s += 20
            if re.search(r"\brt\b", c, flags=re.IGNORECASE):
                s += 10
        else:
            # 日前 / 日前价格 / DA
            if "日前" in c:
                s += 20
            if "日前价格" in c:
                s += 20
            if re.search(r"\bda\b", c, flags=re.IGNORECASE):
                s += 10

        # 通用：现货/价格
        if "现货" in c:
            s += 5
        if "价格" in c:
            s += 5

        return s

    rt_best = max(cols, key=lambda c: score(c, "rt")) if cols else None
    da_best = max(cols, key=lambda c: score(c, "da")) if cols else None

    # 最低分校验：避免误选
    if rt_best is None or score(rt_best, "rt") < 15:
        rt_best = None
    if da_best is None or score(da_best, "da") < 15:
        da_best = None

    return rt_best, da_best

def load_prices_from_xlsx(xlsx_path: str, rt_col: str, da_col: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path)

    if "日期" not in df.columns or "时点" not in df.columns:
        raise ValueError("Excel must contain columns: 日期, 时点")

    # 生成 datetime index（长度必须与 df 行数一致）
    idx = parse_datetime_from_date_timepoint(df["日期"], df["时点"])
    if len(idx) != len(df):
        raise ValueError(f"Datetime index length mismatch: idx={len(idx)} df={len(df)}")

    # 关键修复：用 to_numpy() 按“位置”写入，避免 pandas 按 index 对齐导致全 NaN
    rt = pd.to_numeric(df[rt_col], errors="coerce").to_numpy()
    da = pd.to_numeric(df[da_col], errors="coerce").to_numpy()

    out = pd.DataFrame({"rt": rt, "da": da}, index=idx)

    # 丢掉 NaT 时间戳（如果有）
    out = out[~out.index.isna()]

    # 同一时刻重复数据取均值（你 15min 数据通常不会重复，但保底）
    out = out.sort_index().groupby(level=0).mean()

    return out


def to_hourly(prices_15min: pd.Series) -> pd.Series:
    s = prices_15min.sort_index()

    # 关键修正：时点是“区间结束时刻”，先整体回拨15分钟
    s = s.copy()
    # s.index = s.index - pd.Timedelta(minutes=15)

    # 聚合到小时：00:00会聚合(原0015,0030,0045,0100)
    hourly = s.resample("h").mean()

    # （可选）如果你希望小时必须有4个点才算有效，可以打开下面两行：
    # cnt = s.resample("h").count()
    # hourly = hourly.where(cnt >= 4)

    # 你原来的补齐逻辑保留（看你是否需要连续小时序列）
    hourly = hourly.interpolate(method="time", limit=6)
    hourly = hourly.ffill().bfill()
    return hourly





def hourly_to_daily_matrix(hourly: pd.Series) -> pd.DataFrame:
    df = hourly.to_frame("price")
    df["date"] = df.index.date
    df["hour"] = df.index.hour
    pivot = df.pivot_table(index="date", columns="hour", values="price", aggfunc="mean")
    pivot = pivot.reindex(columns=list(range(24)))
    pivot = pivot.apply(lambda row: row.interpolate(limit_direction="both"), axis=1)
    pivot.index = pd.to_datetime(pivot.index)
    pivot.columns = [f"Hour_{h:02d}" for h in range(24)]
    return pivot


def compute_daily_metrics(daily_profit: pd.Series, duration_h: float, power_mw: float) -> pd.DataFrame:
    e_mwh = duration_h * power_mw
    out = pd.DataFrame({"profit": daily_profit})
    out["profit_per_mw_day"] = out["profit"] / power_mw
    out["profit_per_mwh_day"] = out["profit"] / e_mwh
    out["avg_unit_value_proxy"] = out["profit_per_mwh_day"]
    return out


def month_agg(daily_df: pd.DataFrame) -> pd.DataFrame:
    m = daily_df.copy()
    m["month"] = m.index.to_period("M").astype(str)
    agg = m.groupby("month").agg(
        profit=("profit", "sum"),
        profit_per_mw_day=("profit_per_mw_day", "mean"),
        profit_per_mwh_day=("profit_per_mwh_day", "mean"),
        avg_unit_value_proxy=("avg_unit_value_proxy", "mean"),
        days=("profit", "count"),
    )
    return agg
