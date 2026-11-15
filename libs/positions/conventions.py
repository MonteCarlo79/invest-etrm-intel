"""Position sign/TOU conventions and province name mapper. Pure: no DB, no UI."""
from __future__ import annotations

from dataclasses import astuple, dataclass, fields
from datetime import date

import pandas as pd

TOU_BLOCKS = ("base", "peak", "offpeak")
PEAK_HOURS = frozenset(range(17, 22))     # 17:00-22:00, v1 default all provinces
OFFPEAK_HOURS = frozenset(range(10, 16))  # 10:00-16:00


def tou_of_hour(hour: int) -> str:
    if hour in PEAK_HOURS:
        return "peak"
    if hour in OFFPEAK_HOURS:
        return "offpeak"
    return "flat"


PROVINCE_ALIASES: dict[str, tuple[str, ...]] = {
    "蒙西": ("蒙西", "inner_mongolia_mengxi", "内蒙古", "Inner Mongolia (Mengxi)"),
    "冀南": ("冀南", "河北南网", "hebei_south"),
    "安徽": ("安徽", "anhui"),
    "浙江": ("浙江", "zhejiang"),
    "山东": ("山东", "shandong"),
    # R1: every other name seen in the production probe (2026-11-15)
    "广西": ("广西", "guangxi"),
    "江苏": ("江苏", "jiangsu"),
    "甘肃": ("甘肃", "gansu"),
    "福建": ("福建",), "上海": ("上海",), "广东": ("广东",),
    "陕西": ("陕西",), "湖北": ("湖北",), "山西": ("山西",), "贵州": ("贵州",),
    "海南": ("海南",), "黑龙江": ("黑龙江",), "吉林": ("吉林",), "辽宁": ("辽宁",),
    "云南": ("云南",), "河南": ("河南",), "新疆": ("新疆",), "宁夏": ("宁夏",),
    "豫北": ("豫北",), "豫西": ("豫西",), "豫南": ("豫南",), "豫中东": ("豫中东",),
    "湖南": ("湖南",), "四川": ("四川",), "江西": ("江西",), "青海": ("青海",),
    "海南礼记": ("海南礼记",), "海南那悦": ("海南那悦",), "蒙东": ("蒙东",),
}

_ALIAS_TO_CANON = {a: k for k, al in PROVINCE_ALIASES.items() for a in al}


def canonical_province(name: str) -> str:
    try:
        return _ALIAS_TO_CANON[name]
    except (KeyError, TypeError):
        raise ValueError(f"unknown province: {name!r}") from None


def signed_mwh(direction: str, volume_mwh: float) -> float:
    if direction == "buy":
        return float(volume_mwh)
    if direction == "sell":
        return -float(volume_mwh)
    raise ValueError(f"unknown direction: {direction!r}")


@dataclass(frozen=True)
class Position:
    book_id: int
    province: str
    direction: str
    volume_mwh: float
    delivery_month: date  # first of month
    tou_block: str = "base"
    price_cny_mwh: float | None = None
    source: str = "manual"
    instrument_type: str = "forward"
    channel: str = "annual"
    basis: str = ""  # how a physical volume was estimated


POSITION_COLUMNS: list[str] = [f.name for f in fields(Position)]


def positions_frame(rows: list[Position]) -> pd.DataFrame:
    return pd.DataFrame([astuple(r) for r in rows], columns=POSITION_COLUMNS)
