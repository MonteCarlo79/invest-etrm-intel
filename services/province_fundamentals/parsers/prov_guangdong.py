# services/province_fundamentals/parsers/prov_guangdong.py
"""广东 bespoke parser — 广东电力市场运营简报_2026年1-5月.xlsx (unique long/narrow 格式).

结构 (对 fixture 实勘验证, 2026-11-04):
  sheet 窄表_指标, 表头 R1 (无标题行):
    省份 | 年份 | 月份 | 年月 | 指标类别 | 指标名称 | 数值 | 单位 | 来源文件 | 页码 | ...
  长表 1591 数据行, 30 个指标类别; 月份跨 2026-01..2026-05.
  全书唯一 fundamentals 内容 = 指标类别 "机组类型注册" 下的 10 个 "*装机规模" 指标
  (关键词全书扫描: 发电量/用电量/全社会/统调 零命中 — 发电与用电主题本册不存在,
  按设计不出行, 绝不编造). 机组类型注册 仅覆盖 2026-01..04 (2026-05 无该类别).

列处理 (全部按表头名定位, 绝不按下标):
  指标名称 → metric 映射 (注册口径 = 市场注册机组容量, 非全口径/省调口径 →
    `_registered` 后缀, 词表新口径, 与 _full/_ongrid 分键):
    合计装机规模→cap_total_registered, 燃煤→cap_thermal_registered,
    燃气→cap_gas_registered (词表新词干 gas), 核电→cap_nuclear_registered,
    风电→cap_wind_registered, 太阳能→cap_solar_registered,
    抽水蓄能→cap_pumped_registered (format_b 词干), 储能→cap_storage_registered.
    生物质/综合 装机规模不在燃料词表 → 不映射 (山西批 A 燃气/高炉尾气同规);
    企业注册数量/参与交易数量 (家) 非 fundamentals → 不映射 (蒙东批 B 注册计数同规).
  月份键 = 年月 列 ('YYYY-MM' 字符串); 年份/月份 int 列冗余不取.
  单位闸门: 行级 单位 列必须 == 万千瓦 (全册 装机规模 行实勘全为 万千瓦),
    否则整行跳过并计数. 空 数值 → None 原样出 (reported-but-empty), 绝不写 0.

已知上游异常 (实勘, 原样透传不修正):
  2026-03 装机规模 ×10 异常 — 册内 异常提示 sheet 逐行记录在案
  ("源PDF表4-1原值较相邻月份约放大10倍, 未擅自修正"), e.g. 风电 20516.68,
  合计 232686.73. parser 与源册同立场: 原值照出, 不打标不修正.
  2026-04 仅 6 个装机规模指标 (缺 合计/抽水蓄能/生物质/综合) — 缺什么不出什么.
"""
from __future__ import annotations

import logging
import math
import re
from datetime import date, datetime
from pathlib import Path

from openpyxl import load_workbook

from services.province_fundamentals.models import FundRow

log = logging.getLogger(__name__)

FORMAT_NAME = "prov_guangdong"
SHEET = "窄表_指标"
CATEGORY = "机组类型注册"
HEADER_MARKS = ("年月", "指标类别", "指标名称", "数值")
_UNIT_CAP = "万千瓦"

# 指标名称 (剥空白) → metric; 未列出的一律跳过 (生物质/综合/注册计数)
_NAME_MAP = {
    "合计装机规模": "cap_total_registered",
    "燃煤装机规模": "cap_thermal_registered",
    "燃气装机规模": "cap_gas_registered",
    "核电装机规模": "cap_nuclear_registered",
    "风电装机规模": "cap_wind_registered",
    "太阳能装机规模": "cap_solar_registered",
    "抽水蓄能装机规模": "cap_pumped_registered",
    "储能装机规模": "cap_storage_registered",
}

_MONTH_RE = re.compile(r"^\s*(\d{4})\s*[-/.年]\s*(\d{1,2})\s*月?\s*$")
_WS_RE = re.compile(r"\s+")


def sniff(path) -> bool:
    """结构嗅探: 窄表_指标 sheet + 表头标记. 任何读取失败都视为不命中."""
    try:
        _read_sheet(Path(path))
        return True
    except Exception:
        log.debug("prov_guangdong sniff failed: %s", path, exc_info=True)
        return False


def parse(path, province: str) -> list[FundRow]:
    path = Path(path)
    source = path.name
    _header, data, cols = _read_sheet(path)
    out: list[FundRow] = []
    skipped = bad_unit = 0
    for row in data:
        cat = _cell_text(row, cols["cat"])
        name = _cell_text(row, cols["name"])
        metric = _NAME_MAP.get(name) if cat == CATEGORY else None
        if metric is None:
            continue                                # 非注册类/未映射指标: 按设计忽略
        m = _parse_month(_cell(row, cols["month"]))
        if m is None:
            skipped += 1
            continue
        unit = _cell_text(row, cols["unit"])
        if unit != _UNIT_CAP:
            bad_unit += 1                           # 单位闸门: 非万千瓦整行跳过
            continue
        out.append(FundRow(province=province, month=m, period="month",
                           section="capacity", metric=metric,
                           value=_num(_cell(row, cols["value"])),
                           unit=_UNIT_CAP, source=source))
    if skipped or bad_unit:
        log.info("%s: skipped %d unparseable-month rows, %d bad-unit rows",
                 source, skipped, bad_unit)
    if not out:
        raise ValueError(f"{path}: no {CATEGORY} capacity rows found")
    out.sort(key=lambda r: (r.month, r.metric))
    return out


# ── internals ─────────────────────────────────────────────────────────────────

def _read_sheet(path: Path):
    """返回 (表头行, 数据行列表, {角色: 列下标}). 找不到 sheet/表头抛 ValueError."""
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        if SHEET not in wb.sheetnames:
            raise ValueError(f"{path}: no sheet {SHEET!r}")
        ws = wb[SHEET]
        header = None
        data: list[tuple] = []
        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            if header is None:
                cells = [str(c) for c in row if c is not None]
                if all(any(mark in c for c in cells) for mark in HEADER_MARKS):
                    header = row
                elif i >= 6:                        # 表头扫描窗 (与 format_a 同)
                    break
                continue
            data.append(row)
        if header is None:
            raise ValueError(f"{path}: header row not found in sheet {SHEET!r}")
        cols = {}
        for idx, h in enumerate(header):
            if h is None:
                continue
            n = _WS_RE.sub("", str(h))
            for role, key in (("month", "年月"), ("cat", "指标类别"),
                              ("name", "指标名称"), ("value", "数值"),
                              ("unit", "单位")):
                if n == key and role not in cols:
                    cols[role] = idx
        missing = {"month", "cat", "name", "value", "unit"} - set(cols)
        if missing:
            raise ValueError(f"{path}: header lacks columns {sorted(missing)}")
        return header, data, cols
    finally:
        wb.close()


def _cell(row, idx):
    return row[idx] if idx is not None and idx < len(row) else None


def _cell_text(row, idx) -> str:
    v = _cell(row, idx)
    return _WS_RE.sub("", str(v)) if v is not None else ""


def _parse_month(v) -> date | None:
    if isinstance(v, datetime):
        return date(v.year, v.month, 1)
    if isinstance(v, date):
        return date(v.year, v.month, 1)
    if isinstance(v, str):
        m = _MONTH_RE.match(v)
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            if 1 <= mo <= 12:
                return date(y, mo, 1)
    return None


def _num(v) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return None if math.isnan(f) else f
    s = str(v).strip().replace(",", "")
    if s in ("", "-", "—", "–"):
        return None
    try:
        return float(s)
    except ValueError:
        return None
