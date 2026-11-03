# services/province_fundamentals/parsers/prov_guangxi.py
"""广西 bespoke parser — 广西-信息披露-2025-2026.5.xlsx (unique 混型月键 + 转置汇总).

结构 (对 fixture 实勘验证, 2026-11-04):
  月末装机容量: 标题 R1 (合并), 表头 R2 = 年份|统计时点|水电|燃煤|燃气|核电|风电|光伏|
    生物质|储能|合计; 数据 R3..R19 共 17 个月 (2025-01..2026-05); 月键 = 年份 int +
    统计时点 复合 (2025 全年 int, 2026-05 的统计时点是字符串 '5' — 同列混型).
  月度发受电量: 标题 R1 (合并), 表头 R2 = 月份|煤电|水电|核电|风电|光伏|生物质|气电|
    西电|云电|送广东海南|合计; 数据 R3..R19 共 17 个月; 月键单列混型 —
    2025 行为 Excel serial int (45658 = 2025-01-01, 45992 = 2025-12-01),
    2026 行为中文字符串 ('2026年4月'). 两种形态都归一化为月初 date.
  数据汇总: 转置册 (指标为行, 月份为列), 仅覆盖 2025-01..2026-02 (stale) 且与
    两个主题表同值重复 → 显式不解析 (甘肃批 B 核心指标总览同规: 重复表不取,
    避免同 source 双写; 转置布局在此仅作识别与排除, 见 _SKIP_SHEETS).

列处理 (全部按表头名定位, 绝不按下标):
  装机 (统调口径 → `_full` 后缀, 蒙西批 A 统调→_full 同规):
    合计→cap_total_full, 燃煤→cap_thermal_full, 燃气→cap_gas_full (词表新词干),
    水电→cap_hydro_full, 核电→cap_nuclear_full, 风电→cap_wind_full,
    光伏→cap_solar_full, 储能→cap_storage_full. 生物质 不在燃料词表 → 不映射.
  发受电 (统调发受电口径 — 合计 = 分电源发电 + 西电/云电受电 − 送广东海南,
    非全省发电量 → `_dispatch` 后缀, 词表新口径, 消费方不得当全口径发电量用;
    冀北批 B 曾以"发+受混合口径无标准词表项"整表不取, 本册无其他发电主题表,
    格式图钦定此表为广西发电来源, 故以独立口径如实命名出数):
    合计→gen_total_dispatch, 煤电→gen_thermal_dispatch, 水电→gen_hydro_dispatch,
    核电→gen_nuclear_dispatch, 风电→gen_wind_dispatch, 光伏→gen_solar_dispatch,
    气电→gen_gas_dispatch. 生物质/西电/云电/送广东海南 不在词表 → 不映射.
  用电量 本册无干净主题表 (仅结算口径长表) → 不出行 (绝不编造).
  两表均为当月值, 不差分. 空单元格 → None 原样出 (reported-but-empty), 绝不写 0.
  空行/月键不可解析/全指标空行 → 跳过并计数, 结束 log 一行.
"""
from __future__ import annotations

import logging
import math
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from openpyxl import load_workbook

from services.province_fundamentals.models import FundRow

log = logging.getLogger(__name__)

FORMAT_NAME = "prov_guangxi"
_SHEET_CAP = "月末装机容量"
_SHEET_GEN = "月度发受电量"
_SKIP_SHEETS = ("数据汇总",)               # 转置重复表: 识别并显式排除 (见模块 docstring)
_HEADER_SCAN_ROWS = 6                     # 标题行 R1 + 表头 R2, 给足余量

_CAP_COL_MAP = {
    "合计": "cap_total_full",
    "燃煤": "cap_thermal_full",
    "燃气": "cap_gas_full",
    "水电": "cap_hydro_full",
    "核电": "cap_nuclear_full",
    "风电": "cap_wind_full",
    "光伏": "cap_solar_full",
    "储能": "cap_storage_full",
}

_GEN_COL_MAP = {
    "合计": "gen_total_dispatch",
    "煤电": "gen_thermal_dispatch",
    "水电": "gen_hydro_dispatch",
    "核电": "gen_nuclear_dispatch",
    "风电": "gen_wind_dispatch",
    "光伏": "gen_solar_dispatch",
    "气电": "gen_gas_dispatch",
}

_MONTH_RE = re.compile(r"^\s*(\d{4})\s*[-/.年]\s*(\d{1,2})\s*月?\s*$")
_MONTH_NUM_RE = re.compile(r"^(\d{1,2})\s*月?$")
_WS_RE = re.compile(r"\s+")
_EXCEL_EPOCH = date(1899, 12, 30)         # 1900 日期系统 (含闰年 bug) 的序列日零点


def sniff(path) -> bool:
    """结构嗅探: 双主题 sheet + 各自表头标记. 任何读取失败都视为不命中."""
    try:
        _read_sheet(Path(path), _SHEET_CAP, ("年份", "统计时点", "水电"))
        _read_sheet(Path(path), _SHEET_GEN, ("月份", "煤电", "水电"))
        return True
    except Exception:
        log.debug("prov_guangxi sniff failed: %s", path, exc_info=True)
        return False


def parse(path, province: str) -> list[FundRow]:
    path = Path(path)
    source = path.name
    out: list[FundRow] = []
    out.extend(_parse_capacity(path, province, source))
    out.extend(_parse_generation(path, province, source))
    out.sort(key=lambda r: (r.month, r.metric))
    return out


# ── internals ─────────────────────────────────────────────────────────────────

def _read_sheet(path: Path, sheet: str, marks: tuple[str, ...]):
    """返回 (表头行, 数据行列表). 表头 = 全部标记命中 (剥空白子串) 的首行."""
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        if sheet not in wb.sheetnames:
            raise ValueError(f"{path}: no sheet {sheet!r}")
        ws = wb[sheet]
        header = None
        data: list[tuple] = []
        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            if header is None:
                cells = [_WS_RE.sub("", str(c)) for c in row if c is not None]
                if all(any(mark in c for c in cells) for mark in marks):
                    header = row
                elif i >= _HEADER_SCAN_ROWS:
                    break
                continue
            data.append(row)
        if header is None:
            raise ValueError(f"{path}: header row not found in sheet {sheet!r}")
        return header, data
    finally:
        wb.close()


def _map_columns(header, col_map, key_names: tuple[str, ...]):
    """表头按名称匹配列. 返回 ({角色: 列下标}, {列下标: metric})."""
    keys: dict[str, int] = {}
    colmap: dict[int, str] = {}
    for idx, h in enumerate(header):
        if h is None:
            continue
        n = _WS_RE.sub("", str(h))
        for role in key_names:
            if n == role and role not in keys:
                keys[role] = idx
        metric = col_map.get(n)
        if metric is not None and idx not in colmap:
            colmap[idx] = metric
    missing = set(key_names) - set(keys)
    if missing:
        raise ValueError(f"header lacks key columns {sorted(missing)}")
    if not colmap:
        raise ValueError("header lacks any known metric column")
    return keys, colmap


def _parse_capacity(path: Path, province: str, source: str) -> list[FundRow]:
    header, data = _read_sheet(path, _SHEET_CAP, ("年份", "统计时点", "水电"))
    keys, colmap = _map_columns(header, _CAP_COL_MAP, ("年份", "统计时点"))
    return _collect(data, colmap, province, source,
                    section="capacity", unit="万千瓦",
                    month_of=lambda row: _parse_composite_month(
                        _cell(row, keys["年份"]), _cell(row, keys["统计时点"])))


def _parse_generation(path: Path, province: str, source: str) -> list[FundRow]:
    header, data = _read_sheet(path, _SHEET_GEN, ("月份", "煤电", "水电"))
    keys, colmap = _map_columns(header, _GEN_COL_MAP, ("月份",))
    return _collect(data, colmap, province, source,
                    section="generation", unit="亿千瓦时",
                    month_of=lambda row: _parse_mixed_month(_cell(row, keys["月份"])))


def _collect(data, colmap, province, source, *, section, unit, month_of):
    out: list[FundRow] = []
    skipped = 0
    for row in data:
        m = month_of(row)
        if m is None:
            skipped += 1
            continue
        vals = {metric: _num(_cell(row, idx)) for idx, metric in colmap.items()}
        if all(v is None for v in vals.values()):
            skipped += 1                            # 全指标空行
            continue
        for metric, v in vals.items():
            out.append(FundRow(province=province, month=m, period="month",
                               section=section, metric=metric, value=v,
                               unit=unit, source=source))
    if skipped:
        log.info("%s [%s]: skipped %d empty/unparseable rows",
                 source, section, skipped)
    return out


def _cell(row, idx):
    return row[idx] if idx is not None and idx < len(row) else None


def _parse_mixed_month(v) -> date | None:
    """单列混型月键: Excel serial int/float (2025 行) 或 '2026年4月' 字符串 (2026 行)."""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        if f.is_integer() and 20000 < f < 80000:    # 1954..2118 的合理 serial 窗
            d = _EXCEL_EPOCH + timedelta(days=int(f))
            return date(d.year, d.month, 1)
        return None
    return _parse_month_str(v)


def _parse_month_str(v) -> date | None:
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


def _parse_composite_month(yv, mv) -> date | None:
    """双列复合键: 年份 (int 2025 / '2025') + 统计时点 (int 1 / '1' / '1月')."""
    y = _parse_year(yv)
    mo = _parse_month_num(mv)
    if y is None or mo is None:
        return None
    return date(y, mo, 1)


def _parse_year(v) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return int(f) if f.is_integer() and 1900 < f < 2200 else None
    s = str(v).strip()
    return int(s) if s.isdigit() and len(s) == 4 else None


def _parse_month_num(v) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return int(f) if f.is_integer() and 1 <= f <= 12 else None
    m = _MONTH_NUM_RE.match(str(v).strip())
    if m and 1 <= int(m.group(1)) <= 12:
        return int(m.group(1))
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
