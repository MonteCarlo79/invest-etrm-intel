# services/province_fundamentals/parsers/format_a.py
"""格式 A — 月报数据累计工作簿 (e.g. 安徽电力市场信息披露月报数据-截至YYYY年M月.xlsx).

结构 (对安徽 fixture 实勘验证, 2026-11-02):
  sheet 名含 "装机及发电情况"; 第 1 行标题; 第 2 行表头; 数据行月份形如 'YYYY-MM'.
  识别特征: sheet 名含 "装机及发电情况" 且表头行同时含 "月份" 与 "省调口径装机总容量".

列处理 (表头按名称匹配列, 不按下标; 表头带 "(单位)" 注记先剥除):
  装机列 (省调口径装机总容量/火电/水电/风电/光伏/储能装机) = 期末值, 直接取,
    unit 万千瓦, source = 文件名.
  发电量列 (累计发电量 + 火电/水电/风电/光伏发电量) = 年内累计值 → 当月差分:
    1 月 = 累计原值直取 (不得减上年 12 月), 不打标;
    其余月份 = 本月累计 − 上月累计 (同年), source = 文件名 + "(差分)";
    上月缺失或任一端为空 → value=None (无法差分, 留给 store 层冲突闸门).
  日均发电量 / 火电占比 / 各同比列: 不在 metric 词表 (且占比/同比为累计口径), 跳过.
  空行 / 月份不可解析 (NaT) / 全指标空行: 跳过并计数, 结束 log 一行.

已知上游数据特征: 安徽 2025-09 累计发电量 (1853.97) 低于 2025-08 (2110.06),
差分出负当月值 — 解析器按规则原样输出, 不做静默修正.
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

FORMAT_NAME = "format_a"
SHEET_MARK = "装机及发电情况"
HEADER_MARKS = ("月份", "省调口径装机总容量")
DIFF_TAG = "(差分)"
_HEADER_SCAN_ROWS = 6          # 标题行可能不止一行, 给足余量

_UNITS = {"capacity": "万千瓦", "generation": "亿千瓦时"}

# 规范化表头 (剥空白与 "(单位)" 注记) → (section, metric)
_COL_MAP = {
    "省调口径装机总容量": ("capacity", "cap_total"),
    "火电装机": ("capacity", "cap_thermal"),
    "水电装机": ("capacity", "cap_hydro"),
    "风电装机": ("capacity", "cap_wind"),
    "光伏装机": ("capacity", "cap_solar"),
    "储能装机": ("capacity", "cap_storage"),
    "累计发电量": ("generation", "gen_total"),
    "火电发电量": ("generation", "gen_thermal"),
    "水电发电量": ("generation", "gen_hydro"),
    "风电发电量": ("generation", "gen_wind"),
    "光伏发电量": ("generation", "gen_solar"),
}

_MONTH_RE = re.compile(r"^\s*(\d{4})\s*[-/.年]\s*(\d{1,2})\s*月?\s*$")
_UNIT_NOTE_RE = re.compile(r"[(（\[【].*?[)）\]】]")
_WS_RE = re.compile(r"\s+")


def sniff(path) -> bool:
    """结构嗅探: sheet 名 + 表头标记. 任何读取失败都视为不命中."""
    try:
        _read_sheet(Path(path))
        return True
    except Exception:
        log.debug("format_a sniff failed: %s", path, exc_info=True)
        return False


def parse(path, province: str) -> list[FundRow]:
    path = Path(path)
    source = path.name
    header, data = _read_sheet(path)
    month_col, colmap = _map_columns(header)
    if month_col is None or not colmap:
        raise ValueError(f"{path}: header lacks 月份 or any known metric column")
    series, skipped = _collect(data, month_col, colmap)
    if skipped:
        log.info("%s: skipped %d empty/unparseable rows", source, skipped)
    return _emit(series, colmap, province, source)


# brief 接口名 (registry 鸭子类型用 .parse)
parse_format_a = parse


# ── internals ─────────────────────────────────────────────────────────────────

def _read_sheet(path: Path):
    """返回 (表头行, 数据行列表). 找不到 sheet/表头抛 ValueError."""
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = next((n for n in wb.sheetnames if SHEET_MARK in n), None)
        if sheet is None:
            raise ValueError(f"{path}: no sheet name containing {SHEET_MARK!r}")
        ws = wb[sheet]
        header = None
        data: list[tuple] = []
        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            if header is None:
                if _is_header(row):
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


def _is_header(row) -> bool:
    cells = [str(c) for c in row if c is not None]
    return all(any(mark in c for c in cells) for mark in HEADER_MARKS)


def _norm_header(v) -> str:
    s = _WS_RE.sub("", str(v))
    return _UNIT_NOTE_RE.sub("", s)


def _map_columns(header):
    """表头按名称匹配列, 不按下标. 返回 (月份列下标, {列下标: (section, metric)})."""
    month_col = None
    colmap: dict[int, tuple[str, str]] = {}
    for idx, h in enumerate(header):
        if h is None:
            continue
        n = _norm_header(h)
        if n == "月份":
            month_col = idx
        elif n in _COL_MAP:
            colmap[idx] = _COL_MAP[n]
    return month_col, colmap


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


def _collect(data, month_col, colmap):
    """月份 → {metric: 原始累计/期末值}. 空行/NaT/全空行跳过并计数."""
    series: dict[date, dict[str, float | None]] = {}
    skipped = 0
    for row in data:
        m = _parse_month(row[month_col] if month_col < len(row) else None)
        if m is None:
            skipped += 1
            continue
        vals: dict[str, float | None] = {}
        present = 0
        for idx, (_section, metric) in colmap.items():
            v = _num(row[idx] if idx < len(row) else None)
            vals[metric] = v
            if v is not None:
                present += 1
        if present == 0:
            skipped += 1
            continue
        series[m] = vals                    # 同月重复行: 后写覆盖
    return series, skipped


def _emit(series, colmap, province: str, source: str) -> list[FundRow]:
    section_of = {m: s for s, m in colmap.values()}
    out: list[FundRow] = []
    for month in sorted(series):
        vals = series[month]
        for metric in sorted(vals):
            section = section_of[metric]
            cur = vals[metric]
            if section == "generation" and month.month != 1:
                prev = series.get(date(month.year, month.month - 1, 1),
                                  {}).get(metric)
                value = (cur - prev) if (cur is not None and prev is not None) else None
                src = f"{source}{DIFF_TAG}"
            else:
                # capacity 期末直取; 1 月发电量 = 累计原值, 不得减上年 12 月
                value, src = cur, source
            out.append(FundRow(province=province, month=month, period="month",
                               section=section, metric=metric, value=value,
                               unit=_UNITS[section], source=src))
    return out
