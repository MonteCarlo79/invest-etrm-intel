# services/province_fundamentals/parsers/prov_jiangsu.py
"""江苏 bespoke parser — 江苏-信息披露月报数据_截至2026年5月.xlsx (无装机册 + 口径陷阱).

结构 (对 fixture 实勘验证, 2026-11-04):
  全省月度用电量: 表头 R1 (无标题行), 月份 'YYYY-MM' 行向, 数据 R2..R18 共 17 个月
    (2025-01..2026-05). ⚠ 名为"月度"实为年内累计 — 全社会用电量 667.07 → 8803.31
    递增至 2025-12, 2026-01 重置为 815.58. 同比列存小数分数 (0.0111 = 1.11%).
  发电侧结算-只有电量: 表头 R1, 结算月份 'YYYY-MM' 行向, 当月值 (上网口径,
    非单调: 423.02 → 405.36 → 441.82), 17 个月. NOT_FOUND 文本字面量散布
    (2026-03/04/05 全行, 2026-02 部分列, 2025-11/12 分燃料空格).
  装机主题本册不存在 (另文件 江苏-装机数据_2026年5月.xlsx, 不在本 parser 范围)
  → 一行不出, 绝不编造.

列处理 (全部按表头名定位 — 剥空白与 "(单位)" 注记后精确匹配, 绝不按下标):
  用电 (累计 → 差分, format_a 规则作用于 demand section):
    全社会用电量→demand_total, 第一产业→demand_p1, 第二产业→demand_p2,
    第三产业→demand_p3, 城乡居民→demand_residential;
    1 月 = 累计原值直取 (不得减上年 12 月), 不打标;
    其余月份 = 本月累计 − 上月累计 (同年), source = 文件名 + "(差分)";
    任一端为空 → value=None. 工业 列与 二产 重叠不在词表 → 不映射;
    各产业同比 (一产/二产/工业/三产/居民同比) 无词表项 → 不映射 (甘肃批 B 同规).
  全社会用电量同比(%) → demand_yoy: 小数分数 ×100 出百分数, unit %, 不差分
    (原值为累计同比口径, 与差分后当月不同语义 — 如实出累计同比, 消费方须知);
    空格 (2025 全年 + 2026-01) → None 原样出.
  发电 (当月, 上网口径 → `_ongrid` 后缀, 甘肃批 B 同规):
    发电企业上网电量→gen_total_ongrid, 火电→gen_thermal_ongrid,
    核电→gen_nuclear_ongrid, 风电→gen_wind_ongrid, 太阳能→gen_solar_ongrid,
    水电→gen_hydro_ongrid. 其他/直接交易/绿电/均价/备注 不在词表 → 不映射.
    NOT_FOUND → _num → None (reported-but-empty), 绝不写 0; 全指标空月
    (2026-01 全空, 2026-03..05 全 NOT_FOUND) → 整月跳过并计数.
  表头单位注记闸门 (与 month_rows 同表): 认可 亿kWh/亿千瓦时→亿千瓦时, %→%;
    注记不识别或与默认冲突 → 整列跳过并计数.
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

FORMAT_NAME = "prov_jiangsu"
_SHEET_DEMAND = "全省月度用电量"
_SHEET_GEN = "发电侧结算-只有电量"
DIFF_TAG = "(差分)"
_HEADER_SCAN_ROWS = 6                     # 表头 R1, 与 format_a 同余量

# 规范化表头名 → metric. 用电: level 列差分, 同比列直取 ×100.
_DEMAND_LEVEL_MAP = {
    "全社会用电量": "demand_total",
    "第一产业": "demand_p1",
    "第二产业": "demand_p2",
    "第三产业": "demand_p3",
    "城乡居民": "demand_residential",
}
_DEMAND_YOY_MAP = {
    "全社会用电量同比": "demand_yoy",
}

_GEN_COL_MAP = {
    "发电企业上网电量": "gen_total_ongrid",
    "火电": "gen_thermal_ongrid",
    "核电": "gen_nuclear_ongrid",
    "风电": "gen_wind_ongrid",
    "太阳能": "gen_solar_ongrid",
    "水电": "gen_hydro_ongrid",
}

_MONTH_RE = re.compile(r"^\s*(\d{4})\s*[-/.年]\s*(\d{1,2})\s*月?\s*$")
_UNIT_NOTE_RE = re.compile(r"[(（\[【].*?[)）\]】]")
_UNIT_NOTE_CAPTURE_RE = re.compile(r"[(（\[【](.*?)[)）\]】]")
_WS_RE = re.compile(r"\s+")

# 表头单位注记别名 → 规范单位 (只认这几样; 其它注记整列跳过)
_UNIT_NOTES = {
    "万kW": "万千瓦", "万千瓦": "万千瓦",
    "亿kWh": "亿千瓦时", "亿千瓦时": "亿千瓦时",
    "h": "小时", "小时": "小时",
    "%": "%",
}


def sniff(path) -> bool:
    """结构嗅探: 双 sheet + 各自表头标记. 任何读取失败都视为不命中."""
    try:
        _read_sheet(Path(path), _SHEET_DEMAND, ("月份", "全社会用电量"))
        _read_sheet(Path(path), _SHEET_GEN, ("结算月份", "发电企业上网电量"))
        return True
    except Exception:
        log.debug("prov_jiangsu sniff failed: %s", path, exc_info=True)
        return False


def parse(path, province: str) -> list[FundRow]:
    path = Path(path)
    source = path.name
    out: list[FundRow] = []
    out.extend(_parse_demand(path, province, source))
    out.extend(_parse_generation(path, province, source))
    out.sort(key=lambda r: (r.month, r.metric))
    return out


# ── internals ─────────────────────────────────────────────────────────────────

def _read_sheet(path: Path, sheet: str, marks: tuple[str, ...]):
    """返回 (表头行, 数据行列表). 表头 = 全部标记命中 (剥空白注记子串) 的首行."""
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        if sheet not in wb.sheetnames:
            raise ValueError(f"{path}: no sheet {sheet!r}")
        ws = wb[sheet]
        header = None
        data: list[tuple] = []
        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            if header is None:
                cells = [_norm_header(c) for c in row if c is not None]
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


def _norm_header(v) -> str:
    s = _WS_RE.sub("", str(v))
    return _UNIT_NOTE_RE.sub("", s)


def _header_unit_note(v) -> str | None:
    m = _UNIT_NOTE_CAPTURE_RE.search(str(v))
    if not m:
        return None
    return _WS_RE.sub("", m.group(1))


def _map_columns(header, col_map, month_names: tuple[str, ...], default_unit: str):
    """表头按名称匹配列. 返回 (月份列下标, {列下标: metric}, 被单位闸门拦下数)."""
    month_col = None
    colmap: dict[int, str] = {}
    bad_units = 0
    for idx, h in enumerate(header):
        if h is None:
            continue
        n = _norm_header(h)
        if month_col is None and n in month_names:
            month_col = idx
            continue
        metric = col_map.get(n)
        if metric is None:
            continue                                # 工业/各产业同比/交易列: 忽略
        note = _header_unit_note(h)
        if note is not None:
            canonical = _UNIT_NOTES.get(note)
            if canonical is None or canonical != default_unit:
                bad_units += 1                      # 单位不认可/冲突: 整列跳过
                continue
        colmap[idx] = metric
    return month_col, colmap, bad_units


def _parse_demand(path: Path, province: str, source: str) -> list[FundRow]:
    header, data = _read_sheet(path, _SHEET_DEMAND, ("月份", "全社会用电量"))
    month_col, level_cols, bad1 = _map_columns(header, _DEMAND_LEVEL_MAP,
                                               ("月份",), "亿千瓦时")
    _, yoy_cols, bad2 = _map_columns(header, _DEMAND_YOY_MAP,
                                     ("月份",), "%")
    if month_col is None or not level_cols:
        raise ValueError(f"{path}: demand header lacks 月份 or any known level column")
    if bad1 or bad2:
        log.info("%s [demand]: skipped %d columns on unit gate", source, bad1 + bad2)
    series: dict[date, dict[str, float | None]] = {}
    yoy: dict[date, float | None] = {}
    skipped = 0
    for row in data:
        m = _parse_month(_cell(row, month_col))
        if m is None:
            skipped += 1
            continue
        vals = {metric: _num(_cell(row, idx)) for idx, metric in level_cols.items()}
        if all(v is None for v in vals.values()):
            skipped += 1                            # 全指标空行
            continue
        series[m] = vals
        for idx, metric in yoy_cols.items():
            f = _num(_cell(row, idx))
            yoy[m] = f * 100 if f is not None else None   # 小数分数 → 百分数
    if skipped:
        log.info("%s [demand]: skipped %d empty/unparseable rows", source, skipped)
    out: list[FundRow] = []
    for month in sorted(series):
        vals = series[month]
        for metric in sorted(vals):
            cur = vals[metric]
            if month.month == 1:
                value, src = cur, source            # 1 月累计原值, 不打标
            else:
                prev = series.get(date(month.year, month.month - 1, 1),
                                  {}).get(metric)
                value = (cur - prev) if (cur is not None and prev is not None) else None
                src = f"{source}{DIFF_TAG}"
            out.append(FundRow(province=province, month=month, period="month",
                               section="demand", metric=metric, value=value,
                               unit="亿千瓦时", source=src))
        for metric in sorted(set(yoy_cols.values())):
            out.append(FundRow(province=province, month=month, period="month",
                               section="demand", metric=metric, value=yoy.get(month),
                               unit="%", source=source))
    return out


def _parse_generation(path: Path, province: str, source: str) -> list[FundRow]:
    header, data = _read_sheet(path, _SHEET_GEN, ("结算月份", "发电企业上网电量"))
    month_col, colmap, bad = _map_columns(header, _GEN_COL_MAP,
                                          ("结算月份",), "亿千瓦时")
    if month_col is None or not colmap:
        raise ValueError(f"{path}: generation header lacks 结算月份 or metric columns")
    if bad:
        log.info("%s [generation]: skipped %d columns on unit gate", source, bad)
    out: list[FundRow] = []
    skipped = 0
    for row in data:
        m = _parse_month(_cell(row, month_col))
        if m is None:
            skipped += 1
            continue
        vals = {metric: _num(_cell(row, idx)) for idx, metric in colmap.items()}
        if all(v is None for v in vals.values()):
            skipped += 1                            # NOT_FOUND 全行/全空月: 整月跳过
            continue
        for metric, v in vals.items():
            out.append(FundRow(province=province, month=m, period="month",
                               section="generation", metric=metric, value=v,
                               unit="亿千瓦时", source=source))
    if skipped:
        log.info("%s [generation]: skipped %d empty/unparseable rows", source, skipped)
    return out


def _cell(row, idx):
    return row[idx] if idx is not None and idx < len(row) else None


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
    """数值解析: NOT_FOUND/空/破折号等一切不可解析文本 → None (绝不写 0)."""
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
