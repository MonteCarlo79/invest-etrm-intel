# services/province_fundamentals/parsers/format_b.py
"""格式 B — N-省装机-月 利用小时月度工作簿 (e.g. 2-安徽装机-7月.xlsx).

结构 (对安徽 fixture 实勘验证, 2026-11-02):
  sheet 名含 "利用小时数数据"; 第 1 行表头; 数据行 年月 形如 'YYYY-MM' (2025-03..12
  数值列为文本, 其余月份为浮点 — 混型按值解析).
  识别特征: sheet 名含 "利用小时数数据" 且表头行同时含 "年月"/"指标名称"/"期末发电设备容量".

口径: 全口径 (安徽 2025-01 总计期末容量 12635.64 万千瓦 ≠ 格式 A 省调 6831.49) —
  capacity 与 generation 指标一律 `_full` 后缀 (cap_total_full / gen_total_full /
  gen_total_full_yoy), 不得与格式 A 省调口径的 cap_*/gen_* 同名撞键;
  util_hours 无口径区分 (hours_total).

列处理 (表头按名称匹配列, 不按下标; 表头带 "(单位)" 注记先剥除):
  期末发电设备容量 → cap_<stem>_full, 期末直取, unit 万千瓦.
  本月发电量 → gen_<stem>_full, 本身即当月值 (非累计, 不差分), unit 亿千瓦时.
  本月同比 → gen_<stem>_full_yoy, unit %, 存数值.
  设备累计平均利用小时 → hours_<stem>, 原样直取 (累计平均口径, 不差分), unit 小时;
    该格缺 → 本月发电量÷期末容量×10000 推算 (亿千瓦时÷万千瓦×1e4 = 小时),
    写入 hours_<stem>_derived (月度点估与累计平均不同语义, 绝不混入 hours_*),
    source 标注 "(推算)"; 容量缺/为 0 无法推算 → hours_<stem> value=None,
    source 不打标 (记录"上报格为空", 留给 builder 层 S3 兜底).
  年份/月份/供热设备容量/本年累计发电量/本年累计同比/利用小时增减: 跳过
    (累计口径同比与小时增减不在 metric 词表).

指标名映射 (strip "其中：" 前缀后精确匹配; 只取最粗口径行):
  总计→total, 水电→hydro, 抽水蓄能→pumped (单列, 不并入 hydro), 火电→thermal,
  风电→wind, 太阳能发电/光伏发电→solar.
  燃煤机组/煤矸石/燃气机组/储能/6000千瓦以上 等子项: 跳过 (避免与父行双计).
  同 (month, stem) 多行 (如 太阳能发电 + 其中：光伏发电): 先见者胜 (父行在前),
  计数并于结束 log 一行.

空行 / 年月不可解析 / 四指标全空行: 跳过并计数, 结束 log 一行.
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

FORMAT_NAME = "format_b"
SHEET_MARK = "利用小时数数据"
HEADER_MARKS = ("年月", "指标名称", "期末发电设备容量")
DERIVE_TAG = "(推算)"
_HEADER_SCAN_ROWS = 6          # 标题行可能不止一行, 给足余量

# 规范化表头 (剥空白与 "(单位)" 注记) → 列角色
_COL_MAP = {
    "年月": "month",
    "指标名称": "label",
    "期末发电设备容量": "cap",
    "本月发电量": "gen",
    "本月同比": "yoy",
    "设备累计平均利用小时": "hours",
}

# 规范化指标名 (strip "其中：" 前缀) → metric 词干; 未列出的一律跳过
_LABEL_MAP = {
    "总计": "total",
    "水电": "hydro",
    "抽水蓄能": "pumped",
    "火电": "thermal",
    "风电": "wind",
    "太阳能发电": "solar",
    "光伏发电": "solar",
}

_MONTH_RE = re.compile(r"^\s*(\d{4})\s*[-/.年]\s*(\d{1,2})\s*月?\s*$")
_UNIT_NOTE_RE = re.compile(r"[(（\[【].*?[)）\]】]")
_WS_RE = re.compile(r"\s+")
_QIZHONG_RE = re.compile(r"^其中\s*[:：]")


def sniff(path) -> bool:
    """结构嗅探: sheet 名 + 表头标记. 任何读取失败都视为不命中."""
    try:
        _read_sheet(Path(path))
        return True
    except Exception:
        log.debug("format_b sniff failed: %s", path, exc_info=True)
        return False


def parse(path, province: str) -> list[FundRow]:
    path = Path(path)
    source = path.name
    header, data = _read_sheet(path)
    cols = _map_columns(header)
    if cols.get("month") is None or cols.get("label") is None or len(cols) < 3:
        raise ValueError(f"{path}: header lacks 年月/指标名称 or any known value column")
    rows, skipped, dups = _collect(data, cols, province, source)
    if skipped or dups:
        log.info("%s: skipped %d empty/unparseable rows, %d duplicate (month, indicator) rows",
                 source, skipped, dups)
    return rows


# brief 接口名 (registry 鸭子类型用 .parse)
parse_format_b = parse


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


def _map_columns(header) -> dict[str, int]:
    """表头按名称匹配列, 不按下标. 返回 {列角色: 列下标}."""
    cols: dict[str, int] = {}
    for idx, h in enumerate(header):
        if h is None:
            continue
        role = _COL_MAP.get(_norm_header(h))
        if role is not None and role not in cols:
            cols[role] = idx
    return cols


def _norm_label(v) -> str:
    s = _WS_RE.sub("", str(v))
    return _QIZHONG_RE.sub("", s)


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


def _cell(row, idx) -> float | None:
    return _num(row[idx]) if idx is not None and idx < len(row) else None


def _collect(data, cols, province: str, source: str):
    """逐行出 FundRow; 同 (month, stem) 先见者胜. 返回 (rows, skipped, dups)."""
    out: list[FundRow] = []
    seen: set[tuple[date, str]] = set()
    skipped = dups = 0
    for row in data:
        m = _parse_month(row[cols["month"]] if cols["month"] < len(row) else None)
        if m is None:
            skipped += 1
            continue
        label_v = row[cols["label"]] if cols["label"] < len(row) else None
        if label_v is None:
            skipped += 1
            continue
        stem = _LABEL_MAP.get(_norm_label(label_v))
        if stem is None:
            continue                                # 子项/未映射指标: 按设计忽略
        if (m, stem) in seen:
            dups += 1                               # 父行在前, 先见者胜
            continue
        cap = _cell(row, cols.get("cap"))
        gen = _cell(row, cols.get("gen"))
        yoy = _cell(row, cols.get("yoy"))
        hours = _cell(row, cols.get("hours"))
        if cap is None and gen is None and yoy is None and hours is None:
            skipped += 1                            # 全空行只计数, 不占 (month, stem) 位
            continue
        seen.add((m, stem))
        hours_metric = f"hours_{stem}"
        hours_src = source
        if hours is None and gen is not None and cap:
            hours = gen / cap * 10000               # 亿千瓦时÷万千瓦×1e4 = 小时
            hours_metric = f"hours_{stem}_derived"  # 月度点估, 不混入累计平均 hours_*
            hours_src = f"{source}{DERIVE_TAG}"
        out.append(FundRow(province=province, month=m, period="month",
                           section="capacity", metric=f"cap_{stem}_full",
                           value=cap, unit="万千瓦", source=source))
        out.append(FundRow(province=province, month=m, period="month",
                           section="generation", metric=f"gen_{stem}_full",
                           value=gen, unit="亿千瓦时", source=source))
        out.append(FundRow(province=province, month=m, period="month",
                           section="generation", metric=f"gen_{stem}_full_yoy",
                           value=yoy, unit="%", source=source))
        out.append(FundRow(province=province, month=m, period="month",
                           section="util_hours", metric=hours_metric,
                           value=hours, unit="小时", source=hours_src))
    out.sort(key=lambda r: (r.month, r.metric))
    return out, skipped, dups
