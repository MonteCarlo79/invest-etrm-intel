# services/province_fundamentals/parsers/prov_interprov.py
"""省间 (inter-provincial) 月度受入/外送电量 parser — Wave 1 专用旁册四省.

格式图: .superpowers/sdd/interprov-format-map.md (Family A — dedicated 省间 side files).
四省四种版式, 收进一个模块: 共享 section="interprov" / metric 词表
(import_gwh=受入·购入, export_gwh=外送·送出) / unit=亿千瓦时 / 月份解析与数值解析机制,
版式差异只落在各自 parse 实现里 (与 month_rows 一模块多省同例).

口径与已知上游特征 (均对 fixture 实勘验证, 2026-11-04):

安徽 — 安徽跨区跨省省间购入电量_截至2026年5月.xlsx · 省间购入电量 (15×3)
  表头 R1: 年份 | 月份 | 省间中长期购入电量实际执行电量（亿千瓦时）; 数据 R2..R15
  共 14 个月 (2025-04→2026-05). 年份 int + 月份 "N月" 双列复合键. 购入 only.
  中长期购入实际执行口径 (结算口径), 当月值非累计 — 无差分.

江苏 — 江苏跨区跨省月度交易计划分类汇总_2025-09至2026-05.xlsx · 月度明细 (13×10)
  R1 标题 / R2 单位来源注记 / R3 空行 / R4 真表头 (扫描窗 R1..R6 覆盖):
  年份 | 月份 | 年月 | 受进 | 送出 | 净受进 | ... | 数据状态 | 来源文件.
  9 个月 (2025-09→2026-05). 月键取 年月 列 ('YYYY-MM'). ⚠ 计划口径 (月度交易计划,
  非结算): source 一律打 " (计划)" 标 (可追溯口径), metric 不变.
  净受进=派生列 (受进−送出), 不在词表 → 不取. 分类长表/年度汇总/期间汇总/来源校验
  为同值长表 twin 或预聚合 → 不解析 (山西批 A 累计表同规).

河北南网 — 河北南网省间交易结算汇总-2025.4-2026.5.xlsx · 窄表 (93×6)
  R1 标题 / R2 表头: 年份 | 月份 | 交易类型 | 发电类别 | 电量（亿千瓦时） | 电价.
  结算口径. 外送 only: 交易类型 ∈ {省间现货, 省间中长期外送}, 发电类别 ∈
  {火电,光伏,风电,合计}. 只取 合计 行 (分燃料行按设计忽略, 不计数), 月出口 =
  现货合计 + 中长期外送合计; 任一腿缺失或为空 → 当月 value=None (与 format_a
  差分 "任一端为空→None" 同规 — 2026-01 中长期合计 '—' → 当月 None, 绝不半腿出数).
  13 个月 (2025-04→2026-05, ⚠ 原册缺 2026-04 整月). 宽表为复合表头同值 twin
  → 不解析 (格式图 §冀南: parse 窄表 instead).

冀北 — 冀北2024年以来电力市场交易结算通报数据（含省间）-2026-05.xlsx (19 sheets)
  购入腿 = 01_电源侧总览 (178×6): 表头 R1, 月份 'YYYY-MM' × 电源类型 长表,
    滤 电源类型=购华北 → import_gwh (电量(亿kWh) 列).
  外送腿 = 12_省间外送 (116×7): 表头 R1, 滤 类别=合计 行 → export_gwh
    (结算电量(亿kWh) 列). ⚠ 分项行存在完全重复行 (2024-01 跨京津唐中长期火电
    0.06 ×2) 且该重复值计入合计 (分项去重求和 23.32 ≠ 合计 23.38) — 重复行实为
    同值两笔, 故只读 合计 行 (逐月唯一, 已实勘 27 月 27 行), 绝不求和分项.
    ⚠ 2026-05 合计行单元格右移 (结算电量格空, 17.36 错落在均价列) — 按表头名列
    读取得 None (reported-but-empty), 不做位移修正 (原样出, 与框架同立场).
  两表均 27 个月 (2024-01→2026-05, 全书缺 2025-10/2025-11 两月).
  03_燃煤合同成分 的 送北京天津/跨区外送 为同一出口的燃煤子集视图 → 不取 (防双计).

sniff 隔离 (实勘): 四省披露主册均无本族 sheet (省间购入电量/月度明细/窄表/
01_电源侧总览+12_省间外送), 披露 parser 也不命中旁册 → registry 内互不截胡.
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

FORMAT_NAME = "prov_interprov"
_SECTION = "interprov"
_UNIT = "亿千瓦时"
PLAN_TAG = " (计划)"
_HEADER_SCAN_ROWS = 6          # 标题/单位注记行可能不止一行, 给足余量 (江苏 R4)

_MONTH_RE = re.compile(r"^\s*(\d{4})\s*[-/.年]\s*(\d{1,2})\s*月?\s*$")
_MONTH_NUM_RE = re.compile(r"^(\d{1,2})\s*月?$")
_UNIT_NOTE_RE = re.compile(r"[(（\[【].*?[)）\]】]")
_UNIT_NOTE_CAPTURE_RE = re.compile(r"[(（\[【](.*?)[)）\]】]")
_WS_RE = re.compile(r"\s+")

# 表头单位注记别名 → 规范单位 (与 month_rows 同表之子集; 其它注记整列弃用)
_UNIT_NOTES = {"亿kWh": "亿千瓦时", "亿千瓦时": "亿千瓦时"}


# ── 安徽: 省间购入电量 单列平表 (年份 int + "N月" 复合键) ──────────────────────

class _AnhuiInterprov:
    FORMAT_NAME = FORMAT_NAME
    _SHEET = "省间购入电量"
    _MARKS = ("年份", "月份", "购入电量")

    def sniff(self, path) -> bool:
        try:
            _read_sheet(Path(path), self._SHEET, self._MARKS)
            return True
        except Exception:
            log.debug("prov_interprov[安徽] sniff failed: %s", path, exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "安徽"
        source = path.name
        header, data = _read_sheet(path, self._SHEET, self._MARKS)
        cols = _map_columns(header, year="年份", month="月份",
                            values={"购入电量": "import_gwh"})
        out: list[FundRow] = []
        skipped = 0
        for row in data:
            m = _parse_composite_month(_cell(row, cols["year"]),
                                       _cell(row, cols["month"]))
            if m is None:
                skipped += 1
                continue
            v = _num(_cell(row, cols["import_gwh"]))
            if v is None:
                skipped += 1                        # 全指标空行 (单列即全空)
                continue
            out.append(FundRow(province=province, month=m, period="month",
                               section=_SECTION, metric="import_gwh", value=v,
                               unit=_UNIT, source=source))
        if skipped:
            log.info("%s [安徽/interprov]: skipped %d empty/unparseable rows",
                     source, skipped)
        out.sort(key=lambda r: (r.month, r.metric))
        return out


# ── 江苏: 月度明细 (3 行前导, 计划口径) ─────────────────────────────────────────

class _JiangsuInterprov:
    FORMAT_NAME = FORMAT_NAME
    _SHEET = "月度明细"
    _MARKS = ("年月", "受进", "送出")

    def sniff(self, path) -> bool:
        try:
            _read_sheet(Path(path), self._SHEET, self._MARKS)
            return True
        except Exception:
            log.debug("prov_interprov[江苏] sniff failed: %s", path, exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "江苏"
        source = path.name + PLAN_TAG              # 计划口径打标, 可追溯
        header, data = _read_sheet(path, self._SHEET, self._MARKS)
        cols = _map_columns(header, month="年月",
                            values={"受进": "import_gwh", "送出": "export_gwh"})
        out: list[FundRow] = []
        skipped = 0
        for row in data:
            m = _parse_month(_cell(row, cols["month"]))
            if m is None:
                skipped += 1
                continue
            vals = {met: _num(_cell(row, idx))
                    for met, idx in cols.items()
                    if met in ("import_gwh", "export_gwh")}
            if all(v is None for v in vals.values()):
                skipped += 1                        # 全指标空行
                continue
            for metric in sorted(vals):
                out.append(FundRow(province=province, month=m, period="month",
                                   section=_SECTION, metric=metric,
                                   value=vals[metric], unit=_UNIT, source=source))
        if skipped:
            log.info("%s [江苏/interprov]: skipped %d empty/unparseable rows",
                     source, skipped)
        out.sort(key=lambda r: (r.month, r.metric))
        return out


# ── 河北南网: 窄表 tidy (交易类型×发电类别), 合计行双腿加总 ─────────────────────

class _JinanInterprov:
    FORMAT_NAME = FORMAT_NAME
    _SHEET = "窄表"
    _MARKS = ("交易类型", "发电类别", "电量")
    _TOTAL = "合计"
    # 月出口 = 现货合计 + 中长期外送合计; 两腿缺一/任一空 → None (绝不半腿出数)
    _LEGS = ("省间现货", "省间中长期外送")

    def sniff(self, path) -> bool:
        try:
            _read_sheet(Path(path), self._SHEET, self._MARKS)
            return True
        except Exception:
            log.debug("prov_interprov[河北南网] sniff failed: %s", path,
                      exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "河北南网"
        source = path.name
        header, data = _read_sheet(path, self._SHEET, self._MARKS)
        cols = _map_columns(header, year="年份", month="月份",
                            values={"电量": "value"}, extra={"交易类型": "ttype",
                                                             "发电类别": "fuel"})
        legs: dict[date, dict[str, float | None]] = {}
        skipped = 0
        for row in data:
            if _cell_text(row, cols["fuel"]) != self._TOTAL:
                continue                            # 分燃料行: 按设计忽略, 不计数
            m = _parse_composite_month(_cell(row, cols["year"]),
                                       _cell(row, cols["month"]))
            if m is None:
                skipped += 1
                continue
            legs.setdefault(m, {})[_cell_text(row, cols["ttype"])] = \
                _num(_cell(row, cols["value"]))
        out: list[FundRow] = []
        for month in sorted(legs):
            got = legs[month]
            present = [t for t in self._LEGS if t in got]
            extra = [t for t in got if t not in self._LEGS]
            if not present and not extra:
                skipped += 1
                continue
            if len(present) < len(self._LEGS) or any(
                    got[t] is None for t in present + extra):
                value = None                        # 腿缺失或任一空 → 无法加总
            else:
                value = sum(got[t] for t in present + extra)
            out.append(FundRow(province=province, month=month, period="month",
                               section=_SECTION, metric="export_gwh", value=value,
                               unit=_UNIT, source=source))
        if skipped:
            log.info("%s [河北南网/interprov]: skipped %d unparseable rows",
                     source, skipped)
        out.sort(key=lambda r: (r.month, r.metric))
        return out


# ── 冀北: 01_电源侧总览 (购华北) + 12_省间外送 (类别=合计) 双 sheet ─────────────

class _JibeiInterprov:
    FORMAT_NAME = FORMAT_NAME
    _SHEET_IN = "01_电源侧总览"
    _SHEET_OUT = "12_省间外送"
    _MARKS_IN = ("电源类型", "电量")
    _MARKS_OUT = ("类别", "结算电量")
    _IMPORT_LABEL = "购华北"
    _TOTAL = "合计"

    def sniff(self, path) -> bool:
        try:
            path = Path(path)
            _read_sheet(path, self._SHEET_IN, self._MARKS_IN)
            _read_sheet(path, self._SHEET_OUT, self._MARKS_OUT)
            return True
        except Exception:
            log.debug("prov_interprov[冀北] sniff failed: %s", path, exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "冀北"
        source = path.name
        out: list[FundRow] = []
        out.extend(self._parse_import(path, province, source))
        out.extend(self._parse_export(path, province, source))
        out.sort(key=lambda r: (r.month, r.metric))
        return out

    def _parse_import(self, path: Path, province: str, source: str):
        header, data = _read_sheet(path, self._SHEET_IN, self._MARKS_IN)
        cols = _map_columns(header, month="月份", values={"电量": "import_gwh"},
                            extra={"电源类型": "stype"})
        out: list[FundRow] = []
        skipped = 0
        for row in data:
            if _cell_text(row, cols["stype"]) != self._IMPORT_LABEL:
                continue                            # 燃煤/新能源等行: 按设计忽略
            m = _parse_month(_cell(row, cols["month"]))
            if m is None:
                skipped += 1
                continue
            out.append(FundRow(province=province, month=m, period="month",
                               section=_SECTION, metric="import_gwh",
                               value=_num(_cell(row, cols["import_gwh"])),
                               unit=_UNIT, source=source))
        if skipped:
            log.info("%s [冀北/interprov-import]: skipped %d rows", source, skipped)
        return out

    def _parse_export(self, path: Path, province: str, source: str):
        header, data = _read_sheet(path, self._SHEET_OUT, self._MARKS_OUT)
        cols = _map_columns(header, month="月份", values={"结算电量": "export_gwh"},
                            extra={"类别": "cat"})
        series: dict[date, float | None] = {}
        skipped = 0
        for row in data:
            if _cell_text(row, cols["cat"]) != self._TOTAL:
                continue                  # 分项行 (含同值重复行): 不读, 绝不求和
            m = _parse_month(_cell(row, cols["month"]))
            if m is None:
                skipped += 1
                continue
            series[m] = _num(_cell(row, cols["export_gwh"]))   # 同月后写覆盖
        if skipped:
            log.info("%s [冀北/interprov-export]: skipped %d rows", source, skipped)
        return [FundRow(province=province, month=m, period="month",
                        section=_SECTION, metric="export_gwh", value=series[m],
                        unit=_UNIT, source=source)
                for m in sorted(series)]


# registry 鸭子类型实例
anhui_parser = _AnhuiInterprov()
jiangsu_parser = _JiangsuInterprov()
jinan_parser = _JinanInterprov()
jibei_parser = _JibeiInterprov()


# ── internals ─────────────────────────────────────────────────────────────────

def _read_sheet(path: Path, sheet: str, marks: tuple[str, ...]):
    """返回 (表头行, 数据行列表). 表头 = 全部标记命中 (规范化子串) 的首行.
    找不到 sheet/表头抛 ValueError (sniff 接住视为不命中)."""
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


def _map_columns(header, *, year=None, month=None, values: dict, extra=None):
    """表头按名称匹配列, 绝不按下标. values: 表头名 → 角色名 (角色名即 metric 名
    或占位名), 先全表精确匹配再子串兜底 (江苏 受进/送出 精确列必须先于 净受进/
    受进占比 等同含子串列落位); extra: 结构列 {表头名: 角色} (交易类型/类别等);
    year/month: 月键列名. 值列带单位注记时过闸门 (亿kWh/亿千瓦时→亿千瓦时),
    不认可 → 整列弃用. 返回 {角色: 列下标}; 月键列缺失或任一 values 列缺失
    → ValueError."""
    cols: dict[str, int] = {}
    extra = extra or {}
    for idx, h in enumerate(header):
        if h is None:
            continue
        n = _norm_header(h)
        if year is not None and "year" not in cols and n == year:
            cols["year"] = idx
            continue
        if month is not None and "month" not in cols and n == month:
            cols["month"] = idx
            continue
        matched = False
        for name, key in extra.items():
            if key not in cols and n == name:
                cols[key] = idx
                matched = True
                break
        if matched:
            continue
        for pat, role in values.items():          # pass 1: 精确
            if role not in cols and n == pat:
                if _unit_ok(h, pat):
                    cols[role] = idx
                break
    for idx, h in enumerate(header):              # pass 2: 子串兜底
        if h is None:
            continue
        n = _norm_header(h)
        if idx in cols.values():
            continue
        for pat, role in values.items():
            if role not in cols and pat in n:
                if _unit_ok(h, pat):
                    cols[role] = idx
                break
    if month is not None and "month" not in cols:
        raise ValueError(f"header lacks month column {month!r}")
    if year is not None and "year" not in cols:
        raise ValueError(f"header lacks year column {year!r}")
    missing = [r for r in values.values() if r not in cols]
    if missing:
        raise ValueError(f"header lacks value columns {missing}")
    return cols


def _unit_ok(h, pat) -> bool:
    """值列单位注记闸门: 无注记 → 采默认 (亿千瓦时); 注记不认可 → 弃用该列."""
    note = _header_unit_note(h)
    if note is not None and _UNIT_NOTES.get(note) != _UNIT:
        log.info("interprov: column %r unit note %r rejected", h, note)
        return False
    return True


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


def _parse_composite_month(yv, mv) -> date | None:
    """双列复合键: 年份单元格 (int/"2025") + 月份单元格 (int/"4"/"4月")."""
    y, mo = _parse_year(yv), _parse_month_num(mv)
    if y is None or mo is None:
        return None
    return date(y, mo, 1)


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
