# services/province_fundamentals/parsers/prov_interprov2.py
"""省间 (inter-provincial) 月度受入/外送电量 parser — Wave 2 聚合册内省间 sheet 七省.

格式图: .superpowers/sdd/interprov-format-map.md (Family B — 省间 sheets inside
aggregate workbooks). 与 wave 1 (prov_interprov, 专用旁册) 同一词汇表:
section="interprov" / metric import_gwh(受入·购入)·export_gwh(外送·送出) /
unit=亿千瓦时; 表头扫描/列映射/月份与数值解析机制直接复用 wave 1 模块内部件.

与 wave 1 的结构差异: 省间 sheet 与该省披露主册同处一个工作簿 → sniff 以
sheet 名 + 表头标记命中册内目标 sheet; 注册在披露 parser 之后 (registry 首个
sniff 命中者解析), 披露册 dispatch 不变, 本波 parser 由直接调用触发
(陕西结算册例外: 无披露 parser 命中, dispatch 自然落本 parser).

七省七版式 (均对 fixture 实勘验证, 2026-11-04):

蒙东 — 蒙东-电力交易市场信息披露数据_截至2026年5月.xlsx · 外送结算(月度) (20×16)
  表头 R1: 年份 | 结算月份 | 数据类型 | 总外送(亿kWh) | …; 数据 R2..R17.
  数据类型 ∈ {当月, 累计} 混合同 sheet; 结算月份 int/str 混杂 (1 / '3' / '1-2月' /
  '全年'). 取 总外送 列: 当月行为主; 当月缺失或为空 → 同年相邻累计行差分
  (差分出数 source 打 "(差分)" 标, 与 format_a 同规; 任一端为空 → None).
  外送 only. 9 个月 (2024-01..2025-02 稀疏覆盖; 册止 2025-02).
  ⚠ 省间外送交易(累计) 表 2025-04 起口径突变 (842.42→327.68 非单调) 不可差分
  → 不解析; 购电结算情况 为网内购电非省间 → 不解析 (格式图 §蒙东).

甘肃 — 甘肃-信息披露月报数据-截至2026年5月.xlsx
  售电腿 = 跨省售电_月度 (18×14): 年份 int + 月份 'N月', 取 合计(亿kWh) → export.
  购电腿 = 跨省购电_月度 (18×10): 同构, 合计 → import. 各 17 个月
  (2025-01→2026-05). 分项通道/燃料列 (祁韶/庆东/消纳李家峡/…) 不取;
  分项格现 'NOT_FOUND' 字符串 (_num → None), 合计列干净.
  跨省售电_累计 = 同值累计 twin → 不解析; 省间外送计划_安排 = 计划口径 → 不解析.

山西 — 山西信息披露月报数据_2025至2026年5月.xlsx · 外送交易 (18×11)
  表头 R1: 月份 | 外送总量(亿kWh) | 外送均价 | 长南荆量 | …; 月份为 datetime
  (2025-01-01). 外送 only, 通道分列不取. 17 个月 (2025-01→2026-05).

陕西 — 陕西结算及分类构成_2025至2026年5月.xlsx · 结算及分类构成表-省间交易结算
  (34×7): 表头 R1: 年份 | 月份 | 交易类别 | 当月结算电量(亿千瓦时) | 当月同比 |
  年度累计电量 | …; 长表, 交易类别 ∈ {主网外送→export, 主网外购→import},
  取 当月结算电量 (年度累计同列不取 — 免差分). 年份 int + 月份 int 复合键.
  33 数据行 = 17 外送 + 16 外购 (⚠ 2026-05 外购整行缺失, 原样出, 不补).
  披露册的 省间市场交易结算 宽表为同口径 twin 且均价列多空 → 不解析
  (格式图 §陕西: prefer 结算长表).

辽宁 — 辽宁-信息披露月报-2025至2026年5月.xlsx · 联络线电量结算（当月） (200×10)
  双行复合表头: R1 标题 / R2 方向组行 (月份 | 受入电量（亿kWh）×5 merged |
  送出电量（亿kWh）×4 merged, merged 格右端为 None → 前向填充归组) /
  R3 通道行 (统分水电|东北分部火电|蒙东|吉林|黑龙江 || 高岭通道外送|鲁固通道外送|
  东北区域内外送|蒙东送出). 数据 R4..R18, 月份 'YYYY-MM'.
  月受入 = Σ 受入活通道; 月送出 = Σ 送出活通道. 活通道 = 全列至少一个非空值
  (蒙东送出 全列 None/'NOT_FOUND' → 死通道剔除); 活通道当月任一空 → 该方向
  None (冀南同规, 绝不半腿出数). 15 个月 (2025-02→2026-05, 缺 2025-01/2025-05).
  省间购电（累计）/跨区外送（累计）为同流向累计 twin → 不解析 (免双计).

山东 — 山东电力市场信息披露月报-2022至2026年5月.xlsx
  购入腿 = 06_跨省跨区交易 (200×14): R1 标题 / R2 表头 (表头含字面 \\n,
  规范化后命中), 月份 'YYYY-MM', 取 购入合计(亿kWh) → import.
  17 个月 (2025-01→2026-05).
  外送腿 = 15_跨省外送明细 (32×5): R1 标题 / R2 表头 月份 | 通道名称 | 交易类型 |
  本月完成(万kWh) | 累计完成(万kWh); 长表, 只取 通道名称=合计 行 (分项行不读).
  ⚠ 单位 万kWh — 全书唯一非亿kWh 表: 解析时强制校验表头含 '万kWh'
  (单位若悄悄变动 → 抛错, 绝不静默出数), 值 ÷10000 → 亿千瓦时.
  5 个月 (2026-01→2026-05). 14_跨省购入明细 与 06 同向重叠 (仅 2026 月)
  → 不解析 (免双计, 06 覆盖更长).

广西 — 广西-信息披露-2025-2026.5.xlsx · 月度发受电量 (19×13)
  R1 标题 / R2 表头: 月份 | 煤电 | … | 西电 | 云电 | 送广东海南 | 合计.
  省间受入 = 西电 + 云电 双腿加总 (任一腿空 → 当月 None, 冀南同规; 册内显式 0
  与 None 有别 — None 即未报). 送广东海南 为出口但本波按格式图口径只取受入
  (partial import). 月份列 Excel 序列日 (45658=2025-01-01) 与 '2026年1月'
  字符串混杂. 17 个月 (2025-01→2026-05); 2026-03 西电 None → 当月 None.
  ⚠ 本 sheet 同被 prov_guangxi 披露 parser 解析 (装机/发电 section) — 注册
  顺序保证 dispatch 仍归 prov_guangxi, 本 parser 只补 interprov section.

sniff 隔离 (实勘, 2026-11-04): 七个目标 sheet 名在全部 fixture 工作簿中各自
唯一出现 (sheets_all 实勘), wave 1 旁册四文件均无本族 sheet; 披露主册
dispatch 归原 parser (本波注册在后, 见 registry).
"""
from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from pathlib import Path

from openpyxl import load_workbook

from services.province_fundamentals.models import FundRow
from services.province_fundamentals.parsers import prov_interprov as _ip1

log = logging.getLogger(__name__)

FORMAT_NAME = "prov_interprov2"
_SECTION = "interprov"
_UNIT = "亿千瓦时"
DIFF_TAG = "(差分)"                     # 与 format_a 同标

_EXCEL_EPOCH = date(1899, 12, 30)       # Excel serial day 0
_NUM_RUN_RE = re.compile(r"\d{1,2}")


# ── 蒙东: 外送结算(月度) 当月/累计混合 → 当月主 + 累计差分 ─────────────────────

class _MengdongInterprov2:
    FORMAT_NAME = FORMAT_NAME
    _SHEET = "外送结算(月度)"
    _MARKS = ("年份", "结算月份", "数据类型")

    def sniff(self, path) -> bool:
        try:
            _ip1._read_sheet(Path(path), self._SHEET, self._MARKS)
            return True
        except Exception:
            log.debug("prov_interprov2[蒙东] sniff failed: %s", path, exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "蒙东"
        source = path.name
        header, data = _ip1._read_sheet(path, self._SHEET, self._MARKS)
        cols = _ip1._map_columns(header, year="年份", month="结算月份",
                                 values={"总外送": "total"},
                                 extra={"数据类型": "dtype"})
        direct: dict[date, float | None] = {}
        cum: dict[date, float | None] = {}
        skipped = 0
        for row in data:
            y = _ip1._parse_year(_ip1._cell(row, cols["year"]))
            dtype = _ip1._cell_text(row, cols["dtype"])
            v = _ip1._num(_ip1._cell(row, cols["total"]))
            if y is None:
                skipped += 1
                continue
            if dtype == "当月":
                mo = _ip1._parse_month_num(_ip1._cell(row, cols["month"]))
                if mo is None:
                    skipped += 1
                    continue
                direct[date(y, mo, 1)] = v
            elif dtype == "累计":
                mo = _cum_end_month(_ip1._cell(row, cols["month"]))
                if mo is None:
                    skipped += 1
                    continue
                cum[date(y, mo, 1)] = v
            else:
                skipped += 1                # 空行/未知类型行
        out: list[FundRow] = []
        for m in sorted(set(direct) | set(cum)):
            dv = direct.get(m)
            if dv is not None:
                out.append(_row(province, m, "export_gwh", dv, source))
                continue
            # 当月缺失/为空 → 同年相邻累计差分 (format_a 同规)
            cv = cum.get(m)
            prev = (date(m.year - 1, 12, 1) if m.month == 1
                    else date(m.year, m.month - 1, 1))
            pv = cum.get(prev)
            if pv is None and prev.month == 1:
                pv = direct.get(prev)       # 1 月累计 ≡ 1 月当月
            if cv is not None and pv is not None:
                out.append(_row(province, m, "export_gwh", cv - pv,
                                source + DIFF_TAG))
            elif m in direct:
                out.append(_row(province, m, "export_gwh", None, source))
            # 仅累计行且无法差分 → 不出 Phantom 行
        if skipped:
            log.info("%s [蒙东/interprov2]: skipped %d rows", source, skipped)
        return out


# ── 甘肃: 跨省售电_月度 + 跨省购电_月度 双 sheet, 合计列 ───────────────────────

class _GansuInterprov2:
    FORMAT_NAME = FORMAT_NAME
    _SHEET_OUT = "跨省售电_月度"
    _SHEET_IN = "跨省购电_月度"
    _MARKS = ("年份", "月份", "合计")

    def sniff(self, path) -> bool:
        try:
            path = Path(path)
            _ip1._read_sheet(path, self._SHEET_OUT, self._MARKS)
            _ip1._read_sheet(path, self._SHEET_IN, self._MARKS)
            return True
        except Exception:
            log.debug("prov_interprov2[甘肃] sniff failed: %s", path, exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "甘肃"
        source = path.name
        out: list[FundRow] = []
        out.extend(self._parse_leg(path, province, source, self._SHEET_OUT,
                                   "export_gwh"))
        out.extend(self._parse_leg(path, province, source, self._SHEET_IN,
                                   "import_gwh"))
        out.sort(key=lambda r: (r.month, r.metric))
        return out

    def _parse_leg(self, path: Path, province: str, source: str, sheet: str,
                   metric: str):
        header, data = _ip1._read_sheet(path, sheet, self._MARKS)
        cols = _ip1._map_columns(header, year="年份", month="月份",
                                 values={"合计": metric})
        out: list[FundRow] = []
        skipped = 0
        for row in data:
            m = _ip1._parse_composite_month(_ip1._cell(row, cols["year"]),
                                            _ip1._cell(row, cols["month"]))
            if m is None:
                skipped += 1
                continue
            v = _ip1._num(_ip1._cell(row, cols[metric]))
            if v is None:
                skipped += 1                # 合计空行
                continue
            out.append(_row(province, m, metric, v, source))
        if skipped:
            log.info("%s [甘肃/interprov2 %s]: skipped %d rows",
                     source, sheet, skipped)
        return out


# ── 山西: 外送交易 (datetime 月份) ─────────────────────────────────────────────

class _ShanxiInterprov2:
    FORMAT_NAME = FORMAT_NAME
    _SHEET = "外送交易"
    _MARKS = ("月份", "外送总量")

    def sniff(self, path) -> bool:
        try:
            _ip1._read_sheet(Path(path), self._SHEET, self._MARKS)
            return True
        except Exception:
            log.debug("prov_interprov2[山西] sniff failed: %s", path, exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "山西"
        source = path.name
        header, data = _ip1._read_sheet(path, self._SHEET, self._MARKS)
        cols = _ip1._map_columns(header, month="月份",
                                 values={"外送总量": "export_gwh"})
        out: list[FundRow] = []
        skipped = 0
        for row in data:
            m = _ip1._parse_month(_ip1._cell(row, cols["month"]))
            if m is None:
                skipped += 1
                continue
            v = _ip1._num(_ip1._cell(row, cols["export_gwh"]))
            if v is None:
                skipped += 1
                continue
            out.append(_row(province, m, "export_gwh", v, source))
        if skipped:
            log.info("%s [山西/interprov2]: skipped %d rows", source, skipped)
        out.sort(key=lambda r: (r.month, r.metric))
        return out


# ── 陕西: 结算及分类构成表-省间交易结算 长表 (主网外送/主网外购) ────────────────

class _ShaanxiInterprov2:
    FORMAT_NAME = FORMAT_NAME
    _SHEET = "结算及分类构成表-省间交易结算"
    _MARKS = ("交易类别", "当月结算电量")
    _DIR = {"主网外送": "export_gwh", "主网外购": "import_gwh"}

    def sniff(self, path) -> bool:
        try:
            _ip1._read_sheet(Path(path), self._SHEET, self._MARKS)
            return True
        except Exception:
            log.debug("prov_interprov2[陕西] sniff failed: %s", path, exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "陕西"
        source = path.name
        header, data = _ip1._read_sheet(path, self._SHEET, self._MARKS)
        cols = _ip1._map_columns(header, year="年份", month="月份",
                                 values={"当月结算电量": "value"},
                                 extra={"交易类别": "ttype"})
        out: list[FundRow] = []
        skipped = 0
        for row in data:
            metric = self._DIR.get(_ip1._cell_text(row, cols["ttype"]))
            if metric is None:
                skipped += 1                # 未知类别/空行
                continue
            m = _ip1._parse_composite_month(_ip1._cell(row, cols["year"]),
                                            _ip1._cell(row, cols["month"]))
            if m is None:
                skipped += 1
                continue
            out.append(_row(province, m, metric,
                            _ip1._num(_ip1._cell(row, cols["value"])), source))
        if skipped:
            log.info("%s [陕西/interprov2]: skipped %d rows", source, skipped)
        out.sort(key=lambda r: (r.month, r.metric))
        return out


# ── 辽宁: 联络线电量结算（当月） 双行复合表头, 活通道加总 ───────────────────────

class _LiaoningInterprov2:
    FORMAT_NAME = FORMAT_NAME
    _SHEET = "联络线电量结算（当月）"
    _GROUP_IN = "受入电量"
    _GROUP_OUT = "送出电量"
    _CHANNEL_MARK = "高岭通道外送"

    def sniff(self, path) -> bool:
        try:
            self._read_header(Path(path))
            return True
        except Exception:
            log.debug("prov_interprov2[辽宁] sniff failed: %s", path, exc_info=True)
            return False

    def _read_header(self, path: Path):
        """返回 (受入通道 {列: 名}, 送出通道 {列: 名}, 数据行列表).
        R 方向组行: 月份 | 受入电量（亿kWh） | None… | 送出电量（亿kWh） | None…
        (merged 格右端 None → 前向填充); 下一行为通道名行. 组行须带单位注记
        亿kWh (闸门), 通道行须含 _CHANNEL_MARK."""
        wb = load_workbook(path, read_only=True, data_only=True)
        try:
            if self._SHEET not in wb.sheetnames:
                raise ValueError(f"{path}: no sheet {self._SHEET!r}")
            ws = wb[self._SHEET]
            rows = list(ws.iter_rows(values_only=True))
        finally:
            wb.close()
        group_idx = None
        for i, row in enumerate(rows[:_ip1._HEADER_SCAN_ROWS]):
            cells = [_ip1._norm_header(c) for c in row if c is not None]
            if any(self._GROUP_IN in c for c in cells) and \
                    any(self._GROUP_OUT in c for c in cells):
                group_idx = i
                break
        if group_idx is None or group_idx + 1 >= len(rows):
            raise ValueError(f"{path}: composite header not found")
        group_row, chan_row = rows[group_idx], rows[group_idx + 1]
        if not any(self._CHANNEL_MARK in _ip1._norm_header(c)
                   for c in chan_row if c is not None):
            raise ValueError(f"{path}: channel row lacks {self._CHANNEL_MARK}")
        for c in group_row:                 # 单位闸门: 方向组格须为亿kWh系
            if c is None:
                continue
            note = _ip1._header_unit_note(c)
            if note is not None and _ip1._UNIT_NOTES.get(note) != _UNIT:
                raise ValueError(f"{path}: unit note {note!r} rejected")
        in_cols: dict[int, str] = {}
        out_cols: dict[int, str] = {}
        direction = None
        for idx in range(1, max(len(group_row), len(chan_row))):
            g = (_ip1._norm_header(group_row[idx])
                 if idx < len(group_row) and group_row[idx] is not None else "")
            if self._GROUP_IN in g:
                direction = "in"
            elif self._GROUP_OUT in g:
                direction = "out"
            chan = (_ip1._norm_header(chan_row[idx])
                    if idx < len(chan_row) and chan_row[idx] is not None else "")
            if not chan or direction not in ("in", "out"):
                continue
            (in_cols if direction == "in" else out_cols)[idx] = chan
        if not in_cols or not out_cols:
            raise ValueError(f"{path}: direction channels incomplete")
        return in_cols, out_cols, rows[group_idx + 2:]

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "辽宁"
        source = path.name
        in_cols, out_cols, data = self._read_header(path)
        series: dict[date, dict[str, list[float | None]]] = {}
        skipped = 0
        for row in data:
            m = _ip1._parse_month(_ip1._cell(row, 0))
            if m is None:
                if any(c is not None for c in row):
                    skipped += 1
                continue
            series[m] = {
                "in": [_ip1._num(_ip1._cell(row, i)) for i in in_cols],
                "out": [_ip1._num(_ip1._cell(row, i)) for i in out_cols],
            }
        # 活通道 = 全列至少一个非空 (蒙东送出 全列 None/'NOT_FOUND' → 死通道剔除)
        live_in = [j for j in range(len(in_cols))
                   if any(v["in"][j] is not None for v in series.values())]
        live_out = [j for j in range(len(out_cols))
                    if any(v["out"][j] is not None for v in series.values())]
        out: list[FundRow] = []
        for m in sorted(series):
            for metric, vals, live in (("import_gwh", series[m]["in"], live_in),
                                       ("export_gwh", series[m]["out"], live_out)):
                leg = [vals[j] for j in live]
                if not leg:
                    continue
                value = None if any(v is None for v in leg) else sum(leg)
                out.append(_row(province, m, metric, value, source))
        if skipped:
            log.info("%s [辽宁/interprov2]: skipped %d rows", source, skipped)
        return out


# ── 山东: 06_跨省跨区交易 (购入) + 15_跨省外送明细 (外送, 万kWh→亿) ────────────

class _ShandongInterprov2:
    FORMAT_NAME = FORMAT_NAME
    _SHEET_IN = "06_跨省跨区交易"
    _SHEET_OUT = "15_跨省外送明细"
    _MARKS_IN = ("月份", "购入合计")
    _MARKS_OUT = ("月份", "通道名称", "本月完成")
    _TOTAL = "合计"
    _WAN_UNIT = "万kWh"

    def sniff(self, path) -> bool:
        try:
            path = Path(path)
            _ip1._read_sheet(path, self._SHEET_IN, self._MARKS_IN)
            _ip1._read_sheet(path, self._SHEET_OUT, self._MARKS_OUT)
            return True
        except Exception:
            log.debug("prov_interprov2[山东] sniff failed: %s", path, exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "山东"
        source = path.name
        out: list[FundRow] = []
        out.extend(self._parse_import(path, province, source))
        out.extend(self._parse_export(path, province, source))
        out.sort(key=lambda r: (r.month, r.metric))
        return out

    def _parse_import(self, path: Path, province: str, source: str):
        header, data = _ip1._read_sheet(path, self._SHEET_IN, self._MARKS_IN)
        cols = _ip1._map_columns(header, month="月份",
                                 values={"购入合计": "import_gwh"})
        out: list[FundRow] = []
        skipped = 0
        for row in data:
            m = _ip1._parse_month(_ip1._cell(row, cols["month"]))
            if m is None:
                skipped += 1
                continue
            v = _ip1._num(_ip1._cell(row, cols["import_gwh"]))
            if v is None:
                skipped += 1
                continue
            out.append(_row(province, m, "import_gwh", v, source))
        if skipped:
            log.info("%s [山东/interprov2-import]: skipped %d rows",
                     source, skipped)
        return out

    def _parse_export(self, path: Path, province: str, source: str):
        header, data = _ip1._read_sheet(path, self._SHEET_OUT, self._MARKS_OUT)
        # 单位闸门: 15 表为全书唯一 万kWh 表 — 注记不认可即抛错 (绝不静默出数);
        # 认可则 ÷10000 → 亿千瓦时. 不走 _map_columns (其单位闸门会弃用该列).
        month_idx = chan_idx = val_idx = None
        for idx, h in enumerate(header):
            if h is None:
                continue
            n = _ip1._norm_header(h)
            if n == "月份" and month_idx is None:
                month_idx = idx
            elif n == "通道名称" and chan_idx is None:
                chan_idx = idx
            elif n == "本月完成" and val_idx is None:
                if self._WAN_UNIT not in str(h):
                    raise ValueError(
                        f"{path}: 15_跨省外送明细 本月完成 unit changed: {h!r}")
                val_idx = idx
        if month_idx is None or chan_idx is None or val_idx is None:
            raise ValueError(f"{path}: 15_跨省外送明细 header incomplete")
        out: list[FundRow] = []
        skipped = 0
        for row in data:
            if _ip1._cell_text(row, chan_idx) != self._TOTAL:
                continue                    # 分项通道行: 不读, 绝不求和
            m = _ip1._parse_month(_ip1._cell(row, month_idx))
            if m is None:
                skipped += 1
                continue
            v = _ip1._num(_ip1._cell(row, val_idx))
            if v is None:
                skipped += 1                # 合计空行
                continue
            out.append(_row(province, m, "export_gwh", v / 10000, source))
        if skipped:
            log.info("%s [山东/interprov2-export]: skipped %d rows",
                     source, skipped)
        return out


# ── 广西: 月度发受电量 西电+云电 双腿加总 (Excel 序列/中文串混合月键) ──────────

class _GuangxiInterprov2:
    FORMAT_NAME = FORMAT_NAME
    _SHEET = "月度发受电量"
    _MARKS = ("月份", "西电", "云电")

    def sniff(self, path) -> bool:
        try:
            _ip1._read_sheet(Path(path), self._SHEET, self._MARKS)
            return True
        except Exception:
            log.debug("prov_interprov2[广西] sniff failed: %s", path, exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        path = Path(path)
        province = province or "广西"
        source = path.name
        header, data = _ip1._read_sheet(path, self._SHEET, self._MARKS)
        cols = _ip1._map_columns(header, month="月份",
                                 values={"西电": "xilian", "云电": "yunnan"})
        out: list[FundRow] = []
        skipped = 0
        for row in data:
            m = _parse_month_ex(_ip1._cell(row, cols["month"]))
            if m is None:
                skipped += 1
                continue
            legs = [_ip1._num(_ip1._cell(row, cols["xilian"])),
                    _ip1._num(_ip1._cell(row, cols["yunnan"]))]
            if all(v is None for v in legs):
                skipped += 1                # 双腿全空行
                continue
            # 任一腿空 → None (冀南同规: 册内显式 0 与 None 有别, 绝不半腿出数)
            value = None if any(v is None for v in legs) else sum(legs)
            out.append(_row(province, m, "import_gwh", value, source))
        if skipped:
            log.info("%s [广西/interprov2]: skipped %d rows", source, skipped)
        out.sort(key=lambda r: (r.month, r.metric))
        return out


# registry 鸭子类型实例
mengdong_parser = _MengdongInterprov2()
gansu_parser = _GansuInterprov2()
shanxi_parser = _ShanxiInterprov2()
shaanxi_parser = _ShaanxiInterprov2()
liaoning_parser = _LiaoningInterprov2()
shandong_parser = _ShandongInterprov2()
guangxi_parser = _GuangxiInterprov2()


# ── internals ─────────────────────────────────────────────────────────────────

def _row(province: str, month: date, metric: str, value, source: str) -> FundRow:
    return FundRow(province=province, month=month, period="month",
                   section=_SECTION, metric=metric, value=value,
                   unit=_UNIT, source=source)


def _cum_end_month(v) -> int | None:
    """累计行月末键: '1月'→1 / '1-2月'→2 / '全年'→12 / int N→N (取末个数字)."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        n = int(f) if f.is_integer() else None
        return n if n is not None and 1 <= n <= 12 else None
    s = str(v).strip()
    if s == "全年":
        return 12
    nums = _NUM_RUN_RE.findall(s)
    if not nums:
        return None
    n = int(nums[-1])
    return n if 1 <= n <= 12 else None


def _parse_month_ex(v) -> date | None:
    """月键: 先走 wave 1 的 datetime/date/str 解析, 再兜 Excel 序列日
    (45658 → 2025-01-01; 40000<n<60000 闸门, 广西专用)."""
    m = _ip1._parse_month(v)
    if m is not None:
        return m
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        if f.is_integer() and 40000 < f < 60000:
            d = _EXCEL_EPOCH + timedelta(days=int(f))
            return date(d.year, d.month, 1)
    return None
