# services/province_fundamentals/parsers/month_rows.py
"""月行宽表族 — 单个 sheet 内月份为行、燃料指标为列的月报工作簿 (参考: 山东).

结构 (对山东 fixture 实勘验证, 2026-11-03):
  sheet 按名选取 (精确名或 predicate, 绝不按下标 — 山东 21 个 sheet);
  R1 合并标题行; R2 表头 (单元格可带 "\\n" 与 "(单位)" 注记); 数据行月份 'YYYY-MM'.
  山东: 10_装机与发电结构 (装机+发电同表, 当月值) + 01_用电与发电概况 (用电).

列处理 (表头按名称匹配列, 规范化 = 剥一切空白(含 \\n) + 剥 "(单位)" 注记):
  fuel_col_map: 规范化表头名 → 完整 metric 名 ("火电装机" → "cap_thermal_full").
  section 由 metric 前缀推断: cap_*→capacity, gen_*→generation,
    hours_*→util_hours, demand_*→demand.
  unit 默认: capacity→万千瓦, generation→亿千瓦时, util_hours→小时,
    demand→亿千瓦时, *_yoy→%; unit_map 可按 metric 覆盖.
  表头单位注记校验: 认可注记 万kW/万千瓦→万千瓦, 亿kWh/亿千瓦时→亿千瓦时,
    h/小时→小时, %→% (万千瓦↔MW 的 1:10 换算不在此做 — 本族只出原生口径,
    除注记别名外没有任何静默换算). 注记存在但不识别 / 与默认单位冲突 →
    整列跳过并计数; 无注记 → 采默认单位. 未映射列 (占比等) 一律忽略.

累计差分 (cumulative_gen=True 时, 与 format_a 同规则, 仅作用 generation section):
  1 月 = 累计原值直取 (不得减上年 12 月), 不打标;
  其余月份 = 本月累计 − 上月累计 (同年), source = 文件名 + "(差分)";
  任一端为空 → value=None. 山东为当月值 (cumulative_gen=False).

空燃料列 (山东 水电/核电 2025 全年为空): value=None 原样出, 绝不写 0.
空行 / 月份不可解析 / 全指标空行: 跳过并计数, 结束 log 一行.
skip_labels: 月份列中的已知标签串 (如 "合计"), 跳过且不计入 skip 数.
"""
from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Mapping, Union

from openpyxl import load_workbook

from services.province_fundamentals.models import FundRow

log = logging.getLogger(__name__)

DIFF_TAG = "(差分)"
_HEADER_SCAN_ROWS = 6          # 标题行可能不止一行, 给足余量

_MONTH_RE = re.compile(r"^\s*(\d{4})\s*[-/.年]\s*(\d{1,2})\s*月?\s*$")
_UNIT_NOTE_RE = re.compile(r"[(（\[【].*?[)）\]】]")
_UNIT_NOTE_CAPTURE_RE = re.compile(r"[(（\[【](.*?)[)）\]】]")
_WS_RE = re.compile(r"\s+")

# 表头单位注记别名 → 规范单位 (只认这四种; 其它注记整列跳过)
_UNIT_NOTES = {
    "万kW": "万千瓦", "万千瓦": "万千瓦",
    "亿kWh": "亿千瓦时", "亿千瓦时": "亿千瓦时",
    "h": "小时", "小时": "小时",
    "%": "%",
}


@dataclass(frozen=True)
class MonthRowsConfig:
    """月行宽表单 sheet 解析配置.

    province:      写入 FundRow.province.
    sheet_select:  sheet 精确名 (str) 或 predicate(sheet_name) -> bool.
    header_markers: 表头行必须全部出现的标记 (规范化后子串匹配), 防选错 sheet.
    month_col:     月份列的候选规范化表头名, 先中者胜.
    fuel_col_map:  规范化表头名 → 完整 metric 名 (cap_*/gen_*/hours_*/demand_*).
    cumulative_gen: True → generation section 按年内累计差分 (format_a 规则).
    unit_map:      metric → unit 覆盖 (默认见 _default_unit).
    skip_labels:   月份列中跳过且不计数的标签串 (如 "合计").
    ignore_notes:  括号注记白名单 — 注记是限定语而非单位时 (蒙西 "光伏(含光热)"
                   / "储能(含配储)"), 剥空白后命中此表的注记视同无注记,
                   不进单位闸门 (否则整列被误判跳过).
    """
    province: str
    sheet_select: Union[str, Callable[[str], bool]]
    header_markers: tuple[str, ...]
    month_col: tuple[str, ...]
    fuel_col_map: Mapping[str, str]
    cumulative_gen: bool = False
    unit_map: Mapping[str, str] = field(default_factory=dict)
    skip_labels: tuple[str, ...] = ()
    ignore_notes: tuple[str, ...] = ()


def parse_month_rows(path, cfg: MonthRowsConfig) -> list[FundRow]:
    path = Path(path)
    source = path.name
    header, data = _read_sheet(path, cfg)
    month_col, colmap, bad_units = _map_columns(header, cfg)
    if month_col is None or not colmap:
        raise ValueError(f"{path}: header lacks 月份 or any known metric column")
    if bad_units:
        log.info("%s: skipped %d columns on unit gate: %s",
                 source, len(bad_units), ", ".join(sorted(bad_units)))
    series, skipped = _collect(data, month_col, colmap, cfg)
    if skipped:
        log.info("%s [%s]: skipped %d empty/unparseable rows",
                 source, _sheet_label(cfg), skipped)
    return _emit(series, cfg, source)


class MonthRowsParser:
    """registry 鸭子类型: 一省可挂多个 MonthRowsConfig (山东 = 装机发电 + 用电)."""

    FORMAT_NAME = "month_rows"

    def __init__(self, configs):
        self.configs = tuple(configs)

    def sniff(self, path) -> bool:
        try:
            for cfg in self.configs:
                try:
                    _read_sheet(Path(path), cfg)
                    return True
                except Exception:
                    continue
            return False
        except Exception:
            log.debug("month_rows sniff failed: %s", path, exc_info=True)
            return False

    def parse(self, path, province: str | None = None) -> list[FundRow]:
        out: list[FundRow] = []
        for cfg in self.configs:
            out.extend(parse_month_rows(path, cfg))
        out.sort(key=lambda r: (r.month, r.metric))
        return out


# ── 山东内建配置 (格式图 §1, 对 fixture 实勘) ─────────────────────────────────

SHANDONG_CAP_GEN = MonthRowsConfig(
    province="山东",
    sheet_select="10_装机与发电结构",
    header_markers=("月份", "火电装机", "风电发电量"),
    month_col=("月份",),
    fuel_col_map={
        "火电装机": "cap_thermal_full",
        "风电装机": "cap_wind_full",
        "太阳能装机": "cap_solar_full",
        "水电装机": "cap_hydro_full",
        "核电装机": "cap_nuclear_full",
        "火电发电量": "gen_thermal_full",
        "风电发电量": "gen_wind_full",
        "太阳能发电量": "gen_solar_full",
        "水电发电量": "gen_hydro_full",
        "核电发电量": "gen_nuclear_full",
    },
    cumulative_gen=False,                        # 山东月报为当月值
)

SHANDONG_DEMAND = MonthRowsConfig(
    province="山东",
    sheet_select="01_用电与发电概况",
    header_markers=("月份", "全社会用电量"),
    month_col=("月份",),
    fuel_col_map={
        "全社会用电量": "demand_total",
        "同比": "demand_yoy",
    },
)

shandong_parser = MonthRowsParser([SHANDONG_CAP_GEN, SHANDONG_DEMAND])


# ── 山西内建配置 (格式图 §5, 对 fixture 实勘) ─────────────────────────────────
# datetime 月份单元格; 表头 R1 (无标题行). 发电量当月/累计双表 — 只取当月表
# (月度发电量), 累计表不解析 (避免同 metric 双写). 月度水电 2026-04 = −2.53
# (累计重述倒挤), 原样出.

SHANXI_CAP = MonthRowsConfig(
    province="山西",
    sheet_select="装机容量",
    header_markers=("月份", "常规火电", "风电"),
    month_col=("月份",),
    fuel_col_map={
        "总装机": "cap_total_full",
        "常规火电": "cap_thermal_full",
        "水电": "cap_hydro_full",
        "风电": "cap_wind_full",
        "光伏": "cap_solar_full",
        "储能": "cap_storage_full",
        # 抽水蓄能/燃气/高炉尾气 不在六燃料词表, 暂不映射 (加一行即可扩展)
    },
    cumulative_gen=False,
)

SHANXI_GEN = MonthRowsConfig(
    province="山西",
    sheet_select="月度发电量",
    header_markers=("月份", "月度总发电量", "火电"),
    month_col=("月份",),
    fuel_col_map={
        "月度总发电量": "gen_total_full",
        "火电": "gen_thermal_full",
        "水电": "gen_hydro_full",
        "风电": "gen_wind_full",
        "光伏": "gen_solar_full",
        "储能": "gen_storage_full",
    },
    cumulative_gen=False,                        # 当月表直取; 累计表不解析
)

shanxi_parser = MonthRowsParser([SHANXI_CAP, SHANXI_GEN])


# ── 陕西内建配置 (格式图 §6, 对 fixture 实勘) ─────────────────────────────────
# 表头行漂移: 装机规模/全网发电量 R3, 全社会用电 R2 (扫描窗 R1..R6 覆盖).
# 装机规模 2025-04~08 空行 (跳过不计数以外处理); R44 备注说明行 (月份列不可解析,
# 自动跳过); 2026-05 断行搁浅在 R45 备注之下 — _collect 不停于首个空行, 照收.
# 同比列数值即百分数 (非江苏的小数分数). "—" em-dash 经 _num → None.

SHAANXI_CAP = MonthRowsConfig(
    province="陕西",
    sheet_select="装机规模",
    header_markers=("月份", "水电", "火电", "新型储能"),
    month_col=("月份",),
    fuel_col_map={
        "水电": "cap_hydro_full",
        "火电": "cap_thermal_full",
        "风电": "cap_wind_full",
        "太阳能": "cap_solar_full",
        "新型储能": "cap_storage_full",
        "合计": "cap_total_full",
    },
    cumulative_gen=False,
)

SHAANXI_GEN = MonthRowsConfig(
    province="陕西",
    sheet_select="全网发电量",
    header_markers=("月份", "火电电量", "合计电量"),
    month_col=("月份",),
    fuel_col_map={
        "火电电量": "gen_thermal_full",
        "风电电量": "gen_wind_full",
        "光伏电量": "gen_solar_full",
        "水电电量": "gen_hydro_full",
        "合计电量": "gen_total_full",
        "火电同比%": "gen_thermal_full_yoy",
        "风电同比%": "gen_wind_full_yoy",
        "光伏同比%": "gen_solar_full_yoy",
        "水电同比%": "gen_hydro_full_yoy",
        "合计同比%": "gen_total_full_yoy",
        # 其他电量 不在六燃料词表, 不映射
    },
    cumulative_gen=False,
)

SHAANXI_DEMAND = MonthRowsConfig(
    province="陕西",
    sheet_select="全社会用电",
    header_markers=("月份", "第一产业电量", "合计电量"),
    month_col=("月份",),
    fuel_col_map={
        "合计电量": "demand_total",
        "合计同比%": "demand_yoy",
    },
    cumulative_gen=False,
)

shaanxi_parser = MonthRowsParser([SHAANXI_CAP, SHAANXI_GEN, SHAANXI_DEMAND])


# ── 蒙西内建配置 (格式图 §3, 对 fixture 实勘) ─────────────────────────────────
# 行预留 2022-01→2026-05, 2025-03 及之前全空 (整行跳过); 2025-04/05 仅风电有值
# (其余出 None, 绝不写 0); 2025-06 起全填. 光伏(含光热)/储能(含配储) 括号为
# 限定语非单位 → ignore_notes, 否则单位闸门误杀整列. province 恒为 "蒙西"
# (cfg.province 决定, sheet 内容如 "内蒙古" 字样不影响).

MENGXI_CAP = MonthRowsConfig(
    province="蒙西",
    sheet_select="1_电网概况",
    header_markers=("月份", "总装机", "火电", "储能"),
    month_col=("月份",),
    fuel_col_map={
        "总装机": "cap_total_full",
        "火电": "cap_thermal_full",
        "水电": "cap_hydro_full",
        "风电": "cap_wind_full",
        "光伏": "cap_solar_full",
        "储能": "cap_storage_full",
        # 环比变化(总装机) 不映射
    },
    cumulative_gen=False,
    ignore_notes=("含光热", "含配储"),
)

mengxi_parser = MonthRowsParser([MENGXI_CAP])


# ── 辽宁内建配置 (格式图 §7, 对 fixture 实勘) ─────────────────────────────────
# 总量版: 无分燃料列 (按设计). 只出总量 metric; 全社会最大电力 无标准 metric
# 词表对应 (且单位为 万kW 与 demand 默认单位冲突), 不映射. 2025-05 全空行跳过.

LIAONING_OVERVIEW = MonthRowsConfig(
    province="辽宁",
    sheet_select="月度概况",
    header_markers=("月份", "全社会用电量", "全省发电量"),
    month_col=("月份",),
    fuel_col_map={
        "全社会用电量": "demand_total",
        "用电量同比": "demand_yoy",
        "全省发电量": "gen_total_full",
        "发电量同比": "gen_total_full_yoy",
        "发电装机容量": "cap_total_full",
    },
    cumulative_gen=False,
)

liaoning_parser = MonthRowsParser([LIAONING_OVERVIEW])


# ── internals ─────────────────────────────────────────────────────────────────

def _sheet_label(cfg: MonthRowsConfig) -> str:
    return cfg.sheet_select if isinstance(cfg.sheet_select, str) else "<pred>"


def _sheet_match(cfg: MonthRowsConfig, name: str) -> bool:
    if isinstance(cfg.sheet_select, str):
        return name == cfg.sheet_select
    return bool(cfg.sheet_select(name))


def _read_sheet(path: Path, cfg: MonthRowsConfig):
    """返回 (表头行, 数据行列表). 找不到 sheet/表头抛 ValueError."""
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = next((n for n in wb.sheetnames if _sheet_match(cfg, n)), None)
        if sheet is None:
            raise ValueError(f"{path}: no sheet matching {_sheet_label(cfg)!r}")
        ws = wb[sheet]
        header = None
        data: list[tuple] = []
        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            if header is None:
                if _is_header(row, cfg):
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


def _is_header(row, cfg: MonthRowsConfig) -> bool:
    cells = [_norm_header(c) for c in row if c is not None]
    return all(any(mark in c for c in cells) for mark in cfg.header_markers)


def _header_unit_note(v) -> str | None:
    """表头第一个 "(…)" 注记, 剥空白; 无 → None."""
    m = _UNIT_NOTE_CAPTURE_RE.search(str(v))
    if not m:
        return None
    return _WS_RE.sub("", m.group(1))


def _default_unit(metric: str) -> str:
    if metric.endswith("_yoy"):
        return "%"
    if metric.startswith("cap_"):
        return "万千瓦"
    if metric.startswith("gen_"):
        return "亿千瓦时"
    if metric.startswith("hours_"):
        return "小时"
    if metric.startswith("demand_"):
        return "亿千瓦时"
    raise ValueError(f"no default unit for metric {metric!r}")


def _section_of(metric: str) -> str:
    if metric.startswith("cap_"):
        return "capacity"
    if metric.startswith("gen_"):
        return "generation"
    if metric.startswith("hours_"):
        return "util_hours"
    if metric.startswith("demand_"):
        return "demand"
    raise ValueError(f"no section for metric {metric!r}")


def _map_columns(header, cfg: MonthRowsConfig):
    """表头按名称匹配列, 不按下标.
    返回 (月份列下标, {列下标: (metric, unit)}, {被单位闸门拦下的表头名})."""
    month_col = None
    colmap: dict[int, tuple[str, str]] = {}
    bad_units: set[str] = set()
    for idx, h in enumerate(header):
        if h is None:
            continue
        n = _norm_header(h)
        if month_col is None and n in cfg.month_col:
            month_col = idx
            continue
        metric = cfg.fuel_col_map.get(n)
        if metric is None:
            continue                            # 占比等未映射列: 忽略
        unit = cfg.unit_map.get(metric) or _default_unit(metric)
        note = _header_unit_note(h)
        if note is not None and note in cfg.ignore_notes:
            note = None                         # 限定语注记 (非单位): 视同无注记
        if note is not None:
            canonical = _UNIT_NOTES.get(note)
            if canonical is None or canonical != unit:
                bad_units.add(n)                # 单位不认可/冲突: 整列跳过
                continue
        colmap[idx] = (metric, unit)
    return month_col, colmap, bad_units


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


def _collect(data, month_col, colmap, cfg: MonthRowsConfig):
    """月份 → {metric: (原值, unit)}. 空行/NaT/全空行跳过并计数; skip_labels 不计."""
    series: dict[date, dict[str, tuple[float | None, str]]] = {}
    skipped = 0
    skip_labels = set(cfg.skip_labels)
    for row in data:
        raw = row[month_col] if month_col < len(row) else None
        if isinstance(raw, str) and raw.strip() in skip_labels:
            continue
        m = _parse_month(raw)
        if m is None:
            skipped += 1
            continue
        vals: dict[str, tuple[float | None, str]] = {}
        present = 0
        for idx, (metric, unit) in colmap.items():
            v = _num(row[idx] if idx < len(row) else None)
            vals[metric] = (v, unit)
            if v is not None:
                present += 1
        if present == 0:
            skipped += 1
            continue
        series[m] = vals                        # 同月重复行: 后写覆盖
    return series, skipped


def _emit(series, cfg: MonthRowsConfig, source: str) -> list[FundRow]:
    out: list[FundRow] = []
    for month in sorted(series):
        vals = series[month]
        for metric in sorted(vals):
            cur, unit = vals[metric]
            section = _section_of(metric)
            if cfg.cumulative_gen and section == "generation" and month.month != 1:
                prev = series.get(date(month.year, month.month - 1, 1),
                                  {}).get(metric, (None, unit))[0]
                value = (cur - prev) if (cur is not None and prev is not None) else None
                src = f"{source}{DIFF_TAG}"
            else:
                # capacity 期末直取; 1 月累计发电量 = 原值, 不得减上年 12 月
                value, src = cur, source
            out.append(FundRow(province=cfg.province, month=month, period="month",
                               section=section, metric=metric, value=value,
                               unit=unit, source=src))
    return out
