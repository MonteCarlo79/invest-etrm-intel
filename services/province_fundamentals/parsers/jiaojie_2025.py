# services/province_fundamentals/parsers/jiaojie_2025.py
"""交接表 2025 年度解析器 — 2025年1-12月各省分类型利用小时.xlsx (单文件多 sheet).

结构 (对源文件实勘验证, 2026-11-02):
  sheet `2025各省年度利用小时数`: 表头 区域|省份|类型|利用小时数|利用小时数同比变化|备注,
    省×类型长表。同比变化列单位是**小时** (如 安徽总计 -389 小时), 不是 %。
    四川同一类型有 全口径/省调 双行 (备注列区分) — 只取全口径行, 省调行跳过并计数。
  sheet `全社会用电量`: 表头 省份|用电类别|用电量(亿千瓦时)|占比(%) — 源文件仅广东一省;
    其中：工业 是第二产业子项, 跳过; 占比列不在词表, 跳过。
  sheet `全国发电量`: 两行表头 (组行: 截至2025年12月累计发电量 / 截至2025年12月装机
    (单位：万千瓦) / 年度利用小时数; 子行: 火电/水电/风电/太阳能或光伏/核电/储能),
    31 省行。装机/发电量/年度小时均为全口径 → 一律 `_full` 后缀
    (cap_*_full / gen_*_full / hours_*_full; 与格式 B 全口径约定一致,
    gen_*_full 不与格式 A 省调口径 gen_* 撞键)。储能小时不在 metric 词表, 跳过
    (cap_storage_full 保留: cap_storage 在词表)。
  sheet `2025各省分类型年度利用小时数` 不解析: 其 利用小时数/同比变化 与 sheet1
    逐值一致 (抽查 山东/安徽/甘肃), 仅多 浙江 一省; 其额外 装机/发电量 列不在本
    parser 范围 (且 发电量 列有可疑值, 如 山东 591.44)。选择记录于 task-4 报告。
  分省月度 sheet (广东/广西/…) 大多空白 (待查询省份 tracker) — 不解析, 出范围。

省份规范化: 剥 省/市/自治区 类后缀后须命中白名单 (含 冀南/冀北/蒙东/蒙西);
  河北省→河北 不在白名单 (河北南网/冀北 归属不定) → 跳过; 其余未命中名跳过并计数,
  结束 log 一行。全部行 period='year', month=date(2025,1,1), source=文件名。
空单元格 / 占位符 (-, —) → 跳过并计数, 不出 value=None 行 (年度表空格即缺报)。
"""
from __future__ import annotations

import logging
import math
import re
from datetime import date
from pathlib import Path

from openpyxl import load_workbook

from services.province_fundamentals.models import FundRow

log = logging.getLogger(__name__)

YEAR = date(2025, 1, 1)
SHEET_HOURS = "2025各省年度利用小时数"
SHEET_DEMAND = "全社会用电量"
SHEET_GEN = "全国发电量"

_UNIT_HOUR = "小时"
_UNIT_CAP = "万千瓦"
_UNIT_GEN = "亿千瓦时"

# 省份白名单 (剥后缀后)。河北 不在其中: 本表 河北省 无法归属 河北南网/冀北 → 跳过。
_CANON = frozenset({
    "北京", "天津", "山西", "内蒙古", "辽宁", "吉林", "黑龙江", "上海", "江苏",
    "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南", "广东",
    "广西", "海南", "重庆", "四川", "贵州", "云南", "西藏", "陕西", "甘肃",
    "青海", "宁夏", "新疆", "冀南", "冀北", "蒙东", "蒙西",
})
_PROV_SUFFIX_RE = re.compile(r"(维吾尔自治区|壮族自治区|回族自治区|自治区|省|市)\s*$")

# 利用小时 sheet 类型映射 (规范化后精确匹配; 只取最粗口径, 子项/口径行不猜映射):
#   合计/全省平均→total; 太阳能/太阳能发电/太阳→solar (光伏列惯用写法)。
# 跳过 (报告列出): 煤电/燃煤/燃气/燃气发电/常规火电/火电（含自备）(火电子项),
#   集中式光伏/分布式光伏/光伏（集中式）/光伏（含光热）(光伏子项),
#   储能/新型储能/储能（含配储）/其他（储能）, 抽水蓄能, 垃圾/垃圾发电/农林生物质发电/
#   高炉尾气/其他/其它, 其中:*, 统调机组（含自备）/非统调机组/省调机组平均/
#   全口径装机/总计（统调）(口径注记行)。
_HOURS_TYPE_MAP = {
    "总计": "total", "合计": "total", "全省平均": "total",
    "火电": "thermal", "水电": "hydro", "核电": "nuclear", "风电": "wind",
    "光伏": "solar", "太阳能": "solar", "太阳能发电": "solar", "太阳": "solar",
}

# 全社会用电量 sheet 用电类别映射 (规范化: 剥空白 + 括号注记 + 其中：前缀)
_DEMAND_MAP = {
    "全社会用电量": "demand_total",      # 原文 "全社会用电量（合计）"
    "第一产业": "demand_p1",
    "第二产业": "demand_p2",
    "第三产业": "demand_p3",
    "城乡居民生活用电": "demand_residential",
}

# 全国发电量 sheet 子表头 → stem (按组行分 section)
_GEN_SUB = {"总发电量": "total", "火电": "thermal", "水电": "hydro",
            "风电": "wind", "太阳能": "solar", "核电": "nuclear"}
_CAP_SUB = {"合计": "total", "火电": "thermal", "水电": "hydro",
            "风电": "wind", "光伏": "solar", "核电": "nuclear", "储能": "storage"}
_HOURS_SUB = {"总计": "total", "火电": "thermal", "水电": "hydro",
              "风电": "wind", "光伏": "solar", "核电": "nuclear"}   # 储能小时跳过

_UNIT_NOTE_RE = re.compile(r"[(（\[【].*?[)）\]】]")
_WS_RE = re.compile(r"\s+")
_QIZHONG_RE = re.compile(r"^其中\s*[:：]")


def parse_jiaojie(path) -> list[FundRow]:
    """解析交接表三个年度 sheet → FundRow 列表 (period='year', month=YYYY-01-01)."""
    path = Path(path)
    source = path.name
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        for sheet in (SHEET_HOURS, SHEET_DEMAND, SHEET_GEN):
            if sheet not in wb.sheetnames:
                raise ValueError(f"{path}: missing sheet {sheet!r}")
        out: list[FundRow] = []
        out += _parse_hours_sheet(wb[SHEET_HOURS], source)
        out += _parse_demand_sheet(wb[SHEET_DEMAND], source)
        out += _parse_gen_sheet(wb[SHEET_GEN], source)
    finally:
        wb.close()
    out.sort(key=lambda r: (r.province, r.section, r.metric))
    return out


# ── sheet: 2025各省年度利用小时数 ─────────────────────────────────────────────

def _locate_hours_header(ws):
    """定位表头, 返回 (列下标 dict, 数据行迭代器)."""
    rows = ws.iter_rows(values_only=True)
    for i, row in enumerate(rows, start=1):
        cells = {_norm(c) for c in row if c is not None}
        if {"省份", "类型", "利用小时数"} <= cells:
            cols = {}
            for idx, c in enumerate(row):
                n = _norm(c) if c is not None else ""
                if n == "省份":
                    cols["province"] = idx
                elif n == "类型":
                    cols["type"] = idx
                elif n == "利用小时数":
                    cols["hours"] = idx
                elif n == "利用小时数同比变化":
                    cols["yoy"] = idx
                elif n == "备注":
                    cols["note"] = idx
            return cols, rows
        if i >= 6:
            break
    raise ValueError(f"{ws.title}: header row (省份/类型/利用小时数) not found")


def _parse_hours_sheet(ws, source: str) -> list[FundRow]:
    cols, data = _locate_hours_header(ws)
    out: list[FundRow] = []
    skip_empty = skip_note = 0
    skip_types: dict[str, int] = {}
    skip_provs: dict[str, int] = {}
    for row in data:
        prov = _cell_text(row, cols.get("province"))
        typ = _cell_text(row, cols.get("type"))
        if prov is None and typ is None:
            skip_empty += 1
            continue
        province = _norm_province(prov)
        if province is None:
            skip_provs[prov or "?"] = skip_provs.get(prov or "?", 0) + 1
            continue
        stem = _HOURS_TYPE_MAP.get(_norm_type(typ))
        if stem is None:
            skip_types[typ or "?"] = skip_types.get(typ or "?", 0) + 1
            continue
        note = _cell_text(row, cols.get("note")) or ""
        if "省调" in note:
            skip_note += 1                       # 四川 省调行: 与全口径双口径不并取
            continue
        hours = _num(_cell_raw(row, cols.get("hours")))
        yoy = _num(_cell_raw(row, cols.get("yoy")))
        if hours is None and yoy is None:
            skip_empty += 1
            continue
        if hours is not None:
            out.append(FundRow(province=province, month=YEAR, period="year",
                               section="util_hours", metric=f"hours_{stem}",
                               value=hours, unit=_UNIT_HOUR, source=source))
        if yoy is not None:
            # 同比变化列单位是小时 (安徽总计 -389 小时), 不是 %
            out.append(FundRow(province=province, month=YEAR, period="year",
                               section="util_hours", metric=f"hours_{stem}_yoy",
                               value=yoy, unit=_UNIT_HOUR, source=source))
    _log_skips(source, ws.title, skip_empty, skip_types, skip_provs, skip_note)
    return out


# ── sheet: 全社会用电量 ────────────────────────────────────────────────────────

def _parse_demand_sheet(ws, source: str) -> list[FundRow]:
    rows = ws.iter_rows(values_only=True)
    cols = None
    out: list[FundRow] = []
    skip_empty = 0
    skip_labels: dict[str, int] = {}
    skip_provs: dict[str, int] = {}
    for i, row in enumerate(rows, start=1):
        if cols is None:
            cells = {_norm(c) for c in row if c is not None}
            if "省份" in cells and "用电类别" in cells:
                cols = {}
                for idx, c in enumerate(row):
                    n = _norm(c) if c is not None else ""
                    if n == "省份":
                        cols["province"] = idx
                    elif n == "用电类别":
                        cols["label"] = idx
                    elif n.startswith("用电量"):
                        cols["value"] = idx
                if "value" not in cols:
                    raise ValueError(f"{ws.title}: header lacks 用电量 column")
            elif i >= 6:
                raise ValueError(f"{ws.title}: header row (省份/用电类别) not found")
            continue
        prov = _cell_text(row, cols["province"])
        label = _cell_text(row, cols["label"])
        if prov is None and label is None:
            skip_empty += 1
            continue
        province = _norm_province(prov)
        if province is None:
            skip_provs[prov or "?"] = skip_provs.get(prov or "?", 0) + 1
            continue
        metric = _DEMAND_MAP.get(_norm_label(label))
        if metric is None:                       # 其中：工业 等子项
            skip_labels[label or "?"] = skip_labels.get(label or "?", 0) + 1
            continue
        value = _num(_cell_raw(row, cols["value"]))
        if value is None:
            skip_empty += 1
            continue
        out.append(FundRow(province=province, month=YEAR, period="year",
                           section="demand", metric=metric, value=value,
                           unit=_UNIT_GEN, source=source))
    if cols is None:
        raise ValueError(f"{ws.title}: header row (省份/用电类别) not found")
    _log_skips(source, ws.title, skip_empty, skip_labels, skip_provs, 0)
    return out


# ── sheet: 全国发电量 (两行表头: 组行 + 子行) ──────────────────────────────────

def _parse_gen_sheet(ws, source: str) -> list[FundRow]:
    rows = ws.iter_rows(values_only=True)
    colmap: dict[int, tuple[str, str, str]] = {}  # idx -> (section, metric, unit)
    out: list[FundRow] = []
    skip_empty = 0
    skip_provs: dict[str, int] = {}
    data_started = False
    for i, row in enumerate(rows, start=1):
        if not data_started:
            if _cell_text(row, 0) == "地区":
                sub = next(rows, None)             # 子表头行
                if sub is None:
                    break
                colmap = _map_gen_columns(row, sub)
                if not colmap:
                    raise ValueError(f"{ws.title}: no known metric columns")
                data_started = True
            elif i >= 6:
                raise ValueError(f"{ws.title}: header row (地区) not found")
            continue
        prov = _cell_text(row, 0)
        if prov is None:
            skip_empty += 1
            continue
        province = _norm_province(prov)
        if province is None:
            skip_provs[prov] = skip_provs.get(prov, 0) + 1
            continue
        emitted = 0
        for idx, (section, metric, unit) in colmap.items():
            v = _num(row[idx] if idx < len(row) else None)
            if v is None:
                continue
            out.append(FundRow(province=province, month=YEAR, period="year",
                               section=section, metric=metric, value=v,
                               unit=unit, source=source))
            emitted += 1
        if emitted == 0:
            skip_empty += 1
    if not data_started:
        raise ValueError(f"{ws.title}: header row (地区) not found")
    _log_skips(source, ws.title, skip_empty, {}, skip_provs, 0)
    return out


def _map_gen_columns(group_row, sub_row) -> dict[int, tuple[str, str, str]]:
    """组行 (前向填充) + 子行 → {列下标: (section, metric, unit)}."""
    colmap: dict[int, tuple[str, str, str]] = {}
    group = ""
    for idx in range(max(len(group_row), len(sub_row))):
        g = group_row[idx] if idx < len(group_row) else None
        if g is not None:
            group = _WS_RE.sub("", str(g))
        sub = _norm(sub_row[idx]) if idx < len(sub_row) else ""
        if "装机" in group:
            stem = _CAP_SUB.get(sub)
            if stem:
                colmap[idx] = ("capacity", f"cap_{stem}_full", _UNIT_CAP)
        elif "发电量" in group:
            stem = _GEN_SUB.get(sub)
            if stem:
                colmap[idx] = ("generation", f"gen_{stem}_full", _UNIT_GEN)
        elif "利用小时" in group:
            stem = _HOURS_SUB.get(sub)
            if stem:
                colmap[idx] = ("util_hours", f"hours_{stem}_full", _UNIT_HOUR)
    return colmap


# ── helpers (与 format_a/format_b 同款小工具, 按 controller 裁定就地复制) ──────

def _norm(v) -> str:
    """表头规范化: 剥空白 + 括号注记."""
    s = _WS_RE.sub("", str(v))
    return _UNIT_NOTE_RE.sub("", s)


def _norm_type(v) -> str:
    """利用小时类型规范化: 只剥空白, 保留括号 — 括号是口径限定
    (火电（含自备）≠火电, 总计（统调）≠总计, 光伏（集中式）≠光伏), 剥了就是猜映射."""
    return _WS_RE.sub("", str(v))


def _norm_label(v) -> str:
    """用电类别规范化: 剥空白 + 括号注记 (全社会用电量（合计）→全社会用电量)
    + 其中：前缀 (其中：工业→工业, 仍不在词表 → 跳过)."""
    return _QIZHONG_RE.sub("", _norm(v))


def _norm_province(name) -> str | None:
    """剥 省/市/自治区 类后缀, 命中白名单才返回; 否则 None (不猜映射)."""
    if name is None:
        return None
    s = _WS_RE.sub("", str(name))
    s = _PROV_SUFFIX_RE.sub("", s)
    return s if s in _CANON else None


def _cell_raw(row, idx):
    return row[idx] if idx is not None and idx < len(row) else None


def _cell_text(row, idx) -> str | None:
    v = _cell_raw(row, idx)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


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


def _log_skips(source, sheet, empty, types, provs, note) -> None:
    if empty or types or provs or note:
        log.info("%s[%s]: skipped %d empty, %d 省调-note rows; "
                 "unmapped types %s; unmapped provinces %s",
                 source, sheet, empty, note,
                 dict(sorted(types.items())), dict(sorted(provs.items())))
