# services/province_fundamentals/parsers/summary_2025.py
"""2023-2025 全国汇总册年度解析器 — 2023-2025 全国各省电力市场基础信息汇总2026-03-30.xlsx.

解析策略 (controller 裁定: REUSE, don't reparse): 锚点解析全部复用
services/market_fundamentals/loader.py — 本 parser 只做 loader 输出 → FundRow
映射, 不重实现 需求13/需求14 锚点逻辑。loader 的 load_workbook_data(path)
是 load_province_data() 的去缓存取路径版 (同一函数体), fixture 测试走它,
不吃 live data/ 路径。

结构 (对源文件实勘验证, 2026-11-03):
  37 个 sheet, 每省一个 (数字前缀命名: 1北京 / 3.1冀北 / 12安徽)。
  loader 的 _sheet_province 跳过 14江西-暂未上线 + 陕西旧/甘肃旧/青海旧/宁夏旧
  共 5 个 sheet → 解析 32 省 (江西全省缺席, 4 个"旧"sheet 有 27/28/29/30
  新 sheet 顶替, 无省损失)。
  年份: loader 位置约定 — 需求13/需求14 锚点按列排序第 i 块 → 2024+i,
  只出 2024/2025 两年 (全册 0 省有 2023, loader 不产出)。
  总计: 册内无 总计/合计 燃料行; 总计以转置单元格零星出现 (北京 2024 block:
  数值 1408 在类型列, 标签 全省装机量（总装机容量）在数值列), 多数省块空缺。
  核验: 北京 2024 六燃料加总 24+130+1153+101 = 1408 = 陈述总计 →
  cap_total_full / gen_total_full 一律按六燃料加总派生, 任一分项非空才出,
  不去读转置总计格 (零星且布局不一, 读它=重实现锚点解析)。
  vintage 警示 (实勘, 按 loader 原样透传不在 parser 层修补):
  安徽 block1 统计截止 2025-03-31 (Q1 快照, 非 2024 年末), loader 位置约定
  仍记 2024; 安徽 需求14 两年数值逐值相同 (源册 2025 块照抄 2024)。

口径: 全册全口径 → 一律 _full 后缀 (cap_*_full / gen_*_full, 与
jiaojie_2025/format_b 全口径约定一致; 储能 → cap_storage_full /
gen_storage_full)。单位固定 万千瓦 (装机) / 亿千瓦时 (发电量)。
全部行 period='year', month=date(YYYY,1,1), source=文件名。
空单元格 / '-' 占位 → loader 产出 value=None → 跳过并计数 (年度表空格即缺报,
同 jiaojie 年度约定, 不出 value=None 行)。
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from openpyxl import load_workbook

from services.market_fundamentals.loader import (
    PROVINCE_EN,
    _find_anchors,
    _load_sheet,
    _sheet_province,
    load_workbook_data,
)
from services.province_fundamentals.models import FundRow

log = logging.getLogger(__name__)

FORMAT_NAME = "summary_2025"

_UNIT_CAP = "万千瓦"
_UNIT_GEN = "亿千瓦时"

# 燃料中文名 → metric stem (loader FUEL_TYPES_CN 词表; 总计不在其中, 派生见上)
_FUEL_STEM = {"火电": "thermal", "水电": "hydro", "核电": "nuclear",
              "风电": "wind", "光伏": "solar", "储能": "storage"}
_SECTION_PREFIX = {"capacity": "cap", "generation": "gen"}
_SECTION_UNIT = {"capacity": _UNIT_CAP, "generation": _UNIT_GEN}

# 省份白名单 = loader 自己的省词表 (33 省含 冀北/冀南/蒙西/蒙东) — 单一事实源,
# 不复制 jiaojie 的 _CANON。loader 产出的 sheet 名不在词表 → 跳过并计数, 不猜映射。
_PROVINCES = frozenset(PROVINCE_EN)


def parse_summary_2025(path) -> list[FundRow]:
    """解析全国汇总册 → FundRow 年度行 (period='year', month=YYYY-01-01)."""
    path = Path(path)
    source = path.name
    data = load_workbook_data(path)
    out: list[FundRow] = []
    skip_provs: dict[str, int] = {}
    skip_empty = 0
    parsed_provs = 0
    for prov in sorted(data):
        if prov not in _PROVINCES:
            skip_provs[prov] = skip_provs.get(prov, 0) + 1
            continue
        parsed_provs += 1
        info = data[prov]
        for section in ("capacity", "generation"):
            prefix = _SECTION_PREFIX[section]
            unit = _SECTION_UNIT[section]
            for year, fuels in sorted(info[section].items()):
                comp_sum = 0.0
                emitted = 0
                for fuel, stem in _FUEL_STEM.items():
                    v = fuels.get(fuel, {}).get("value")
                    if v is None:
                        continue
                    comp_sum += v
                    out.append(FundRow(province=prov, month=date(year, 1, 1),
                                       period="year", section=section,
                                       metric=f"{prefix}_{stem}_full",
                                       value=v, unit=unit, source=source))
                    emitted += 1
                if emitted:
                    # 总计行 = 六燃料加总 (北京 2024 核验等于册内陈述总计 1408)
                    out.append(FundRow(province=prov, month=date(year, 1, 1),
                                       period="year", section=section,
                                       metric=f"{prefix}_total_full",
                                       value=comp_sum, unit=unit,
                                       source=source))
                else:
                    skip_empty += 1
    if parsed_provs == 0:
        raise ValueError(f"{path}: no known province sheets (summary_2025)")
    if skip_provs or skip_empty:
        log.info("%s: skipped %d empty province-year blocks; "
                 "unmapped sheet names %s",
                 source, skip_empty, dict(sorted(skip_provs.items())))
    out.sort(key=lambda r: (r.province, r.month, r.section, r.metric))
    return out


# ── registry 鸭子类型 (全国册注册, 同 jiaojie_2025 通道: 路径无省份名时由
#    national parser 补探; province 形参为协议凑位, 本册自带 32 省 sheet, 忽略之) ──

def sniff(path) -> bool:
    """结构嗅探: ≥10 个 sheet 剥数字前缀后命中 loader 省词表, 且首个命中 sheet
    含 需求13 + 需求14 锚点。永不看文件名。任何读取失败都视为不命中。"""
    try:
        wb = load_workbook(Path(path), read_only=True)
        try:
            hits = [s for s in wb.sheetnames
                    if _sheet_province(s) in _PROVINCES]
            if len(hits) < 10:
                return False
            data = _load_sheet(wb[hits[0]])
            return bool(_find_anchors(data, "需求13")
                        and _find_anchors(data, "需求14"))
        finally:
            wb.close()
    except Exception:
        log.debug("summary_2025 sniff failed: %s", path, exc_info=True)
        return False


def parse(path, province: str | None = None) -> list[FundRow]:
    return parse_summary_2025(path)
