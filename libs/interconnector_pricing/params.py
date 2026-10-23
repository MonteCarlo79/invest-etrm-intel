"""Interconnector (跨省跨区) green-power landed-cost pricing — 4th-cycle parameter tables.

Verbatim port of models/【卡师傅27】跨省绿电落地测算-第四周期.xlsx
(sheets 送出省价格 / 区域电网价格 / 专项工程价格 / 路径库 / 四川外送特例 / 区域容量分摊).

Source document: 发改价格〔2026〕1077号 (filed 2026-07-22). All tariffs 元/MWh,
VAT-inclusive, excluding generation-node losses; blank in the source means
"not listed" — never treated as 0.

Stop conditions carried by these tables (engine enforces):
- 西藏 has no published tariff yet -> PROVINCE_GRID["西藏"]["priced"] is False.
- 辛洹线 is capacity-priced only (40 元/kW·年, no energy tariff) -> computable False.
- 云霄 has both an energy tariff and a capacity price (115 元/kW·年) with a
  restricted applicability (non-福建/广东 trades) -> computable False.
"""

CYCLE = "第四周期 (发改价格〔2026〕1077号, 2026-07-22)"

# 送出省价格 — 27 送出省 (PDF pp.5–6). tariff 元/MWh, loss 外送线损率.
PROVINCE_GRID = {
    "北京": {"tariff": 30.0, "loss": 0.0},
    "天津": {"tariff": 30.0, "loss": 0.0},
    "冀北": {"tariff": 23.8, "loss": 0.0094},
    "河北": {"tariff": 30.0, "loss": 0.0},
    "山西": {"tariff": 21.7, "loss": 0.0019},
    "山东": {"tariff": 30.0, "loss": 0.0},
    "上海": {"tariff": 30.0, "loss": 0.0},
    "江苏": {"tariff": 30.0, "loss": 0.0},
    "浙江": {"tariff": 30.0, "loss": 0.0},
    "安徽": {"tariff": 30.0, "loss": 0.0},
    "福建": {"tariff": 22.0, "loss": 0.011},
    "湖北": {"tariff": 32.8, "loss": 0.0082},
    "湖南": {"tariff": 24.0, "loss": 0.0088},
    "河南": {"tariff": 25.5, "loss": 0.0057},
    "江西": {"tariff": 30.0, "loss": 0.0},
    "四川": {"tariff": 35.0, "loss": 0.0099},
    "重庆": {"tariff": 21.6, "loss": 0.0085},
    "辽宁": {"tariff": 25.9, "loss": 0.0069},
    "吉林": {"tariff": 25.1, "loss": 0.0045},
    "黑龙江": {"tariff": 23.5, "loss": 0.0154},
    "蒙东": {"tariff": 26.8, "loss": 0.0071},
    "陕西": {"tariff": 34.8, "loss": 0.0059},
    "甘肃": {"tariff": 28.0, "loss": 0.0101},
    "青海": {"tariff": 33.9, "loss": 0.0139},
    "宁夏": {"tariff": 25.0, "loss": 0.005},
    "新疆": {"tariff": 34.3, "loss": 0.0027},
    # 尚未公布核价结果，后续按照相关政策执行。
    "西藏": {"priced": False},
}

# 区域电网价格 — 5 大区域 (PDF pp.3–5). 容量电价已通过省级输配电价回收，不重复叠加。
REGIONAL_GRID = {
    "华北": {"tariff": 10.8, "loss": 0.0215},
    "华东": {"tariff": 9.2, "loss": 0.0152},
    "华中（西南）": {"tariff": 25.6, "loss": 0.0232},
    "东北": {"tariff": 16.4, "loss": 0.0183},
    "西北": {"tariff": 12.1, "loss": 0.019},
}

_LISTED = "本表列示（未逐工程区分正式或临时核价）"
_FILED = "已行文报备"

# 专项工程价格 — 40 项 (PDF pp.2–3). tariff 元/MWh 含增值税、不含上网环节线损。
# 辛洹线: 单列容量电价 40 元/kW·年，无电量电价 -> 自动测算不支持。
# 云霄(直流): 电量价 25.6 + 容量价 115 元/kW·年，适用边界特殊 -> 自动测算暂不支持。
SPECIAL_PROJECTS = {
    "灵宝直流": {"tariff": 40.3, "loss": 0.01, "status": _LISTED, "pdf_page": 2, "computable": True},
    "德宝直流": {"tariff": 33.6, "loss": 0.03, "status": _LISTED, "pdf_page": 2, "computable": True},
    "锦苏直流": {"tariff": 51.1, "loss": 0.07, "status": _LISTED, "pdf_page": 2, "computable": True},
    "高岭直流": {"tariff": 23.5, "loss": 0.017, "status": _LISTED, "pdf_page": 2, "computable": True},
    "龙政直流": {"tariff": 67.5, "loss": 0.075, "status": _LISTED, "pdf_page": 2, "computable": True},
    "葛南直流": {"tariff": 55.8, "loss": 0.075, "status": _LISTED, "pdf_page": 2, "computable": True},
    "林枫直流": {"tariff": 43.9, "loss": 0.075, "status": _LISTED, "pdf_page": 2, "computable": True},
    "宜华直流": {"tariff": 68.5, "loss": 0.075, "status": _LISTED, "pdf_page": 2, "computable": True},
    "江城直流": {"tariff": 38.5, "loss": 0.0765, "status": _LISTED, "pdf_page": 2, "computable": True},
    "中俄直流": {"tariff": 37.1, "loss": 0.013, "status": _LISTED, "pdf_page": 2, "computable": True},
    "呼辽直流": {"tariff": 42.0, "loss": 0.0412, "status": _LISTED, "pdf_page": 2, "computable": True},
    "阳城送出": {"tariff": 20.7, "loss": 0.03, "status": _LISTED, "pdf_page": 2, "computable": True},
    "锦界送出": {"tariff": 18.1, "loss": 0.025, "status": _LISTED, "pdf_page": 2, "computable": True},
    "府谷送出": {"tariff": 14.5, "loss": 0.025, "status": _LISTED, "pdf_page": 2, "computable": True},
    "辛洹线": {"tariff": None, "loss": None, "status": _LISTED, "pdf_page": 2, "computable": False},
    "三峡送华中": {"tariff": 45.1, "loss": 0.007, "status": _LISTED, "pdf_page": 2, "computable": True},
    "长南荆特高压交流": {"tariff": 25.1, "loss": 0.015, "status": _LISTED, "pdf_page": 2, "computable": True},
    "天中直流": {"tariff": 61.3, "loss": 0.072, "status": _LISTED, "pdf_page": 2, "computable": True},
    "复奉直流": {"tariff": 57.1, "loss": 0.07, "status": _LISTED, "pdf_page": 2, "computable": True},
    "宾金直流": {"tariff": 45.4, "loss": 0.065, "status": _LISTED, "pdf_page": 2, "computable": True},
    "银东直流": {"tariff": 50.8, "loss": 0.07, "status": _LISTED, "pdf_page": 2, "computable": True},
    "灵绍直流": {"tariff": 48.8, "loss": 0.0426, "status": _LISTED, "pdf_page": 2, "computable": True},
    "祁韶直流": {"tariff": 63.7, "loss": 0.0414, "status": _LISTED, "pdf_page": 2, "computable": True},
    "鲁固直流": {"tariff": 41.2, "loss": 0.0269, "status": _LISTED, "pdf_page": 2, "computable": True},
    "锡泰直流": {"tariff": 48.3, "loss": 0.0332, "status": _LISTED, "pdf_page": 2, "computable": True},
    "雁淮直流": {"tariff": 35.9, "loss": 0.0277, "status": _LISTED, "pdf_page": 2, "computable": True},
    "吉泉直流": {"tariff": 82.9, "loss": 0.07, "status": _FILED, "pdf_page": 2, "computable": True},
    "昭沂直流": {"tariff": 58.95, "loss": 0.065, "status": _FILED, "pdf_page": 2, "computable": True},
    "青豫直流": {"tariff": 64.89, "loss": 0.0583, "status": _LISTED, "pdf_page": 2, "computable": True},
    "盂县送出": {"tariff": 8.1, "loss": 0.025, "status": _LISTED, "pdf_page": 2, "computable": True},
    "雅湖直流": {"tariff": 68.5, "loss": 0.06, "status": _LISTED, "pdf_page": 3, "computable": True},
    "陕武直流": {"tariff": 51.2, "loss": 0.05, "status": _LISTED, "pdf_page": 3, "computable": True},
    "建苏直流": {"tariff": 83.6, "loss": 0.06, "status": _LISTED, "pdf_page": 3, "computable": True},
    "金塘直流": {"tariff": 81.4, "loss": 0.06, "status": _LISTED, "pdf_page": 3, "computable": True},
    "金永直流": {"tariff": 82.41, "loss": 0.0562, "status": _FILED, "pdf_page": 3, "computable": True},
    "中衡直流": {"tariff": 73.45, "loss": 0.055, "status": _FILED, "pdf_page": 3, "computable": True},
    "坤渝直流": {"tariff": 74.74, "loss": 0.054, "status": _FILED, "pdf_page": 3, "computable": True},
    "庆东直流": {"tariff": 52.78, "loss": 0.055, "status": _FILED, "pdf_page": 3, "computable": True},
    "云霄": {"tariff": 25.6, "loss": 0.01515, "status": _LISTED, "pdf_page": 3, "computable": False},
    "宝合直流": {"tariff": 53.23, "loss": 0.05, "status": _FILED, "pdf_page": 3, "computable": True},
}

# 路径库 — 18 条预置组合，按实际计费顺序排列，每条最多 5 段。
# 省网段由送出省选择自动添加，不在此列。段标签格式：区域：<name> / 专项：<name>。
PATHS = {
    "西北—宝合—华东": ["区域：西北", "专项：宝合直流", "区域：华东"],
    "西北—灵绍—华东": ["区域：西北", "专项：灵绍直流", "区域：华东"],
    "灵绍—华东": ["专项：灵绍直流", "区域：华东"],
    "西北—银东—华北—雁淮—华东": ["区域：西北", "专项：银东直流", "区域：华北", "专项：雁淮直流", "区域：华东"],
    "银东—华北—雁淮—华东": ["专项：银东直流", "区域：华北", "专项：雁淮直流", "区域：华东"],
    "雁淮—华东": ["专项：雁淮直流", "区域：华东"],
    "西北—陕武—华中—葛南—华东": ["区域：西北", "专项：陕武直流", "区域：华中（西南）", "专项：葛南直流", "区域：华东"],
    "西北—陕武—华中—龙政—华东": ["区域：西北", "专项：陕武直流", "区域：华中（西南）", "专项：龙政直流", "区域：华东"],
    "西北—陕武—华中—宜华—华东": ["区域：西北", "专项：陕武直流", "区域：华中（西南）", "专项：宜华直流", "区域：华东"],
    "西北—陕武—华中—林枫—华东": ["区域：西北", "专项：陕武直流", "区域：华中（西南）", "专项：林枫直流", "区域：华东"],
    "西北—庆东—华北": ["区域：西北", "专项：庆东直流", "区域：华北"],
    "西北—庆东—华北—锡泰—华东": ["区域：西北", "专项：庆东直流", "区域：华北", "专项：锡泰直流", "区域：华东"],
    "青豫—华中—宜华—华东": ["专项：青豫直流", "区域：华中（西南）", "专项：宜华直流", "区域：华东"],
    "东北—高岭—华北—锡泰—华东": ["区域：东北", "专项：高岭直流", "区域：华北", "专项：锡泰直流", "区域：华东"],
    "吉泉—华东": ["专项：吉泉直流", "区域：华东"],
    "昭沂—华北—锡泰—华东": ["专项：昭沂直流", "区域：华北", "专项：锡泰直流", "区域：华东"],
    "华北区域": ["区域：华北"],
    "华东区域": ["区域：华东"],
    # v1 extension beyond Excel verbatim: 甘浙线 commissioned H2-2027; tariff from 金永直流 已行文报备
    "甘浙—华东": ["专项：金永直流", "区域：华东"],
}

# 四川外送特例 — 特定水电站年度电量上限与差异价。展示用；不自动套用，
# 使用时须先核对电站、目的省与年度累计电量。
SICHUAN_EXCEPTIONS = [
    {"station": "向家坝、溪洛渡左岸、白鹤滩", "destination": "依原文适用范围",
     "annual_cap_100m_kwh": 1044.0, "tariff": 23.2, "loss": 0.0,
     "scope": "年度合计1044亿千瓦时以内的外送电量"},
    {"station": "苏洼龙、巴塘", "destination": "依原文适用范围",
     "annual_cap_100m_kwh": 47.0, "tariff": 30.0, "loss": 0.0,
     "scope": "年度47亿千瓦时以内的外送电量"},
    {"station": "锦屏官地", "destination": "江苏",
     "annual_cap_100m_kwh": 301.0, "tariff": 30.4, "loss": 0.0,
     "scope": "年度301亿千瓦时以内的外送电量"},
    {"station": "锦屏官地", "destination": "重庆",
     "annual_cap_100m_kwh": 100.0, "tariff": 14.4, "loss": 0.0,
     "scope": "年度100亿千瓦时以内的外送电量"},
    {"station": "长海", "destination": "依原文适用范围",
     "annual_cap_100m_kwh": 64.0, "tariff": 30.0, "loss": 0.0,
     "scope": "年度64亿千瓦时以内的外送电量"},
    {"station": "二滩", "destination": "依原文适用范围",
     "annual_cap_100m_kwh": 46.0, "tariff": 43.0, "loss": 0.0,
     "scope": "年度46亿千瓦时以内的外送电量"},
]

# 区域容量分摊参考 — 27 行，单位已换算为 元/MWh（原表为 分/kWh）。
# 按省级电网销售电量收取，通过省级输配电价回收；展示用，绝不在路径电量费用中重复添加。
_CAP_NOTE = "按省级电网销售电量收取，通过省级输配电价回收；不是元/kW年容量费，不应在路径电量价外再次叠加。"
REGIONAL_CAPACITY_SHARE = [
    {"region": "华北", "province": "北京", "share": 13.5, "note": _CAP_NOTE},
    {"region": "华北", "province": "天津", "share": 13.5, "note": _CAP_NOTE},
    {"region": "华北", "province": "冀北", "share": 6.0, "note": _CAP_NOTE},
    {"region": "华北", "province": "河北", "share": 5.5, "note": _CAP_NOTE},
    {"region": "华北", "province": "山西", "share": 1.6, "note": _CAP_NOTE},
    {"region": "华北", "province": "山东", "share": 3.1, "note": _CAP_NOTE},
    {"region": "华东", "province": "上海", "share": 5.3, "note": _CAP_NOTE},
    {"region": "华东", "province": "江苏", "share": 2.7, "note": _CAP_NOTE},
    {"region": "华东", "province": "浙江", "share": 3.3, "note": _CAP_NOTE},
    {"region": "华东", "province": "安徽", "share": 3.0, "note": _CAP_NOTE},
    {"region": "华东", "province": "福建", "share": 1.3, "note": _CAP_NOTE},
    {"region": "华中（西南）", "province": "湖北", "share": 3.9, "note": _CAP_NOTE},
    {"region": "华中（西南）", "province": "湖南", "share": 4.1, "note": _CAP_NOTE},
    {"region": "华中（西南）", "province": "河南", "share": 2.5, "note": _CAP_NOTE},
    {"region": "华中（西南）", "province": "江西", "share": 2.3, "note": _CAP_NOTE},
    {"region": "华中（西南）", "province": "四川", "share": 1.6, "note": _CAP_NOTE},
    {"region": "华中（西南）", "province": "重庆", "share": 3.8, "note": _CAP_NOTE},
    {"region": "华中（西南）", "province": "西藏", "share": 2.4, "note": _CAP_NOTE},
    {"region": "东北", "province": "辽宁", "share": 4.8, "note": _CAP_NOTE},
    {"region": "东北", "province": "吉林", "share": 5.0, "note": _CAP_NOTE},
    {"region": "东北", "province": "黑龙江", "share": 4.4, "note": _CAP_NOTE},
    {"region": "东北", "province": "蒙东", "share": 1.7, "note": _CAP_NOTE},
    {"region": "西北", "province": "陕西", "share": 0.8, "note": _CAP_NOTE},
    {"region": "西北", "province": "甘肃", "share": 2.9, "note": _CAP_NOTE},
    {"region": "西北", "province": "青海", "share": 2.0, "note": _CAP_NOTE},
    {"region": "西北", "province": "宁夏", "share": 1.2, "note": _CAP_NOTE},
    {"region": "西北", "province": "新疆", "share": 0.9, "note": _CAP_NOTE},
]

# Profile-matched RT buckets (hours of day). None = flat average over all hours.
# Solar window aligned to the official 2026-2028 PPA contract curve (9–18点;
# Jul–Sep extended to 19点 — the +18 hour extension is applied per month via
# agreements.profile_hours_for_month, not in this base list).
PROFILE_BUCKETS = {
    "solar": [9, 10, 11, 12, 13, 14, 15, 16, 17],
    "wind": [0, 1, 2, 3, 4, 5, 19, 20, 21, 22, 23],
    "mixed": None,
    "hydro": None,
}

RESOURCE_MIXES = ("solar", "wind", "mixed", "hydro")


# ── Province → regional grid (for receiver-aware path filtering) ─────────────
# Derived from the price tables' own structure (5 regional grids + 送出省价格).
# 蒙西 mapped to 华北 (flagged in UI; 蒙西 is NOT a valid 送出省 in this model —
# absent from the 27 送出省价格 rows). 南方电网 receivers are unpriceable here:
# no 南方 regional price row exists in the 4th-cycle tables.
PROVINCE_TO_REGION: dict[str, str] = {
    "北京": "华北", "天津": "华北", "河北": "华北", "河北南网": "华北",
    "冀北": "华北", "山西": "华北", "山东": "华北", "蒙西": "华北",
    "上海": "华东", "江苏": "华东", "浙江": "华东", "安徽": "华东", "福建": "华东",
    "湖北": "华中（西南）", "湖南": "华中（西南）", "河南": "华中（西南）",
    "江西": "华中（西南）", "四川": "华中（西南）", "重庆": "华中（西南）",
    "辽宁": "东北", "吉林": "东北", "黑龙江": "东北", "蒙东": "东北",
    "陕西": "西北", "甘肃": "西北", "青海": "西北", "宁夏": "西北", "新疆": "西北",
}

UNPRICEABLE_RECEIVERS: tuple = ("广州", "广东", "广西", "云南", "贵州", "海南")

# Send region of 专项 when one leads a preset path (only those used as first
# segments are pinned; middle segments need no entry).
SPECIAL_SEND_REGION: dict[str, str] = {
    "灵绍直流": "西北", "银东直流": "西北", "雁淮直流": "华北", "青豫直流": "西北",
    "吉泉直流": "西北", "昭沂直流": "西北", "高岭直流": "东北", "庆东直流": "西北",
    "金永直流": "西北",
}


def path_regions(path_segments: list[str]) -> tuple[str, str]:
    """(start_region, end_region) of a preset path from its own segments."""
    first, last = path_segments[0], path_segments[-1]
    start = first.split("：", 1)[1] if first.startswith("区域：") else SPECIAL_SEND_REGION[first.split("：", 1)[1]]
    end = last.split("：", 1)[1]
    return start, end


def filter_paths(province: str, receiver: str) -> list[str]:
    """Preset paths whose start region == sender's and end region == receiver's."""
    send_r = PROVINCE_TO_REGION.get(province)
    recv_r = PROVINCE_TO_REGION.get(receiver)
    if not send_r or not recv_r:
        return []
    return [name for name, segs in PATHS.items()
            if path_regions(segs) == (send_r, recv_r)]


# ── Channel anchor provinces (clearing priority) ─────────────────────────────
# Anchor = province hosting the flow-origin converter station. Anchor-province
# flows clear ahead of intermediary provinces relaying through the same channel
# (e.g. 陕西 > 甘肃 on 宝合). 柴拉直流 is anchored at 西藏 for the 反向 outbound
# (拉萨端) PPA flow to 上海; its 正向 anchor is 青海 (柴达木).
CHANNEL_ANCHOR_PROVINCE: dict[str, str] = {
    "宝合直流": "陕西",
    "庆东直流": "甘肃",
    "灵绍直流": "宁夏",
    "雁淮直流": "山西",
    "银东直流": "宁夏",
    "陕武直流": "陕西",
    "青豫直流": "青海",
    "吉泉直流": "新疆",
    "高岭直流": "内蒙古",
    "锡泰直流": "内蒙古",
    "金永直流": "甘肃",
    "甘浙线": "甘肃",
    "柴拉直流": "西藏",
    "柴拉+宝合": "西藏",
}


# Canonical receiver province list: 上海 (main PPA destination, not a LingFeng
# market) + the 29 LingFeng markets. UI receiver selectors must use THIS list,
# never DB-distinct province values (which carry contaminated entities like
# 豫中东/豫北/豫南/豫西, 海南礼记/海南那悦, 甘肃西河).
RECEIVER_PROVINCES: list = [
    "上海",
    "河南", "新疆", "吉林", "海南", "湖北", "四川", "黑龙江", "福建", "浙江",
    "江苏", "广西", "安徽", "陕西", "贵州", "云南", "广东", "蒙东", "湖南",
    "宁夏", "辽宁", "河北南网", "甘肃", "蒙西", "山东", "山西", "冀北",
    "广州", "青海", "江西",
]
