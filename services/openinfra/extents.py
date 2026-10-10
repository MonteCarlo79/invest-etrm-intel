"""Province extents for the OpenInfraMap extraction.

Tag = extent_tag stored in staging.openinfra_*; label = Chinese display name.
Values are (lon_min, lon_max, lat_min, lat_max), kept in sync with
scripts/openinfra_preview_mp.py PROVINCES (the preview builder).
min_voltage: 330kV for the 西北 750-backbone provinces, 500kV elsewhere.
"""
from __future__ import annotations

EXTENTS: dict[str, dict] = {
    "mengxi":       {"label": "蒙西",   "extent": (100.0, 117.5, 37.0, 44.8)},
    "mengdong":     {"label": "蒙东",   "extent": (114.0, 126.5, 41.5, 53.5)},
    "guangxi":      {"label": "广西",   "extent": (104.0, 112.8, 20.4, 26.8)},
    "liaoning":     {"label": "辽宁",   "extent": (118.5, 126.0, 38.5, 44.0)},
    "shanxi":       {"label": "山西",   "extent": (110.0, 114.8, 34.0, 40.8)},
    "gansu":        {"label": "甘肃",   "extent": (92.0, 109.5, 32.0, 43.0), "min_voltage": 330000},
    "shandong":     {"label": "山东",   "extent": (114.5, 122.8, 34.3, 38.5)},
    "shaanxi":      {"label": "陕西",   "extent": (105.4, 111.3, 31.6, 39.6), "min_voltage": 330000},
    "jinan":        {"label": "冀南",   "extent": (112.8, 118.8, 35.8, 39.2)},
    "jibei":        {"label": "冀北",   "extent": (113.5, 120.2, 39.0, 43.2)},
    "heilongjiang": {"label": "黑龙江", "extent": (121.0, 135.5, 43.0, 53.8)},
    "jilin":        {"label": "吉林",   "extent": (121.0, 132.0, 40.0, 47.0)},
    "qinghai":      {"label": "青海",   "extent": (89.0, 104.0, 31.0, 39.5), "min_voltage": 330000},
    "ningxia":      {"label": "宁夏",   "extent": (104.0, 107.5, 35.0, 39.5), "min_voltage": 330000},
    "xinjiang":     {"label": "新疆",   "extent": (73.0, 97.0, 34.0, 50.0), "min_voltage": 330000},
    "henan":        {"label": "河南",   "extent": (110.0, 117.0, 31.5, 37.0)},
    "anhui":        {"label": "安徽",   "extent": (114.5, 120.0, 29.0, 34.5)},
    "zhejiang":     {"label": "浙江",   "extent": (118.0, 123.0, 27.0, 31.5)},
    "yunnan":       {"label": "云南",   "extent": (97.0, 106.5, 21.0, 29.5)},
    "hubei":        {"label": "湖北",   "extent": (108.0, 116.5, 29.0, 33.5)},
    # added for the Nodal Maps tab's province selector (no preview counterpart yet)
    "hunan":        {"label": "湖南",   "extent": (108.8, 114.3, 24.6, 30.2)},
    "guizhou":      {"label": "贵州",   "extent": (103.5, 109.6, 24.5, 29.3)},
    "guangdong":    {"label": "广东",   "extent": (109.6, 117.3, 20.2, 25.5)},
    "hainan":       {"label": "海南",   "extent": (108.6, 111.1, 18.2, 20.1)},
    "jiangxi":      {"label": "江西",   "extent": (113.5, 118.5, 24.5, 30.1)},
}

# Nodal Maps tab province label -> extent_tag (only 河北南网 differs)
TAB_PROVINCE_TO_TAG: dict[str, str] = {
    cfg["label"]: tag for tag, cfg in EXTENTS.items()
}
TAB_PROVINCE_TO_TAG["河北南网"] = "jinan"
