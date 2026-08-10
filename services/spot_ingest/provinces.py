"""Province CN → EN name map for spot market ingest pipelines.

Single source of truth — previously duplicated in services/hermes/spot_ingest_bridge.py
and apps/spot-watcher/pipeline.py (the latter keeps its own copy, out of scope).
"""
from __future__ import annotations

PROVINCES_MAP: dict[str, str] = {
    "山东": "Shandong",
    "山西": "Shanxi",
    "蒙西": "Mengxi",
    "内蒙古": "Mengxi",
    "甘肃": "Gansu",
    "广东": "Guangdong",
    "四川": "Sichuan",
    "云南": "Yunnan",
    "贵州": "Guizhou",
    "广西": "Guangxi",
    "湖南": "Hunan",
    "湖北": "Hubei",
    "安徽": "Anhui",
    "浙江": "Zhejiang",
    "江苏": "Jiangsu",
    "福建": "Fujian",
    "河南": "Henan",
    "陕西": "Shaanxi",
    "宁夏": "Ningxia",
    "新疆": "Xinjiang",
    "辽宁": "Liaoning",
    "吉林": "Jilin",
    "黑龙江": "Heilongjiang",
    "蒙东": "Mengdong",
    "河北": "Hebei",
    "冀北": "Hebei-North",
    "冀南": "Hebei-South",
    "河北南网": "Hebei-South",
    "青海": "Qinghai",
    "江西": "Jiangxi",
    "海南": "Hainan",
    "重庆": "Chongqing",
    "上海": "Shanghai",
    "北京": "Beijing",
    "天津": "Tianjin",
}
