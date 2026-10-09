# scripts/openinfra_preview_mp.py
"""Multi-province OpenInfraMap × 网架图 preview data builder.

Provinces:
  - mengxi: 蒙西 (extent 100-117.5E/37-44.8N; vision stations VISION_500KV)
  - guangxi: 广西 (extent 104-112.8E/20.4-26.8N; 500kV nodes + adjacency from
    广西500KV画图.xlsx — the map author's own data, no vision needed)

Output: data/nodal/open-infra-map/preview_mp.json
Match semantics: OK (normalized hit) / COLLAPSED (city-prefix substring
fallback onto a less-specific OIM station) / MISSING.
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from openinfra_preview import (VISION_500KV, norm_name, _parse_wkb_coords,
                               _read_gpb_wkb, _downsample)

GPKG = Path("data/nodal/open-infra-map/CHN.gpkg")
OUT = Path("data/nodal/open-infra-map/preview_mp.json")

PROVINCES = {
    "mengxi": {"label": "蒙西", "extent": (100.0, 117.5, 37.0, 44.8)},
    "guangxi": {"label": "广西", "extent": (104.0, 112.8, 20.4, 26.8)},
    "mengdong": {"label": "蒙东", "extent": (114.0, 126.5, 41.5, 53.5)},
    "liaoning": {"label": "辽宁", "extent": (118.5, 126.0, 38.5, 44.0)},
    "shanxi": {"label": "山西", "extent": (110.0, 114.8, 34.0, 40.8)},
    "gansu": {"label": "甘肃", "extent": (92.0, 109.5, 32.0, 43.0)},
    "shandong": {"label": "山东", "extent": (114.5, 122.8, 34.3, 38.5), "schematic": True},
    "shaanxi": {"label": "陕西", "extent": (105.4, 111.3, 31.6, 39.6), "schematic": True},
    "jinan": {"label": "冀南", "extent": (112.8, 118.8, 35.8, 39.2)},
    "jibei": {"label": "冀北", "extent": (113.5, 120.2, 39.0, 43.2)},
    "heilongjiang": {"label": "黑龙江", "extent": (121.0, 135.5, 43.0, 53.8)},
    "jilin": {"label": "吉林", "extent": (121.0, 132.0, 40.0, 47.0)},
    "qinghai": {"label": "青海", "extent": (89.0, 104.0, 31.0, 39.5)},
    "ningxia": {"label": "宁夏", "extent": (104.0, 107.5, 35.0, 39.5)},
    "xinjiang": {"label": "新疆", "extent": (73.0, 97.0, 34.0, 50.0)},
    "henan": {"label": "河南", "extent": (110.0, 117.0, 31.5, 37.0)},
    "anhui": {"label": "安徽", "extent": (114.5, 120.0, 29.0, 34.5)},
    "zhejiang": {"label": "浙江", "extent": (118.0, 123.0, 27.0, 31.5)},
    "yunnan": {"label": "云南", "extent": (97.0, 106.5, 21.0, 29.5)},
    "hubei": {"label": "湖北", "extent": (108.0, 116.5, 29.0, 33.5)},
}

# Guangxi 500kV nodes whose OIM presence should be tried in the generator
# layers as well (power plants, not substations)
_GX_PLANT_RE = re.compile(r"(电厂|厂|核电)$")


def load_substations(db, extent):
    lon_min, lon_max, lat_min, lat_max = extent
    subs = {}
    for table, rtree in (("power_substation_point", "rtree_power_substation_point_geometry"),
                         ("power_substation_polygon", "rtree_power_substation_polygon_geometry")):
        for row in db.execute(
                f"""SELECT p.name, p.voltages, p.max_voltage, p.operator,
                           (r.minx + r.maxx)/2.0 AS lon, (r.miny + r.maxy)/2.0 AS lat
                    FROM {table} p JOIN {rtree} r ON p.fid = r.id
                    WHERE r.minx >= ? AND r.maxx <= ? AND r.miny >= ? AND r.maxy <= ?
                      AND p.name IS NOT NULL AND p.name != ''""",
                (lon_min, lon_max, lat_min, lat_max)):
            key = norm_name(row["name"])
            if key and (key not in subs or (row["max_voltage"] or 0) > (subs[key]["max_voltage"] or 0)):
                subs[key] = dict(row)
    return subs


def load_lines(db, extent, min_voltage=500000):
    lon_min, lon_max, lat_min, lat_max = extent
    lines = []
    for row in db.execute(
            f"""SELECT p.geometry, p.name, p.max_voltage
                FROM power_line p JOIN rtree_power_line_geometry r ON p.fid = r.id
                WHERE r.minx >= ? AND r.maxx <= ? AND r.miny >= ? AND r.maxy <= ?
                  AND p.max_voltage >= ?""",
            (lon_min, lon_max, lat_min, lat_max, min_voltage)):
        for path in _parse_wkb_coords(_read_gpb_wkb(row["geometry"])):
            if len(path) >= 2:
                lines.append({"path": _downsample(path, 100),
                              "name": row["name"] or "", "max_voltage": row["max_voltage"]})
    return lines


_NON_GRID_RE = re.compile(r"(管理局|管理分局|公司|政府|学校|医院|园区管委会|牵引)")


def match_one(name: str, by_norm: dict, city_prefix: bool):
    """Return (status, hit). OK exact/normalized; COLLAPSED via prefix-drop or
    shorter-key substring; MISSING. Fallback hits onto non-grid entities
    (管理局/公司/政府/牵引…) or sub-220kV stations are rejected; the
    reverse-substring direction needs a >=3-char candidate (blocks 合肥南->肥南)."""
    keys = [norm_name(name)]
    if city_prefix and len(keys[0]) > 2:
        keys.append(keys[0][2:])

    def ok_fallback(s):
        return not _NON_GRID_RE.search(s["name"]) and (s["max_voltage"] or 0) >= 220000

    for k in keys:
        if k in by_norm:
            return "OK", by_norm[k]
    for k in keys:
        if not k:
            continue
        cands = [kk for kk in by_norm if k in kk and ok_fallback(by_norm[kk])]
        if cands:
            return "COLLAPSED", by_norm[min(cands, key=len)]
        cands = [kk for kk in by_norm if len(kk) >= 3 and kk in k and ok_fallback(by_norm[kk])]
        if cands:
            return "COLLAPSED", by_norm[max(cands, key=len)]
    return "MISSING", None


def build_mengxi(db):
    cfg = PROVINCES["mengxi"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in MX25_500:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {
        "stations": [{"name": s["name"], "key": norm_name(s["name"]),
                      "voltages": s["voltages"], "max_voltage": s["max_voltage"],
                      "lon": s["lon"], "lat": s["lat"]} for s in subs.values()],
        "lines": load_lines(db, cfg["extent"]),
        "match": match,
        "adjacency": [],
    }


def build_guangxi(db):
    cfg = PROVINCES["guangxi"]
    gx = json.load(open("data/nodal/open-infra-map/guangxi_grid.json", encoding="utf-8"))
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    coord = {}
    for node in gx["nodes_500kv"]:
        st, hit = match_one(node, by_norm, city_prefix=True)
        match.append({"vision": node, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
        if hit:
            coord[node] = [hit["lon"], hit["lat"]]
    adj = [[a, b] for a, b in gx["adjacency"] if a in coord and b in coord]
    return {
        "stations": [{"name": s["name"], "key": norm_name(s["name"]),
                      "voltages": s["voltages"], "max_voltage": s["max_voltage"],
                      "lon": s["lon"], "lat": s["lat"]} for s in subs.values()],
        "lines": load_lines(db, cfg["extent"]),
        "match": match,
        "adjacency": adj,
    }


# 蒙东 union: 2022 地理接线图 (vision) + 2024/2026 通道示意图 (dark map reads)
MD_500 = ["荣泰", "海北", "兴隆", "岭东", "铝都", "巴林", "阿拉坦", "兴安",
          "科尔沁", "青山", "金沙", "红城", "巴彦托海", "伊敏换流站",
          "扎鲁特", "开鲁", "通辽", "恩和", "桃合木", "科右中", "玉山",
          "紫城", "庆丰", "阿荣北", "珠日河", "大板", "连场", "赤峰",
          "高林", "平川"]


def build_mengdong(db):
    cfg = PROVINCES["mengdong"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in MD_500:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {
        "stations": [{"name": s["name"], "key": norm_name(s["name"]),
                      "voltages": s["voltages"], "max_voltage": s["max_voltage"],
                      "lon": s["lon"], "lat": s["lat"]} for s in subs.values()],
        "lines": load_lines(db, cfg["extent"]),
        "match": match,
        "adjacency": [],
    }


# 辽宁 union: earlier edition (39) + 2025/2026 架构图 png reads (plants 清河/庄河 dropped)
LN_500 = ["川州", "阜新", "北宁", "鹤乡", "辽滨", "营口", "历林", "京诚",
          "北海", "南海", "辽中", "白清寨", "抚顺", "徐家", "程家", "张台",
          "辽阳", "鞍山", "唐家", "王石", "析木", "虎官", "丹东北", "黄海",
          "瓦房店", "登台", "金家", "南关岭", "大连湾", "甘井子", "玉华", "雁水",
          "石岭", "东港", "龙王", "凤城", "冷家",
          "丰田", "燕南", "董家", "利州", "西泉", "永安", "蒲河", "沙岭",
          "盛京", "沈东", "穆家", "宽邦", "徐大堡", "沙河营", "高岭", "渤海"]


def build_liaoning(db):
    cfg = PROVINCES["liaoning"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in LN_500:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {
        "stations": [{"name": s["name"], "key": norm_name(s["name"]),
                      "voltages": s["voltages"], "max_voltage": s["max_voltage"],
                      "lon": s["lon"], "lat": s["lat"]} for s in subs.values()],
        "lines": load_lines(db, cfg["extent"]),
        "match": match,
        "adjacency": [],
    }


SX_500 = ["明海湖", "五寨", "朔州", "忻州", "北岳", "大同", "云岗", "平城",
          "繁峙", "灵丘", "原平", "吕梁", "晋中", "介休", "霍州", "临汾",
          "运城", "晋城", "长治", "潞城", "阳泉", "榆社", "稷山", "孟门",
          "侯马", "垣曲", "雁门关"]

GS_750 = ["沙州", "敦煌", "莫高", "花海", "鼎新", "酒泉", "甘州", "河西",
          "水源", "武威北", "凉州北", "武胜", "秦川", "白银", "天都山",
          "兰州东", "熙州", "郭隆", "官亭", "麦积山", "曲子", "六盘山",
          "平凉", "乾县", "宝鸡", "夏州", "祁连换流站", "武威换流站", "庆阳换流站"]

# 山东 corridor schematic: city blocks + 500kV corridor names (red lines from the map)
SD_BLOCKS = ["聊城", "德州", "滨州", "东营", "济南", "淄博", "泰安", "潍坊",
             "青岛", "烟台", "威海", "日照", "临沂", "枣庄", "菏泽", "济宁"]
SD_CORRIDORS = ["柴贝线", "齐乐双线", "海垦双线", "滨油线", "油惠线", "川滨线",
                "兴弥线", "弥青线", "临潍线", "固临双线", "固淄线", "益管线",
                "管石线", "济淄双线", "川泰线", "城岱线", "郓泰线", "东上线",
                "上泰双线", "东麟线", "泰龙线", "东长双线", "枣蒙线", "衡兰双线",
                "蒙沂线", "蒙照I线", "蒙阳双线", "沂鲁线", "邹蒙线", "邹新线",
                "儒川线", "邹川线", "圣仓线", "登泽线", "阳泽线", "核神双线",
                "核泽双线", "崂阳线", "弥油线", "寿油线", "益潍线", "乐密双线",
                "密胸线", "潍石线", "潍亭线", "潍泽线", "密琅双线", "观照双线",
                "乐亭双线", "济天线", "济长线", "龙韶线", "聊韶线", "聊长线",
                "沂照I线", "泰天线"]

# 陕西 channel schematic (2026-01 通道示意图): region blocks + channels with MW
SX_CHANNELS = [
    {"name": "陕甘断面 (西电东送)", "mw": 9500, "from": "甘肃", "to": "关中/陕南"},
    {"name": "洛信道泾断面 (陕北送断)", "mw": 6050, "from": "陕北", "to": "关中/陕南"},
    {"name": "陕武 (规划/在建)", "mw": None, "from": "陕北", "to": "武汉"},
    {"name": "德宝", "mw": None, "from": "关中/陕南", "to": "四川"},
    {"name": "灵宝", "mw": None, "from": "关中/陕南", "to": "河南"},
    {"name": "吉泉直流", "mw": None, "from": "新疆/乾县", "to": "安徽宣城"},
    {"name": "天中直流", "mw": None, "from": "新疆/夏州", "to": "郑州"},
    {"name": "祁韶直流", "mw": None, "from": "祁连", "to": "湖南"},
    {"name": "威越直流", "mw": None, "from": "武威", "to": "浙江"},
    {"name": "庆东直流", "mw": None, "from": "庆阳", "to": "山东"},
]


def _stations_payload(subs):
    return [{"name": s["name"], "key": norm_name(s["name"]),
             "voltages": s["voltages"], "max_voltage": s["max_voltage"],
             "lon": s["lon"], "lat": s["lat"]} for s in subs.values()]


def build_shanxi(db):
    cfg = PROVINCES["shanxi"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in SX_500:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {"stations": _stations_payload(subs),
            "lines": load_lines(db, cfg["extent"]),
            "match": match, "adjacency": []}


def build_gansu(db):
    cfg = PROVINCES["gansu"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in GS_750:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {"stations": _stations_payload(subs),
            "lines": load_lines(db, cfg["extent"], min_voltage=330000),
            "match": match, "adjacency": []}


def build_shandong(db):
    cfg = PROVINCES["shandong"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in SD_500:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {"stations": _stations_payload(subs),
            "lines": load_lines(db, cfg["extent"]),
            "match": match, "adjacency": [],
            "corridors": SD_CORRIDORS}


def build_shaanxi(db):
    cfg = PROVINCES["shaanxi"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in SXS_750:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {"stations": _stations_payload(subs),
            "lines": load_lines(db, cfg["extent"], min_voltage=330000),
            "match": match, "adjacency": [],
            "channels": SX_CHANNELS}


# --- Station lists extracted 2026-10-10 from PDF text layers (青海/宁夏/新疆/
# 吉林/黑龙江26/安徽/蒙西25/山东) and vision reads (云南/河南500/浙江).
# 浙江's map is a dense line-name schematic — no reliable station list,
# built OIM-only (empty match).

# 蒙西-2025年底内蒙古电网500kV主网架图 (text layer; supersedes the 2022 vision
# list for the mengxi match — 万全/沽源 are 冀北 edge stations shown on the map)
MX25_500 = ["万全", "沽源", "阿勒泰", "汗海", "灰腾梁", "丰泉", "察右中", "塔拉",
            "庆云", "苏敦", "巨宝庄", "伊旗", "高新", "乌海", "永圣域", "达一",
            "德岭山", "布日都", "吉兰太", "响沙湾", "包头北", "旗下营", "达二",
            "千里山", "萨拉齐", "春坤山", "河套", "梅力更", "威俊", "武川",
            "祥泰", "百灵", "定远营", "巴中", "甘迪尔", "赛罕", "宁格尔", "路华",
            "常胜", "渡口", "英华", "德义", "谷山梁", "红梁", "白音高勒", "宝拉格",
            "敕勒川", "开林河", "克仁珠", "阿拉腾", "昆都仑", "耳字壕", "芒哈图",
            "泊江海", "苏尼特", "瑞升", "努如", "乌梁素海", "涌泉", "乌后旗",
            "沙井", "凤凰岭", "金湖", "土默特", "杭锦宋家渠", "磴口", "包头"]

# 青海-2026-01 主网架结构图 (750kV; 熙州/武胜/沙州 are 甘肃 edge stations)
QH_750 = ["官亭", "拉西瓦", "西宁", "郭隆", "日月山", "塔拉", "青南", "香加",
          "海西", "柴达木", "鱼卡", "托素", "杜鹃", "红旗", "云杉", "大漠",
          "桥头", "格尔木", "昆仑山", "熙州", "武胜", "沙州"]

# 宁夏-2026-06 目标网架 (750kV; 白银/平凉/伊克昭 are neighbour edge stations)
NX_750 = ["贺兰山", "方家庄", "黎阳", "杞乡", "妙岭", "六盘山", "北地",
          "银川东", "烽燧", "中宁", "明山", "灵州", "沙坡头", "沙湖", "黄河",
          "天都山", "白银", "平凉", "伊克昭"]

# 新疆-2026-01 网架结构示意图 (750kV backbone)
XJ_750 = ["达坂城", "凤凰", "天山换流站", "南湖", "烟墩", "库车", "亚中",
          "巴楚", "阿克苏", "喀什", "莎车", "和田", "鄯善", "青格达", "阿尔金",
          "昌安", "民丰", "柳毛湾", "乌北", "五家渠", "蒋家湾", "孚远",
          "巴里坤换流站", "伊吾", "中湖", "三塘湖", "哈密",
          "巴州", "若羌", "罗布泊", "昌吉换流站", "古海", "北庭", "芨芨湖",
          "紫荆", "信友", "木垒", "英格玛", "照壁山", "塔城", "伊犁", "乌苏",
          "赛里木", "将军庙", "青石峡", "吐鲁番", "玫瑰泉", "喀纳斯", "五彩湾"]

# 吉林-2026年系统图 (text layer; 丰满/敦化/长山/九台/双辽/白城 are plants)
JL_500 = ["龙嘉", "东丰", "包家", "吉林东", "通化", "茂胜", "延吉", "平安",
          "松原", "甜水", "瞻榆", "龙凤", "向阳", "昌盛", "金城", "合心",
          "庆德", "布苏", "傅家", "乐胜", "梨树"]

# 黑龙江-2026 通道示意图 (text layer; 鹤/双 appear split — read as 鹤岗/双鸭山)
HLJ_500 = ["林海", "前进", "庆云", "方正", "黑换", "哈南", "鸡西", "鹤岗",
           "七台河", "双鸭山", "冯屯", "松北", "群林", "清源", "华民", "永源",
           "宝清", "牡丹江", "大庆", "兴福", "集贤", "五家", "国富", "安源",
           "哈平南", "四季青", "新村", "海永", "胜德", "安北"]

# 安徽-2026 通道示意图 (text layer, station tokens only — line names excluded)
AH_500 = ["肥北", "众兴", "铜池", "合肥南", "当涂", "六安", "安庆", "楚城",
          "宣黄", "昭福", "皖北", "芜湖南", "庙集", "阜阳", "岱河"]

# 河南-2026-500千伏主要通道示意图 (vision read, 4 crops; 冀州 is 河北 edge)
HN_500 = ["丰鹤", "彰德", "洹安", "朝歌", "冀州", "获嘉", "博爱", "沁北",
          "竹贤", "峪宝泉", "潞南", "多宝山", "塔铺", "仓颉", "卫都", "济源",
          "孟津", "邙山", "瀛州", "牡丹", "马寺", "汉都", "洛宁", "嘉和",
          "陕州", "灵宝", "绿城", "惠济", "官渡", "嵩山", "怀德", "航空",
          "郑州", "密东", "武周", "中州", "菊城", "祥符", "龙亭", "沙盟",
          "庄周", "圣临", "广成", "鲁阳", "姚孟", "香山", "湛河", "墨公",
          "郾城", "龙岗", "涂会", "花都", "邵陵", "迟营", "周口", "郸城",
          "旺河", "嫘祖", "豫南", "嵖岈", "周湾", "挚亭", "金牛", "春申",
          "浉河", "华豫", "五岳", "南阳", "白河", "奚贤", "鸭河口", "内乡",
          "群英", "玉都", "天池"]

# 云南-2026 通道示意图 (vision read, clean schematic)
YN_500 = ["建塘", "太安", "金官", "桂中", "德茂", "永仁", "仁和", "光辉",
          "昆北", "柳州", "东方", "新松", "黄坪", "禾甸", "大理", "和平",
          "楚雄", "穗东", "厂口", "铜都", "龙海", "隆阳", "鹿城", "草铺",
          "白邑", "禄劝", "麒麟", "乐业", "永丰", "明通", "甘顶", "牛寨",
          "从西", "多乐", "鹤城", "荣兴", "喜平", "曲靖", "罗平", "高坡",
          "兰城", "宝峰", "庄乔", "七甸", "圭山", "玉溪", "宁州", "天星",
          "博尚", "墨江", "惠历", "红河", "砚山", "德宏", "甜美", "思茅",
          "通宝", "版纳", "普洱", "侨乡", "鲁西", "富宁", "柳井"]

# 山东 500kV named stations from 山东电网架构 PDF text layer (2026-04 edition) —
# replaces the city-block match with real station matching
SD_500 = ["日照", "济南南", "济南北", "潍坊东", "潍坊西", "淄博南", "淄博北",
          "莱芜", "泰安东", "泰安西", "枣庄", "德州", "滨州", "东营", "菏泽",
          "济宁", "烟台", "威海", "临沂", "青岛", "聊城", "王家", "沭河"]

# 湖北 省间通道示意图 (2025 png + 2026 pdf — same schematic, merged read)
HB_500 = ["龙泉", "团林", "江陵", "葛洲坝", "恩施", "荆门", "宜都", "奚贤",
          "卧龙", "孝感", "武汉", "永兴", "黄石", "咸宁"]

# 陕西 2543.png geographic map (750kV ◎ double-ring stations + red-line
# junctions; magenta city labels are 政府所在地, NOT stations, per legend)
SXS_750 = ["统万", "榆横", "神木", "府谷", "绥德", "朱家", "延安", "洛川",
           "黄陵", "东塬", "西安", "信义", "罗敷", "乾县", "宝鸡", "雍城",
           "汉中", "安康", "柞水", "商州", "鹿城", "龙泉", "郝家"]


def _generic_build(db, key, station_list, min_voltage=500000, city_prefix=False):
    cfg = PROVINCES[key]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in station_list:
        st, hit = match_one(v, by_norm, city_prefix=city_prefix)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {"stations": _stations_payload(subs),
            "lines": load_lines(db, cfg["extent"], min_voltage=min_voltage),
            "match": match, "adjacency": []}




JN_500 = ["保定", "邢台", "黄骅", "潞城"]

JB_500 = ["张家口", "廊坊", "康保", "尚义", "解放", "白土窑", "沽源", "千松坝",
          "御道口", "金山岭", "承德", "宽城", "太平", "姜家营", "张南", "万全",
          "张北", "木兰", "隆城", "阜康换流站", "坝上"]


def build_jinan(db):
    cfg = PROVINCES["jinan"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in JN_500:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {"stations": _stations_payload(subs),
            "lines": load_lines(db, cfg["extent"]),
            "match": match, "adjacency": []}


def build_jibei(db):
    cfg = PROVINCES["jibei"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in JB_500:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {"stations": _stations_payload(subs),
            "lines": load_lines(db, cfg["extent"]),
            "match": match, "adjacency": []}


def main() -> None:
    db = sqlite3.connect(GPKG)
    db.row_factory = sqlite3.Row
    out = {"provinces": {}}
    out["provinces"]["mengxi"] = {**PROVINCES["mengxi"], **build_mengxi(db)}
    out["provinces"]["guangxi"] = {**PROVINCES["guangxi"], **build_guangxi(db)}
    out["provinces"]["mengdong"] = {**PROVINCES["mengdong"], **build_mengdong(db)}
    out["provinces"]["liaoning"] = {**PROVINCES["liaoning"], **build_liaoning(db)}
    out["provinces"]["shanxi"] = {**PROVINCES["shanxi"], **build_shanxi(db)}
    out["provinces"]["gansu"] = {**PROVINCES["gansu"], **build_gansu(db)}
    out["provinces"]["shandong"] = {**PROVINCES["shandong"], **build_shandong(db)}
    out["provinces"]["shaanxi"] = {**PROVINCES["shaanxi"], **build_shaanxi(db)}
    out["provinces"]["jinan"] = {**PROVINCES["jinan"], **build_jinan(db)}
    out["provinces"]["jibei"] = {**PROVINCES["jibei"], **build_jibei(db)}
    out["provinces"]["heilongjiang"] = {**PROVINCES["heilongjiang"],
                                        **_generic_build(db, "heilongjiang", HLJ_500)}
    out["provinces"]["jilin"] = {**PROVINCES["jilin"],
                                 **_generic_build(db, "jilin", JL_500)}
    out["provinces"]["qinghai"] = {**PROVINCES["qinghai"],
                                   **_generic_build(db, "qinghai", QH_750, min_voltage=330000)}
    out["provinces"]["ningxia"] = {**PROVINCES["ningxia"],
                                   **_generic_build(db, "ningxia", NX_750, min_voltage=330000)}
    out["provinces"]["xinjiang"] = {**PROVINCES["xinjiang"],
                                    **_generic_build(db, "xinjiang", XJ_750, min_voltage=330000)}
    out["provinces"]["henan"] = {**PROVINCES["henan"],
                                 **_generic_build(db, "henan", HN_500)}
    out["provinces"]["anhui"] = {**PROVINCES["anhui"],
                                 **_generic_build(db, "anhui", AH_500)}
    out["provinces"]["zhejiang"] = {**PROVINCES["zhejiang"],
                                    **_generic_build(db, "zhejiang", [])}
    out["provinces"]["yunnan"] = {**PROVINCES["yunnan"],
                                  **_generic_build(db, "yunnan", YN_500)}
    out["provinces"]["hubei"] = {**PROVINCES["hubei"],
                                 **_generic_build(db, "hubei", HB_500)}
    for k, p in out["provinces"].items():
        if "match" in p:
            p["counts"] = {
                "stations": len(p["stations"]),
                "lines": len(p["lines"]),
                "ok": sum(1 for m in p["match"] if m["status"] == "OK"),
                "collapsed": sum(1 for m in p["match"] if m["status"] == "COLLAPSED"),
                "missing": sum(1 for m in p["match"] if m["status"] == "MISSING"),
                "adjacency": len(p["adjacency"]),
            }
    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    for k, p in out["provinces"].items():
        print(k, p.get("counts", "pending"))


if __name__ == "__main__":
    sys.exit(main())
