"""Mengxi BESS nodal analysis — zone & asset configuration.

Python mirror of knowledge/mengxi/bess_node_registry.md (the human doc).
The Docker image does not ship knowledge/, so the app consumes this module.
Keep the two in sync when the registry changes.
"""

# Current portfolio — 6 assets.
# own_node: the asset's settlement meter node in the Fengxing registry (None =
# no own node; its price follows the parent substation nodes).
# parent_nodes: Fengxing nodes of the parent substation at connection voltage.
CURRENT_ASSETS = [
    {
        "asset_code": "suyou",
        "plant_name": "景蓝乌尔图储能电站",
        "capacity_mw": 100,
        "substation": "苏尼特500kV变电站",
        "conn_kv": 220,
        "own_node": "内蒙.景蓝乌尔图储能电站/220kV.1M",
        "parent_nodes": ["内蒙.苏尼特站/220kV.1M", "内蒙.苏尼特站/220kV.2M",
                         "内蒙.苏尼特站/220kV.4M", "内蒙.苏尼特站/500kV.1M"],
    },
    {
        "asset_code": "hangjinqi",
        "plant_name": "悦杭独贵储能电站",
        "capacity_mw": None,
        "substation": "谷山梁500kV变电站",
        "conn_kv": 220,
        "own_node": "内蒙.悦杭独贵储能电站/220kV.1M",
        "parent_nodes": ["内蒙.谷山梁站/220kV.1M", "内蒙.谷山梁站/220kV.2M",
                         "内蒙.谷山梁站/220kV.3M", "内蒙.谷山梁站/220kV.4M"],
    },
    {
        "asset_code": "siziwangqi",
        "plant_name": "景通四益堂储能电站",
        "capacity_mw": None,
        "substation": "杜尔伯特220kV变电站",
        "conn_kv": 110,
        "own_node": None,
        "parent_nodes": ["内蒙.杜尔伯特站/220kV.1M", "内蒙.杜尔伯特站/220kV.2M"],
    },
    {
        "asset_code": "gushanliang",
        "plant_name": "裕昭沙子坝储能电站",
        "capacity_mw": 500,
        "substation": "谷山梁500kV变电站",
        "conn_kv": 220,
        "own_node": "内蒙.裕昭沙子坝储能电站/220kV.1M",
        "parent_nodes": ["内蒙.谷山梁站/220kV.1M", "内蒙.谷山梁站/220kV.2M",
                         "内蒙.谷山梁站/220kV.3M", "内蒙.谷山梁站/220kV.4M"],
    },
    {
        "asset_code": "bameng",
        "plant_name": "景怡查干哈达储能电站",
        "capacity_mw": None,
        "substation": "河套500kV变电站",
        "conn_kv": 220,
        "own_node": "内蒙.景怡查干哈达储能电站/220kV.1M",
        "parent_nodes": ["内蒙.河套站/220kV.1M", "内蒙.河套站/220kV.2M",
                         "内蒙.河套站/220kV.3M", "内蒙.河套站/220kV.4M"],
    },
    {
        "asset_code": "wulate",
        "plant_name": "远景乌拉特储能电站",
        "capacity_mw": 100,
        "substation": "德岭山500kV变电站",
        "conn_kv": 220,
        "own_node": "内蒙.远景乌拉特储能电站/220kV.1M",
        "parent_nodes": ["内蒙.德岭山站/220kV.1M", "内蒙.德岭山站/220kV.2M",
                         "内蒙.德岭山站/220kV.3M", "内蒙.德岭山站/220kV.4M"],
    },
]

UPCOMING_ASSETS = [
    {"asset_code": "alashan", "capacity": "1000MW/4000MWh", "substation": "阿拉腾敖包500kV变电站", "conn_kv": 220},
    {"asset_code": "wuchuan", "capacity": "1000MW/4000MWh", "substation": "武川500kV变电站", "conn_kv": 220},
    {"asset_code": "xixier", "capacity": "1000MW/4000MWh", "substation": "锡西二500kV变电站 (在建)", "conn_kv": 220},
]

# Congestion zones — substation → our assets + sibling plants/BESS.
# Sources: 接入系统报告, 2025-12-22 主接线图, price-cluster analysis.
ZONES = [
    {
        "zone": "苏尼特变",
        "our_assets": ["suyou"],
        "sibling_bess": ["蒙能吉博尔#1,2", "蒙能卓拉#1,2", "蒙能呼其", "蒙能百利格"],
        "sibling_plants": ["京东方楚鲁图", "乌日希勒", "深能那仁", "巴音塔拉", "伊林一场/二场",
                           "二连恩和", "蒙科敖都", "天宏阳光", "乌日根", "二连协合", "中海油二连", "宏晖二连微网"],
    },
    {
        "zone": "努如变 (adjacent)",
        "our_assets": [],
        "sibling_bess": ["蒙能亿和1/2号", "蒙能亿力齐1/2站", "大航都林", "星能乌勒吉", "明阳浩来图"],
        "sibling_plants": ["环昕满都拉图", "沪能呼博", "嘉泽图和木", "塔林贡"],
    },
    {
        "zone": "谷山梁变",
        "our_assets": ["hangjinqi", "gushanliang"],
        "sibling_bess": ["熠能沙壕", "熠储新荣", "博梁海纳", "星辰科创", "明阳亚什图", "综能万成功"],
        "sibling_plants": ["库布其凝光", "库布其洁能", "库布其云恒", "沙日召光伏", "正利光伏", "亿利治沙",
                           "朔方", "卓越", "河日恒", "夜鸣沙", "波特尔", "蓝晖#1,2", "中节能众新"],
    },
    {
        "zone": "杜尔伯特变",
        "our_assets": ["siziwangqi"],
        "sibling_bess": [],
        "sibling_plants": ["港建乌兰花风电", "席边河风光", "杜尔伯特(中广核)风电", "夏日(三峡)风电",
                           "四子王", "清泉", "锡拉木伦", "布力格", "中光", "明杰汗乌拉", "准兴纳兰"],
    },
    {
        "zone": "河套变",
        "our_assets": ["bameng"],
        "sibling_bess": [],
        "sibling_plants": ["京能伊力更", "国华川井", "获各琦", "乌后旗开闭站", "曙光变", "杭后", "布拉格", "厂汉"],
    },
    {
        "zone": "德岭山变",
        "our_assets": ["wulate"],
        "sibling_bess": ["巴能峰塔"],
        "sibling_plants": ["陵翔吉乐图", "晶澳包白", "中利腾晖", "天晟达明安", "氢能绿电高位", "润风孟格图", "伊热都"],
    },
    {
        "zone": "武川变 (upcoming)",
        "our_assets": [],
        "sibling_bess": [],
        "sibling_plants": ["东山永光伏", "南卜子", "天能久远", "国龙白山风电", "东方新能源西南壕", "风盛北疆", "恒润", "呼市抽水蓄能"],
    },
]
