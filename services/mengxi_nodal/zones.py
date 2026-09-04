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
# Sources: 接入系统报告, 2025-12-22 主接线图 (roster boxes, capacities MW), price-cluster analysis.
# transformers: main-transformer config; firm_mva: N-1 firm capacity (total minus largest unit).
# sibling entries: (name, installed_mw or None if unknown).
ZONES = [
    {
        "zone": "苏尼特变",
        "transformers": "4×750 MVA",
        "firm_mva": 2250,
        "our_assets": ["suyou"],
        "sibling_bess": [("蒙能吉博尔#1,2", 1000), ("蒙能卓拉#1,2", 1000), ("蒙能呼其", 500), ("蒙能百利格", 450)],
        "sibling_plants": [("京东方楚鲁图", 242.5), ("乌日希勒", 49.5), ("深能那仁", 20), ("巴音塔拉", 49.5),
                           ("伊林一场/二场", 129.5), ("二连恩和", 69.5), ("蒙科敖都", 10), ("天宏阳光", 30),
                           ("乌日根", 10), ("二连协合", 21), ("中海油二连", 102), ("宏晖二连微网", 125)],
    },
    {
        "zone": "努如变 (adjacent)",
        "transformers": None,
        "firm_mva": None,
        "our_assets": [],
        "sibling_bess": [("蒙能亿和1/2号", 1000), ("蒙能亿力齐1/2站", 200), ("大航都林", 100), ("星能乌勒吉", 100), ("明阳浩来图", 10)],
        "sibling_plants": [("环昕满都拉图", 50), ("沪能呼博", 10.8), ("嘉泽图和木", 11), ("塔林贡", 30)],
    },
    {
        "zone": "谷山梁变",
        "transformers": "4×1200 MVA",
        "firm_mva": 3600,
        "our_assets": ["hangjinqi", "gushanliang"],
        "sibling_bess": [("熠能沙壕", 500), ("熠储新荣", 500), ("博梁海纳", 500), ("星辰科创", 300), ("明阳亚什图", 100), ("综能万成功", 200)],
        "sibling_plants": [("库布其凝光", 480), ("库布其洁能", 960), ("库布其云恒", 960), ("沙日召光伏", 100),
                           ("正利光伏", 10), ("亿利治沙", 200), ("朔方", 200), ("卓越", 200), ("河日恒", 10),
                           ("夜鸣沙", 260), ("波特尔", 20), ("蓝晖#1,2", 1000), ("中节能众新", 575)],
    },
    {
        "zone": "杜尔伯特变",
        "transformers": "2×150+1×120 MVA",
        "firm_mva": 270,
        "our_assets": ["siziwangqi"],
        "sibling_bess": [],
        "sibling_plants": [("港建乌兰花风电", 99), ("席边河风光", 220), ("杜尔伯特(中广核)风电", 49.5),
                           ("夏日(三峡)风电", 49.5), ("四子王", 49.5), ("清泉", 100), ("锡拉木伦", 50),
                           ("布力格", 20), ("中光", 20), ("明杰汗乌拉", 10), ("准兴纳兰", 10)],
    },
    {
        "zone": "河套变",
        "transformers": "2×750 MVA",
        "firm_mva": 750,
        "our_assets": ["bameng"],
        "sibling_bess": [],
        "sibling_plants": [("华能特木尔", 506), ("光森太荣", 345)],
    },
    {
        "zone": "德岭山变",
        "transformers": "3×750 MVA",
        "firm_mva": 1500,
        "our_assets": ["wulate"],
        "sibling_bess": [("巴能峰塔", 100)],
        "sibling_plants": [("陵翔吉乐图", 49.5), ("晶澳包白", 10), ("中利腾晖", 60), ("天晟达明安", 10),
                           ("氢能绿电高位", 14), ("润风孟格图", 15), ("伊热都", 220)],
    },
    {
        "zone": "武川变 (upcoming)",
        "transformers": "3×750 MVA",
        "firm_mva": 1500,
        "our_assets": [],
        "sibling_bess": [],
        "sibling_plants": [("东山永光伏", None), ("南卜子", None), ("天能久远", None), ("国龙白山风电", None),
                           ("东方新能源西南壕", None), ("风盛北疆", None), ("恒润", 198.5), ("呼市抽水蓄能", 1200)],
    },
]
