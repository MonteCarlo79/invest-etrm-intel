"""蒙西 station registry — 6 current BESS assets (spec 2026-10-31).

Transcribed verbatim from knowledge/mengxi/bess_node_registry.md,
"Current portfolio" table. price_nodes = own fengxing node (where present)
+ parent nodes split on ';'. Parent-node shorthand fragments (e.g. "2M",
"4M" continuing 内蒙.苏尼特站/220kV.1M, and range forms like
"内蒙.谷山梁站/220kV.1M–4M") are kept as written in the source.
Upcoming assets (alashan/wuchuan/xixier) are NOT included.
"""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class StationRef:
    asset_code: str
    plant_name: str
    capacity_mw: int | None          # blank in source table -> None
    substation: str
    conn_kv: int
    price_nodes: list[str]           # own node (where present) + parent nodes

STATIONS: list[StationRef] = [
    StationRef(
        asset_code="suyou",
        plant_name="景蓝乌尔图储能电站",
        capacity_mw=100,
        substation="苏尼特500kV变电站",
        conn_kv=220,
        price_nodes=[
            "内蒙.景蓝乌尔图储能电站/220kV.1M",
            "内蒙.苏尼特站/220kV.1M", "2M", "4M",
            "内蒙.苏尼特站/500kV.1M",
        ],
    ),
    StationRef(
        asset_code="hangjinqi",
        plant_name="悦杭独贵储能电站",
        capacity_mw=None,
        substation="谷山梁500kV变电站",
        conn_kv=220,
        price_nodes=[
            "内蒙.悦杭独贵储能电站/220kV.1M",
            "内蒙.谷山梁站/220kV.1M–4M",
        ],
    ),
    StationRef(
        asset_code="siziwangqi",
        plant_name="景通四益堂储能电站",
        capacity_mw=None,
        substation="杜尔伯特220kV变电站",
        conn_kv=110,
        price_nodes=[                  # (无自有节点) — parents only
            "内蒙.杜尔伯特站/220kV.1M–2M",
        ],
    ),
    StationRef(
        asset_code="gushanliang",
        plant_name="裕昭沙子坝储能电站",
        capacity_mw=500,
        substation="谷山梁500kV变电站",
        conn_kv=220,
        price_nodes=[
            "内蒙.裕昭沙子坝储能电站/220kV.1M",
            "内蒙.谷山梁站/220kV.1M–4M",
        ],
    ),
    StationRef(
        asset_code="bameng",
        plant_name="景怡查干哈达储能电站",
        capacity_mw=None,
        substation="河套500kV变电站",
        conn_kv=220,
        price_nodes=[
            "内蒙.景怡查干哈达储能电站/220kV.1M",
            "内蒙.河套站/220kV.1M–4M",
        ],
    ),
    StationRef(
        asset_code="wulate",
        plant_name="远景乌拉特储能电站",
        capacity_mw=100,
        substation="德岭山500kV变电站",
        conn_kv=220,
        price_nodes=[
            "内蒙.远景乌拉特储能电站/220kV.1M",
            "内蒙.德岭山站/220kV.1M–4M",
        ],
    ),
]

_BY_CODE: dict[str, StationRef] = {s.asset_code: s for s in STATIONS}

def get_stations() -> list[StationRef]:
    """All 6 current 蒙西 stations (defensive copy)."""
    return list(STATIONS)

def get_station(asset_code: str) -> StationRef:
    """One station by asset_code; KeyError on unknown code."""
    return _BY_CODE[asset_code]
