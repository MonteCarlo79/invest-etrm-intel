"""蒙西 station registry — full rm_assets fleet, 12 stations (Task 11 scope
extension, spec 2026-10-31): 9 BESS + 3 wind.

The 6 original BESS (Task 2) are transcribed verbatim from
knowledge/mengxi/bess_node_registry.md, "Current portfolio" table.
price_nodes = own fengxing node (where present) + parent nodes split on ';'.
Parent-node shorthand fragments (e.g. "2M", "4M" continuing 内蒙.苏尼特站/220kV.1M,
and range forms like "内蒙.谷山梁站/220kV.1M–4M") are kept as written in the
source.

Task 11 additions (user-decided fleet):
- BESS +3: wuhai (settlement_only — bills exist, but no 接入系统报告 in the
  repo, so substation/node are TBD), wuchuan + alashan (registry_only —
  substation and node candidates from the knowledge registry's upcoming-asset
  notes; no cleared/dispatch/forecast data path yet, node presence probed by
  consumers).
- wind +3: lingt46 (零碳46风电, settlement_only — rm book 6 exists),
  alashanzuoqi + erlian (registry_only — no rm_assets entry yet).
- 乌兰察布储能 (B-1) explicitly excluded per user; 魏桥 (coal) out of scope.

data_path tiers (drive AssetBook honest-degradation guards, Task 11):
  'full'            — cleared/actual/strategies/forecast/settlement/attribution
  'settlement_only' — settlement + attribution; no strategy/dispatch layer
  'registry_only'   — identity only; settlement refresh skipped loudly
"""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class StationRef:
    asset_code: str
    plant_name: str
    capacity_mw: int | None          # blank in source table -> None
    substation: str                  # "" when no 接入系统报告 in repo (unknown)
    conn_kv: int | None              # None when unknown (wind books)
    price_nodes: list[str]           # own node (where present) + parent nodes
    asset_type: str = "bess"         # 'bess' | 'wind'
    data_path: str = "full"          # 'full' | 'settlement_only' | 'registry_only'
    subsidy_cny_per_mwh: float | None = None  # 蒙西 discharge compensation
                                       # (350 listed / 280 post-2026 default /
                                       #  None for wind — knowledge/mengxi/cap_payment_list.md)

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
        subsidy_cny_per_mwh=350,
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
        subsidy_cny_per_mwh=350,
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
        subsidy_cny_per_mwh=350,
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
        subsidy_cny_per_mwh=350,
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
        subsidy_cny_per_mwh=350,
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
        subsidy_cny_per_mwh=350,
    ),
    # ── Task 11: BESS +3 ────────────────────────────────────────────────────
    StationRef(
        asset_code="wuhai",
        plant_name="乌海康富储能电站",
        capacity_mw=100,
        substation="",                 # unknown — no 接入系统报告 in repo
        conn_kv=220,
        price_nodes=[],                # node TBD
        data_path="settlement_only",
        subsidy_cny_per_mwh=350,       # list #8 金湖一标段 (远景能源+中国康富)
    ),
    StationRef(
        asset_code="wuchuan",
        plant_name="武川储能电站",
        capacity_mw=1000,
        substation="武川500kV变电站",
        conn_kv=220,
        price_nodes=[                  # per knowledge/mengxi/bess_node_registry.md
            "内蒙.武川站/220kV.1M–4M",
            "内蒙.武川站/500kV.1M–2M",
        ],
        data_path="registry_only",
        subsidy_cny_per_mwh=280,       # post-2026 unlisted default (COD 2026-08)
    ),
    StationRef(
        asset_code="alashan",
        plant_name="阿拉善储能电站",
        capacity_mw=1000,
        substation="阿拉腾敖包500kV变电站",
        conn_kv=220,
        price_nodes=[                  # candidates — station had not appeared
            "内蒙.阿拉腾敖包站/220kV.1M–4M",   # in the price table as of 2026-09
        ],
        data_path="registry_only",
        subsidy_cny_per_mwh=280,       # post-2026 unlisted default (COD 2026-07)
    ),
    # ── Task 11: wind +3 (settlement/attribution books; no strategy layer) ──
    StationRef(
        asset_code="lingt46",
        plant_name="零碳46风电",
        capacity_mw=405,
        substation="",
        conn_kv=None,
        price_nodes=[],
        asset_type="wind",
        data_path="settlement_only",   # rm_assets id 1, book 6 exists
    ),
    StationRef(
        asset_code="alashanzuoqi",
        plant_name="阿拉善左旗风储电站",
        capacity_mw=195,
        substation="",
        conn_kv=None,
        price_nodes=[],
        asset_type="wind",
        data_path="registry_only",     # no rm_assets entry yet
    ),
    StationRef(
        asset_code="erlian",
        plant_name="二连风电",
        capacity_mw=200,
        substation="",
        conn_kv=None,
        price_nodes=[],
        asset_type="wind",
        data_path="registry_only",     # no rm_assets entry yet
    ),
]

_BY_CODE: dict[str, StationRef] = {s.asset_code: s for s in STATIONS}

def get_stations(asset_type: str | None = None,
                 data_path: str | None = None) -> list[StationRef]:
    """All 12 registered 蒙西 stations (9 BESS + 3 wind), optionally filtered
    by asset_type ('bess'|'wind') and/or data_path ('full'|'settlement_only'|
    'registry_only'). Default: everything (defensive copy)."""
    out = STATIONS
    if asset_type is not None:
        out = [s for s in out if s.asset_type == asset_type]
    if data_path is not None:
        out = [s for s in out if s.data_path == data_path]
    return list(out)

def get_station(asset_code: str) -> StationRef:
    """One station by asset_code; KeyError on unknown code."""
    return _BY_CODE[asset_code]
