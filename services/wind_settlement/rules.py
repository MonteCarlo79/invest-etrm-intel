# -*- coding: utf-8 -*-
"""2026 Mengxi trading-rules logic for settlement replication.

规则体系 (2024-12) 第十四条 [发电侧电能量电费]:
  现货全电量电能量电费 = Σ_t 上网电量_t × 节点电价_t
  中长期差价合约电能量电费 = Σ_t Σ_c 合约电量_c,t × (合约价_c − 用户侧区域结算参考点电价_t)
  省间现货售电中标合约视为省内中长期合约 — 按中标价与送出节点所在区域结算参考点电价之差结算.

Zone reference mapping (per contract counterparty):
  - 呼和浩特 / 乌兰察布 / 锡林郭勒 users → 呼包以东 (east)
  - 包头 / 鄂尔多斯 / 巴彦淖尔 / 乌海 / 薛家湾 / 阿拉善 users → 呼包以西 (west)
  - 跨省跨区送出 (超高压供电局 / 跨省 channel) → 送出节点所在区域 = west (悦盛 in 鄂尔多斯)
  - 电网代理工商业 (代购) → 全网统一结算点 (sys)
  - 2026-07 起合约东西合区 (复盘 deck): all contracts settle at the unified
    system reference.
"""
from __future__ import annotations

import pandas as pd

EAST_BUREAUS = {"呼和浩特供电局", "乌兰察布电业局", "锡林郭勒电业局"}
WEST_BUREAUS = {"包头供电局", "鄂尔多斯电业局", "巴彦淖尔电业局", "乌海电业局",
                "薛家湾供电局", "阿拉善电业局"}
EAST_REGIONS = {"呼和浩特", "乌兰察布", "锡林郭勒"}
WEST_REGIONS = {"包头", "鄂尔多斯", "巴彦淖尔", "乌海", "薛家湾", "阿拉善"}
UNIFIED_FROM = "2026-07"  # 东西合区 effective month


def _s(v) -> str:
    """Coerce pandas-missing (NaN/None) to '' for string matching."""
    return v if isinstance(v, str) else ""


def zone_of(channel: str, trade_type: str, consumer_unit: str | None,
            region: str | None, bureau: str | None) -> str:
    """Settlement reference zone for a contract: 'east' | 'west' | 'sys'.

    Bureau (供电局) is the primary signal — the 所属地区 column changes
    semantics across file versions (Jan–Mar = user's region; Jun–Jul =
    generator's region, constant 鄂尔多斯). Region is the fallback.
    """
    consumer_unit = _s(consumer_unit)
    bureau = _s(bureau)
    region = _s(region)
    trade_type = _s(trade_type)

    if "代购" in trade_type or ("内蒙古电力" in bureau and "超高压" not in bureau):
        return "sys"  # 电网代理工商业 → 全网统一结算点
    if channel == "跨省" or "超高压" in bureau or "跨省跨区" in trade_type:
        return "west"  # 省间售电 → 送出节点所在区域 (呼包以西)
    if bureau in EAST_BUREAUS:
        return "east"
    if bureau in WEST_BUREAUS:
        return "west"
    if region in EAST_REGIONS:
        return "east"
    return "west"  # default: 悦盛 home zone (and unknown users)


def _ref_for(zone: str, ref_east: float, ref_west: float, ref_sys: float) -> float:
    return {"east": ref_east, "west": ref_west, "sys": ref_sys}[zone]


def cfd_zones(intra: pd.DataFrame, cross: pd.DataFrame,
              ref_east: float, ref_west: float, ref_sys: float,
              unified: bool) -> float:
    """Zone-aware CfD: Σ_c vol_c × (price_c − ref_zone(c)).

    正常 rows give the zone split per 品种; volumes scale to the platform's
    汇总 net position (置换 swaps uniform across zones). Cross file rows are
    positions themselves (no 汇总).
    """
    total = 0.0
    for df, has_summary in ((intra, True), (cross, False)):
        if df is None or df.empty:
            continue
        for trade_type, grp in df.groupby("trade_type"):
            normal = grp[grp["energy_kind"] == "正常"] if has_summary else grp
            if normal.empty:
                continue
            if has_summary:
                summ = grp[grp["energy_kind"] == "汇总"]
                target_vol = float(summ["volume_mwh"].sum()) if not summ.empty else float(normal["volume_mwh"].sum())
            else:
                target_vol = float(normal["volume_mwh"].sum())
            normal_vol = float(normal["volume_mwh"].sum())
            if not normal_vol or not target_vol:
                continue
            scale = target_vol / normal_vol
            for _, row in normal.iterrows():
                zone = zone_of(row["channel"], trade_type, row.get("consumer_unit"),
                               row.get("region"), row.get("bureau"))
                ref = ref_sys if unified else _ref_for(zone, ref_east, ref_west, ref_sys)
                vol = float(row["volume_mwh"]) * scale
                price = row["energy_price"]
                if price is None or pd.isna(price):
                    continue
                total += vol * (float(price) - ref)
    return total
