# -*- coding: utf-8 -*-
"""
Asset registry — canonical DB entity for the investment-ops lifecycle
(screening → upcoming → operating). Seeded from
knowledge/mengxi/bess_node_registry.md (2026-09-01 extraction, capacities
cross-checked against cleared-energy p99 where the table left them blank).

Ardian Opta lesson: assets are first-class onboarded entities with lifecycle
state, not rows scattered across pipelines.
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.asset_registry (
    asset_code      TEXT PRIMARY KEY,
    plant_name      TEXT NOT NULL,
    asset_type      TEXT NOT NULL DEFAULT 'bess',
    status          TEXT NOT NULL DEFAULT 'operating'
        CHECK (status IN ('screening', 'upcoming', 'operating', 'retired')),
    province        TEXT NOT NULL DEFAULT '蒙西',
    zone            TEXT,
    capacity_mw     DOUBLE PRECISION,
    duration_h      DOUBLE PRECISION,
    capacity_source TEXT,
    substation      TEXT,
    conn_kv         TEXT,
    price_node_own  TEXT,
    price_node_parents TEXT,
    zone_price_node TEXT,
    cod_date        DATE,
    ops_data_since  DATE,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# Seed from knowledge/mengxi/bess_node_registry.md (2026-09-01).
# Capacities left blank in the markdown are inferred from cleared-energy p99
# (2026-09-19 probe) and marked as such.
_SEED = [
    dict(asset_code="suyou", plant_name="景蓝乌尔图储能电站", status="operating",
         zone="二连/苏尼特", capacity_mw=100.0, duration_h=2.0,
         capacity_source="接入系统报告",
         substation="苏尼特500kV变电站", conn_kv="220",
         price_node_own="内蒙.景蓝乌尔图储能电站/220kV.1M",
         price_node_parents="内蒙.苏尼特站/220kV.1M;2M;4M;内蒙.苏尼特站/500kV.1M",
         zone_price_node="内蒙.苏尼特站/220kV.1M",
         ops_data_since="2025-02-12",
         notes="运营数据自 2025-02-12；锡西二投运后同区"),
    dict(asset_code="hangjinqi", plant_name="悦杭独贵储能电站", status="operating",
         zone="谷山梁", capacity_mw=100.0, duration_h=2.0,
         capacity_source="cleared_p99 推断（注册表空缺）",
         substation="谷山梁500kV变电站", conn_kv="220",
         price_node_own="内蒙.悦杭独贵储能电站/220kV.1M",
         price_node_parents="内蒙.谷山梁站/220kV.1M;2M;3M;4M",
         zone_price_node="内蒙.谷山梁站/220kV.1M",
         ops_data_since="2026-01-01",
         notes="与裕昭沙子坝同 220kV 母线区，平日同价、阻塞日分叉"),
    dict(asset_code="siziwangqi", plant_name="景通四益堂储能电站", status="operating",
         zone="杜尔伯特/乌兰察布", capacity_mw=100.0, duration_h=2.0,
         capacity_source="cleared_p99 推断（注册表空缺）",
         substation="杜尔伯特220kV变电站", conn_kv="110",
         price_node_own=None,
         price_node_parents="内蒙.杜尔伯特站/220kV.1M;2M",
         zone_price_node="内蒙.杜尔伯特站/220kV.1M",
         ops_data_since="2026-01-01",
         notes="无自有节点，价格节点=杜尔伯特站220kV"),
    dict(asset_code="gushanliang", plant_name="裕昭沙子坝储能电站", status="operating",
         zone="谷山梁", capacity_mw=500.0, duration_h=2.0,
         capacity_source="接入系统报告（子项目1, 500MW）",
         substation="谷山梁500kV变电站", conn_kv="220",
         price_node_own="内蒙.裕昭沙子坝储能电站/220kV.1M",
         price_node_parents="内蒙.谷山梁站/220kV.1M;2M;3M;4M",
         zone_price_node="内蒙.谷山梁站/220kV.1M",
         ops_data_since="2026-01-06",
         notes="子项目1(500MW)串入子项目2升压站"),
    dict(asset_code="bameng", plant_name="景怡查干哈达储能电站", status="operating",
         zone="河套", capacity_mw=1000.0, duration_h=2.0,
         capacity_source="cleared_p99 推断（注册表空缺）",
         substation="河套500kV变电站", conn_kv="220",
         price_node_own="内蒙.景怡查干哈达储能电站/220kV.1M",
         price_node_parents="内蒙.河套站/220kV.1M;2M;3M;4M",
         zone_price_node="内蒙.河套站/220kV.1M",
         ops_data_since="2026-01-05",
         notes="巴彦淖尔河套新型储能专项行动项目"),
    dict(asset_code="wulate", plant_name="远景乌拉特储能电站", status="operating",
         zone="德岭山/巴彦淖尔", capacity_mw=100.0, duration_h=2.0,
         capacity_source="接入系统报告",
         substation="德岭山500kV变电站", conn_kv="220",
         price_node_own="内蒙.远景乌拉特储能电站/220kV.1M",
         price_node_parents="内蒙.德岭山站/220kV.1M;2M;3M;4M",
         zone_price_node="内蒙.德岭山站/220kV.1M",
         ops_data_since="2025-01-22",
         notes="打捆国电投乌中旗钠离子储能"),
    dict(asset_code="alashan", plant_name="阿拉腾敖包储能电站(规划)", status="upcoming",
         zone="阿拉善", capacity_mw=1000.0, duration_h=4.0,
         capacity_source="接入系统报告2410",
         substation="阿拉腾敖包500kV变电站", conn_kv="220",
         price_node_own=None, price_node_parents=None,
         zone_price_node=None,  # 阿拉腾敖包站尚未出现在节点表 — 用德岭山代理
         notes="约5km接入阿拉腾敖包变220kV侧；价格节点待定，暂用德岭山站代理"),
    dict(asset_code="wuchuan", plant_name="武川储能电站(规划)", status="upcoming",
         zone="武川", capacity_mw=1000.0, duration_h=4.0,
         capacity_source="评审意见+通知",
         substation="武川500kV变电站", conn_kv="220",
         price_node_own=None,
         price_node_parents="内蒙.武川站/220kV.1M;2M;3M;4M;内蒙.武川站/500kV.1M;2M",
         zone_price_node="内蒙.武川站/220kV.1M",
         notes="约10km接入武川变220kV侧"),
    dict(asset_code="xixier", plant_name="锡西二储能电站(在建)", status="upcoming",
         zone="二连/苏尼特", capacity_mw=1000.0, duration_h=4.0,
         capacity_source="接入系统报告1016",
         substation="锡西二500kV变电站", conn_kv="220",
         price_node_own=None, price_node_parents=None,
         zone_price_node="内蒙.苏尼特站/220kV.1M",  # 与苏右同一母站区
         notes="锡西二站在建；投运后与景蓝乌尔图同处二连/苏尼特价格区"),
]


def ensure_table(engine: Engine, schema: str = "marketdata") -> None:
    with engine.begin() as conn:
        conn.execute(sql_text(_DDL.format(schema=schema)))


def seed_if_empty(engine: Engine, schema: str = "marketdata") -> int:
    """Insert the registry seed only when the table is empty. Returns rows inserted."""
    ensure_table(engine, schema)
    with engine.begin() as conn:
        n = conn.execute(sql_text(f"SELECT COUNT(*) FROM {schema}.asset_registry")).scalar()
        if n:
            logger.info("asset_registry already has %d rows — seed skipped", n)
            return 0
        for row in _SEED:
            conn.execute(sql_text(f"""
                INSERT INTO {schema}.asset_registry
                    (asset_code, plant_name, status, zone, capacity_mw, duration_h,
                     capacity_source, substation, conn_kv, price_node_own,
                     price_node_parents, zone_price_node, ops_data_since, notes)
                VALUES (:asset_code, :plant_name, :status, :zone, :capacity_mw,
                        :duration_h, :capacity_source, :substation, :conn_kv,
                        :price_node_own, :price_node_parents, :zone_price_node,
                        :ops_data_since, :notes)
            """), row)
    logger.info("asset_registry seeded with %d rows", len(_SEED))
    return len(_SEED)


def load_assets(engine: Engine, schema: str = "marketdata",
                status: Optional[list[str]] = None) -> pd.DataFrame:
    sql = f"SELECT * FROM {schema}.asset_registry"
    params = {}
    if status:
        sql += " WHERE status = ANY(:status)"
        params["status"] = status
    sql += " ORDER BY CASE status WHEN 'operating' THEN 0 WHEN 'upcoming' THEN 1 ELSE 2 END, zone, asset_code"
    return pd.read_sql(sql_text(sql), engine, params=params)


def fleet_by_zone(engine: Engine, schema: str = "marketdata") -> pd.DataFrame:
    """Operating assets with capacity, grouped by zone_price_node for the
    portfolio cashflow proxy. Assets with no capacity or no price node are
    excluded (reported separately by the caller)."""
    return pd.read_sql(sql_text(f"""
        SELECT asset_code, plant_name, zone, capacity_mw, duration_h,
               zone_price_node, capacity_source
        FROM {schema}.asset_registry
        WHERE status = 'operating' AND capacity_mw IS NOT NULL
          AND zone_price_node IS NOT NULL
        ORDER BY zone_price_node, asset_code
    """), engine)
