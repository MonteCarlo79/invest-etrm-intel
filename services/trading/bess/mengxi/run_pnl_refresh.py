
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 12:47:32 2026

@author: dipeng.chen
"""

# services/trading/bess/mengxi/run_pnl_refresh.py
from __future__ import annotations

import os
import logging
from datetime import date, datetime, timedelta
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from apps.trading.bess.mengxi.pnl_attribution.calc import (
    ASSET_ALIAS_MAP,
    SCENARIOS,
    build_daily_attribution_row,
    build_daily_scenario_rows,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

DB_URL = os.getenv("DB_DSN") or os.getenv("PGURL")
if not DB_URL:
    raise ValueError("Missing DB_DSN / PGURL")

ENGINE = create_engine(DB_URL)
DEFAULT_COMPENSATION_YUAN_PER_MWH = float(
    os.getenv("DEFAULT_COMPENSATION_YUAN_PER_MWH", "350"))
REFRESH_LOOKBACK_DAYS = int(os.getenv("PNL_REFRESH_LOOKBACK_DAYS", "7"))
ENABLE_CANON_COMPAT_VIEWS = os.getenv("PNL_ENABLE_CANON_COMPAT_VIEWS", "0").strip().lower() in ("1", "true", "yes", "on")
PREFER_DIRECT_MD = os.getenv("PNL_PREFER_DIRECT_MD", "1").strip().lower() in ("1", "true", "yes", "on")


def _normalize_time_column(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "time" not in df.columns:
        return df
    out = df.copy()
    out["time"] = pd.to_datetime(out["time"], errors="coerce")
    try:
        if getattr(out["time"].dt, "tz", None) is not None:
            out["time"] = out["time"].dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    except Exception:
        try:
            out["time"] = out["time"].dt.tz_localize(None)
        except Exception:
            pass
    return out


def _relation_exists(engine: Engine, schema_name: str, relation_name: str) -> bool:
    sql = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema_name
              AND table_name = :relation_name
        )
        OR EXISTS (
            SELECT 1
            FROM information_schema.views
            WHERE table_schema = :schema_name
              AND table_name = :relation_name
        ) AS exists_flag
        """
    )
    with engine.begin() as con:
        val = con.execute(
            sql,
            {"schema_name": schema_name, "relation_name": relation_name},
        ).scalar()
    return bool(val)


def _find_relation_schema(
    engine: Engine,
    relation_name: str,
    candidate_schemas: tuple[str, ...],
) -> str | None:
    sql = text(
        """
        SELECT table_schema
        FROM (
            SELECT table_schema, table_name FROM information_schema.tables
            UNION ALL
            SELECT table_schema, table_name FROM information_schema.views
        ) t
        WHERE t.table_name = :relation_name
          AND t.table_schema = ANY(:candidate_schemas)
        ORDER BY array_position(:candidate_schemas, t.table_schema)
        LIMIT 1
        """
    )
    with engine.begin() as con:
        row = con.execute(
            sql,
            {"relation_name": relation_name, "candidate_schemas": list(candidate_schemas)},
        ).first()
    return str(row[0]) if row else None


def ensure_canonical_compat_views(engine: Engine) -> None:
    """
    Create additive compatibility views used by the P&L refresh job.

    These are bridge views so the job can run against existing production tables
    before full canonical pipelines are in place.
    """
    with engine.begin() as con:
        con.execute(text("CREATE SCHEMA IF NOT EXISTS canon"))

    price_sources = {
        "suyou": "hist_mengxi_suyou_clear_15min",
        "wulate": "hist_mengxi_wulate_clear_15min",
        "wuhai": "hist_mengxi_wuhai_clear_15min",
        "wulanchabu": "hist_mengxi_wulanchabu_clear_15min",
    }
    price_selects = []
    for asset_code, table_name in price_sources.items():
        source_schema = _find_relation_schema(
            engine,
            table_name,
            candidate_schemas=("public", "marketdata"),
        )
        if source_schema:
            price_selects.append(
                f"""
                SELECT
                    time::timestamptz AS time,
                    '{asset_code}'::text AS asset_code,
                    price::numeric AS price
                FROM {source_schema}."{table_name}"
                WHERE time IS NOT NULL
                """
            )

    alias_table_exists = _relation_exists(engine, "core", "asset_alias_map")
    md_rt_schema = _find_relation_schema(
        engine,
        "md_rt_nodal_price",
        candidate_schemas=("marketdata", "public"),
    )
    if (not price_selects) and md_rt_schema and alias_table_exists:
        price_selects.append(
            f"""
            SELECT
                p.datetime::timestamptz AS time,
                a.asset_code::text AS asset_code,
                AVG(p.node_price::numeric) AS price
            FROM {md_rt_schema}.md_rt_nodal_price p
            JOIN core.asset_alias_map a
              ON a.active_flag = TRUE
             AND LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(p.node_name, '')))
            WHERE a.asset_code IN (
                'suyou',
                'wulate',
                'wuhai',
                'wulanchabu',
                'hetao',
                'hangjinqi',
                'siziwangqi',
                'gushanliang'
            )
              AND p.datetime IS NOT NULL
              AND p.node_price IS NOT NULL
            GROUP BY p.datetime, a.asset_code
            """
        )

    if price_selects:
        nodal_view_sql = f"""
            CREATE OR REPLACE VIEW canon.nodal_rt_price_15min AS
            {" UNION ALL ".join(price_selects)}
        """
    else:
        nodal_view_sql = """
            CREATE OR REPLACE VIEW canon.nodal_rt_price_15min AS
            SELECT
                NULL::timestamptz AS time,
                NULL::text AS asset_code,
                NULL::numeric AS price
            WHERE FALSE
        """

    md_table_schema = _find_relation_schema(
        engine,
        "md_id_cleared_energy",
        candidate_schemas=("marketdata", "public"),
    )
    md_table_exists = md_table_schema is not None
    dispatch_view_sql = """
        CREATE OR REPLACE VIEW canon.scenario_dispatch_15min AS
        SELECT
            NULL::timestamptz AS time,
            NULL::text AS asset_code,
            NULL::text AS scenario_name,
            NULL::numeric AS dispatch_mw
        WHERE FALSE
    """
    if md_table_exists and alias_table_exists:
        dispatch_view_sql = f"""
            CREATE OR REPLACE VIEW canon.scenario_dispatch_15min AS
            WITH mapped AS (
                SELECT
                    m.datetime::timestamptz AS time,
                    a.asset_code::text AS asset_code,
                    MAX((m.cleared_energy_mwh::numeric) * 4.0) AS dispatch_mw
                FROM {md_table_schema}.md_id_cleared_energy m
                JOIN core.asset_alias_map a
                  ON a.active_flag = TRUE
                 AND (
                    LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.plant_name, '')))
                    OR LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.dispatch_unit_name, '')))
                 )
                WHERE a.asset_code IN (
                    'suyou',
                    'wulate',
                    'wuhai',
                    'wulanchabu',
                    'hetao',
                    'hangjinqi',
                    'siziwangqi',
                    'gushanliang'
                )
                  AND m.datetime IS NOT NULL
                  AND m.cleared_energy_mwh IS NOT NULL
                GROUP BY m.datetime, a.asset_code
            )
            SELECT
                time,
                asset_code,
                'cleared_actual'::text AS scenario_name,
                dispatch_mw
            FROM mapped
        """
    elif md_table_exists:
        dispatch_view_sql = f"""
            CREATE OR REPLACE VIEW canon.scenario_dispatch_15min AS
            SELECT
                m.datetime::timestamptz AS time,
                CASE
                    WHEN LOWER(COALESCE(m.plant_name, '') || ' ' || COALESCE(m.dispatch_unit_name, '')) LIKE '%suyou%' THEN 'suyou'
                    WHEN LOWER(COALESCE(m.plant_name, '') || ' ' || COALESCE(m.dispatch_unit_name, '')) LIKE '%wulate%' THEN 'wulate'
                    WHEN LOWER(COALESCE(m.plant_name, '') || ' ' || COALESCE(m.dispatch_unit_name, '')) LIKE '%wuhai%' THEN 'wuhai'
                    WHEN LOWER(COALESCE(m.plant_name, '') || ' ' || COALESCE(m.dispatch_unit_name, '')) LIKE '%wulanchabu%' THEN 'wulanchabu'
                    ELSE NULL
                END::text AS asset_code,
                'cleared_actual'::text AS scenario_name,
                (m.cleared_energy_mwh::numeric) * 4.0 AS dispatch_mw
            FROM {md_table_schema}.md_id_cleared_energy m
            WHERE m.datetime IS NOT NULL
              AND m.cleared_energy_mwh IS NOT NULL
              AND (
                LOWER(COALESCE(m.plant_name, '') || ' ' || COALESCE(m.dispatch_unit_name, '')) LIKE '%suyou%'
                OR LOWER(COALESCE(m.plant_name, '') || ' ' || COALESCE(m.dispatch_unit_name, '')) LIKE '%wulate%'
                OR LOWER(COALESCE(m.plant_name, '') || ' ' || COALESCE(m.dispatch_unit_name, '')) LIKE '%wuhai%'
                OR LOWER(COALESCE(m.plant_name, '') || ' ' || COALESCE(m.dispatch_unit_name, '')) LIKE '%wulanchabu%'
              )
        """

    with engine.begin() as con:
        con.execute(text(nodal_view_sql))
        con.execute(text(dispatch_view_sql))


def ensure_report_tables(engine: Engine) -> None:
    ddl = """
    CREATE SCHEMA IF NOT EXISTS reports;
    CREATE SCHEMA IF NOT EXISTS core;

    CREATE TABLE IF NOT EXISTS reports.bess_asset_daily_scenario_pnl (
        trade_date date NOT NULL,
        asset_code text NOT NULL,
        scenario_name text NOT NULL,
        scenario_available boolean NOT NULL,
        compensation_yuan_per_mwh numeric,
        market_revenue_yuan numeric,
        subsidy_revenue_yuan numeric,
        total_revenue_yuan numeric,
        discharge_mwh numeric,
        charge_mwh numeric,
        avg_daily_cycles numeric,
        compensation_blocked boolean,
        compensation_block_reason text,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (trade_date, asset_code, scenario_name)
    );
    CREATE TABLE IF NOT EXISTS core.asset_monthly_compensation (
        asset_code text NOT NULL,
        effective_month date NOT NULL,
        compensation_yuan_per_mwh numeric NOT NULL,
        source_system text,
        notes text,
        active_flag boolean NOT NULL DEFAULT TRUE,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (asset_code, effective_month)
    );

    CREATE TABLE IF NOT EXISTS reports.bess_asset_daily_attribution (
        trade_date date NOT NULL,
        asset_code text NOT NULL,
        pf_unrestricted_pnl numeric,
        pf_grid_feasible_pnl numeric,
        cleared_actual_pnl numeric,
        nominated_pnl numeric,
        tt_forecast_optimal_pnl numeric,
        tt_strategy_pnl numeric,
        grid_restriction_loss numeric,
        forecast_error_loss numeric,
        strategy_error_loss numeric,
        nomination_loss numeric,
        execution_clearing_loss numeric,
        realisation_gap_vs_pf numeric,
        realisation_gap_vs_pf_grid numeric,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (trade_date, asset_code)
    );
    """
    with engine.begin() as con:
        for stmt in ddl.split(";"):
            sql = stmt.strip()
            if sql:
                con.execute(text(sql))
        con.execute(text("""
            ALTER TABLE reports.bess_asset_daily_scenario_pnl
            ADD COLUMN IF NOT EXISTS compensation_blocked boolean
        """))
        con.execute(text("""
            ALTER TABLE reports.bess_asset_daily_scenario_pnl
            ADD COLUMN IF NOT EXISTS compensation_block_reason text
        """))
        con.execute(text("""
            ALTER TABLE reports.bess_asset_daily_scenario_pnl
            ADD COLUMN IF NOT EXISTS interval_hours numeric
        """))

        # --- core.asset_alias_map: create table + seed (idempotent) ---
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS core.asset_alias_map (
                asset_code  text        NOT NULL,
                alias_type  text        NOT NULL,
                alias_value text        NOT NULL,
                province    text,
                city_cn     text,
                active_flag boolean     NOT NULL DEFAULT TRUE,
                created_at  timestamptz NOT NULL DEFAULT now(),
                updated_at  timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (asset_code, alias_type, alias_value)
            )
        """))
        con.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_asset_alias_map_alias_value
                ON core.asset_alias_map (lower(alias_value))
        """))
        con.execute(text("""
            INSERT INTO core.asset_alias_map
                (asset_code, alias_type, alias_value, province, city_cn)
            VALUES
                ('suyou','dispatch_unit_name_cn','景蓝乌尔图储能电站','Mengxi','锡林郭勒'),
                ('suyou','short_name_cn','苏右储能','Mengxi','锡林郭勒'),
                ('suyou','display_name_cn','苏右','Mengxi','锡林郭勒'),
                ('suyou','tt_asset_name_en','SuYou','Mengxi','锡林郭勒'),
                ('suyou','market_key','Mengxi_SuYou','Mengxi','锡林郭勒'),
                ('wulate','dispatch_unit_name_cn','远景乌拉特储能电站','Mengxi','巴彦淖尔'),
                ('wulate','short_name_cn','乌拉特中期储能','Mengxi','巴彦淖尔'),
                ('wulate','display_name_cn','乌拉特','Mengxi','巴彦淖尔'),
                ('wulate','tt_asset_name_en','WuLaTe','Mengxi','巴彦淖尔'),
                ('wulate','market_key','Mengxi_WuLaTe','Mengxi','巴彦淖尔'),
                ('wuhai','dispatch_unit_name_cn','富景五虎山储能电站','Mengxi','乌海'),
                ('wuhai','short_name_cn','乌海储能','Mengxi','乌海'),
                ('wuhai','display_name_cn','乌海','Mengxi','乌海'),
                ('wuhai','tt_asset_name_en','WuHai','Mengxi','乌海'),
                ('wuhai','market_key','Mengxi_WuHai','Mengxi','乌海'),
                ('wulanchabu','dispatch_unit_name_cn','景通红丰储能电站','Mengxi','乌兰察布'),
                ('wulanchabu','short_name_cn','乌兰察布储能','Mengxi','乌兰察布'),
                ('wulanchabu','display_name_cn','乌兰察布','Mengxi','乌兰察布'),
                ('wulanchabu','tt_asset_name_en','WuLanChaBu','Mengxi','乌兰察布'),
                ('wulanchabu','market_key','Mengxi_WuLanChaBu','Mengxi','乌兰察布'),
                ('hetao','dispatch_unit_name_cn','景怡查干哈达储能电站','Mengxi','巴彦淖尔'),
                ('hetao','short_name_cn','河套储能','Mengxi','巴彦淖尔'),
                ('hetao','display_name_cn','河套','Mengxi','巴彦淖尔'),
                ('hangjinqi','dispatch_unit_name_cn','悦杭独贵储能电站','Mengxi','鄂尔多斯'),
                ('hangjinqi','short_name_cn','杭锦旗储能','Mengxi','鄂尔多斯'),
                ('hangjinqi','display_name_cn','杭锦旗','Mengxi','鄂尔多斯'),
                ('siziwangqi','dispatch_unit_name_cn','景通四益堂储能电站','Mengxi','乌兰察布'),
                ('siziwangqi','short_name_cn','四子王旗储能','Mengxi','乌兰察布'),
                ('siziwangqi','display_name_cn','四子王旗','Mengxi','乌兰察布'),
                ('gushanliang','dispatch_unit_name_cn','裕昭沙子坝储能电站','Mengxi','鄂尔多斯'),
                ('gushanliang','short_name_cn','谷山梁储能','Mengxi','鄂尔多斯'),
                ('gushanliang','display_name_cn','谷山梁','Mengxi','鄂尔多斯')
            ON CONFLICT (asset_code, alias_type, alias_value) DO NOTHING
        """))
    logger.info("core.asset_alias_map: table and seed verified")


def fetch_asset_monthly_compensation(engine: Engine) -> pd.DataFrame:
    sql = text("""
        SELECT
            asset_code,
            effective_month,
            compensation_yuan_per_mwh,
            source_system,
            notes
        FROM core.asset_monthly_compensation
        WHERE active_flag = TRUE
    """)
    try:
        return pd.read_sql(sql, engine)
    
    except Exception:
        return pd.DataFrame(
            columns=[
                "asset_code",
                "effective_month",
                "compensation_yuan_per_mwh",
                "source_system",
                "notes",
            ]
        )
def fetch_scenario_availability(engine: Engine) -> pd.DataFrame:
    sql = text("""
        SELECT asset_code, scenario_name, available_flag
        FROM core.asset_scenario_availability
        WHERE active_flag = TRUE
    """)
    try:
        return pd.read_sql(sql, engine)
    except Exception:
        rows = []
        from apps.trading.bess.mengxi.pnl_attribution.calc import scenario_availability_df
        return scenario_availability_df()


def fetch_compensation_coverage(engine: Engine) -> pd.DataFrame:
    """
    Coverage helper:
    - discharge_known=True if settlement extraction exists for asset/month
    - compensation_known=True if compensation extraction exists for asset/month
    """
    sql = text("""
        WITH settlement AS (
            SELECT asset_code, settlement_month::date AS effective_month
            FROM staging.mengxi_settlement_extracted
            WHERE asset_code IS NOT NULL
              AND settlement_month IS NOT NULL
              AND discharge_mwh IS NOT NULL
              AND parse_confidence != 'pending'
            GROUP BY asset_code, settlement_month
        ),
        compensation AS (
            SELECT asset_code, settlement_month::date AS effective_month
            FROM staging.mengxi_compensation_extracted
            WHERE asset_code IS NOT NULL
              AND settlement_month IS NOT NULL
              AND compensation_yuan IS NOT NULL
              AND parse_confidence != 'pending'
            GROUP BY asset_code, settlement_month
        )
        SELECT
            COALESCE(s.asset_code, c.asset_code) AS asset_code,
            COALESCE(s.effective_month, c.effective_month) AS effective_month,
            (s.asset_code IS NOT NULL) AS discharge_known,
            (c.asset_code IS NOT NULL) AS compensation_known
        FROM settlement s
        FULL OUTER JOIN compensation c
          ON s.asset_code = c.asset_code
         AND s.effective_month = c.effective_month
    """)
    try:
        return pd.read_sql(sql, engine)
    except Exception:
        return pd.DataFrame(
            columns=["asset_code", "effective_month", "discharge_known", "compensation_known"]
        )


def fetch_trade_dates(engine: Engine, lookback_days: int) -> list[date]:
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=lookback_days)
    return [start_dt + timedelta(days=i) for i in range((end_dt - start_dt).days + 1)]


def load_actual_price(engine: Engine, asset_code: str, trade_date: date) -> pd.DataFrame:
    """
    v1 assumption:
    - canonical view exists and resolves whichever source is authoritative.
    """
    start_ts = pd.Timestamp(trade_date)
    end_ts = start_ts + pd.Timedelta(days=1)

    md_schema = _find_relation_schema(engine, "md_id_cleared_energy", ("marketdata", "public"))
    if PREFER_DIRECT_MD and md_schema:
        if _relation_exists(engine, "core", "asset_alias_map"):
            fb_sql = text(
                f"""
                SELECT
                    m.datetime AS time,
                    AVG(m.cleared_price::numeric) AS price
                FROM {md_schema}.md_id_cleared_energy m
                JOIN core.asset_alias_map a
                  ON a.active_flag = TRUE
                 AND a.asset_code = :asset_code
                 AND (
                    LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.plant_name, '')))
                    OR LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.dispatch_unit_name, '')))
                 )
                WHERE m.datetime >= :start_ts
                  AND m.datetime < :end_ts
                  AND m.cleared_price IS NOT NULL
                GROUP BY m.datetime
                ORDER BY m.datetime
                """
            )
            alias_df = pd.read_sql(
                fb_sql,
                engine,
                params={"asset_code": asset_code, "start_ts": start_ts, "end_ts": end_ts},
            )
            alias_df = _normalize_time_column(alias_df)
            if not alias_df.empty:
                return alias_df
            logger.debug(
                "load_actual_price branch=direct_md_alias empty asset=%s date=%s", asset_code, trade_date
            )

    sql = text("""
        SELECT time, price
        FROM canon.nodal_rt_price_15min
        WHERE asset_code = :asset_code
          AND time >= :start_ts
          AND time < :end_ts
        ORDER BY time
    """)
    df = pd.read_sql(sql, engine, params={"asset_code": asset_code, "start_ts": start_ts, "end_ts": end_ts})
    df = _normalize_time_column(df)
    if not df.empty:
        return df
    logger.debug(
        "load_actual_price branch=canon_view empty asset=%s date=%s", asset_code, trade_date
    )

    if not md_schema:
        logger.debug(
            "load_actual_price branch=no_md_schema asset=%s date=%s", asset_code, trade_date
        )
        return df

    if _relation_exists(engine, "core", "asset_alias_map"):
        fb_sql = text(
            f"""
            SELECT
                m.datetime AS time,
                AVG(m.cleared_price::numeric) AS price
            FROM {md_schema}.md_id_cleared_energy m
            JOIN core.asset_alias_map a
              ON a.active_flag = TRUE
             AND a.asset_code = :asset_code
             AND (
                LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.plant_name, '')))
                OR LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.dispatch_unit_name, '')))
             )
            WHERE m.datetime >= :start_ts
              AND m.datetime < :end_ts
              AND m.cleared_price IS NOT NULL
            GROUP BY m.datetime
            ORDER BY m.datetime
            """
        )
        alias_df = pd.read_sql(
            fb_sql,
            engine,
            params={"asset_code": asset_code, "start_ts": start_ts, "end_ts": end_ts},
        )
        alias_df = _normalize_time_column(alias_df)
        if not alias_df.empty:
            return alias_df
        logger.debug(
            "load_actual_price branch=fallback_alias empty asset=%s date=%s", asset_code, trade_date
        )

    fb_sql = text(
        f"""
        SELECT
            m.datetime AS time,
            AVG(m.cleared_price::numeric) AS price
        FROM {md_schema}.md_id_cleared_energy m
        WHERE m.datetime >= :start_ts
          AND m.datetime < :end_ts
          AND m.cleared_price IS NOT NULL
          AND LOWER(COALESCE(m.plant_name, '') || ' ' || COALESCE(m.dispatch_unit_name, '')) LIKE :asset_like
        GROUP BY m.datetime
        ORDER BY m.datetime
        """
    )
    fb_df = pd.read_sql(
        fb_sql,
        engine,
        params={"start_ts": start_ts, "end_ts": end_ts, "asset_like": f"%{asset_code.lower()}%"},
    )
    fb_df = _normalize_time_column(fb_df)
    if fb_df.empty:
        logger.debug(
            "load_actual_price branch=fallback_like empty asset=%s date=%s", asset_code, trade_date
        )
    return fb_df


def load_dispatch_scenario(engine: Engine, asset_code: str, scenario_name: str, trade_date: date) -> pd.DataFrame:
    start_ts = pd.Timestamp(trade_date)
    end_ts = start_ts + pd.Timedelta(days=1)
    md_schema = _find_relation_schema(engine, "md_id_cleared_energy", ("marketdata", "public"))
    if PREFER_DIRECT_MD and scenario_name == "cleared_actual" and md_schema:
        if _relation_exists(engine, "core", "asset_alias_map"):
            fb_sql = text(
                f"""
                SELECT
                    m.datetime AS time,
                    AVG((m.cleared_energy_mwh::numeric) * 4.0) AS dispatch_mw
                FROM {md_schema}.md_id_cleared_energy m
                JOIN core.asset_alias_map a
                  ON a.active_flag = TRUE
                 AND a.asset_code = :asset_code
                 AND (
                    LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.plant_name, '')))
                    OR LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.dispatch_unit_name, '')))
                 )
                WHERE m.datetime >= :start_ts
                  AND m.datetime < :end_ts
                  AND m.cleared_energy_mwh IS NOT NULL
                GROUP BY m.datetime
                ORDER BY m.datetime
                """
            )
            alias_df = pd.read_sql(
                fb_sql,
                engine,
                params={"asset_code": asset_code, "start_ts": start_ts, "end_ts": end_ts},
            )
            alias_df = _normalize_time_column(alias_df)
            if not alias_df.empty:
                return alias_df
            logger.debug(
                "load_dispatch_scenario branch=direct_md_alias empty asset=%s scenario=%s date=%s",
                asset_code, scenario_name, trade_date,
            )

    sql = text("""
        SELECT time, dispatch_mw
        FROM canon.scenario_dispatch_15min
        WHERE asset_code = :asset_code
          AND scenario_name = :scenario_name
          AND time >= :start_ts
          AND time < :end_ts
        ORDER BY time
    """)
    df = pd.read_sql(
        sql,
        engine,
        params={
            "asset_code": asset_code,
            "scenario_name": scenario_name,
            "start_ts": start_ts,
            "end_ts": end_ts,
        },
    )
    df = _normalize_time_column(df)
    if not df.empty or scenario_name != "cleared_actual":
        if df.empty:
            logger.debug(
                "load_dispatch_scenario branch=canon_view empty asset=%s scenario=%s date=%s",
                asset_code, scenario_name, trade_date,
            )
        return df

    if not md_schema:
        logger.debug(
            "load_dispatch_scenario branch=no_md_schema asset=%s scenario=%s date=%s",
            asset_code, scenario_name, trade_date,
        )
        return df

    if _relation_exists(engine, "core", "asset_alias_map"):
        fb_sql = text(
            f"""
            SELECT
                m.datetime AS time,
                AVG((m.cleared_energy_mwh::numeric) * 4.0) AS dispatch_mw
            FROM {md_schema}.md_id_cleared_energy m
            JOIN core.asset_alias_map a
              ON a.active_flag = TRUE
             AND a.asset_code = :asset_code
             AND (
                LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.plant_name, '')))
                OR LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.dispatch_unit_name, '')))
             )
            WHERE m.datetime >= :start_ts
              AND m.datetime < :end_ts
              AND m.cleared_energy_mwh IS NOT NULL
            GROUP BY m.datetime
            ORDER BY m.datetime
            """
        )
        alias_df = pd.read_sql(
            fb_sql,
            engine,
            params={"asset_code": asset_code, "start_ts": start_ts, "end_ts": end_ts},
        )
        alias_df = _normalize_time_column(alias_df)
        if not alias_df.empty:
            return alias_df
        logger.debug(
            "load_dispatch_scenario branch=fallback_alias empty asset=%s scenario=%s date=%s",
            asset_code, scenario_name, trade_date,
        )

    fb_sql = text(
        f"""
        SELECT
            m.datetime AS time,
            AVG((m.cleared_energy_mwh::numeric) * 4.0) AS dispatch_mw
        FROM {md_schema}.md_id_cleared_energy m
        WHERE m.datetime >= :start_ts
          AND m.datetime < :end_ts
          AND m.cleared_energy_mwh IS NOT NULL
          AND LOWER(COALESCE(m.plant_name, '') || ' ' || COALESCE(m.dispatch_unit_name, '')) LIKE :asset_like
        GROUP BY m.datetime
        ORDER BY m.datetime
        """
    )
    fb_df = pd.read_sql(
        fb_sql,
        engine,
        params={"start_ts": start_ts, "end_ts": end_ts, "asset_like": f"%{asset_code.lower()}%"},
    )
    fb_df = _normalize_time_column(fb_df)
    if fb_df.empty:
        logger.debug(
            "load_dispatch_scenario branch=fallback_like empty asset=%s scenario=%s date=%s",
            asset_code, scenario_name, trade_date,
        )
    return fb_df


def build_availability_map(df: pd.DataFrame, asset_code: str) -> Dict[str, bool]:
    out = {s: False for s in SCENARIOS}
    hit = df[df["asset_code"] == asset_code]
    for _, row in hit.iterrows():
        out[str(row["scenario_name"])] = bool(row["available_flag"])
    return out


def upsert_df(engine: Engine, table_name: str, df: pd.DataFrame, pk_cols: list[str]) -> None:
    if df.empty:
        return

    stage_name = f"_tmp_{table_name.replace('.', '_')}_{int(pd.Timestamp.now('UTC').timestamp())}"
    schema_name, bare_name = table_name.split(".", 1)

    with engine.begin() as con:
        df.to_sql(stage_name, con=con, schema=schema_name, if_exists="replace", index=False)

        cols = list(df.columns)
        insert_cols = ", ".join(cols)
        select_cols = ", ".join(cols)
        update_cols = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in pk_cols])

        con.execute(
            text(f"""
                INSERT INTO {table_name} ({insert_cols})
                SELECT {select_cols}
                FROM {schema_name}."{stage_name}"
                ON CONFLICT ({", ".join(pk_cols)})
                DO UPDATE SET
                    {update_cols},
                    updated_at = now()
            """)
        )
        con.execute(text(f'DROP TABLE IF EXISTS {schema_name}."{stage_name}"'))


def main() -> None:
    logger.info("Starting Mengxi P&L refresh")
    ensure_report_tables(ENGINE)
    if ENABLE_CANON_COMPAT_VIEWS:
        ensure_canonical_compat_views(ENGINE)
        logger.info("Canon compatibility views refreshed")
    else:
        logger.info("Skipping canon compatibility view refresh (PNL_ENABLE_CANON_COMPAT_VIEWS disabled)")

    # --- source table diagnostics ---
    _md_schema = _find_relation_schema(ENGINE, "md_id_cleared_energy", ("marketdata", "public"))
    if _md_schema:
        with ENGINE.begin() as _c:
            _src_count = _c.execute(text(
                f"SELECT COUNT(*) FROM {_md_schema}.md_id_cleared_energy "
                f"WHERE datetime >= current_date - interval '{REFRESH_LOOKBACK_DAYS} days'"
            )).scalar()
        logger.info("md_id_cleared_energy rows in lookback window (%d days): %s", REFRESH_LOOKBACK_DAYS, _src_count)
    else:
        logger.warning("md_id_cleared_energy not found in any schema — dispatch data unavailable")
    with ENGINE.begin() as _c:
        _alias_count = _c.execute(text(
            "SELECT COUNT(*) FROM core.asset_alias_map WHERE active_flag=TRUE"
        )).scalar()
    logger.info("core.asset_alias_map active rows: %s", _alias_count)

    availability_df = fetch_scenario_availability(ENGINE)
    dates = fetch_trade_dates(ENGINE, REFRESH_LOOKBACK_DAYS)
    logger.info("Processing %d trade dates for %d assets", len(dates), len(ASSET_ALIAS_MAP))
    compensation_df = fetch_asset_monthly_compensation(ENGINE)
    compensation_coverage_df = fetch_compensation_coverage(ENGINE)
    scenario_rows_all = []
    attribution_rows_all = []

    for trade_date in dates:
        for asset_code in ASSET_ALIAS_MAP.keys():
            availability_map = build_availability_map(availability_df, asset_code)
            if not any(availability_map.values()):
                continue

            actual_price_df = load_actual_price(ENGINE, asset_code, trade_date)
            if actual_price_df.empty:
                logger.warning("No actual price found for asset=%s trade_date=%s", asset_code, trade_date)
                continue

            scenario_dispatch_map = {}
            for scenario_name, available_flag in availability_map.items():
                if not available_flag:
                    continue
                scenario_dispatch_map[scenario_name] = load_dispatch_scenario(
                    ENGINE, asset_code, scenario_name, trade_date
                )
            empty_scenarios = [s for s, df in scenario_dispatch_map.items() if df is None or df.empty]
            if empty_scenarios:
                logger.debug(
                    "asset=%s date=%s empty dispatch scenarios: %s", asset_code, trade_date, empty_scenarios
                )

            scenario_rows = build_daily_scenario_rows(
                trade_date=pd.Timestamp(trade_date),
                asset_code=asset_code,
                actual_price_df=actual_price_df,
                scenario_dispatch_map=scenario_dispatch_map,
                availability_map=availability_map,
                compensation_df=compensation_df,
                compensation_coverage_df=compensation_coverage_df,
                default_compensation_yuan_per_mwh=DEFAULT_COMPENSATION_YUAN_PER_MWH,
            )
            attribution_row = build_daily_attribution_row(scenario_rows)

            scenario_rows_all.append(scenario_rows)
            attribution_rows_all.append(attribution_row)

    if scenario_rows_all:
        scenario_df = pd.concat(scenario_rows_all, ignore_index=True)
        upsert_df(
            ENGINE,
            "reports.bess_asset_daily_scenario_pnl",
            scenario_df,
            pk_cols=["trade_date", "asset_code", "scenario_name"],
        )
        _assets = sorted(scenario_df["asset_code"].dropna().unique().tolist())
        _n_dates = scenario_df["trade_date"].nunique()
        _n_scenarios = scenario_df["scenario_name"].nunique()
        logger.info(
            "Wrote %d scenario rows to reports.bess_asset_daily_scenario_pnl "
            "(assets=%s, dates=%d, scenarios=%d)",
            len(scenario_df), _assets, _n_dates, _n_scenarios,
        )
    else:
        logger.warning("No scenario rows produced — reports.bess_asset_daily_scenario_pnl NOT updated")

    if attribution_rows_all:
        attribution_df = pd.concat(attribution_rows_all, ignore_index=True)
        upsert_df(
            ENGINE,
            "reports.bess_asset_daily_attribution",
            attribution_df,
            pk_cols=["trade_date", "asset_code"],
        )
        logger.info(
            "Wrote %d attribution rows to reports.bess_asset_daily_attribution",
            len(attribution_df),
        )
    else:
        logger.warning("No attribution rows produced — reports.bess_asset_daily_attribution NOT updated")

    logger.info("Mengxi P&L refresh completed")


if __name__ == "__main__":
    main()