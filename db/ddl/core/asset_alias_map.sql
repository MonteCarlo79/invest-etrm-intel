-- db/ddl/core/asset_alias_map.sql
-- Mengxi trading foundation: asset alias mapping
-- Created: 2026-03-24
-- Updated: 2026-03-25
-- Author: Matrix Agent

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.asset_alias_map (
    asset_code      text        NOT NULL,
    alias_type      text        NOT NULL,
    alias_value     text        NOT NULL,
    province        text,
    city_cn         text,
    active_flag     boolean     NOT NULL DEFAULT TRUE,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_code, alias_type, alias_value)
);

CREATE INDEX IF NOT EXISTS idx_asset_alias_map_alias_value
    ON core.asset_alias_map (lower(alias_value));

CREATE INDEX IF NOT EXISTS idx_asset_alias_map_asset_code
    ON core.asset_alias_map (asset_code);

COMMENT ON TABLE core.asset_alias_map IS 'Maps stable asset_code to various naming systems (dispatch names, TT keys, short names, etc.)';
COMMENT ON COLUMN core.asset_alias_map.asset_code IS 'Stable internal asset identifier (e.g., suyou, wulate)';
COMMENT ON COLUMN core.asset_alias_map.alias_type IS 'Type of alias: dispatch_unit_name_cn, short_name_cn, display_name_cn, tt_asset_name_en, market_key';
COMMENT ON COLUMN core.asset_alias_map.alias_value IS 'The alias value in that naming system';
COMMENT ON COLUMN core.asset_alias_map.province IS 'Province/grid region (e.g., Mengxi)';
COMMENT ON COLUMN core.asset_alias_map.city_cn IS 'City in Chinese (e.g., 锡林郭勒)';
