-- db/ddl/core/asset_scenario_availability.sql
-- Mengxi trading foundation: scenario availability by asset
-- Created: 2026-03-25
-- Author: Matrix Agent

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.asset_scenario_availability (
    asset_code      text        NOT NULL,
    scenario_name   text        NOT NULL,
    available_flag  boolean     NOT NULL,
    source_system   text,
    notes           text,
    active_flag     boolean     NOT NULL DEFAULT TRUE,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_code, scenario_name)
);

COMMENT ON TABLE core.asset_scenario_availability IS 'Tracks which P&L attribution scenarios are available for each asset';
COMMENT ON COLUMN core.asset_scenario_availability.asset_code IS 'Stable internal asset identifier (e.g., suyou, wulate)';
COMMENT ON COLUMN core.asset_scenario_availability.scenario_name IS 'Scenario identifier: perfect_foresight_unrestricted, perfect_foresight_grid_feasible, cleared_actual, nominated_dispatch, tt_forecast_optimal, tt_strategy';
COMMENT ON COLUMN core.asset_scenario_availability.available_flag IS 'TRUE if this scenario can be computed for this asset';
COMMENT ON COLUMN core.asset_scenario_availability.source_system IS 'Data source: optimizer, mengxi_ingestion, manual_excel, tt_enos';
