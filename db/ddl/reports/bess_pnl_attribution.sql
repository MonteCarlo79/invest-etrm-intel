-- db/ddl/reports/bess_pnl_attribution.sql
-- BESS P&L Attribution Tables
-- Created: 2026-03-26
-- Author: Matrix Agent
--
-- Purpose: Store daily P&L attribution results for BESS assets.
-- Supports coverage-aware scenarios by asset.
-- Uses monthly compensation from core.asset_monthly_compensation (350 is fallback only).

CREATE SCHEMA IF NOT EXISTS reports;

-- =============================================================================
-- Daily scenario P&L by asset
-- =============================================================================
CREATE TABLE IF NOT EXISTS reports.bess_asset_daily_scenario_pnl (
    pnl_id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identity
    asset_code          text        NOT NULL,
    trade_date          date        NOT NULL,
    scenario_name       text        NOT NULL,  -- From core.asset_scenario_availability
    
    -- Energy volumes (MWh)
    discharge_mwh       numeric,
    charge_mwh          numeric,
    net_energy_mwh      numeric,    -- discharge - charge
    
    -- Prices (yuan/MWh, volume-weighted averages)
    avg_discharge_price numeric,
    avg_charge_price    numeric,
    spread              numeric,    -- avg_discharge_price - avg_charge_price
    
    -- Revenue components (yuan)
    market_revenue      numeric,    -- discharge_mwh * avg_discharge_price
    charge_cost         numeric,    -- charge_mwh * avg_charge_price (positive value)
    arbitrage_pnl       numeric,    -- market_revenue - charge_cost
    
    -- Compensation (yuan)
    compensation_rate   numeric,    -- yuan/MWh from core.asset_monthly_compensation
    compensation_revenue numeric,   -- discharge_mwh * compensation_rate
    
    -- Total P&L (yuan)
    total_pnl           numeric,    -- arbitrage_pnl + compensation_revenue
    
    -- Metadata
    source_system       text,       -- 'optimizer', 'mengxi_ingestion', 'tt_enos', etc.
    computed_at         timestamptz NOT NULL DEFAULT now(),
    
    -- Audit
    active_flag         boolean     NOT NULL DEFAULT TRUE,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    
    UNIQUE (asset_code, trade_date, scenario_name)
);

CREATE INDEX IF NOT EXISTS idx_bess_scenario_pnl_asset ON reports.bess_asset_daily_scenario_pnl (asset_code);
CREATE INDEX IF NOT EXISTS idx_bess_scenario_pnl_date ON reports.bess_asset_daily_scenario_pnl (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_bess_scenario_pnl_scenario ON reports.bess_asset_daily_scenario_pnl (scenario_name);

-- =============================================================================
-- Daily attribution breakdown (loss buckets)
-- =============================================================================
CREATE TABLE IF NOT EXISTS reports.bess_asset_daily_attribution (
    attribution_id      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identity
    asset_code          text        NOT NULL,
    trade_date          date        NOT NULL,
    
    -- Baseline scenario P&L (yuan)
    pf_unrestricted_pnl     numeric,    -- Perfect foresight unrestricted
    pf_grid_feasible_pnl    numeric,    -- Perfect foresight grid feasible
    tt_forecast_optimal_pnl numeric,    -- TT forecast optimal (may be NULL for partial-coverage)
    tt_strategy_pnl         numeric,    -- TT strategy (may be NULL)
    nominated_dispatch_pnl  numeric,    -- Nominated dispatch (may be NULL)
    cleared_actual_pnl      numeric,    -- Cleared actual
    
    -- Attribution ladder (loss buckets, yuan)
    -- NULL if upstream scenario is not available for this asset
    grid_restriction_loss   numeric,    -- pf_unrestricted - pf_grid_feasible
    forecast_error_loss     numeric,    -- pf_grid_feasible - tt_forecast_optimal
    strategy_error_loss     numeric,    -- tt_forecast_optimal - tt_strategy
    nomination_loss         numeric,    -- tt_strategy - nominated_dispatch
    execution_clearing_loss numeric,    -- nominated_dispatch - cleared_actual
    
    -- Total explained gap
    total_explained_loss    numeric,    -- Sum of non-NULL loss buckets
    unexplained_gap         numeric,    -- Residual if any
    
    -- Metadata
    scenarios_available     text[],     -- Array of scenario names available for this asset/date
    computed_at             timestamptz NOT NULL DEFAULT now(),
    
    -- Audit
    active_flag             boolean     NOT NULL DEFAULT TRUE,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    
    UNIQUE (asset_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_bess_attribution_asset ON reports.bess_asset_daily_attribution (asset_code);
CREATE INDEX IF NOT EXISTS idx_bess_attribution_date ON reports.bess_asset_daily_attribution (trade_date DESC);

-- =============================================================================
-- Comments
-- =============================================================================
COMMENT ON TABLE reports.bess_asset_daily_scenario_pnl IS 'Daily P&L by asset and scenario. Uses coverage-aware scenarios from core.asset_scenario_availability.';
COMMENT ON TABLE reports.bess_asset_daily_attribution IS 'Daily P&L attribution with loss buckets. NULL values in loss buckets indicate unavailable upstream scenarios.';

COMMENT ON COLUMN reports.bess_asset_daily_scenario_pnl.compensation_rate IS 'From core.asset_monthly_compensation; 350 yuan/MWh is fallback only.';
COMMENT ON COLUMN reports.bess_asset_daily_attribution.grid_restriction_loss IS 'Loss due to grid restrictions: PF_unrestricted - PF_grid_feasible';
COMMENT ON COLUMN reports.bess_asset_daily_attribution.forecast_error_loss IS 'Loss due to forecast errors: PF_grid_feasible - TT_forecast_optimal (NULL if no TT forecast)';
