-- db/ddl/reports/bess_strategy_dispatch_15min.sql
--
-- Pre-computed 15-min dispatch time series for LP-solved strategies.
-- Populated by services/decision_models/run_daily_strategy_batch.py after
-- actual prices (and, optionally, ops data) are loaded to DB.
--
-- Storing dispatch here lets the daily-ops UI read from DB instead of
-- re-running the CBC LP solver on every button click.
--
-- Covered scenarios:
--   perfect_foresight_hourly    — LP solved on actual 15-min prices
--   forecast_ols_rt_time_v1     — LP solved on RT-OLS-forecasted prices (no DA market in IM)
--
-- Sign convention: same as LP output (positive = discharge / net injection,
-- negative = charge / net absorption).

CREATE TABLE IF NOT EXISTS reports.bess_strategy_dispatch_15min (
    trade_date        date            NOT NULL,
    asset_code        text            NOT NULL,
    scenario_name     text            NOT NULL,
    interval_start    timestamptz     NOT NULL,
    dispatch_grid_mw  numeric,
    charge_mw         numeric,
    discharge_mw      numeric,
    soc_mwh           numeric,
    price             numeric,         -- CNY/MWh at this interval (used by LP)
    created_at        timestamptz     NOT NULL DEFAULT now(),
    updated_at        timestamptz     NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, asset_code, scenario_name, interval_start)
);

CREATE INDEX IF NOT EXISTS idx_bess_strategy_dispatch_asset_date
    ON reports.bess_strategy_dispatch_15min (asset_code, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_bess_strategy_dispatch_scenario
    ON reports.bess_strategy_dispatch_15min (scenario_name);

COMMENT ON TABLE reports.bess_strategy_dispatch_15min IS
    'Pre-computed 15-min dispatch for LP strategies (PF + forecast). '
    'Populated by run_daily_strategy_batch.py, read by daily-ops UI to skip LP.';
