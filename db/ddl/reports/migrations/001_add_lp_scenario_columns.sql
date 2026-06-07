-- db/ddl/reports/migrations/001_add_lp_scenario_columns.sql
--
-- Adds the two columns needed by write_lp_results_to_db / load_precomputed_scenario_pnl
-- (introduced when the LP pre-compute batch was added in May 2026).
-- Safe to run multiple times (IF NOT EXISTS).

ALTER TABLE reports.bess_asset_daily_scenario_pnl
    ADD COLUMN IF NOT EXISTS scenario_available  boolean DEFAULT true,
    ADD COLUMN IF NOT EXISTS avg_daily_cycles    numeric;

COMMENT ON COLUMN reports.bess_asset_daily_scenario_pnl.scenario_available IS
    'TRUE when the LP solve succeeded and results are valid. '
    'Set by write_lp_results_to_db (run_daily_strategy_batch pre-compute).';

COMMENT ON COLUMN reports.bess_asset_daily_scenario_pnl.avg_daily_cycles IS
    'discharge_mwh / energy_capacity_mwh; computed at write time by write_lp_results_to_db.';
