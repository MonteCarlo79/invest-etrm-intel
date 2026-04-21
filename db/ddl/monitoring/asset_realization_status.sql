-- db/ddl/monitoring/asset_realization_status.sql
--
-- Daily realization status snapshot per BESS asset.
-- Populated by services/monitoring/run_realization_monitor.py.
-- Agents read this table directly (Pattern A) — no model dispatch.

CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.asset_realization_status (
    status_id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_code            text        NOT NULL,
    snapshot_date         date        NOT NULL,

    -- Rolling window parameters
    lookback_days         integer     NOT NULL DEFAULT 30,
    days_in_window        integer,            -- actual days with attribution data in window

    -- Scenario PnL averages over lookback window (Yuan/day)
    avg_cleared_actual_pnl     numeric,
    avg_pf_grid_feasible_pnl   numeric,

    -- Realization ratio: avg_cleared_actual / avg_pf_grid_feasible
    -- 1.0 = perfect execution vs grid-feasible benchmark
    realization_ratio          numeric,

    -- Attribution loss averages over lookback window (Yuan/day)
    avg_grid_restriction_loss  numeric,
    avg_forecast_error_loss    numeric,
    avg_strategy_error_loss    numeric,
    avg_nomination_loss        numeric,
    avg_execution_clearing_loss numeric,

    -- Dominant loss bucket (field name of largest avg loss)
    dominant_loss_bucket       text,

    -- Status classification
    -- NORMAL       : realization_ratio >= 0.70
    -- WARN         : realization_ratio in [0.50, 0.70)
    -- ALERT        : realization_ratio in [0.30, 0.50)
    -- CRITICAL     : realization_ratio < 0.30  (data present, ratio computable)
    -- DATA_ABSENT  : days_in_window < 5  (not enough data to assess)
    -- INDETERMINATE: avg_pf_grid_feasible_pnl <= 0  (benchmark unavailable)
    status_level               text        NOT NULL,

    narrative                  text,
    computed_at                timestamptz NOT NULL DEFAULT now(),

    UNIQUE (asset_code, snapshot_date, lookback_days)
);

CREATE INDEX IF NOT EXISTS idx_realization_status_asset_date
    ON monitoring.asset_realization_status (asset_code, snapshot_date DESC);

COMMENT ON TABLE monitoring.asset_realization_status IS
    'Daily realization ratio monitoring for BESS assets. '
    'Compares rolling cleared actual PnL to grid-feasible benchmark. '
    'Updated by services/monitoring/run_realization_monitor.py.';
