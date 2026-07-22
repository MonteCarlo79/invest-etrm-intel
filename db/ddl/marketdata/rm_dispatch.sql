-- db/ddl/marketdata/rm_dispatch.sql
--
-- BESS daily operations summary and 15-min dispatch plan tables.

CREATE TABLE IF NOT EXISTS marketdata.rm_dispatch_daily (
    id                        SERIAL PRIMARY KEY,
    asset_id                  INTEGER NOT NULL REFERENCES marketdata.rm_assets(id),
    dispatch_date             DATE NOT NULL,
    operator_name             TEXT,
    charge_mwh                NUMERIC(10,4),
    discharge_mwh             NUMERIC(10,4),
    auxiliary_consumption_mwh NUMERIC(10,4),
    cumulative_charge_mwh     NUMERIC(12,4),
    cumulative_discharge_mwh  NUMERIC(12,4),
    cycle_count_day           NUMERIC(6,2),
    cycle_count_month         NUMERIC(8,2),
    conversion_ratio          NUMERIC(6,4),
    charge_windows            TEXT[],
    discharge_windows         TEXT[],
    discharge_revenue_cny     NUMERIC(14,2),
    charge_cost_cny           NUMERIC(14,2),
    system_op_fee_cny         NUMERIC(14,2),
    net_margin_cny            NUMERIC(14,2),
    anomaly_notes             TEXT,
    upload_batch_id           TEXT,
    UNIQUE (asset_id, dispatch_date)
);

COMMENT ON TABLE marketdata.rm_dispatch_daily IS
    'Daily BESS operations summary from 运营统计 Excel. One row per asset per day.';

CREATE INDEX IF NOT EXISTS idx_rm_dd_asset_date ON marketdata.rm_dispatch_daily(asset_id, dispatch_date);

CREATE TABLE IF NOT EXISTS marketdata.rm_dispatch_plan (
    id                    SERIAL PRIMARY KEY,
    asset_id              INTEGER NOT NULL REFERENCES marketdata.rm_assets(id),
    interval_start        TIMESTAMPTZ NOT NULL,
    soc_pct               NUMERIC(6,2),
    nominated_mw          NUMERIC(10,4),
    forecast_mw           NUMERIC(10,4),
    dispatched_mw         NUMERIC(10,4),
    actual_mw             NUMERIC(10,4),
    upload_batch_id       TEXT,
    UNIQUE (asset_id, interval_start)
);

COMMENT ON TABLE marketdata.rm_dispatch_plan IS
    '15-min dispatch plan. BESS: nominated/forecast/dispatched/actual MW. '
    'Wind: D+1 forecast + actual output. Positive=discharge/generation, Negative=charge.';

CREATE INDEX IF NOT EXISTS idx_rm_dp_asset_interval ON marketdata.rm_dispatch_plan(asset_id, interval_start);
