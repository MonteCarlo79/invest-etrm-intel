-- db/ddl/marketdata/rm_dispatch_chain.sql
--
-- Trader nominations (申报策略, 5-min) and full dispatch chain
-- (调度计划表, 15-min: nominated → DA cleared → RT cleared → actual).
-- Ingested from data/raw/nomination/ trees by services/dispatch_ingest/.

CREATE TABLE IF NOT EXISTS marketdata.rm_nominations (
    id               SERIAL PRIMARY KEY,
    asset_id         INTEGER NOT NULL REFERENCES marketdata.rm_assets(id),
    interval_start   TIMESTAMPTZ NOT NULL,
    planned_mw       NUMERIC(10,4),          -- 预计划功率
    nominated_mw     NUMERIC(10,4),          -- 正式申报 (the trader's bid)
    source_file      TEXT,
    upload_batch_id  TEXT,
    UNIQUE (asset_id, interval_start)
);

COMMENT ON TABLE marketdata.rm_nominations IS
    'Trader daily nominations (申报策略). 5-min intervals. '
    'Positive MW = discharge, negative = charge.';

CREATE INDEX IF NOT EXISTS idx_rm_nom_asset_interval
    ON marketdata.rm_nominations(asset_id, interval_start);


CREATE TABLE IF NOT EXISTS marketdata.rm_dispatch_chain (
    id               SERIAL PRIMARY KEY,
    asset_id         INTEGER NOT NULL REFERENCES marketdata.rm_assets(id),
    interval_start   TIMESTAMPTZ NOT NULL,
    soc_pct          NUMERIC(6,2),
    nominated_mw     NUMERIC(10,4),          -- 交易员申报计划
    da_cleared_mw    NUMERIC(10,4),          -- 日前出清
    rt_cleared_mw    NUMERIC(10,4),          -- 实时调度出清
    actual_mw        NUMERIC(10,4),          -- 实际执行功率
    -- Restriction window from the time-cell colour:
    --   NULL = both charge+discharge allowed (green or no fill)
    --   'charge_only' = orange fill / 'discharge_only' = red fill
    restriction      TEXT CHECK (restriction IN ('charge_only','discharge_only')),
    source_file      TEXT,
    upload_batch_id  TEXT,
    UNIQUE (asset_id, interval_start)
);

COMMENT ON TABLE marketdata.rm_dispatch_chain IS
    '15-min dispatch chain from 调度计划表: nominated (交易员申报计划) → '
    'da_cleared (日前出清) → rt_cleared (实时调度出清) → actual (实际执行功率). '
    'restriction from time-cell colour: green/none=NULL(both), orange=charge_only, '
    'red=discharge_only (colour semantics confirmed by user 2026-08-28).';

CREATE INDEX IF NOT EXISTS idx_rm_dc_asset_interval
    ON marketdata.rm_dispatch_chain(asset_id, interval_start);
