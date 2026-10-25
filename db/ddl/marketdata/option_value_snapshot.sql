-- db/ddl/marketdata/option_value_snapshot.sql
-- Option-engine value snapshots (spec §5.7). Only services/option_engine/batch.py
-- writes; app tabs read latest per key. duration_h is 0 for wind/solar.
-- Numpy paths are never persisted (deal_committee lesson).
CREATE TABLE IF NOT EXISTS marketdata.option_value_snapshot (
    province            TEXT NOT NULL,
    asset_type          TEXT NOT NULL,
    duration_h          DOUBLE PRECISION NOT NULL,
    asof_date           DATE NOT NULL,
    window_start        DATE,
    window_end          DATE,
    intrinsic_yuan_kw   DOUBLE PRECISION,
    extrinsic_yuan_kw   DOUBLE PRECISION,
    total_yuan_kw       DOUBLE PRECISION,
    capture_adj_yuan_kw DOUBLE PRECISION,
    policy_json         JSONB,
    greeks_json         JSONB,
    mc_params           JSONB,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (province, asset_type, duration_h, asof_date)
);

CREATE INDEX IF NOT EXISTS idx_option_value_snapshot_asof
    ON marketdata.option_value_snapshot (asof_date);

-- Plan-2 app-surface seam: top-level queryable columns (nullable; populated
-- by the writer on the next upsert after deploy).
ALTER TABLE marketdata.option_value_snapshot
    ADD COLUMN IF NOT EXISTS policy_net_yuan_kw DOUBLE PRECISION;
ALTER TABLE marketdata.option_value_snapshot
    ADD COLUMN IF NOT EXISTS extrinsic_p10 DOUBLE PRECISION;
ALTER TABLE marketdata.option_value_snapshot
    ADD COLUMN IF NOT EXISTS extrinsic_p90 DOUBLE PRECISION;
