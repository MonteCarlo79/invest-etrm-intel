-- 零碳46 (悦盛昌渠) per-15-min dispatch extract for fast tab rendering.
-- md_id_cleared_energy (MW, ×0.25 → MWh) joined to md_rt_nodal_price at the
-- station node. Backfilled by services/wind_settlement/backfill_dispatch.py.

CREATE TABLE IF NOT EXISTS marketdata.wind_dispatch_15min (
    plant_name      TEXT NOT NULL,
    node_name       TEXT NOT NULL,
    datetime        TIMESTAMP NOT NULL,
    gen_mw          NUMERIC,                  -- raw md_id_cleared_energy value (MW)
    gen_mwh         NUMERIC,                  -- GREATEST(gen_mw,0) × 0.25
    rt_price        NUMERIC,                  -- 元/MWh, NULL if nodal price missing
    data_date       DATE,
    PRIMARY KEY (plant_name, datetime)
);

CREATE INDEX IF NOT EXISTS idx_wind_dispatch_dt ON marketdata.wind_dispatch_15min (datetime);
