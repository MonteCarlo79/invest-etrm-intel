-- Province capacity compensation policy table
-- Stores per-province 储能容量补偿标准 (yuan/kW) and 年最高净负荷 (hours)
-- status: 'confirmed' | 'conflict' | 'superseded'

CREATE TABLE IF NOT EXISTS marketdata.province_cap_comp (
    id                  SERIAL PRIMARY KEY,
    province            TEXT        NOT NULL,
    effective_date      DATE        NOT NULL,   -- policy effective year start, e.g. 2026-01-01
    cap_comp_yuan_kw    NUMERIC,                -- 容量补偿标准 (yuan/kW)
    peak_duration_hours NUMERIC,                -- 年最高净负荷峰值时段 (hours, e.g. 6)
    source              TEXT,
    status              TEXT        NOT NULL DEFAULT 'confirmed',
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pcc_prov_date_src
    ON marketdata.province_cap_comp (province, effective_date, COALESCE(source, ''));

CREATE INDEX IF NOT EXISTS idx_pcc_prov_date
    ON marketdata.province_cap_comp (province, effective_date DESC);
