-- Province frequency regulation market policy table
-- Stores per-province 调频价格 (yuan/kW/h) and 全省调频总资金池 (亿元/年)
-- status: 'confirmed' | 'conflict' | 'superseded'

CREATE TABLE IF NOT EXISTS marketdata.province_fr_market (
    id                   SERIAL PRIMARY KEY,
    province             TEXT        NOT NULL,
    effective_date       DATE        NOT NULL,   -- policy effective year start, e.g. 2026-01-01
    fr_price_yuan_kw_h   NUMERIC,                -- 调频容量价格 (yuan/kW/h)
    fr_pool_billion_yuan NUMERIC,                -- 全省调频总资金池 (亿元/年)
    source               TEXT,
    status               TEXT        NOT NULL DEFAULT 'confirmed',
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pfr_prov_date_src
    ON marketdata.province_fr_market (province, effective_date, COALESCE(source, ''));

CREATE INDEX IF NOT EXISTS idx_pfr_prov_date
    ON marketdata.province_fr_market (province, effective_date DESC);
