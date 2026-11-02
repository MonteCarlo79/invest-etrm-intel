-- Province fundamentals monthly long table (Market Fundamentals tab rebuild, 2026-11-02)
-- Populated by: services/province_fundamentals backfill parsers, Intel Intake route
--   fundamentals_stat, RECORD_FACT fact_type fundamentals_stat (draft -> confirmed)
-- period: 'month' = month-start date | 'year' = YYYY-01-01 (year row never collides
--   with the January month row because period is part of the unique key)
-- status: 'confirmed' | 'draft' | 'conflict'
--   A write restating a confirmed value by >0.5% does NOT overwrite; it lands as a
--   new 'conflict' row (source-scoped key keeps it separate). Reads are confirmed-only.

CREATE TABLE IF NOT EXISTS marketdata.province_fundamentals_monthly (
    id         SERIAL PRIMARY KEY,
    province   TEXT        NOT NULL,
    month      DATE        NOT NULL,                     -- month start; year rows use YYYY-01-01
    period     TEXT        NOT NULL DEFAULT 'month',     -- month | year
    section    TEXT        NOT NULL,                     -- capacity | generation | util_hours | demand
    metric     TEXT        NOT NULL,                     -- cap_total / gen_wind / hours_solar / demand_total ...
    value      NUMERIC,
    unit       TEXT        NOT NULL,                     -- 万千瓦 | 亿千瓦时 | 小时 | %
    source     TEXT        NOT NULL,                     -- filename or hermes-record-fact-YYYYMMDD
    status     TEXT        NOT NULL DEFAULT 'confirmed',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pfm_nat
    ON marketdata.province_fundamentals_monthly
    (province, month, period, section, metric, COALESCE(source, ''));

CREATE INDEX IF NOT EXISTS idx_pfm_read
    ON marketdata.province_fundamentals_monthly (province, section, status, month DESC);
