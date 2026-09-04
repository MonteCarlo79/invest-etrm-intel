-- staging.spot_interprov_flow
-- Inter-provincial spot market trading summary
-- Parsed from: 二、X月XX日省间现货交易情况 tables in daily PDF reports
--
-- One row per (report_date, direction, metric_type).
-- direction:   '送端' (exporting) | '受端' (importing)
-- metric_type: '最高均价' | '最低均价' | '最高电量' | '最高价'
-- total_vol_100gwh: populated for the '最高均价' row only (= direction subtotal)

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.spot_interprov_flow (
    id                bigserial    PRIMARY KEY,
    report_date       date         NOT NULL,
    direction         text         NOT NULL CHECK (direction IN ('送端', '受端')),
    metric_type       text         NOT NULL,
    province_cn       text,
    province_share    numeric(5,2),            -- e.g. 35.00 for the "(35%)" annotation
    price_yuan_kwh    numeric(8,4),
    price_chg_pct     numeric(8,4),            -- percentage points vs previous day
    time_period       text,                    -- e.g. '09:00-11:15 13:15-14:30'
    total_vol_100gwh  numeric(16,4),           -- 亿千瓦时; only for first metric row per direction
    source_pdf        text,
    created_at        timestamptz  DEFAULT now(),

    UNIQUE (report_date, direction, metric_type)
);

CREATE INDEX IF NOT EXISTS idx_spot_interprov_flow_date
    ON staging.spot_interprov_flow (report_date);
