-- Monthly installed capacity by province, scraped from each province's
-- official market disclosure Excel files in data/market-fundamentals/各省份装机数据/
--
-- Units stored in MW throughout.
-- Populated by:  scripts/scan_installed_capacity.py  (run monthly via Task Scheduler)

CREATE TABLE IF NOT EXISTS province_installed_monthly (
    id          SERIAL PRIMARY KEY,
    province    TEXT    NOT NULL,          -- Chinese province name (e.g. 山东)
    year_month  DATE    NOT NULL,          -- always first day of month
    wind_mw     NUMERIC,
    solar_mw    NUMERIC,
    thermal_mw  NUMERIC,
    hydro_mw    NUMERIC,
    nuclear_mw  NUMERIC,
    bess_mw     NUMERIC,
    total_mw    NUMERIC,
    source_file TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (province, year_month)
);

CREATE INDEX IF NOT EXISTS idx_pim_province_ym
    ON province_installed_monthly (province, year_month DESC);
