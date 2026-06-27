-- Province-level market fundamentals
-- Populated by: scripts/ingest_province_fundamentals.py
-- Source: data/market-fundamentals/2023-2025 全国各省电力市场基础信息汇总*.xlsx

CREATE TABLE IF NOT EXISTS marketdata.province_fundamentals (
    province_cn          TEXT             NOT NULL,
    province_en          TEXT             NOT NULL,
    year                 INT              NOT NULL,
    -- Installed capacity (万kW = 10 MW)
    wind_cap_10kw        DOUBLE PRECISION,
    solar_cap_10kw       DOUBLE PRECISION,
    thermal_cap_10kw     DOUBLE PRECISION,
    hydro_cap_10kw       DOUBLE PRECISION,
    nuclear_cap_10kw     DOUBLE PRECISION,
    storage_cap_10kw     DOUBLE PRECISION,
    -- Generation (亿kWh = 100 GWh)
    wind_gen_100gwh      DOUBLE PRECISION,
    solar_gen_100gwh     DOUBLE PRECISION,
    thermal_gen_100gwh   DOUBLE PRECISION,
    hydro_gen_100gwh     DOUBLE PRECISION,
    nuclear_gen_100gwh   DOUBLE PRECISION,
    storage_gen_100gwh   DOUBLE PRECISION,
    -- Peak load (MW)
    peak_summer_mw       DOUBLE PRECISION,
    peak_winter_mw       DOUBLE PRECISION,
    peak_other_mw        DOUBLE PRECISION,
    -- Metadata
    updated_at           TIMESTAMPTZ      DEFAULT now(),
    PRIMARY KEY (province_cn, year)
);

CREATE INDEX IF NOT EXISTS idx_pf_year
    ON marketdata.province_fundamentals (year);
