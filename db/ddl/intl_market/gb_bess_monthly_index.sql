CREATE TABLE IF NOT EXISTS intl_market.gb_bess_monthly_index (
    month                       DATE NOT NULL,
    duration                    TEXT NOT NULL,
    market                      TEXT NOT NULL,
    total_assets_rated_power    NUMERIC,
    total_assets_energy_capacity NUMERIC,
    revenue                     NUMERIC,
    revenue_permw               NUMERIC,
    revenue_permwh              NUMERIC,
    ingested_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (month, duration, market)
);
