CREATE TABLE IF NOT EXISTS intl_market.gb_epex_da_hh (
    delivery_date           DATE NOT NULL,
    settlement_period       INTEGER NOT NULL,
    start_time              TIMESTAMPTZ,
    price                   NUMERIC,
    volume                  NUMERIC,
    daily_offpeak           NUMERIC,
    daily_peakload          NUMERIC,
    daily_baseload          NUMERIC,
    daily_total_volume      NUMERIC,
    price_unit              TEXT,
    volume_unit             TEXT,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (delivery_date, settlement_period)
);
