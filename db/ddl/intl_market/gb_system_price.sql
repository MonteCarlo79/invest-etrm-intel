CREATE TABLE IF NOT EXISTS intl_market.gb_system_price (
    date                DATE NOT NULL,
    settlement_period   INTEGER NOT NULL,
    system_price        NUMERIC,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date, settlement_period)
);
