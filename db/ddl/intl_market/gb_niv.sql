CREATE TABLE IF NOT EXISTS intl_market.gb_niv (
    date                DATE NOT NULL,
    settlement_period   INTEGER NOT NULL,
    niv                 NUMERIC,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date, settlement_period)
);
