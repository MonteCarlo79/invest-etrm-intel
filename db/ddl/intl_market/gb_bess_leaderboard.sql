CREATE TABLE IF NOT EXISTS intl_market.gb_bess_leaderboard (
    settlement_date     DATE NOT NULL,
    settlement_period   INTEGER NOT NULL,
    asset               TEXT NOT NULL,
    market              TEXT NOT NULL,
    rated_power         NUMERIC,
    energy_capacity     NUMERIC,
    export_mwh          NUMERIC,
    import_mwh          NUMERIC,
    revenue             NUMERIC,
    contract_capacity   NUMERIC,
    buy_price_mwh       NUMERIC,
    sell_price_mwh      NUMERIC,
    revspermw           NUMERIC,
    revspermwh          NUMERIC,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (settlement_date, settlement_period, asset, market)
);
