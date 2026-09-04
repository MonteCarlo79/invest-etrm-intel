CREATE TABLE IF NOT EXISTS intl_market.gb_dx_results (
    efa_date        DATE NOT NULL,
    efa             INTEGER NOT NULL,
    service         TEXT NOT NULL,
    auction_id      INTEGER NOT NULL,
    delivery_start  TIMESTAMPTZ,
    delivery_end    TIMESTAMPTZ,
    cleared_volume  NUMERIC,
    clearing_price  NUMERIC,
    service_type    TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (efa_date, efa, service, auction_id)
);
