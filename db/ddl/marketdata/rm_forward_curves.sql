-- db/ddl/marketdata/rm_forward_curves.sql

CREATE TABLE IF NOT EXISTS marketdata.rm_forward_curves (
    id               SERIAL PRIMARY KEY,
    province         TEXT NOT NULL,
    product          TEXT NOT NULL,
    curve_date       DATE NOT NULL,
    delivery_date    DATE NOT NULL,
    delivery_hour    SMALLINT,
    price_cny_kwh    NUMERIC(10,6) NOT NULL,
    source           TEXT NOT NULL CHECK (source IN ('lingfeng','manual','exchange')),
    uploaded_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (province, product, curve_date, delivery_date, delivery_hour, source)
);

COMMENT ON TABLE marketdata.rm_forward_curves IS
    'Forward price curves by province and product. Sources: LingFeng API, manual CSV upload, exchange data.';

CREATE INDEX IF NOT EXISTS idx_rm_fc_province_date ON marketdata.rm_forward_curves(province, delivery_date);
CREATE INDEX IF NOT EXISTS idx_rm_fc_source ON marketdata.rm_forward_curves(source);
