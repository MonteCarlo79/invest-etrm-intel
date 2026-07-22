-- db/ddl/marketdata/rm_assets_books.sql
--
-- Asset registry and trading book tables for risk management.
-- All tables in marketdata schema with rm_ prefix.

CREATE TABLE IF NOT EXISTS marketdata.rm_assets (
    id               SERIAL PRIMARY KEY,
    name             TEXT NOT NULL,
    asset_type       TEXT NOT NULL CHECK (asset_type IN ('wind','solar','bess','thermal')),
    province         TEXT NOT NULL,
    capacity_mw      NUMERIC(10,2) NOT NULL,
    bess_duration_h  NUMERIC(5,2),
    bess_dod_pct     NUMERIC(5,2),
    fuel_type        TEXT,
    commission_date  DATE,
    status           TEXT DEFAULT 'active' CHECK (status IN ('active','retired')),
    notes            TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE marketdata.rm_assets IS
    'Registry of power generation and storage assets (wind, solar, BESS, thermal).';
COMMENT ON COLUMN marketdata.rm_assets.bess_duration_h IS
    'Battery duration in hours. Only populated for BESS assets.';
COMMENT ON COLUMN marketdata.rm_assets.bess_dod_pct IS
    'Depth of discharge percentage (0-100). Only populated for BESS assets.';

CREATE INDEX IF NOT EXISTS idx_rm_assets_type ON marketdata.rm_assets(asset_type);
CREATE INDEX IF NOT EXISTS idx_rm_assets_province ON marketdata.rm_assets(province);

CREATE TABLE IF NOT EXISTS marketdata.rm_books (
    id               SERIAL PRIMARY KEY,
    name             TEXT NOT NULL,
    book_type        TEXT NOT NULL CHECK (book_type IN ('asset','load')),
    asset_id         INTEGER REFERENCES marketdata.rm_assets(id),
    currency         TEXT DEFAULT 'CNY',
    description      TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE marketdata.rm_books IS
    'Trading books. Each asset gets an auto-created book (book_type=asset). '
    'Virtual/aggregated books may exist without a linked asset.';

CREATE INDEX IF NOT EXISTS idx_rm_books_asset ON marketdata.rm_books(asset_id);
CREATE INDEX IF NOT EXISTS idx_rm_books_type ON marketdata.rm_books(book_type);
