-- db/ddl/marketdata/rm_retail.sql
--
-- Retail risk management tables: customers, contracts, profiles, CRM import config.
-- Depends on rm_assets (from rm_assets_books.sql) for the bound_asset_id FK.

CREATE TABLE IF NOT EXISTS marketdata.rm_customers (
    id                      SERIAL PRIMARY KEY,
    name                    TEXT NOT NULL,
    province                TEXT NOT NULL,
    district                TEXT,
    customer_type           TEXT CHECK (customer_type IN ('industrial','commercial','residential')),
    voltage_level           TEXT,
    contracted_capacity_kva NUMERIC(12,2),
    bd_name                 TEXT,
    customer_source         TEXT,
    channel_name            TEXT,
    fixed_spread_cny_mwh    NUMERIC(10,4),
    revenue_share_ratio     NUMERIC(6,4),
    status                  TEXT DEFAULT 'active' CHECK (status IN ('active','prospect','churned')),
    notes                   TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE marketdata.rm_customers IS
    'Retail customer master. Each customer belongs to a province and may have channel/spread/share revenue info.';

CREATE INDEX IF NOT EXISTS idx_rm_customers_province ON marketdata.rm_customers(province);
CREATE INDEX IF NOT EXISTS idx_rm_customers_status ON marketdata.rm_customers(status);
CREATE INDEX IF NOT EXISTS idx_rm_customers_bd ON marketdata.rm_customers(bd_name);

CREATE TABLE IF NOT EXISTS marketdata.rm_customer_contracts (
    id                    SERIAL PRIMARY KEY,
    customer_id           INTEGER NOT NULL REFERENCES marketdata.rm_customers(id),
    contract_ref          TEXT,
    energy_source         TEXT CHECK (energy_source IN ('wind','solar','bess','mixed')),
    contract_type         TEXT NOT NULL CHECK (contract_type IN
                            ('fixed','indexed','peak_offpeak','indexed_band')),
    price_cny_mwh         NUMERIC(10,4),
    price_formula         JSONB,
    peak_price_cny_mwh    NUMERIC(10,4),
    offpeak_price_cny_mwh NUMERIC(10,4),
    k1                    NUMERIC(8,4),
    k2                    NUMERIC(8,4),
    k3                    NUMERIC(8,4),
    bound_asset_id        INTEGER REFERENCES marketdata.rm_assets(id),
    start_date            DATE NOT NULL,
    end_date              DATE NOT NULL,
    signing_date          DATE,
    annual_forecast_mwh   NUMERIC(14,4),
    monthly_forecast      JSONB,
    contract_status       TEXT DEFAULT 'active' CHECK (contract_status IN
                            ('active','expired','pending','terminated')),
    notes                 TEXT,
    created_at            TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE marketdata.rm_customer_contracts IS
    'Customer contracts with pricing details. contract_type determines which price fields are used. '
    'price_formula JSONB holds indexed/band/K-factor formulas per province convention.';

CREATE INDEX IF NOT EXISTS idx_rm_cc_customer ON marketdata.rm_customer_contracts(customer_id);
CREATE INDEX IF NOT EXISTS idx_rm_cc_status ON marketdata.rm_customer_contracts(contract_status);
CREATE INDEX IF NOT EXISTS idx_rm_cc_dates ON marketdata.rm_customer_contracts(start_date, end_date);

CREATE TABLE IF NOT EXISTS marketdata.rm_customer_profiles (
    id               SERIAL PRIMARY KEY,
    customer_id      INTEGER NOT NULL REFERENCES marketdata.rm_customers(id),
    profile_date     DATE NOT NULL,
    hour             SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
    load_mwh         NUMERIC(10,4),
    nominated_mwh    NUMERIC(10,4),
    settled_mwh      NUMERIC(10,4),
    upload_batch_id  TEXT,
    UNIQUE (customer_id, profile_date, hour)
);

COMMENT ON TABLE marketdata.rm_customer_profiles IS
    'Hourly customer load profiles. Aggregated from 15-min exchange data (Jiangsu) or daily .xls (Shandong).';

CREATE INDEX IF NOT EXISTS idx_rm_cp_customer_date ON marketdata.rm_customer_profiles(customer_id, profile_date);
CREATE INDEX IF NOT EXISTS idx_rm_cp_date ON marketdata.rm_customer_profiles(profile_date);

CREATE TABLE IF NOT EXISTS marketdata.rm_crm_import_configs (
    id               SERIAL PRIMARY KEY,
    province         TEXT NOT NULL UNIQUE,
    column_map       JSONB NOT NULL,
    notes            TEXT,
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE marketdata.rm_crm_import_configs IS
    'Province-keyed column mapping for CRM import (各省份台账.xlsx). New provinces added via UI.';

-- Retail-specific settlement tables (separate from asset-risk rm_settlements which use book_id)

CREATE TABLE IF NOT EXISTS marketdata.rm_retail_settlements (
    id               SERIAL PRIMARY KEY,
    customer_id      INTEGER NOT NULL REFERENCES marketdata.rm_customers(id),
    settlement_month DATE NOT NULL,
    file_name        TEXT NOT NULL,
    file_type        TEXT NOT NULL CHECK (file_type IN ('excel','csv','pdf')),
    upload_date      TIMESTAMPTZ DEFAULT NOW(),
    status           TEXT DEFAULT 'pending' CHECK (status IN ('pending','processed','flagged')),
    total_amount_cny NUMERIC(16,2),
    raw_data         JSONB
);

COMMENT ON TABLE marketdata.rm_retail_settlements IS
    'Retail settlement file records. One row per uploaded file per customer per month.';

CREATE INDEX IF NOT EXISTS idx_rm_rs_customer ON marketdata.rm_retail_settlements(customer_id);
CREATE INDEX IF NOT EXISTS idx_rm_rs_month ON marketdata.rm_retail_settlements(settlement_month);

CREATE TABLE IF NOT EXISTS marketdata.rm_retail_settlement_items (
    id                    SERIAL PRIMARY KEY,
    settlement_id         INTEGER NOT NULL REFERENCES marketdata.rm_retail_settlements(id),
    category              TEXT NOT NULL CHECK (category IN (
                            'retail_revenue','energy_procurement','capacity_compensation',
                            'transmission_distribution','system_service_fee','ancillary_service',
                            'imbalance_penalty','other')),
    volume_mwh            NUMERIC(14,4),
    price_cny_mwh         NUMERIC(10,4),
    amount_cny            NUMERIC(16,2) NOT NULL,
    delivery_date         DATE,
    notes                 TEXT
);

COMMENT ON TABLE marketdata.rm_retail_settlement_items IS
    'Line items within a retail settlement. Categories cover revenue, procurement, T&D, penalties, etc.';

CREATE INDEX IF NOT EXISTS idx_rm_rsi_settlement ON marketdata.rm_retail_settlement_items(settlement_id);
CREATE INDEX IF NOT EXISTS idx_rm_rsi_category ON marketdata.rm_retail_settlement_items(category);
