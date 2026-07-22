-- db/ddl/marketdata/rm_settlements.sql

CREATE TABLE IF NOT EXISTS marketdata.rm_settlements (
    id               SERIAL PRIMARY KEY,
    book_id          INTEGER NOT NULL REFERENCES marketdata.rm_books(id),
    settlement_month DATE NOT NULL,
    file_name        TEXT NOT NULL,
    file_type        TEXT NOT NULL CHECK (file_type IN ('excel','csv','pdf')),
    upload_date      TIMESTAMPTZ DEFAULT NOW(),
    status           TEXT DEFAULT 'pending' CHECK (status IN ('pending','processed','flagged')),
    total_amount_cny NUMERIC(16,2),
    raw_data         JSONB
);

COMMENT ON TABLE marketdata.rm_settlements IS
    'Settlement file upload records. One row per uploaded file per book per month.';

CREATE INDEX IF NOT EXISTS idx_rm_settlements_book ON marketdata.rm_settlements(book_id);
CREATE INDEX IF NOT EXISTS idx_rm_settlements_month ON marketdata.rm_settlements(settlement_month);

CREATE TABLE IF NOT EXISTS marketdata.rm_settlement_items (
    id                    SERIAL PRIMARY KEY,
    settlement_id         INTEGER NOT NULL REFERENCES marketdata.rm_settlements(id),
    category              TEXT NOT NULL CHECK (category IN (
                            'charge_energy','discharge_energy','generation_revenue',
                            'capacity_compensation','bilateral_energy',
                            'transmission','govt_surcharges','system_operation',
                            'coal_capacity_charge','basic_fee','curtailment',
                            'flex_fees','imbalance','market_redistribution',
                            'rule_charges','frequency',
                            'penalty','rebate','subsidy','other')),
    peak_period           TEXT CHECK (peak_period IN ('peak','valley','flat','super_peak')),
    delivery_date         DATE,
    volume_mwh            NUMERIC(14,4),
    price_cny_kwh         NUMERIC(10,6),
    amount_cny            NUMERIC(16,2) NOT NULL,
    amount_receivable_cny NUMERIC(16,2),
    amount_settled_cny    NUMERIC(16,2),
    amount_diff_cny       NUMERIC(16,2),
    counterparty          TEXT,
    notes                 TEXT
);

COMMENT ON TABLE marketdata.rm_settlement_items IS
    'Line items within a settlement. Categories cover BESS charge/discharge, wind generation, '
    'capacity compensation, T&D fees, surcharges, penalties, subsidies.';

CREATE INDEX IF NOT EXISTS idx_rm_si_settlement ON marketdata.rm_settlement_items(settlement_id);
CREATE INDEX IF NOT EXISTS idx_rm_si_category ON marketdata.rm_settlement_items(category);
