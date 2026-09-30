-- db/migrations/2026-09-30_rm_retail_categories_benchmarks.sql
-- Widen rm_settlement_items categories for retail wholesale invoices + add benchmark table.
-- Rollback: DELETE rows using the 4 new categories, then reverse the ALTER; DROP TABLE rm_market_benchmarks.

BEGIN;

ALTER TABLE marketdata.rm_settlement_items DROP CONSTRAINT rm_settlement_items_category_check;
ALTER TABLE marketdata.rm_settlement_items ADD CONSTRAINT rm_settlement_items_category_check
  CHECK (category IN ('charge_energy','discharge_energy','generation_revenue',
    'capacity_compensation','bilateral_energy','transmission','govt_surcharges',
    'system_operation','coal_capacity_charge','basic_fee','curtailment','flex_fees',
    'imbalance','market_redistribution','rule_charges','frequency','penalty','rebate',
    'subsidy','other',
    'spot_energy','midlong_energy','retail_revenue','green_premium'));

CREATE TABLE IF NOT EXISTS marketdata.rm_market_benchmarks (
    id                SERIAL PRIMARY KEY,
    province          TEXT NOT NULL,
    channel           TEXT NOT NULL,           -- exchange stat label: 双边协商交易/集中竞价交易/挂牌交易/月内集中竞价
    month             DATE NOT NULL,           -- 1st of delivery month
    avg_price_cny_mwh NUMERIC(10,4) NOT NULL,
    volume_mwh        NUMERIC(14,4),
    source            TEXT NOT NULL DEFAULT 'infohub',
    source_file       TEXT,
    uploaded_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (province, channel, month, source)
);

COMMIT;
