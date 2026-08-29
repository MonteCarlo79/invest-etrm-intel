-- Stage 2: match invoice arbitrage P&L to actual dispatch.
-- rm_arb_match_daily: per asset-day modeled charge cost / discharge revenue
-- from rm_dispatch_chain actual power priced at RT nodal prices
-- (charge at hourly-average price, discharge at 15-min price — settlement rule).

ALTER TABLE marketdata.rm_assets ADD COLUMN IF NOT EXISTS enos_node_name TEXT;

CREATE TABLE IF NOT EXISTS marketdata.rm_arb_match_daily (
    id SERIAL PRIMARY KEY,
    asset_id INTEGER NOT NULL REFERENCES marketdata.rm_assets(id),
    date DATE NOT NULL,
    charge_mwh NUMERIC(12,4),
    discharge_mwh NUMERIC(12,4),
    modeled_charge_cost_cny NUMERIC(14,2),   -- negative = cost
    modeled_discharge_rev_cny NUMERIC(14,2), -- positive = revenue
    intervals_actual INTEGER NOT NULL,       -- dispatch intervals present
    intervals_priced INTEGER NOT NULL,       -- of those, intervals with a price
    price_source TEXT NOT NULL,              -- 'nodal:<node>' | 'cleared:<plant>'
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (asset_id, date)
);
