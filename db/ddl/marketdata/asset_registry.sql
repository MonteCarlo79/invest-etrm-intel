-- Asset registry: canonical DB entity for the investment-ops lifecycle
-- (screening → upcoming → operating). Seeded from knowledge/mengxi/bess_node_registry.md.
-- Rollback: DROP TABLE marketdata.asset_registry.
CREATE TABLE IF NOT EXISTS marketdata.asset_registry (
    asset_code      TEXT PRIMARY KEY,
    plant_name      TEXT NOT NULL,
    asset_type      TEXT NOT NULL DEFAULT 'bess',
    status          TEXT NOT NULL DEFAULT 'operating'
        CHECK (status IN ('screening', 'upcoming', 'operating', 'retired')),
    province        TEXT NOT NULL DEFAULT '蒙西',
    zone            TEXT,
    capacity_mw     DOUBLE PRECISION,
    duration_h      DOUBLE PRECISION,
    capacity_source TEXT,
    substation      TEXT,
    conn_kv         TEXT,
    price_node_own  TEXT,
    price_node_parents TEXT,
    zone_price_node TEXT,           -- node used for zone price series (220kV.1M convention)
    cod_date        DATE,
    ops_data_since  DATE,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
