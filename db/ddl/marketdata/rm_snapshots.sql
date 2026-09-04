-- db/ddl/marketdata/rm_snapshots.sql

CREATE TABLE IF NOT EXISTS marketdata.rm_pnl_snapshots (
    id                              SERIAL PRIMARY KEY,
    book_id                         INTEGER NOT NULL REFERENCES marketdata.rm_books(id),
    snapshot_date                   DATE NOT NULL,
    realized_cny                    NUMERIC(16,2),
    unrealized_mtm_cny              NUMERIC(16,2),
    spot_pnl_cny                    NUMERIC(16,2),
    bilateral_pnl_cny               NUMERIC(16,2),
    ancillary_pnl_cny               NUMERIC(16,2),
    deviation_pnl_cny               NUMERIC(16,2),
    curtailment_mwh                 NUMERIC(14,4),
    curtailment_rate_pct            NUMERIC(6,4),
    curtailment_opportunity_cost_cny NUMERIC(16,2),
    equivalent_hours                NUMERIC(8,2),
    other_pnl_cny                   NUMERIC(16,2),
    UNIQUE (book_id, snapshot_date)
);

COMMENT ON TABLE marketdata.rm_pnl_snapshots IS
    'Monthly P&L snapshots per book. Includes wind-specific KPIs (curtailment, equivalent hours).';

CREATE INDEX IF NOT EXISTS idx_rm_pnl_book_date ON marketdata.rm_pnl_snapshots(book_id, snapshot_date);

CREATE TABLE IF NOT EXISTS marketdata.rm_var_snapshots (
    id               SERIAL PRIMARY KEY,
    book_id          INTEGER NOT NULL REFERENCES marketdata.rm_books(id),
    snapshot_date    DATE NOT NULL,
    var_1d_95_cny    NUMERIC(16,2),
    var_1d_99_cny    NUMERIC(16,2),
    var_10d_95_cny   NUMERIC(16,2),
    method           TEXT CHECK (method IN ('historical','parametric')),
    delta_mwh        NUMERIC(14,4),
    gamma            NUMERIC(14,6),
    vega             NUMERIC(14,6),
    UNIQUE (book_id, snapshot_date, method)
);

COMMENT ON TABLE marketdata.rm_var_snapshots IS
    'VaR and Greeks snapshots. Two methods: historical simulation, parametric delta-normal.';

CREATE INDEX IF NOT EXISTS idx_rm_var_book_date ON marketdata.rm_var_snapshots(book_id, snapshot_date);
