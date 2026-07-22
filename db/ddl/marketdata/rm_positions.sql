-- db/ddl/marketdata/rm_positions.sql
--
-- Individual trade positions and unified hourly position volume table.

CREATE TABLE IF NOT EXISTS marketdata.rm_positions (
    id               SERIAL PRIMARY KEY,
    book_id          INTEGER NOT NULL REFERENCES marketdata.rm_books(id),
    instrument_type  TEXT NOT NULL CHECK (instrument_type IN
                       ('bilateral','spot','futures','option','forward','profile')),
    province         TEXT NOT NULL,
    channel          TEXT NOT NULL CHECK (channel IN (
                       'DA','RT','monthly_auction','monthly_listed',
                       'intramonth_match','annual','ancillary','capacity')),
    direction        TEXT NOT NULL CHECK (direction IN ('buy','sell')),
    volume_mwh       NUMERIC(14,4) NOT NULL,
    price_cny_mwh    NUMERIC(10,4),
    start_date       DATE NOT NULL,
    end_date         DATE NOT NULL,
    counterparty     TEXT,
    status           TEXT DEFAULT 'open' CHECK (status IN ('open','closed','expired')),
    uploaded_at      TIMESTAMPTZ DEFAULT NOW(),
    upload_batch_id  TEXT
);

COMMENT ON TABLE marketdata.rm_positions IS
    'Individual trade/position records by channel. '
    'Channels: DA=日前, RT=实时, monthly_auction=月度竞价, monthly_listed=月度挂牌, '
    'intramonth_match=月内撮合, annual=年度, ancillary/capacity=non-energy.';

CREATE INDEX IF NOT EXISTS idx_rm_positions_book ON marketdata.rm_positions(book_id);
CREATE INDEX IF NOT EXISTS idx_rm_positions_dates ON marketdata.rm_positions(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_rm_positions_status ON marketdata.rm_positions(status);

CREATE TABLE IF NOT EXISTS marketdata.rm_position_volumes (
    id                              SERIAL PRIMARY KEY,
    book_id                         INTEGER NOT NULL REFERENCES marketdata.rm_books(id),
    delivery_date                   DATE NOT NULL,
    hour                            SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),

    -- Trading channel prices (CNY/MWh)
    da_price_cny_mwh                NUMERIC(10,4),
    rt_price_cny_mwh                NUMERIC(10,4),
    monthly_auction_price_cny_mwh   NUMERIC(10,4),
    monthly_listed_price_cny_mwh    NUMERIC(10,4),
    intramonth_match_price_cny_mwh  NUMERIC(10,4),
    annual_price_cny_mwh            NUMERIC(10,4),

    -- Trading channel volumes (MWh)
    da_volume_mwh                   NUMERIC(10,4),
    rt_volume_mwh                   NUMERIC(10,4),
    monthly_auction_volume_mwh      NUMERIC(10,4),
    monthly_listed_volume_mwh       NUMERIC(10,4),
    intramonth_match_volume_mwh     NUMERIC(10,4),
    annual_volume_mwh               NUMERIC(10,4),

    -- Derived / computed
    market_price_cny_mwh            NUMERIC(10,4),
    actual_price_cny_mwh            NUMERIC(10,4),
    pnl_cny                         NUMERIC(14,2),

    -- Volume waterfall
    nominated_mwh                   NUMERIC(10,4),
    cleared_mwh                     NUMERIC(10,4),
    settled_mwh                     NUMERIC(10,4),
    deviation_bid_mwh               NUMERIC(10,4) DEFAULT 0,
    deviation_equipment_mwh         NUMERIC(10,4) DEFAULT 0,
    deviation_sysop_mwh             NUMERIC(10,4) DEFAULT 0,
    deviation_grid_flow_mwh         NUMERIC(10,4) DEFAULT 0,

    upload_batch_id                 TEXT,
    UNIQUE (book_id, delivery_date, hour)
);

COMMENT ON TABLE marketdata.rm_position_volumes IS
    'Unified hourly position volumes per book. One row per book per delivery hour. '
    'Stores price + volume for 6 Chinese electricity market channels. '
    'Used by both asset books (generation) and load books (retail).';

CREATE INDEX IF NOT EXISTS idx_rm_pv_book_date ON marketdata.rm_position_volumes(book_id, delivery_date);
CREATE INDEX IF NOT EXISTS idx_rm_pv_date ON marketdata.rm_position_volumes(delivery_date);
