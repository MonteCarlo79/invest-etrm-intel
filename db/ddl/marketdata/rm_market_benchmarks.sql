-- db/ddl/marketdata/rm_market_benchmarks.sql
--
-- Market-wide （全网） average contract prices by province/channel/month.
-- Source: 信息汇总 workbooks (台账/mtm/*电力市场信息汇总*.xlsx) or manual entry.
-- Used by retail-risk trader-alpha attribution (our price vs market average).

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

COMMENT ON TABLE marketdata.rm_market_benchmarks IS
    'Market-wide average contract prices (全网均价) by province/channel/month. '
    'Pivot for trader/sales P&L attribution in retail-risk.';

CREATE INDEX IF NOT EXISTS idx_rm_mb_province_month ON marketdata.rm_market_benchmarks(province, month);
