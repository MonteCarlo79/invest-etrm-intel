-- 零碳46 (悦盛昌渠) medium/long-term trade confirmations, one row per 成交单 line.
-- Source: data/raw/零碳46交易数据&月度复盘/零碳46交易数据（2026年1~7月）/ monthly 总表 workbooks.
-- Volumes are monthly *delivery* slices (annual contracts repeat in each monthly file).

CREATE TABLE IF NOT EXISTS marketdata.wind_trades (
    id              SERIAL PRIMARY KEY,
    asset_name      TEXT NOT NULL,            -- 新_悦盛昌渠#1期
    delivery_month  DATE NOT NULL,            -- first of month
    channel         TEXT NOT NULL,            -- 省内 | 跨省
    row_no          INTEGER NOT NULL,         -- 序号 in source file
    trade_type      TEXT NOT NULL,            -- 交易品种
    mode            TEXT,                     -- 交易模式 (挂牌/双边协商/集中竞争/竞价)
    period          TEXT,                     -- 交易周期 (多年期/年度/月度/月内)
    energy_kind     TEXT,                     -- 正常 | 发电置换 | 用电置换 | 汇总
    consumer_unit   TEXT,
    generator_unit  TEXT,
    volume_mwh      NUMERIC,
    energy_price    NUMERIC,                  -- 元/MWh 电能量价格
    energy_fee      NUMERIC,                  -- 元 电能量电费
    env_value       NUMERIC,                  -- 元/MWh 环境价值
    all_in_price    NUMERIC,                  -- 元/MWh 综合价格
    source_file     TEXT NOT NULL,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wind_trades_month
    ON marketdata.wind_trades (asset_name, delivery_month);

-- Idempotent reload: one row per source-file line. (Trade lines repeat
-- identical volumes/prices across thousands of 置换 fragments, so only
-- (source_file, row_no) is a safe natural key.)
CREATE UNIQUE INDEX IF NOT EXISTS uq_wind_trades_line
    ON marketdata.wind_trades (source_file, row_no);
