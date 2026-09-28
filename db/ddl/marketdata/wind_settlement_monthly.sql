-- 零碳46 monthly settlement replication results vs book 6 bills.
-- Written by services/wind_settlement/report.py --persist.

CREATE TABLE IF NOT EXISTS marketdata.wind_settlement_monthly (
    asset_name          TEXT NOT NULL,
    settle_month        DATE NOT NULL,        -- first of month
    -- volumes
    gen_proxy_mwh       NUMERIC,              -- ID-cleared ×0.25 generation proxy
    bill_vol_mwh        NUMERIC,              -- bill 现货 energy volume
    -- spot
    capture_price       NUMERIC,              -- 元/MWh generation-weighted RT price
    spot_value_cny      NUMERIC,              -- Σ gen×RT (proxy)
    bill_spot_cny       NUMERIC,              -- bill 现货电费
    implied_cfd_cny     NUMERIC,              -- bill_spot − spot_value
    -- contracts
    contract_vol_mwh    NUMERIC,
    ref_price_west      NUMERIC,
    ref_price_east      NUMERIC,
    ref_price_sys       NUMERIC,
    cfd_west_cny        NUMERIC,              -- Σ vol×(price−ref_west) single-ref
    cfd_sys_cny         NUMERIC,              -- Σ vol×(price−ref_sys) single-ref
    cfd_zone_cny        NUMERIC,              -- Σ vol×(price−ref_zone(c)) per-counterparty zone
    green_cny           NUMERIC,              -- Σ vol×env (aggregate)
    green_min_cny       NUMERIC,              -- Σ_t min(合约曲线_t,实际_t)×env (曲线合理度 basis)
    bill_green_cny      NUMERIC,
    -- fees + totals (bill values)
    bill_fees_cny       NUMERIC,
    bill_total_cny      NUMERIC,
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (asset_name, settle_month)
);
