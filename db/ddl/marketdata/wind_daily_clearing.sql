-- 电力交易中心日清算 (daily clearing) for 悦盛昌渠 — the exchange's own
-- per-15-min settlement record. Source: 日清算202601~202608.xlsx (25,632 rows,
-- 2026-01-01 → 2026-09-24). Metered volumes reconcile to book 6 bills to the MWh.

CREATE TABLE IF NOT EXISTS marketdata.wind_daily_clearing (
    datetime        TIMESTAMP PRIMARY KEY,
    metered_mwh     NUMERIC,        -- 计量电量 (actual metered generation)
    energy_price    NUMERIC,        -- 电能电价 (fee/volume, informational)
    energy_fee      NUMERIC,        -- 电能电费 = 计量×RT + Σ合约×(合约价−区域ref)
    rt_cleared_mw   NUMERIC,        -- 省内实时出清电力
    rt_nodal_price  NUMERIC,        -- 省内实时节点电价
    contract_mwh    NUMERIC,        -- 中长期合约电量 (aggregate contract curve)
    contract_price  NUMERIC,        -- 中长期合约电价 (volume-weighted)
    ic_da_mw        NUMERIC,        -- 省间日前出清电力/电价
    ic_da_price     NUMERIC,
    ic_id_mw        NUMERIC,        -- 省间日内出清电力/电价
    ic_id_price     NUMERIC,
    curve_min       NUMERIC,        -- 曲线合理度取小值 = min(合约, 计量)
    curve_mean      NUMERIC,        -- 曲线合理度取均值
    source_file     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wind_clearing_month ON marketdata.wind_daily_clearing (datetime);
