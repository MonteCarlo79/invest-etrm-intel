-- National + per-province monthly spot market data from 电力现货市场价格与运行月报.
-- Prices: 元/kWh (= yuan/kWh, same convention as spot_daily).
-- Volumes: 亿kWh as printed in the report. Percent columns store percent numbers (4.82 = 4.82%).

CREATE TABLE IF NOT EXISTS spot_monthly_national (
    report_month               DATE PRIMARY KEY,
    rt_total_volume_yi_kwh     NUMERIC,
    rt_avg_price               NUMERIC,
    da_total_volume_yi_kwh     NUMERIC,
    da_avg_price               NUMERIC,
    mlt_coverage_volume_yi_kwh NUMERIC,
    mlt_coverage_pct           NUMERIC,
    mlt_avg_price              NUMERIC,
    source_file                TEXT,
    ingested_at                TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS spot_monthly_province (
    report_month       DATE NOT NULL,
    province_en        TEXT NOT NULL,
    province_cn        TEXT,
    run_status         TEXT,
    mlt_volume_yi_kwh  NUMERIC,
    mlt_avg_price      NUMERIC,
    mlt_coverage_pct   NUMERIC,
    rt_volume_yi_kwh   NUMERIC,
    rt_avg_price       NUMERIC,
    rt_mom_pct         NUMERIC,
    da_volume_yi_kwh   NUMERIC,
    da_avg_price       NUMERIC,
    da_mom_pct         NUMERIC,
    source_file        TEXT,
    ingested_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (report_month, province_en)
);
