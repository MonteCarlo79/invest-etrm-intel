-- Performance indexes for the v2 slot-level anti-join in
-- marketdata.md_mengxi_nodal_price_96 (2026-10-16, user-confirmed).
-- Without these the NOT EXISTS probe runs per-row without index support and
-- every view query scans tens of millions of rows (PF day-fetch 30-60s+).
-- Additive only; rollback: DROP INDEX IF EXISTS both.
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_rtnp_date_node_dt
    ON marketdata.md_rt_nodal_price (data_date, node_name, datetime);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_snp96_mengxi_cst_day
    ON marketdata.md_shanxi_nodal_price_96 (((metric_time AT TIME ZONE 'Asia/Shanghai')::date))
    WHERE market_name = '蒙西';
