-- marketdata.md_mengxi_nodal_price_96
-- Mengxi-only view over the (mis-named) multi-province nodal table.
-- Applied to bess-platform-pg 2026-09-01. Rollback: DROP VIEW marketdata.md_mengxi_nodal_price_96;
CREATE OR REPLACE VIEW marketdata.md_mengxi_nodal_price_96 AS
SELECT node_name, metric_time, time_order_96, avg_node_price
FROM marketdata.md_shanxi_nodal_price_96
WHERE market_name = '蒙西';
