-- marketdata.md_mengxi_nodal_price_96 — v4 FINAL (2026-10-17/18).
-- Thin wrapper over the materialized filled table. Column metric_time is
-- timestamp WITHOUT tz (CST wall clock) — the PF job's naive ::date arithmetic
-- and per-node 96-slot gate are written against exactly this.
-- Consumers: services/bess_map/nodal_pf_daily.py, Nodal Maps tab.
-- Filler: services/mengxi_nodal/mnp96_fill.py (daily EventBridge rule).
-- Rollback: restore single-source definition (see db/views history or
--   SELECT node_name, (datetime AT TIME ZONE 'Asia/Shanghai'), ..., node_price FROM marketdata.md_rt_nodal_price).
DROP VIEW IF EXISTS marketdata.md_mengxi_nodal_price_96;
CREATE VIEW marketdata.md_mengxi_nodal_price_96 AS
SELECT node_name, metric_time, time_order_96, avg_node_price
FROM marketdata.md_mengxi_nodal_price_96_filled;
