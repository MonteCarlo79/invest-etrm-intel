-- marketdata.md_mengxi_nodal_price_96
-- Mengxi nodal prices view. HISTORY:
--   v1 (2026-09-01): view over marketdata.md_shanxi_nodal_price_96 WHERE market_name='蒙西'
--     (Fengxing API source — found degraded for 蒙西 from ~2026-06: smoothed/flat segments,
--      ~67% slot match vs the trading center's official RT prices).
--   v2 (2026-09-02): repointed to marketdata.md_rt_nodal_price (EnOS / trading-center operator
--     files, ingested daily by the bess-inner-mongolia ECS app). Operator-grade: matches the
--     official 实时节点电价 sheet at 99%. Rollback to v1 below if ever needed.
--
-- v2 (applied 2026-09-02 via one-off ECS task; rollback: replace body with v1 definition above)
CREATE OR REPLACE VIEW marketdata.md_mengxi_nodal_price_96 AS
SELECT node_name,
       (datetime AT TIME ZONE 'Asia/Shanghai') AS metric_time,
       (EXTRACT(HOUR FROM datetime) * 4 + EXTRACT(MINUTE FROM datetime) / 15 + 1)::smallint AS time_order_96,
       node_price::numeric(12,4) AS avg_node_price
FROM marketdata.md_rt_nodal_price;
