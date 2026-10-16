-- marketdata.md_mengxi_nodal_price_96 — v2, applied 2026-10-16 (user-confirmed).
--
-- v1 (2026-10-14) unioned exchange + Fengxing with a per-DAY anti-join and
-- UTC ::date math. Two production bugs: (1) Fengxing CST slots 1-32 evaluate
-- ::date as day-1 in UTC sessions and were wrongly excluded whenever day-1
-- existed in the exchange (64-slot days → PF "no results"); (2) a partial
-- exchange day (e.g. 10-11's partial RT sheet) let exchange AND Fengxing serve
-- the same slots → duplicate time_order_96 → dispatch reindex crash.
--
-- v2: slot-level preference. Exchange rows always win; a Fengxing row is
-- served only when NO exchange row exists for the same (CST date, node,
-- absolute instant). CST date math on both sides; absolute-instant equality
-- (both columns timestamptz) avoids per-row tz conversion on the probe side.
--
-- Rollback: restore the original single-source definition (see bottom).
CREATE OR REPLACE VIEW marketdata.md_mengxi_nodal_price_96 AS
SELECT node_name,
       (datetime AT TIME ZONE 'Asia/Shanghai') AS metric_time,
       (EXTRACT(hour FROM datetime) * 4 + EXTRACT(minute FROM datetime) / 15 + 1)::smallint AS time_order_96,
       node_price::numeric(12,4) AS avg_node_price
FROM marketdata.md_rt_nodal_price
UNION ALL
SELECT f.node_name,
       (f.metric_time AT TIME ZONE 'Asia/Shanghai') AS metric_time,
       f.time_order_96::smallint,
       f.avg_node_price::numeric(12,4)
FROM marketdata.md_shanxi_nodal_price_96 f
WHERE f.market_name = '蒙西'
  AND NOT EXISTS (
      SELECT 1
      FROM marketdata.md_rt_nodal_price r
      WHERE r.data_date = (f.metric_time AT TIME ZONE 'Asia/Shanghai')::date
        AND r.node_name = f.node_name
        AND r.datetime  = f.metric_time
  );

-- Original (rollback target):
-- CREATE OR REPLACE VIEW marketdata.md_mengxi_nodal_price_96 AS
-- SELECT node_name,
--        (datetime AT TIME ZONE 'Asia/Shanghai') AS metric_time,
--        (EXTRACT(hour FROM datetime)*4 + EXTRACT(minute FROM datetime)/15 + 1)::smallint AS time_order_96,
--        node_price::numeric(12,4) AS avg_node_price
-- FROM marketdata.md_rt_nodal_price;
