-- marketdata.md_mengxi_nodal_price_96 — applied 2026-10-14 (user-confirmed).
--
-- Exchange-preferred union: the 蒙西 exchange daily workbook (md_rt_nodal_price,
-- reconcile pipeline) is authoritative for every day it contains; Fengxing API
-- rows (md_shanxi_nodal_price_96, market_name='蒙西') fill days the workbook
-- lacks. Motivation: the exchange workbook's 实时节点电价 sheet went empty from
-- 2026-10-09 (source-side outage confirmed by the manually downloaded 2.59 MB
-- file), while the Fengxing API continues serving full days.
--
-- Rollback: restore the original single-source definition:
--   CREATE OR REPLACE VIEW marketdata.md_mengxi_nodal_price_96 AS
--   SELECT node_name,
--          (datetime AT TIME ZONE 'Asia/Shanghai') AS metric_time,
--          (EXTRACT(hour FROM datetime)*4 + EXTRACT(minute FROM datetime)/15 + 1)::smallint AS time_order_96,
--          node_price::numeric(12,4) AS avg_node_price
--   FROM marketdata.md_rt_nodal_price;
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
  AND f.metric_time::date NOT IN (SELECT DISTINCT data_date FROM marketdata.md_rt_nodal_price);
