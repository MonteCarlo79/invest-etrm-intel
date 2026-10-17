-- marketdata.md_mengxi_nodal_price_96_filled — v4 materialized union (2026-10-17).
--
-- The v2/v3 union view was semantically correct but the planner would not
-- produce a usable plan for it on this RDS (every query form scanned tens of
-- millions of rows; three nights of PF "no results" / multi-hour days).
-- v4 materializes the same union SQL once per day into this plainly-indexed
-- table; the view becomes a thin SELECT * wrapper, so no consumer changes.
--
-- Filler SQL (also used by the daily EventBridge rule bess-platform-mnp96-fill):
--   exchange branch: WHERE data_date = :d  (authoritative when present)
--   fengxing branch: fills only (CST date, node, instant) slots absent there
CREATE TABLE IF NOT EXISTS marketdata.md_mengxi_nodal_price_96_filled (
    node_name      TEXT      NOT NULL,
    metric_time    TIMESTAMP NOT NULL,
    time_order_96  SMALLINT  NOT NULL,
    avg_node_price NUMERIC(12,4),
    inserted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (node_name, metric_time)
);
CREATE INDEX IF NOT EXISTS ix_mnp96f_time
    ON marketdata.md_mengxi_nodal_price_96_filled (metric_time);

-- One-day fill (parameter :d, a date). Insert is idempotent via PK conflict.
INSERT INTO marketdata.md_mengxi_nodal_price_96_filled
    (node_name, metric_time, time_order_96, avg_node_price)
SELECT node_name,
       (datetime AT TIME ZONE '+08:00'),
       (EXTRACT(hour FROM datetime) * 4 + EXTRACT(minute FROM datetime) / 15 + 1)::smallint,
       node_price::numeric(12,4)
FROM marketdata.md_rt_nodal_price
WHERE data_date = :d
UNION ALL
SELECT f.node_name,
       (f.metric_time AT TIME ZONE '+08:00'),
       f.time_order_96::smallint,
       f.avg_node_price::numeric(12,4)
FROM marketdata.md_shanxi_nodal_price_96 f
WHERE f.market_name = '蒙西'
  AND (f.metric_time AT TIME ZONE '+08:00')::date = :d
  AND NOT EXISTS (
      SELECT 1 FROM marketdata.md_rt_nodal_price r
      WHERE r.data_date = :d
        AND r.node_name = f.node_name
        AND r.datetime  = f.metric_time)
ON CONFLICT (node_name, metric_time) DO NOTHING;
