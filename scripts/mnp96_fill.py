# scripts/mnp96_fill.py
"""v4 materializer for marketdata.md_mengxi_nodal_price_96_filled.

Creates the table, then per-day idempotent fills (ON CONFLICT DO NOTHING).
Run as a one-shot Fargate task (daily EventBridge bess-platform-mnp96-fill,
23:40 UTC — after the Fengxing fill, before the nodal PF job).

Usage: python scripts/mnp96_fill.py [START END]   (YYYY-MM-DD; default: yesterday)
"""
import os
import sys
import time
from datetime import date, timedelta

from sqlalchemy import create_engine, text

FILL_SQL = """
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
  AND f.metric_time >= :lo AND f.metric_time < :hi
  AND NOT EXISTS (
      SELECT 1 FROM marketdata.md_rt_nodal_price r
      WHERE r.data_date = :d
        AND r.node_name = f.node_name
        AND r.datetime  = f.metric_time)
ON CONFLICT (node_name, metric_time) DO NOTHING
"""

DDL = """
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
"""

COUNT_SQL = ("SELECT COUNT(*) FROM marketdata.md_mengxi_nodal_price_96_filled "
             "WHERE metric_time >= CAST(:d AS timestamp) AND metric_time < CAST(:d AS timestamp) + 1")


def main() -> int:
    if len(sys.argv) == 3:
        start = date.fromisoformat(sys.argv[1])
        end = date.fromisoformat(sys.argv[2])
    else:
        start = end = date.today() - timedelta(days=1)

    e = create_engine(os.environ["PGURL"])
    with e.begin() as conn:
        for stmt in DDL.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    day = start
    while day <= end:
        t0 = time.time()
        # CST day bounds as absolute instants — sargable on the plain
        # ix_shanxi_nodal_metric_time btree (the ::date expression predicate
        # seq-scanned 43M rows per day, ~40 min/day, before this fix).
        lo = f"{day} 00:00:00+08"
        hi = f"{day + timedelta(days=1)} 00:00:00+08"
        with e.begin() as conn:
            res = conn.execute(text(FILL_SQL), {"d": day, "lo": lo, "hi": hi})
            n = conn.execute(text(COUNT_SQL), {"d": day}).scalar()
        print(f"{day}: +{res.rowcount} rows (day total {n}) in {round(time.time() - t0, 1)}s", flush=True)
        day += timedelta(days=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
