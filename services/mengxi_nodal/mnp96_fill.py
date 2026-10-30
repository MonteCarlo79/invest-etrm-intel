# scripts/mnp96_fill.py
"""v5 materializer for marketdata.md_mengxi_nodal_price_96_filled.

v5 (2026-10-30): render via named zone 'Asia/Shanghai', NOT the interval
literal '+08:00' — the literal is POSIX-inverted (renders UTC-8 walls).
Under v4 the exchange branch (naive->timestamptz direction) double-inverted
and landed CST-correct by luck, while the fengxing branch
(timestamptz->naive) landed 16h off, so mixed exchange+fengxing days were
internally incoherent (the Oct-17 class of partial-day failures).

Final semantics (2026-10-17, after 4 failed variants): window by the RENDERED
CST time and merge on RENDERED slot identity.

Why: the exchange table (md_rt_nodal_price) is UTC-day-aligned — a CST day's
rows split across data_date=D (CST 08:00-23:45) and data_date=D-1 (CST
00:00-07:45), and the split varies by publication era. The Fengxing table
stores proper UTC instants with correct CST slot numbers. Any filter on raw
data_date / raw metric_time bounds, and any instant- or raw-slot anti-join,
misselects rows by 8-24h. Rendering first (UTC -> CST wall) and THEN
windowing + slot-matching is the only rule that is correct under every
convention observed.

Per CST day D:
  1. exchange rows whose rendered time falls in D (pulls from data_date D-1,D,D+1
     as needed) — exchange always wins a slot it covers
  2. fengxing rows whose rendered time falls in D, for slots the exchange
     did NOT deliver FOR DAY D for that node (NOT EXISTS on node+slot,
     anti-join windowed to D — fixed 2026-10-30)

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
SELECT node_name, mt, slot, price FROM (
    SELECT node_name,
           ((datetime AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai') AS mt,
           (EXTRACT(hour FROM ((datetime AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai')) * 4
            + EXTRACT(minute FROM ((datetime AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai')) / 15 + 1)::smallint AS slot,
           node_price::numeric(12,4) AS price
    FROM marketdata.md_rt_nodal_price
    WHERE data_date BETWEEN :dminus AND :dplus
) x
WHERE mt >= CAST(:d AS timestamp) AND mt < CAST(:d AS timestamp) + INTERVAL '1 day'
UNION ALL
SELECT f.node_name, f.mt, f.slot, f.price FROM (
    SELECT node_name,
           (metric_time AT TIME ZONE 'Asia/Shanghai') AS mt,
           time_order_96::smallint AS slot,
           avg_node_price::numeric(12,4) AS price
    FROM marketdata.md_shanxi_nodal_price_96
    WHERE market_name = '蒙西'
      AND metric_time >= CAST(:d AS timestamp) - INTERVAL '16 hours'
      AND metric_time < CAST(:d AS timestamp) + INTERVAL '40 hours'
) f
WHERE f.mt >= CAST(:d AS timestamp) AND f.mt < CAST(:d AS timestamp) + INTERVAL '1 day'
  AND NOT EXISTS (
      SELECT 1 FROM (
          SELECT node_name AS n,
                 ((datetime AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai') AS mt,
                 (EXTRACT(hour FROM ((datetime AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai')) * 4
                  + EXTRACT(minute FROM ((datetime AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai')) / 15 + 1)::smallint AS s
          FROM marketdata.md_rt_nodal_price
          WHERE data_date BETWEEN :dminus AND :dplus
      ) r
      WHERE r.n = f.node_name AND r.s = f.slot
        -- windowed to day D (2026-10-30 fix): an unwindowed anti-join shadows
        -- fengxing with ALL 96 slot numbers from ANY full exchange day in
        -- D-1..D+1, so fengxing could never top up a partial exchange day
        AND r.mt >= CAST(:d AS timestamp) AND r.mt < CAST(:d AS timestamp) + INTERVAL '1 day')
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
             "WHERE metric_time >= CAST(:d AS timestamp) "
             "AND metric_time < CAST(:d AS timestamp) + INTERVAL '1 day'")


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
        params = {"d": day, "dminus": day - timedelta(days=1), "dplus": day + timedelta(days=1)}
        with e.begin() as conn:
            conn.execute(text(
                "DELETE FROM marketdata.md_mengxi_nodal_price_96_filled "
                "WHERE metric_time >= CAST(:d AS timestamp) "
                "AND metric_time < CAST(:d AS timestamp) + INTERVAL '1 day'"),
                {"d": day})
            res = conn.execute(text(FILL_SQL), params)
            n = conn.execute(text(COUNT_SQL), {"d": day}).scalar()
        print(f"{day}: +{res.rowcount} rows (day total {n}) in {round(time.time() - t0, 1)}s", flush=True)
        day += timedelta(days=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
