# services/nodal_agents/daily_job.py
"""Daily L3 writer job — produces tomorrow's nominated strategies and scores
yesterday's converged strategies against actuals.

Schedule: daily ~16:00 CST (before the 17:00 D+1 nomination cutoff). Two steps:

1. run_day(tomorrow)      — L2/L3 forecast -> recursive optimal dispatch for
   every active registry asset, upserted to marketdata.nodal_strategy_daily
   (ON CONFLICT plant_name+target_date+model_version, so reruns are clean).
2. register_strategies(yesterday) — yesterday's converged strategies vs
   actual RT prices + PF theoretical -> marketdata.strategy_experiments
   (scope='nodal_agent' promote loop). Gracefully 0 when actuals/theory are
   missing for the date.

Runs as a one-shot Fargate task (EventBridge schedule). Exit non-zero on
failure so the task alarm/ops log can see it.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta

import psycopg2


def main() -> int:
    from services.nodal_agents import writer

    conn = psycopg2.connect(os.environ.get("PGURL") or os.environ.get("DB_DSN"))
    try:
        tomorrow = date.today() + timedelta(days=1)
        out = writer.run_day(conn, tomorrow)
        print(f"RUN_DAY {out}", flush=True)

        yesterday = date.today() - timedelta(days=1)
        try:
            n = writer.register_strategies(conn, yesterday)
            print(f"REGISTER {n}", flush=True)
        except ImportError:
            # services.bess_map.strategy_experiments is owned by the parallel
            # price-forecasting workstream and is not yet committed to main —
            # the promote loop starts working the day their module lands in
            # an image. Strategy production (RUN_DAY above) is unaffected.
            print("REGISTER skipped: strategy_experiments module not in image "
                  "(parallel workstream, not yet on main)", flush=True)
    finally:
        conn.close()

    # A zero-plant run means the registry is empty or the forecast stack is
    # broken — fail loudly rather than silently "succeed" with nothing.
    return 0 if out.get("plants", 0) > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
