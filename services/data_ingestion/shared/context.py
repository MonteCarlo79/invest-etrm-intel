"""RunContext: parsed run parameters for any collector."""
from __future__ import annotations
import argparse
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional


@dataclass
class RunContext:
    collector: str
    mode: str               # daily | reconcile | backfill
    start_date: Optional[date]
    end_date: Optional[date]
    lookback_days: int
    dry_run: bool
    dataset_filter: Optional[str]   # comma-separated allowlist

    @classmethod
    def from_env_and_args(cls, collector: str, argv=None) -> "RunContext":
        p = argparse.ArgumentParser()
        p.add_argument("--mode", default=os.getenv("RUN_MODE", "daily"),
                       choices=["daily", "reconcile", "backfill"])
        p.add_argument("--start-date", default=os.getenv("START_DATE"))
        p.add_argument("--end-date",   default=os.getenv("END_DATE"))
        p.add_argument("--lookback-days", type=int,
                       default=int(os.getenv("LOOKBACK_DAYS", "2")))
        p.add_argument("--dry-run", action="store_true",
                       default=os.getenv("DRY_RUN", "").lower() in ("1", "true"))
        p.add_argument("--dataset-filter", default=os.getenv("DATASET_FILTER"))
        args = p.parse_args(argv)

        today = date.today()
        if args.mode == "daily":
            end   = today - timedelta(days=1)
            start = end - timedelta(days=args.lookback_days - 1)
        else:
            # reconcile / backfill: require explicit dates
            if not args.start_date:
                raise SystemExit("--start-date / START_DATE required for reconcile/backfill")
            start = date.fromisoformat(args.start_date)
            end   = date.fromisoformat(args.end_date) if args.end_date else today - timedelta(days=1)

        return cls(
            collector=collector,
            mode=args.mode,
            start_date=start,
            end_date=end,
            lookback_days=args.lookback_days,
            dry_run=args.dry_run,
            dataset_filter=args.dataset_filter,
        )
