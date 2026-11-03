"""Daily D+1 zone forecast writer: predicts tomorrow's 96-slot zones for all
stations and persists to reports.bess_zone_forecast (model='recency_daytype_v1').

Runs alongside the book-forecast job (16:00 CST, before the 17:00 nomination
cutoff). Tomorrow's zone masks feed the strategy LP."""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta

from sqlalchemy import create_engine

from libs.trading_book.registry import get_stations
from services.mengxi_nodal.zone_forecast import persist_forecast

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("zone_forecast_daily")


def main() -> None:
    engine = create_engine(os.environ["PGURL"])
    tomorrow = date.today() + timedelta(days=1)
    for s in get_stations(asset_type="bess"):
        if s.data_path == "registry_only":
            continue
        try:
            n = persist_forecast(engine, s.asset_code, tomorrow)
            logger.info("FORECAST %s %s: %d rows", s.asset_code, tomorrow, n)
        except Exception as ex:
            logger.warning("FORECAST_ERR %s: %s: %s", s.asset_code, type(ex).__name__, ex)


if __name__ == "__main__":
    main()
