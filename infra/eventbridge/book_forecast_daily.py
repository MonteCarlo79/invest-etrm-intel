# Source of the EventBridge rule bess-platform-book-forecast-daily
# (cron(0 8 * * ? *) = 16:00 CST). The rule embeds this file base64-encoded
# in its container override: sh -c "echo <b64> | base64 -d | python -u",
# running on the bess-platform-mengxi-dashboard task family (latest revision).
# After editing, re-encode and put-targets; this file is the record, AWS the copy.
import os
from datetime import date, timedelta
from sqlalchemy import create_engine
from libs.trading_book.registry import get_stations
from services.mengxi_nodal import book_forecast
from services.mengxi_nodal import market_impact
from services.mengxi_nodal import settlement_bridge
from services.mengxi_nodal import zone_forecast_daily

engine = create_engine(os.environ["PGURL"])
# D+1 artifact: written at 16:00 CST for tomorrow, before the 17:00
# nomination cutoff (same target day as zone_forecast_daily below).
# Was date.today() - 1 until 2026-11-09, a hindcast that was never usable.
fc_day = date.today() + timedelta(days=1)
for s in get_stations(data_path="full"):
    try:
        n = book_forecast.run_day(engine, s.asset_code, fc_day)
        print("FC", s.asset_code, fc_day, n, flush=True)
    except Exception as ex:
        print("FC_ERR", s.asset_code, type(ex).__name__, str(ex)[:150], flush=True)
for s in get_stations(asset_type="bess"):
    if s.data_path == "registry_only":
        continue
    try:
        est = market_impact.persist_lambda(engine, s.asset_code)
        print("LAMBDA", s.asset_code, est["lambda_cny_per_mwh_per_mw"],
              est.get("method_discharge"), est.get("reason"), flush=True)
    except Exception as ex:
        print("LAMBDA_ERR", s.asset_code, type(ex).__name__, str(ex)[:150], flush=True)
t = date.today().replace(day=1)
prev = (t - timedelta(days=1)).replace(day=1)
for s in get_stations(asset_type="bess"):
    if s.data_path == "registry_only":
        continue
    for mth in (str(prev)[:7], str(t)[:7]):
        try:
            n = settlement_bridge.refresh_settled_daily(engine, s.asset_code, mth)
            print("SETTLED", s.asset_code, mth, n, flush=True)
        except Exception as ex:
            print("SETTLED_ERR", s.asset_code, mth, type(ex).__name__, str(ex)[:150], flush=True)
zone_forecast_daily.main()
print("DONE", flush=True)
