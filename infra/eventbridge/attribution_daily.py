# Source of the EventBridge rule bess-platform-attribution-daily
# (cron(45 9 * * ? *) = 17:45 CST). The rule embeds this file base64-encoded
# in its container override: sh -c "echo <b64> | base64 -d | python -u",
# running on the bess-platform-mengxi-dashboard task family (latest revision).
# After editing, re-encode and put-targets; this file is the record, AWS the copy.
#
# Rolling 10-day catch-up, not yesterday-only: prices complete ~2 days late
# and WeCom nominations later, so each day is retried until its inputs land.
# Needs catch_up() in the image (mengxi-dashboard >= v60).
from datetime import date, timedelta
from services.decision_models.run_daily_strategy_batch import catch_up

out = catch_up(date.today() - timedelta(days=1), lookback_days=10)
print("CATCHUP", out, flush=True)
print("DONE", flush=True)
