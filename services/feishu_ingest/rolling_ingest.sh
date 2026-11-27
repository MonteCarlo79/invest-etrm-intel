#!/bin/bash
# Stage a KB ingest batch whenever >=200 exported files are pending and no batch is running;
# after the exports finish, flush the remainder and exit.
cd "$(dirname "$0")/../.." || exit 1
PY=~/.venvs/bess-platform/bin/python; ST=~/feishu-ingest/logs/ingest_state.json; LOG=~/feishu-ingest/logs/rolling.log
n=$(python3 -c "import json;print(len(json.load(open('$ST'))['batches']))")
while true; do
  last=$(python3 -c "import json;b=json.load(open('$ST'))['batches'];print(b[-1]['task'] if b else '')")
  st=$( [ -n "$last" ] && aws ecs describe-tasks --cluster bess-platform-cluster --tasks $last --region ap-southeast-1 --query 'tasks[0].lastStatus' --output text 2>/dev/null || echo STOPPED)
  exporting=$(pgrep -f "export.py --manifest" >/dev/null && echo 1 || echo 0)
  if [ "$st" = "STOPPED" ]; then
    if [ -n "$last" ] && [ "$last" != "$reported" ]; then
      r=$(aws logs get-log-events --region ap-southeast-1 --log-group-name /ecs/bess-platform --log-stream-name "spot-markets/spot-markets/$last" --start-from-head --query 'events[*].message' --output text 2>/dev/null | tr '\t' '\n' | grep -E "^BATCH DONE" | tail -1)
      echo "$(date '+%m-%d %H:%M') batch $last: ${r:-no BATCH DONE line (check log)}" >> $LOG; reported=$last
    fi
    min=200; [ "$exporting" = 0 ] && min=1
    n=$((n+1)); out=$($PY services/feishu_ingest/stage_ingest.py feishu-g-$(printf %02d $n) --min $min 2>&1 | tail -1)
    case "$out" in TASK*) echo "$(date '+%m-%d %H:%M') launched $out" >> $LOG ;; *) n=$((n-1)); [ "$exporting" = 0 ] && { echo "$(date '+%m-%d %H:%M') DONE: exports finished, nothing pending" >> $LOG; exit 0; } ;; esac
  fi
  sleep 600
done
