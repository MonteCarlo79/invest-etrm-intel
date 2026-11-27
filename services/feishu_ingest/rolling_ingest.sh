#!/bin/bash
# Keep up to MAXPAR KB ingest batches running in AWS: whenever fewer are running and >=200
# exported files are pending, stage another. After all exports finish, flush the rest and exit.
cd "$(dirname "$0")/../.." || exit 1
PY=~/.venvs/bess-platform/bin/python; ST=~/feishu-ingest/logs/ingest_state.json; LOG=~/feishu-ingest/logs/rolling.log
MAXPAR=${MAXPAR:-3}; REP=~/feishu-ingest/logs/rolling_reported.txt; touch $REP
while true; do
  running=0
  for t in $(python3 -c "import json;print(' '.join(b['task'] for b in json.load(open('$ST'))['batches']))"); do
    grep -q "^$t$" $REP && continue
    st=$(aws ecs describe-tasks --cluster bess-platform-cluster --tasks $t --region ap-southeast-1 --query 'tasks[0].lastStatus' --output text 2>/dev/null || echo UNKNOWN)
    if [ "$st" = "STOPPED" ]; then
      r=$(aws logs get-log-events --region ap-southeast-1 --log-group-name /ecs/bess-platform --log-stream-name "spot-markets/spot-markets/$t" --start-from-head --query 'events[*].message' --output text 2>/dev/null | tr '\t' '\n' | grep -E "^BATCH DONE" | tail -1)
      echo "$(date '+%m-%d %H:%M') batch $t: ${r:-no BATCH DONE line (check log)}" >> $LOG; echo "$t" >> $REP
    else running=$((running+1)); fi
  done
  exporting=$(pgrep -f "export.py --manifest" >/dev/null && echo 1 || echo 0)
  if [ $running -lt $MAXPAR ]; then
    min=200; [ "$exporting" = 0 ] && min=1
    n=$(( $(python3 -c "import json;print(len(json.load(open('$ST'))['batches']))") + 1 ))
    out=$($PY services/feishu_ingest/stage_ingest.py feishu-g-$(printf %02d $n) --min $min 2>&1 | tail -1)
    case "$out" in
      TASK*) echo "$(date '+%m-%d %H:%M') launched $out (running now $((running+1)))" >> $LOG ;;
      *) [ "$exporting" = 0 ] && [ $running -eq 0 ] && { echo "$(date '+%m-%d %H:%M') DONE: exports finished, nothing pending" >> $LOG; exit 0; } ;;
    esac
  fi
  sleep 300
done
