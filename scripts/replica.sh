#!/usr/bin/env bash
# Local read replica of the platform database (the Mac cannot reach RDS).
#
#   scripts/replica.sh sync      dump in AWS -> S3 -> restore into local Postgres
#   scripts/replica.sh restore   restore the newest S3 snapshot (no new dump)
#   scripts/replica.sh start|stop|status
#   scripts/replica.sh url       print the local PGURL
#
# Local Postgres 18 + pgvector lives in ~/local-replica (port 5433, trust auth,
# localhost only). The replica is disposable: every restore rebuilds it in place
# (apps lose the database for ~5 min; the slow part is the S3 download).
# Big tables carry a recent window only — see services/local_replica/dump.py.
set -euo pipefail

R="${REPLICA_HOME:-$HOME/local-replica}"
PG="$R/pg/bin"
PORT=5433
DB=marketdata
BUCKET=bess-uploader-data-chen-singp-2026
REGION=ap-southeast-1
CLUSTER=bess-platform-cluster
SRC_SERVICE=bess-platform-asset-risk-svc     # borrowed for its image + PGURL only
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SNAP="$R/snapshot"
export PGPORT=$PORT PGUSER=postgres PGHOST=localhost

log() { echo "[$(date '+%H:%M:%S')] $*"; }

start() {
  "$PG/pg_ctl" -D "$R/data" status >/dev/null 2>&1 || "$PG/pg_ctl" -D "$R/data" -l "$R/logs/postgres.log" -w start >/dev/null
}

dump() {
  aws s3 cp --only-show-errors "$REPO/services/local_replica/dump.py" "s3://$BUCKET/replica/dump.py" --region $REGION
  local td cname ov task
  td=$(aws ecs describe-services --cluster $CLUSTER --services $SRC_SERVICE --region $REGION --query 'services[0].taskDefinition' --output text)
  cname=$(aws ecs describe-task-definition --task-definition "$td" --region $REGION --query 'taskDefinition.containerDefinitions[0].name' --output text)
  ov=$(python3 - "$cname" "${REPLICA_WINDOWS:-}" <<'EOF'
import json, sys
cmd = ("python -c \"import boto3;boto3.client('s3').download_file("
       "'bess-uploader-data-chen-singp-2026','replica/dump.py','/tmp/dump.py')\" && python /tmp/dump.py")
print(json.dumps({"containerOverrides": [{"name": sys.argv[1], "command": ["sh", "-c", cmd],
      "environment": [{"name": "REPLICA_WINDOWS", "value": sys.argv[2]}]}],
      "ephemeralStorage": {"sizeInGiB": 60}}))
EOF
)
  task=$(aws ecs run-task --cluster $CLUSTER --task-definition "$td" --launch-type FARGATE --region $REGION \
    --network-configuration 'awsvpcConfiguration={subnets=[subnet-08e93f99a9c0aa3a9,subnet-0e8ae30466a9f72b5],securityGroups=[sg-08576f2bea0274a81],assignPublicIp=DISABLED}' \
    --overrides "$ov" --query 'tasks[0].taskArn' --output text)
  log "dump task ${task##*/} started (on ${td##*/})"
  # tasks-stopped waiter gives up after 10 min; a dump can take longer
  local st=""
  while [ "$st" != "STOPPED" ]; do
    sleep 30
    st=$(aws ecs describe-tasks --cluster $CLUSTER --tasks "$task" --region $REGION --query 'tasks[0].lastStatus' --output text)
  done
  local code
  code=$(aws ecs describe-tasks --cluster $CLUSTER --tasks "$task" --region $REGION --query 'tasks[0].containers[0].exitCode' --output text)
  aws logs get-log-events --region $REGION --log-group-name /ecs/bess-platform \
    --log-stream-name "$cname/$cname/${task##*/}" --start-from-head --query 'events[*].message' --output text \
    | tr '\t' '\n' | grep -E "^\[replica\]|Error|error|FATAL" | tail -15 || true
  [ "$code" = "0" ] || { log "dump task failed (exit $code)"; exit 1; }
}

restore() {
  start
  mkdir -p "$SNAP"
  aws s3 cp --only-show-errors "s3://$BUCKET/replica/latest.json" "$SNAP/latest.json" --region $REGION
  local base; base=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['base'])" "$SNAP/latest.json")
  log "downloading s3://$BUCKET/$base"
  rm -rf "$SNAP/cur"; mkdir -p "$SNAP/cur"
  aws s3 sync --only-show-errors "s3://$BUCKET/$base" "$SNAP/cur" --region $REGION
  tar -xf "$SNAP/cur/dump.tar" -C "$SNAP/cur" && rm "$SNAP/cur/dump.tar"

  # In place, not build-and-swap: two copies (~17 GB each) do not fit on this disk.
  # The download is already complete, so a failed restore is fixed by re-running it.
  local new="$DB"
  "$PG/psql" -q -d postgres \
    -c "select pg_terminate_backend(pid) from pg_stat_activity where datname='$DB'" \
    -c "drop database if exists $DB" -c "create database $DB" >/dev/null
  log "restoring schema + small tables"
  # extension/comment noise from RDS-only objects is non-fatal
  "$PG/pg_restore" -d $new -j 4 --no-owner --no-privileges --section=pre-data --section=data "$SNAP/cur/dump" 2>"$R/logs/restore.err" || true
  python3 - "$SNAP/latest.json" <<'EOF' | while IFS='|' read -r tbl cols; do
import json, sys
for t, w in json.load(open(sys.argv[1]))["windows"].items():
    print(f"{t}|{'' if w['columns'] == '*' else '(' + w['columns'] + ')'}")
EOF
    log "loading window $tbl"
    gunzip -c "$SNAP/cur/win/$tbl.csv.gz" | "$PG/psql" -q -d $new -v ON_ERROR_STOP=1 -c "copy $tbl $cols from stdin with (format csv)"
  done
  log "building indexes and constraints"
  "$PG/pg_restore" -d $new -j 4 --no-owner --no-privileges --section=post-data "$SNAP/cur/dump" 2>>"$R/logs/restore.err" || true
  "$PG/psql" -q -d $new -c "analyze"
  rm -rf "$SNAP/cur"
  local errs; errs=$(grep -c "pg_restore: error" "$R/logs/restore.err" || true)
  log "restore complete: $("$PG/psql" -Atd $DB -c "select pg_size_pretty(pg_database_size('$DB'))"), $errs pg_restore errors (see $R/logs/restore.err)"
}

status() {
  if "$PG/pg_ctl" -D "$R/data" status >/dev/null 2>&1; then
    echo "local Postgres: running on port $PORT"
    "$PG/psql" -Atd $DB -c "select 'size: '||pg_size_pretty(pg_database_size('$DB'))" 2>/dev/null || echo "database $DB not restored yet"
  else
    echo "local Postgres: stopped"
  fi
  [ -f "$SNAP/latest.json" ] && python3 -c "import json,sys;print('snapshot:',json.load(open(sys.argv[1]))['stamp'])" "$SNAP/latest.json" || true
}

case "${1:-status}" in
  sync)    dump; restore ;;
  dump)    dump ;;
  restore) restore ;;
  start)   start; status ;;
  stop)    "$PG/pg_ctl" -D "$R/data" stop ;;
  status)  status ;;
  url)     echo "postgresql://postgres@localhost:$PORT/$DB" ;;
  *)       echo "usage: $0 sync|dump|restore|start|stop|status|url"; exit 2 ;;
esac
