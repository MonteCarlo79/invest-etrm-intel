#!/bin/bash
# CITIC Futures weekly ingest wrapper — launchd daily 09:05.
# Local work is file ingestion + ingest-log only; the digest is generated and
# sent by the ECS hermes cron (Anthropic calls are geo-blocked from this Mac).

set -u

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

# Load environment: PGURL, FEISHU_APP_ID/SECRET, FEISHU_OWNER_OPEN_ID
set -a
# shellcheck disable=SC1091
source config/.env
set +a

mkdir -p logs

exec "$HOME/.venvs/bess-platform/bin/python" \
    services/citic_futures/run_weekly.py "$@" \
    >> logs/citic_weekly.log 2>&1
