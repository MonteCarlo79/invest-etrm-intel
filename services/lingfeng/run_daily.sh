#!/bin/bash
# LingFeng daily collection wrapper — macOS/launchd equivalent of run_daily.bat.
# Registered as a LaunchAgent; runs daily at 04:00 (see setup_schedule_launchd.sh).
# If the Mac is asleep at 04:00, launchd runs the job at next wake.

set -u

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

# Load environment: PGURL, LINGFENG_USERNAME/PASSWORD, FEISHU/TELEGRAM alert keys
set -a
# shellcheck disable=SC1091
source config/.env
set +a

mkdir -p logs

exec "$HOME/.venvs/bess-platform/bin/python" \
    services/lingfeng/run_daily.py \
    --markets all --models ols_rt_time_v1,naive_rt_ar17,ols_fundamentals_v1 \
    >> logs/lingfeng_daily.log 2>&1
