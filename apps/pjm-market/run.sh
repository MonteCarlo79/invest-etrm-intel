#!/bin/bash
set -e
python services/intl_market_common/scheduler_service.py --code pjm --app-dir apps/pjm-market &
SCHED_PID=$!
echo "[RUN] PJM scheduler service started (PID $SCHED_PID)"
cleanup() { kill "$SCHED_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM
exec streamlit run apps/pjm-market/app.py \
    --server.port=8511 \
    --server.address=0.0.0.0 \
    --server.baseUrlPath=pjm-market \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false \
    --server.headless=true \
    --server.fileWatcherType=none
