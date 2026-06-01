#!/bin/bash
set -e
python services/intl_market_common/scheduler_service.py --code caiso --app-dir apps/caiso-market &
SCHED_PID=$!
echo "[RUN] CAISO scheduler service started (PID $SCHED_PID)"
cleanup() { kill "$SCHED_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM
exec streamlit run apps/caiso-market/app.py \
    --server.port=8512 \
    --server.address=0.0.0.0 \
    --server.baseUrlPath=caiso-market \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false \
    --server.headless=true \
    --server.fileWatcherType=none
