#!/bin/bash
set -e
python services/intl_market_common/scheduler_service.py --code au --app-dir apps/au-market &
SCHED_PID=$!
echo "[RUN] AU scheduler service started (PID $SCHED_PID)"
cleanup() { kill "$SCHED_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM
exec streamlit run apps/au-market/app.py \
    --server.port=8509 \
    --server.address=0.0.0.0 \
    --server.baseUrlPath=au-market \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false \
    --server.headless=true \
    --server.fileWatcherType=none
