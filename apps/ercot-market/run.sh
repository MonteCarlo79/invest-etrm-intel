#!/bin/bash
set -e
python services/intl_market_common/scheduler_service.py --code ercot --app-dir apps/ercot-market &
SCHED_PID=$!
echo "[RUN] ERCOT scheduler service started (PID $SCHED_PID)"
cleanup() { kill "$SCHED_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM
exec streamlit run apps/ercot-market/app.py \
    --server.port=8510 \
    --server.address=0.0.0.0 \
    --server.baseUrlPath=ercot-market \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false \
    --server.headless=true \
    --server.fileWatcherType=none
