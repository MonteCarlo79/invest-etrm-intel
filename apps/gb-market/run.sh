#!/bin/bash
set -e

# Start the standalone scheduler service as a background process.
# It runs independently of Streamlit user visits, so jobs fire even if
# nobody has opened the app page.
python apps/gb-market/scheduler_service.py &
SCHED_PID=$!
echo "[RUN] GB scheduler service started (PID $SCHED_PID)"

# When Streamlit exits (or is killed), also kill the scheduler.
cleanup() {
    echo "[RUN] Stopping scheduler (PID $SCHED_PID)"
    kill "$SCHED_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Start Streamlit as the foreground (main) process.
# 'exec' replaces this shell so signals go directly to Streamlit.
exec streamlit run apps/gb-market/app.py \
    --server.port=8508 \
    --server.address=0.0.0.0 \
    --server.baseUrlPath=gb-market \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false \
    --server.headless=true \
    --server.fileWatcherType=none
