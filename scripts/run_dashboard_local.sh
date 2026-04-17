#!/usr/bin/env bash
# ============================================================
#  Local dashboard runner for BESS Platform (Unix/Mac/WSL)
#
#  Runs portal (8500) and inner-mongolia (8504) locally,
#  connecting directly to AWS RDS.  Bypasses ALB OIDC auth.
#
#  BEFORE FIRST RUN:
#    1. Set DB_DSN env var or edit the export line below
#    2. Ensure your IP is in rds-sg inbound on port 5432
#       (see docs/dashboard_local_runbook.md §Network)
#    3. Optionally set AWS_PROFILE for S3 access
#
#  Run from the repo root:
#    cd ~/bess-platform
#    bash scripts/run_dashboard_local.sh
# ============================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# ---- Auth: bypass ALB OIDC, use synthetic dev user ----
export AUTH_MODE=dev
export DEV_USER_EMAIL="${DEV_USER_EMAIL:-dev@local}"
export DEV_USER_ROLE="${DEV_USER_ROLE:-Admin}"

# ---- Database: edit DB_DSN or export it before running this script ----
# export DB_DSN="postgresql://postgres:PASSWORD@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require"
export PGURL="${PGURL:-${DB_DSN:-}}"

# ---- Portal app link overrides: local ports instead of AWS paths ----
# slug=url pairs; slug matches the path-component of APP_CATALOG paths
export APP_URL_MAP="${APP_URL_MAP:-inner-mongolia=http://localhost:8504,bess-map=http://localhost:8503,uploader=http://localhost:8501,model-catalogue=http://localhost:8506}"

# ---- AWS profile (optional) ----
# export AWS_PROFILE="default"
# export AWS_REGION="ap-southeast-1"

# ---- Validate ----
if [ -z "${DB_DSN:-}" ] && [ -z "${PGURL:-}" ]; then
    echo "[WARN] Neither DB_DSN nor PGURL is set. Dashboards will fail to connect to the database."
    echo "       Export DB_DSN before running or uncomment the export line above."
    echo ""
fi

echo ""
echo "Starting portal on http://localhost:8500 ..."
streamlit run apps/portal/app.py \
    --server.port 8500 \
    --server.baseUrlPath "" &
PORTAL_PID=$!

echo "Starting inner-mongolia on http://localhost:8504 ..."
streamlit run apps/bess-inner-mongolia/im/app.py \
    --server.port 8504 \
    --server.baseUrlPath "" &
IM_PID=$!

echo ""
echo "  Portal:         http://localhost:8500  (PID $PORTAL_PID)"
echo "  Inner Mongolia: http://localhost:8504  (PID $IM_PID)"
echo ""
echo "Press Ctrl+C to stop both."

trap "kill $PORTAL_PID $IM_PID 2>/dev/null; echo ''; echo 'Stopped.'" INT TERM
wait
