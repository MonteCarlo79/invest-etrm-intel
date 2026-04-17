#!/usr/bin/env bash
# ============================================================
#  Local dashboard runner for BESS Platform
#  Connects to AWS RDS/S3 directly, bypasses ALB OIDC auth.
#
#  BEFORE FIRST RUN:
#    1. Export DB_DSN with the RDS connection string
#    2. Ensure your IP is added to rds-sg inbound on port 5432
#       (see docs/knowledge_pool_aws_migration_recon.md §6)
#    3. Set AWS_PROFILE if you use named AWS profiles
# ============================================================
set -euo pipefail

# ---- Auth (dev mode: no OIDC, synthetic user) ----
export AUTH_MODE=dev
export DEV_USER_EMAIL="${DEV_USER_EMAIL:-dev@local}"
export DEV_USER_ROLE="${DEV_USER_ROLE:-Admin}"

# ---- Database ----
# export DB_DSN="postgresql://postgres:PASSWORD@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require"
# export PGURL="$DB_DSN"

# ---- AWS (for S3/ECS if needed) ----
# export AWS_PROFILE="your-profile"
# export AWS_REGION="ap-southeast-1"

if [ -z "${DB_DSN:-}" ]; then
    echo "[WARN] DB_DSN is not set. Dashboards will fail to connect to the database."
    echo "       Export DB_DSN before running or uncomment the line above."
    echo ""
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

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
echo "Dashboards running:"
echo "  Portal:         http://localhost:8500  (PID $PORTAL_PID)"
echo "  Inner Mongolia: http://localhost:8504  (PID $IM_PID)"
echo ""
echo "Press Ctrl+C to stop both."

trap "kill $PORTAL_PID $IM_PID 2>/dev/null; echo 'Stopped.'" INT TERM
wait
