@echo off
REM ============================================================
REM  Local dashboard runner for BESS Platform
REM  Connects to AWS RDS/S3 directly, bypasses ALB OIDC auth.
REM
REM  BEFORE FIRST RUN:
REM    1. Set DB_DSN to the RDS connection string below
REM    2. Ensure your IP is added to rds-sg inbound on port 5432
REM       (see docs/knowledge_pool_aws_migration_recon.md §6)
REM    3. Set AWS_PROFILE if you use named AWS profiles
REM ============================================================

REM ---- Auth (dev mode: no OIDC, synthetic user) ----
SET AUTH_MODE=dev
SET DEV_USER_EMAIL=your.email@company.com
SET DEV_USER_ROLE=Admin

REM ---- Database (uncomment and fill in your credentials) ----
REM SET DB_DSN=postgresql://postgres:PASSWORD@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require
REM SET PGURL=%DB_DSN%

REM ---- AWS (for S3/ECS if needed) ----
REM SET AWS_PROFILE=your-profile
REM SET AWS_REGION=ap-southeast-1

REM ---- Confirm DB_DSN is set ----
IF "%DB_DSN%"=="" (
    echo [WARN] DB_DSN is not set. Dashboards will fail to connect to the database.
    echo        Set DB_DSN in this script before running.
    echo.
)

echo.
echo Starting portal   on http://localhost:8500 ...
start "portal" cmd /k "streamlit run apps/portal/app.py --server.port 8500 --server.baseUrlPath """

echo Starting inner-mongolia on http://localhost:8504 ...
start "inner-mongolia" cmd /k "streamlit run apps/bess-inner-mongolia/im/app.py --server.port 8504 --server.baseUrlPath """

echo.
echo Both dashboards are starting in new terminal windows.
echo  Portal:         http://localhost:8500
echo  Inner Mongolia: http://localhost:8504
echo.
echo To stop: close the terminal windows or press Ctrl+C in each.
