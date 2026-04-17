@echo off
REM ============================================================
REM  Local dashboard runner for BESS Platform (Windows)
REM
REM  Runs portal (8500) and inner-mongolia (8504) locally,
REM  connecting directly to AWS RDS.  Bypasses ALB OIDC auth.
REM
REM  BEFORE FIRST RUN:
REM    1. Fill in DB_DSN below with the RDS password
REM    2. Ensure your IP is in rds-sg inbound on port 5432
REM       (see docs/dashboard_local_runbook.md §Network)
REM    3. Optionally set AWS_PROFILE for S3 access
REM
REM  Run from the repo root:
REM    cd C:\...\bess-platform
REM    scripts\run_dashboard_local.bat
REM ============================================================

REM ---- Auth: bypass ALB OIDC, use synthetic dev user ----
SET AUTH_MODE=dev
SET DEV_USER_EMAIL=your.email@company.com
SET DEV_USER_ROLE=Admin

REM ---- Database: fill in the password ----
SET DB_DSN=postgresql://postgres:PASSWORD@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require
SET PGURL=%DB_DSN%

REM ---- Portal app link overrides: local ports instead of AWS paths ----
REM  slug format: <path-slug>=<local-url>  (slugs come from APP_CATALOG paths)
SET APP_URL_MAP=inner-mongolia=http://localhost:8504,bess-map=http://localhost:8503,uploader=http://localhost:8501,model-catalogue=http://localhost:8506

REM ---- AWS profile (optional, for S3/ECS API calls) ----
REM SET AWS_PROFILE=default
REM SET AWS_REGION=ap-southeast-1

REM ---- Validate DB_DSN is set ----
IF "%DB_DSN%"=="postgresql://postgres:PASSWORD@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require" (
    echo [WARN] DB_DSN still has placeholder PASSWORD. Edit this script before running.
    echo.
)

echo.
echo Starting portal          on http://localhost:8500  ...
start "bess-portal" cmd /k "cd /d %~dp0.. && streamlit run apps/portal/app.py --server.port 8500 --server.baseUrlPath """

echo Starting inner-mongolia  on http://localhost:8504  ...
start "bess-inner-mongolia" cmd /k "cd /d %~dp0.. && streamlit run apps/bess-inner-mongolia/im/app.py --server.port 8504 --server.baseUrlPath """

echo.
echo  Portal:         http://localhost:8500
echo  Inner Mongolia: http://localhost:8504
echo.
echo To stop: close the terminal windows or Ctrl+C in each.
