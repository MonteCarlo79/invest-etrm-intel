-- =============================================================================
-- fix_data_gaps.sql
-- Run this BEFORE re-ingesting to unblock the skip logic and clear
-- stale derived data for two known gaps:
--   A) 蒙东 zeros Feb–Apr 2026
--   B) 福建 SSL gap Nov 2025–Mar 2026
--
-- Usage:
--   psql "$PGURL" -f services/lingfeng/fix_data_gaps.sql
-- =============================================================================

-- =============================================================================
-- DIAGNOSTICS (read-only — run first to confirm scope)
-- =============================================================================

\echo '--- 蒙东: zero-price rows by day (Feb–Apr 2026) ---'
SELECT
    date(datetime)                                            AS day,
    count(*)                                                  AS total_rows,
    sum(case when rt_price = 0 OR rt_price IS NULL then 1 else 0 end) AS zero_or_null_rt,
    sum(case when da_price = 0 OR da_price IS NULL then 1 else 0 end) AS zero_or_null_da
FROM marketdata.spot_prices_hourly
WHERE province = '蒙东'
  AND datetime >= '2026-02-01'
  AND datetime < '2026-05-01'
GROUP BY 1
ORDER BY 1;

\echo '--- 蒙东: audit.province_progress ---'
SELECT * FROM audit.province_progress WHERE province = '蒙东';

\echo '--- 福建: price coverage by month (Nov 2025–Mar 2026) ---'
SELECT
    date_trunc('month', datetime)::date                       AS month,
    count(*)                                                  AS rows,
    count(case when rt_price > 0 then 1 end)                  AS nonzero_rt,
    min(datetime)::date                                       AS first_day,
    max(datetime)::date                                       AS last_day
FROM marketdata.spot_prices_hourly
WHERE province = '福建'
  AND datetime >= '2025-11-01'
  AND datetime < '2026-04-01'
GROUP BY 1
ORDER BY 1;

-- =============================================================================
-- FIX A: 蒙东 zeros (Feb–Apr 2026)
-- =============================================================================

\echo ''
\echo '=== FIX A: 蒙东 — deleting stale audit entry and derived data ==='

-- 1. Remove skip guard so run_all_provinces.py re-processes 蒙东
DELETE FROM audit.province_progress
WHERE province = '蒙东' AND duration_h = 0.0;

\echo 'Deleted audit.province_progress for 蒙东.'

-- 2. Derived data — bess_capture_daily
DELETE FROM marketdata.bess_capture_daily
WHERE province = '蒙东'
  AND date >= '2026-02-01'
  AND date <  '2026-05-01';

\echo 'Deleted bess_capture_daily for 蒙东 Feb–Apr 2026.'

-- 3. Theoretical dispatch
DELETE FROM marketdata.spot_dispatch_hourly_theoretical
WHERE province = '蒙东'
  AND datetime >= '2026-02-01'
  AND datetime <  '2026-05-01';

\echo 'Deleted spot_dispatch_hourly_theoretical for 蒙东 Feb–Apr 2026.'

-- 4. Forecast dispatch
DELETE FROM marketdata.spot_dispatch_hourly_rt_forecast
WHERE province = '蒙东'
  AND datetime >= '2026-02-01'
  AND datetime <  '2026-05-01';

\echo 'Deleted spot_dispatch_hourly_rt_forecast for 蒙东 Feb–Apr 2026.'

-- 5. RT forecast predictions
DELETE FROM marketdata.spot_prices_hourly_rt_forecast
WHERE province = '蒙东'
  AND datetime >= '2026-02-01'
  AND datetime <  '2026-05-01';

\echo 'Deleted spot_prices_hourly_rt_forecast for 蒙东 Feb–Apr 2026.'

-- =============================================================================
-- FIX B: 福建 SSL gap (Nov 2025–Mar 2026)
-- =============================================================================

\echo ''
\echo '=== FIX B: 福建 — deleting derived data for gap period ==='

-- 福建 spot_prices_hourly will be fixed by the UPSERT re-ingest (no delete needed).
-- We only need to clear derived data so the capture pipeline recomputes correctly.

DELETE FROM marketdata.bess_capture_daily
WHERE province = '福建'
  AND date >= '2025-11-01'
  AND date <  '2026-04-01';

\echo 'Deleted bess_capture_daily for 福建 Nov 2025–Mar 2026.'

DELETE FROM marketdata.spot_dispatch_hourly_theoretical
WHERE province = '福建'
  AND datetime >= '2025-11-01'
  AND datetime <  '2026-04-01';

\echo 'Deleted spot_dispatch_hourly_theoretical for 福建 Nov 2025–Mar 2026.'

DELETE FROM marketdata.spot_dispatch_hourly_rt_forecast
WHERE province = '福建'
  AND datetime >= '2025-11-01'
  AND datetime <  '2026-04-01';

\echo 'Deleted spot_dispatch_hourly_rt_forecast for 福建 Nov 2025–Mar 2026.'

DELETE FROM marketdata.spot_prices_hourly_rt_forecast
WHERE province = '福建'
  AND datetime >= '2025-11-01'
  AND datetime <  '2026-04-01';

\echo 'Deleted spot_prices_hourly_rt_forecast for 福建 Nov 2025–Mar 2026.'

\echo ''
\echo '=== Cleanup complete. Proceed with re-ingest commands. ==='
