-- db/ddl/core/asset_monthly_compensation_seed.sql
-- Mengxi trading foundation: monthly compensation seed data
-- Created: 2026-03-25
-- Author: Matrix Agent
--
-- NOTE: Compensation varies by asset and by month.
-- 350 yuan/MWh is used as initial seed value only.
-- Update with actual asset-specific values when available.
-- Downstream jobs should fall back to 350 if no row exists.

-- Clear existing seed data for fresh load
DELETE FROM core.asset_monthly_compensation WHERE source_system = 'seed_initial';

-- Insert initial compensation rates for 2025-2026
-- These are placeholder values; replace with actual asset-specific rates
INSERT INTO core.asset_monthly_compensation (asset_code, effective_month, compensation_yuan_per_mwh, source_system, notes) VALUES
-- suyou (richer coverage asset)
('suyou', '2025-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-04-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-05-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-06-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-07-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-08-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-09-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-10-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-11-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2025-12-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2026-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2026-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('suyou', '2026-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),

-- wulate (richer coverage asset)
('wulate', '2025-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-04-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-05-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-06-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-07-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-08-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-09-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-10-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-11-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2025-12-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2026-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2026-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulate', '2026-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),

-- wuhai (richer coverage asset)
('wuhai', '2025-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-04-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-05-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-06-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-07-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-08-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-09-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-10-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-11-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2025-12-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2026-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2026-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wuhai', '2026-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),

-- wulanchabu (richer coverage asset)
('wulanchabu', '2025-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-04-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-05-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-06-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-07-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-08-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-09-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-10-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-11-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2025-12-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2026-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2026-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('wulanchabu', '2026-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),

-- hetao (partial coverage asset)
('hetao', '2025-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-04-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-05-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-06-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-07-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-08-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-09-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-10-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-11-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2025-12-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2026-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2026-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hetao', '2026-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),

-- hangjinqi (partial coverage asset)
('hangjinqi', '2025-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-04-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-05-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-06-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-07-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-08-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-09-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-10-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-11-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2025-12-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2026-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2026-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('hangjinqi', '2026-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),

-- siziwangqi (partial coverage asset)
('siziwangqi', '2025-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-04-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-05-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-06-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-07-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-08-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-09-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-10-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-11-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2025-12-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2026-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2026-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('siziwangqi', '2026-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),

-- gushanliang (partial coverage asset)
('gushanliang', '2025-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-04-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-05-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-06-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-07-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-08-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-09-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-10-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-11-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2025-12-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2026-01-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2026-02-01', 350, 'seed_initial', 'Initial seed; update with actual rate'),
('gushanliang', '2026-03-01', 350, 'seed_initial', 'Initial seed; update with actual rate')

ON CONFLICT (asset_code, effective_month) DO UPDATE
SET
    compensation_yuan_per_mwh = EXCLUDED.compensation_yuan_per_mwh,
    source_system = EXCLUDED.source_system,
    notes = EXCLUDED.notes,
    updated_at = now();
