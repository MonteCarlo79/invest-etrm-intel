# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 12:51:42 2026

@author: dipeng.chen
"""

-- db/ddl/core/asset_scenario_availability_seed.sql
INSERT INTO core.asset_scenario_availability (asset_code, scenario_name, available_flag, source_system, notes) VALUES
-- Full ladder today
('suyou', 'perfect_foresight_unrestricted', true,  'optimizer',      'Use actual nodal price'),
('suyou', 'perfect_foresight_grid_feasible', true, 'optimizer',      'Apply grid restriction mask'),
('suyou', 'cleared_actual', true,                  'mengxi_ingestion','Authoritative cleared dispatch'),
('suyou', 'nominated_dispatch', true,              'manual_excel',    '96-point nomination sheet'),
('suyou', 'tt_forecast_optimal', true,             'tt_enos',         'Forecast price + optimizer'),
('suyou', 'tt_strategy', true,                     'tt_enos',         'TT provided strategy'),

('wulate', 'perfect_foresight_unrestricted', true,  'optimizer',      'Use actual nodal price'),
('wulate', 'perfect_foresight_grid_feasible', true, 'optimizer',      'Apply grid restriction mask'),
('wulate', 'cleared_actual', true,                  'mengxi_ingestion','Authoritative cleared dispatch'),
('wulate', 'nominated_dispatch', true,              'manual_excel',    '96-point nomination sheet'),
('wulate', 'tt_forecast_optimal', true,             'tt_enos',         'Forecast price + optimizer'),
('wulate', 'tt_strategy', true,                     'tt_enos',         'TT provided strategy'),

-- Forecast price coverage exists, but no full TT strategy ladder yet
('wuhai', 'perfect_foresight_unrestricted', true,  'optimizer',      'Use actual nodal price'),
('wuhai', 'perfect_foresight_grid_feasible', true, 'optimizer',      'Apply grid restriction mask'),
('wuhai', 'cleared_actual', true,                  'mengxi_ingestion','Use canonical cleared dispatch'),
('wuhai', 'nominated_dispatch', false,             'manual_excel',    'Not wired yet'),
('wuhai', 'tt_forecast_optimal', true,             'tt_enos',         'Forecast price available'),
('wuhai', 'tt_strategy', false,                    'tt_enos',         'Not wired yet'),

('wulanchabu', 'perfect_foresight_unrestricted', true,  'optimizer',      'Use actual nodal price'),
('wulanchabu', 'perfect_foresight_grid_feasible', true, 'optimizer',      'Apply grid restriction mask'),
('wulanchabu', 'cleared_actual', true,                  'mengxi_ingestion','Use canonical cleared dispatch'),
('wulanchabu', 'nominated_dispatch', false,             'manual_excel',    'Not wired yet'),
('wulanchabu', 'tt_forecast_optimal', true,             'tt_enos',         'Forecast price available'),
('wulanchabu', 'tt_strategy', false,                    'tt_enos',         'Not wired yet'),

-- Additional Mengxi assets: PF vs grid-feasible PF vs cleared actual only
('hetao', 'perfect_foresight_unrestricted', true,  'optimizer',      'Compare PF vs cleared'),
('hetao', 'perfect_foresight_grid_feasible', true, 'optimizer',      'Grid-aware PF'),
('hetao', 'cleared_actual', true,                  'mengxi_ingestion','Use canonical cleared dispatch'),
('hetao', 'nominated_dispatch', false,             'manual_excel',    'Not in v1'),
('hetao', 'tt_forecast_optimal', false,            'tt_enos',         'No TT nodal forecast'),
('hetao', 'tt_strategy', false,                    'tt_enos',         'No TT strategy'),

('hangjinqi', 'perfect_foresight_unrestricted', true,  'optimizer',      'Compare PF vs cleared'),
('hangjinqi', 'perfect_foresight_grid_feasible', true, 'optimizer',      'Grid-aware PF'),
('hangjinqi', 'cleared_actual', true,                  'mengxi_ingestion','Use canonical cleared dispatch'),
('hangjinqi', 'nominated_dispatch', false,             'manual_excel',    'Not in v1'),
('hangjinqi', 'tt_forecast_optimal', false,            'tt_enos',         'No TT nodal forecast'),
('hangjinqi', 'tt_strategy', false,                    'tt_enos',         'No TT strategy'),

('siziwangqi', 'perfect_foresight_unrestricted', true,  'optimizer',      'Compare PF vs cleared'),
('siziwangqi', 'perfect_foresight_grid_feasible', true, 'optimizer',      'Grid-aware PF'),
('siziwangqi', 'cleared_actual', true,                  'mengxi_ingestion','Use canonical cleared dispatch'),
('siziwangqi', 'nominated_dispatch', false,             'manual_excel',    'Not in v1'),
('siziwangqi', 'tt_forecast_optimal', false,            'tt_enos',         'No TT nodal forecast'),
('siziwangqi', 'tt_strategy', false,                    'tt_enos',         'No TT strategy'),

('gushanliang', 'perfect_foresight_unrestricted', true,  'optimizer',      'Compare PF vs cleared'),
('gushanliang', 'perfect_foresight_grid_feasible', true, 'optimizer',      'Grid-aware PF'),
('gushanliang', 'cleared_actual', true,                  'mengxi_ingestion','Use canonical cleared dispatch'),
('gushanliang', 'nominated_dispatch', false,             'manual_excel',    'Not in v1'),
('gushanliang', 'tt_forecast_optimal', false,            'tt_enos',         'No TT nodal forecast'),
('gushanliang', 'tt_strategy', false,                    'tt_enos',         'No TT strategy')
ON CONFLICT (asset_code, scenario_name) DO UPDATE
SET
    available_flag = EXCLUDED.available_flag,
    source_system  = EXCLUDED.source_system,
    notes          = EXCLUDED.notes,
    updated_at     = now();