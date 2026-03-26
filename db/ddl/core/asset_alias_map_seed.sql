-- db/ddl/core/asset_alias_map_seed.sql
-- Mengxi trading foundation: asset alias seed data
-- Created: 2026-03-24
-- Updated: 2026-03-25
-- Author: Matrix Agent
--
-- Asset codes: suyou, wulate, wuhai, wulanchabu, hetao, hangjinqi, siziwangqi, gushanliang
-- Naming mappings per AGENTS.md specifications

TRUNCATE TABLE core.asset_alias_map;

INSERT INTO core.asset_alias_map (asset_code, alias_type, alias_value, province, city_cn) VALUES
-- suyou: 景蓝乌尔图储能电站 = 苏右储能 = SuYou = Mengxi_SuYou
('suyou', 'dispatch_unit_name_cn', '景蓝乌尔图储能电站', 'Mengxi', '锡林郭勒'),
('suyou', 'short_name_cn', '苏右储能', 'Mengxi', '锡林郭勒'),
('suyou', 'display_name_cn', '苏右', 'Mengxi', '锡林郭勒'),
('suyou', 'tt_asset_name_en', 'SuYou', 'Mengxi', '锡林郭勒'),
('suyou', 'market_key', 'Mengxi_SuYou', 'Mengxi', '锡林郭勒'),

-- wulate: 远景乌拉特储能电站 = 乌拉特中期储能 = WuLaTe = Mengxi_WuLaTe
('wulate', 'dispatch_unit_name_cn', '远景乌拉特储能电站', 'Mengxi', '巴彦淖尔'),
('wulate', 'short_name_cn', '乌拉特中期储能', 'Mengxi', '巴彦淖尔'),
('wulate', 'display_name_cn', '乌拉特', 'Mengxi', '巴彦淖尔'),
('wulate', 'tt_asset_name_en', 'WuLaTe', 'Mengxi', '巴彦淖尔'),
('wulate', 'market_key', 'Mengxi_WuLaTe', 'Mengxi', '巴彦淖尔'),

-- wuhai: 富景五虎山储能电站 = 乌海储能 = WuHai = Mengxi_WuHai
('wuhai', 'dispatch_unit_name_cn', '富景五虎山储能电站', 'Mengxi', '乌海'),
('wuhai', 'short_name_cn', '乌海储能', 'Mengxi', '乌海'),
('wuhai', 'display_name_cn', '乌海', 'Mengxi', '乌海'),
('wuhai', 'tt_asset_name_en', 'WuHai', 'Mengxi', '乌海'),
('wuhai', 'market_key', 'Mengxi_WuHai', 'Mengxi', '乌海'),

-- wulanchabu: 景通红丰储能电站 = 乌兰察布储能 = WuLanChaBu = Mengxi_WuLanChaBu
('wulanchabu', 'dispatch_unit_name_cn', '景通红丰储能电站', 'Mengxi', '乌兰察布'),
('wulanchabu', 'short_name_cn', '乌兰察布储能', 'Mengxi', '乌兰察布'),
('wulanchabu', 'display_name_cn', '乌兰察布', 'Mengxi', '乌兰察布'),
('wulanchabu', 'tt_asset_name_en', 'WuLanChaBu', 'Mengxi', '乌兰察布'),
('wulanchabu', 'market_key', 'Mengxi_WuLanChaBu', 'Mengxi', '乌兰察布'),

-- hetao: 景怡查干哈达储能电站 = 河套储能
('hetao', 'dispatch_unit_name_cn', '景怡查干哈达储能电站', 'Mengxi', '巴彦淖尔'),
('hetao', 'short_name_cn', '河套储能', 'Mengxi', '巴彦淖尔'),
('hetao', 'display_name_cn', '河套', 'Mengxi', '巴彦淖尔'),

-- hangjinqi: 悦杭独贵储能电站 = 杭锦旗储能
('hangjinqi', 'dispatch_unit_name_cn', '悦杭独贵储能电站', 'Mengxi', '鄂尔多斯'),
('hangjinqi', 'short_name_cn', '杭锦旗储能', 'Mengxi', '鄂尔多斯'),
('hangjinqi', 'display_name_cn', '杭锦旗', 'Mengxi', '鄂尔多斯'),

-- siziwangqi: 景通四益堂储能电站 = 四子王旗储能
('siziwangqi', 'dispatch_unit_name_cn', '景通四益堂储能电站', 'Mengxi', '乌兰察布'),
('siziwangqi', 'short_name_cn', '四子王旗储能', 'Mengxi', '乌兰察布'),
('siziwangqi', 'display_name_cn', '四子王旗', 'Mengxi', '乌兰察布'),

-- gushanliang: 裕昭沙子坝储能电站 = 谷山梁储能
('gushanliang', 'dispatch_unit_name_cn', '裕昭沙子坝储能电站', 'Mengxi', '鄂尔多斯'),
('gushanliang', 'short_name_cn', '谷山梁储能', 'Mengxi', '鄂尔多斯'),
('gushanliang', 'display_name_cn', '谷山梁', 'Mengxi', '鄂尔多斯');
