-- db/ddl/staging/mengxi_extracted_data_seed.sql
-- Pre-extracted compensation data from Mengxi settlement PDFs
-- Created: 2026-03-26
-- Author: Matrix Agent

-- Insert extracted compensation amounts (July, August, October 2025)
INSERT INTO staging.mengxi_compensation_extracted 
    (source_filename, settlement_month, station_name_raw, asset_code, compensation_yuan, parse_confidence)
VALUES
    -- July 2025
    ('2025年7月储能容量补偿费用统计表.pdf', '2025-07-01', '景蓝乌尔图储能电站', 'suyou', 6709125.24, 'high'),
    ('2025年7月储能容量补偿费用统计表.pdf', '2025-07-01', '远景乌拉特储能电站', 'wulate', 6461216.45, 'high'),
    ('2025年7月储能容量补偿费用统计表.pdf', '2025-07-01', '富景五虎山储能电站', 'wuhai', 7211292.17, 'high'),
    -- August 2025
    ('2025年8月储能容量补偿费用统计表.pdf', '2025-08-01', '景蓝乌尔图储能电站', 'suyou', 7867605.23, 'high'),
    ('2025年8月储能容量补偿费用统计表.pdf', '2025-08-01', '远景乌拉特储能电站', 'wulate', 6570811.76, 'high'),
    ('2025年8月储能容量补偿费用统计表.pdf', '2025-08-01', '富景五虎山储能电站', 'wuhai', 7132685.36, 'high'),
    -- October 2025
    ('2025年10月储能容量补偿费用统计表.pdf', '2025-10-01', '景蓝乌尔图储能电站', 'suyou', 5147092.62, 'high'),
    ('2025年10月储能容量补偿费用统计表.pdf', '2025-10-01', '远景乌拉特储能电站', 'wulate', 4221126.93, 'high'),
    ('2025年10月储能容量补偿费用统计表.pdf', '2025-10-01', '富景五虎山储能电站', 'wuhai', 6050650.96, 'high')
ON CONFLICT DO NOTHING;
