# 蒙西 独立新型储能 放电量补偿 (Discharge-Volume Compensation)

Source documents: `data/mengxi-cap-payment/正文页.ofd` (policy text) + `data/mengxi-cap-payment/独立新型储能电站名单.et` (44-station list, extracted 2026-11-01).

## Rule (per user, 2026-11-01)

- Stations **on the list**: **350 ¥/MWh** discharged (0.35 ¥/kWh; a few list entries carry self-declared variants — 0.335/0.332/0.30 — none are our assets)
- Stations **COD after 2026-01-01 and NOT on the list**: **280 ¥/MWh** discharged by default
- Other provinces' BESS: unchanged (`core.asset_monthly_compensation` was empty for them)

## Our fleet mapping (verified against list rows)

| book | asset | list row | rate ¥/MWh | effective |
|---|---|---|---|---|
| suyou | 景蓝乌尔图 (苏尼特右旗景蓝新能源) | #6 苏尼特右旗 100MW/400MWh | 350 | 2025-11 |
| hangjinqi | 悦杭独贵 (内蒙古悦杭新能源) | #25 杭锦旗谷山梁智慧储能 100MW/400MWh | 350 | 2025-12 (COD) |
| siziwangqi | 四子王旗 (四子王旗景通新能源) | #28 四子王旗新型储能电站 | 350 | 2025-12 (COD) |
| gushanliang | 裕昭沙子坝 (达拉特旗裕昭新能源) | #27 谷山梁新远景子项目1 (50万kW) | 350 | 2025-12 (COD) |
| bameng | 景怡查干哈达 (乌拉特后旗景怡新能源) | #34 巴彦淖尔河套专项行动 100万kW | 350 | 2025-12 (COD) |
| wulate | 远景乌拉特 (乌拉特中旗昭源/鸿瑞) | #5 乌拉特中旗德岭山储能电站 | 350 | 2025-11 |
| wuhai | 乌海康富 (远景能源+中国康富) | #8 乌海市金湖一标段 | 350 | 2025-11 |
| wuchuan | 武川 (1000MW, COD 2026-08; exchange accounts 旭纯哈乐储能电站 + 旭纯哈乐储能电站二, 500MW/2GWh each) | not on list | 280 (default) | 2026-08 (COD) |
| alashan | 阿拉善储能 (1000MW/4GWh, COD 2026-07; exchange account 景敖乌珠日储能电站) | not on list (list's 阿拉腾敖包 rows are 100MW projects, not ours) | 280 (default) | 2026-07 (COD) |

Wind stations: no storage discharge compensation (N/A).

## Notes

- List sections: §1 COD before 2025-11-10 (13 stations, 0.35 flat), §2 COD 2025-11-10→12-31 (30 stations, mostly 0.35, three self-declared 0.335/0.332/0.30), §3 COD 2026-01-01→01-26 (1 station, 0.28).
- Compensation enters P&L via `core.asset_monthly_compensation` (per-asset monthly rate) and the LP's `compensation_yuan_per_mwh` (added to discharge price in the objective).
- Effective-month assumptions: §1/§2 listed stations assumed from 2025-11 (policy reference date) or COD if later — flagged for user confirmation.

## Fleet-wide rates used by the 蒙西 BESS daily ranking report (confirmed by Dipeng 2026-11-11)

Source of truth in code: `services/hermes/mengxi_ranking_report.py` (`_ZERO_COMP_PLANTS`, `_PLANT_COMP_RATES`, `_280_COMP_PLANTS`, `_resolve_comp_rate`). Order of precedence:

1. **0 ¥/MWh** — 大航都林储能电站, 大航额日和图储能电站 (not independent storage); **景通红丰储能电站** (远景, 3.35 MW = asset-risk 乌兰察布储能 — owner 远景 but no compensation).
2. **Self-declared list rates** — 协鑫石拐储能电站 **335** (list #4, 0.335 ¥/kWh); 巴能峰塔储能电站 **300** (list #14 乌拉特中旗德岭山 500kV 电网侧, 巴能, 0.30 ¥/kWh, effective 2025-12-11).
3. **280 ¥/MWh (explicit list)** — post-2025-11-10 projects absent from the 独立新型储能电站名单: 荣鑫地房子, 华腾南园, 旭纯哈乐, 旭纯哈乐二, 烨荣晟, 明阳把栅, 蒙能翁贡, 国能托清, 景敖乌珠日, 盛德二登图 (all `…储能电站`).
4. **280 ¥/MWh (rule)** — first seen in the market after 2026-01-01 and before 2027-01-01.
5. **350 ¥/MWh** — everything else (listed stations).

Owner / capacity notes (station_master, 2026-11-11): 旭纯哈乐 + 旭纯哈乐二 are ONE site split into two exchange accounts (a single account was not allowed), 500 MW / 2 GWh each = rm_assets **武川**; 景敖乌珠日 = rm_assets **阿拉善**, 1 GW / 4 GWh. Observed cleared MW is below nameplate because the export channel (上送通道) is limited. Owner short names follow the list (format A, group names); 远景 rows are highlighted green in the PDF.
