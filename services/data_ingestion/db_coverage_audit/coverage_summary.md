# DB Coverage Audit — Summary

**Generated:** 2026-04-12  
**Database:** marketdata  
**Schemas audited:** public, marketdata  

---

## A. Totals

- **Total tables audited:** 238
- **fresh:** 31
- **slightly_stale:** 24
- **stale:** 177
- **empty:** 4
- **no_temporal_column:** 2
- **error:** 0

## B. Status Counts

| status | count |
|---|---|
| stale | 177 |
| fresh | 31 |
| slightly_stale | 24 |
| empty | 4 |
| no_temporal_column | 2 |

## C. Top 20 Stalest Tables

| table | rows | temporal_col | min | max | stale_days | status |
|---|---|---|---|---|---|---|
| `public.hist_mengxi_wulanchabu_clear_dayahead` | 1 | date | 2023-05-01 | 2023-05-01 | 1077 | **stale** |
| `public.hist_mengxi_wulanchabu_clear_dayahead_15min` | 96 | time | 2023-05-01 | 2023-05-01 | 1077 | **stale** |
| `public.mengxi_hueast` | 487 | date | 2022-06-01 | 2023-09-30 | 925 | **stale** |
| `public.hist_anhui_day_ahead_outer_delivery_trade_plan_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_load_regulation_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_new_energy_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_new_energy_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_not_market_power_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_not_market_power_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_solar_power_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_solar_power_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_tie_line_load_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_tie_line_load_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_total_power_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_water_power_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_water_power_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_wind_power_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_wind_power_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_dayaheadouterdeliverytradeplanforecast_15min` | 3744 | time | 2024-05-01 | 2024-12-27 | 471 | **stale** |
| `public.hourly_power_pricessuyou` | 29 | date | 2025-01-04 | 2025-02-01 | 435 | **stale** |

## D. Top 20 Freshest Large Tables

| table | rows | temporal_col | min | max | stale_days | status |
|---|---|---|---|---|---|---|
| `marketdata.md_id_cleared_energy` | 25655676 | data_date | 2025-01-01 | 2026-04-09 | 3 | **slightly_stale** |
| `marketdata.md_da_cleared_energy` | 23112672 | data_date | 2025-01-02 | 2026-04-10 | 2 | **slightly_stale** |
| `marketdata.md_rt_nodal_price` | 13451040 | data_date | 2025-05-31 | 2026-04-09 | 3 | **slightly_stale** |
| `marketdata.md_id_fuel_summary` | 217935 | data_date | 2025-01-01 | 2026-04-09 | 3 | **slightly_stale** |
| `marketdata.md_da_fuel_summary` | 205920 | data_date | 2025-01-02 | 2026-04-10 | 2 | **slightly_stale** |
| `public.hist_shandong_pumpedstoragepowerforecast_15min` | 117246 | time | 2022-12-03 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_loadregulationreal_15min` | 116030 | time | 2022-12-15 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_loadregulationsubtielineloadreal_15min` | 116030 | time | 2022-12-15 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_newenergyreal_15min` | 116030 | time | 2022-12-15 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_solarpowerreal_15min` | 116030 | time | 2022-12-15 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_thermalpowerbiddingspacereal_15min` | 116030 | time | 2022-12-15 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_tielineloadreal_15min` | 116030 | time | 2022-12-15 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_windpowerreal_15min` | 116030 | time | 2022-12-15 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_loadregulationforecast_15min` | 115902 | time | 2022-12-17 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_loadregulationsubtielineloadforecast_15min` | 115902 | time | 2022-12-17 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_newenergyforecast_15min` | 115902 | time | 2022-12-17 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_solarpowerforecast_15min` | 115902 | time | 2022-12-17 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_testunitforecast_15min` | 115902 | time | 2022-12-17 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_thermalpowerbiddingspaceforecast_15min` | 115902 | time | 2022-12-17 | 2026-04-11 | 1 | **fresh** |
| `public.hist_shandong_tielineloadforecast_15min` | 115902 | time | 2022-12-17 | 2026-04-11 | 1 | **fresh** |

## E. Tables with Missing Dates

| table | rows | temporal_col | missing_count | first_missing | last_missing | status |
|---|---|---|---|---|---|---|
| `public.hist_anhui_provincerealtimeclearprice_15min` | 21195 | time | 447 | 2024-06-01 | 2026-02-25 | stale |
| `public.spot_daily` | 5558 | report_date | 362 | 2024-09-26 | 2026-12-30 | fresh |
| `public.hist_anhui_dingyuan_forecast` | 356 | date | 347 | 2024-06-01 | 2026-04-03 | stale |
| `public.hist_anhui_dingyuan_forecast_15min` | 34175 | time | 347 | 2024-06-01 | 2026-04-03 | stale |
| `public.hist_anhui_dingyuan_forecast_dayahead` | 356 | date | 347 | 2024-06-01 | 2026-04-03 | stale |
| `public.hist_anhui_dingyuan_forecast_dayahead_15min` | 34175 | time | 347 | 2024-06-01 | 2026-04-03 | stale |
| `public.hist_anhui_provincedayaheadclearprice_15min` | 39341 | time | 264 | 2024-06-01 | 2026-01-17 | stale |
| `public.hist_shandong_binzhou` | 621 | date | 250 | 2024-01-01 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_binzhou_clear` | 621 | date | 250 | 2024-01-01 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_binzhou_clear_15min` | 59704 | time | 250 | 2024-01-01 | 2026-04-08 | slightly_stale |
| `public.hist_anhui_tielineloadforecast_15min` | 44218 | time | 245 | 2024-04-29 | 2026-04-03 | stale |
| `public.hist_anhui_tielineloadreal_15min` | 41924 | time | 234 | 2024-05-11 | 2026-01-21 | stale |
| `public.hist_anhui_newenergyforecast_15min` | 45754 | time | 229 | 2024-04-29 | 2026-04-03 | stale |
| `public.hist_anhui_notmarketpowerforecast_15min` | 45754 | time | 229 | 2024-04-29 | 2026-04-03 | stale |
| `public.hist_anhui_solarpowerforecast_15min` | 45754 | time | 229 | 2024-04-29 | 2026-04-03 | stale |
| `public.hist_anhui_waterpowerforecast_15min` | 45754 | time | 229 | 2024-04-29 | 2026-04-03 | stale |
| `public.hist_anhui_windpowerforecast_15min` | 45754 | time | 229 | 2024-04-29 | 2026-04-03 | stale |
| `public.hist_anhui_loadregulationforecast_15min` | 45658 | time | 227 | 2024-06-01 | 2026-04-03 | stale |
| `marketdata.md_rt_nodal_price` | 13451040 | data_date | 223 | 2025-06-01 | 2026-01-29 | slightly_stale |
| `public.hist_anhui_dingyuan_clear` | 329 | date | 214 | 2024-06-01 | 2025-09-05 | stale |
| `public.hist_anhui_dingyuan_clear_15min` | 31584 | time | 214 | 2024-06-01 | 2025-09-05 | stale |
| `public.hist_anhui_dingyuan_clear_dayahead` | 332 | date | 213 | 2024-06-01 | 2024-12-30 | stale |
| `public.hist_anhui_dingyuan_clear_dayahead_15min` | 31872 | time | 213 | 2024-06-01 | 2024-12-30 | stale |
| `public.hist_anhui_newenergyreal_15min` | 44228 | time | 210 | 2024-06-01 | 2026-01-21 | stale |
| `public.hist_anhui_solarpowerreal_15min` | 44228 | time | 210 | 2024-06-01 | 2026-01-21 | stale |
| `public.hist_anhui_windpowerreal_15min` | 44228 | time | 210 | 2024-06-01 | 2026-01-21 | stale |
| `public.hist_anhui_loadregulationreal_15min` | 44323 | time | 209 | 2024-06-01 | 2026-01-21 | stale |
| `public.hist_anhui_negativesparereal_15min` | 44323 | time | 209 | 2024-06-01 | 2026-01-21 | stale |
| `public.hist_anhui_positivesparereal_15min` | 44323 | time | 209 | 2024-06-01 | 2026-01-21 | stale |
| `public.hist_anhui_notmarketpowerreal_15min` | 44419 | time | 208 | 2024-06-01 | 2026-01-21 | stale |
| `public.hist_anhui_waterpowerreal_15min` | 44419 | time | 208 | 2024-06-01 | 2026-01-21 | stale |
| `marketdata.data_quality_status` | 111 | data_date | 203 | 2025-06-01 | 2025-12-31 | slightly_stale |
| `public.hist_anhui_dayaheadouterdeliverytradeplanforecast_15min` | 3744 | time | 202 | 2024-06-01 | 2024-12-22 | stale |
| `public.hist_shandong_binzhou_forecast` | 890 | date | 154 | 2025-10-28 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_binzhou_forecast_15min` | 85621 | time | 154 | 2025-10-28 | 2026-04-08 | fresh |
| `public.hist_shandong_binzhou_forecast_dayahead` | 890 | date | 154 | 2025-10-28 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_binzhou_forecast_dayahead_15min` | 85621 | time | 154 | 2025-10-28 | 2026-04-08 | fresh |
| `public.hist_shandong_newpositivespareforecast_15min` | 1431 | time | 112 | 2025-11-09 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_binzhou_clear_dayahead` | 616 | date | 91 | 2024-01-01 | 2024-03-31 | stale |
| `public.hist_shandong_binzhou_clear_dayahead_15min` | 59136 | time | 91 | 2024-01-01 | 2024-03-31 | stale |
| `public.hist_anhui_tielineloadreal__5901__15min` | 30596 | time | 88 | 2025-01-23 | 2026-01-21 | stale |
| `public.hist_anhui_tielineloadreal__5904__15min` | 30596 | time | 88 | 2025-01-23 | 2026-01-21 | stale |
| `public.hist_anhui_tielineloadreal__5907__15min` | 30596 | time | 88 | 2025-01-23 | 2026-01-21 | stale |
| `public.hist_anhui_tielineloadreal__5914__15min` | 30596 | time | 88 | 2025-01-23 | 2026-01-21 | stale |
| `public.hist_anhui_tielineloadreal__5917__15min` | 30596 | time | 88 | 2025-01-23 | 2026-01-21 | stale |
| `public.hist_anhui_tielineloadreal__5921__15min` | 30596 | time | 88 | 2025-01-23 | 2026-01-21 | stale |
| `public.hist_anhui_tielineloadreal__5931__15min` | 30596 | time | 88 | 2025-01-23 | 2026-01-21 | stale |
| `public.hist_anhui_tielineloadreal__i__15min` | 30596 | time | 88 | 2025-01-23 | 2026-01-21 | stale |
| `public.hist_anhui_tielineloadreal__ii__15min` | 30596 | time | 88 | 2025-01-23 | 2026-01-21 | stale |
| `marketdata.md_rt_total_cleared_energy` | 38976 | data_date | 58 | 2025-05-31 | 2026-03-23 | slightly_stale |
| `public.hist_shandong_localpowerplantforecast_15min` | 111437 | time | 50 | 2026-01-25 | 2026-04-08 | fresh |
| `public.hist_mengxi_wuhai_clear_15min` | 24288 | time | 43 | 2025-02-05 | 2025-10-07 | stale |
| `marketdata.md_da_cleared_energy` | 23112672 | data_date | 36 | 2025-06-01 | 2026-04-03 | slightly_stale |
| `marketdata.md_da_fuel_summary` | 205920 | data_date | 35 | 2025-06-01 | 2026-03-25 | slightly_stale |
| `public.hist_shandong_notmarketnuclearpowerreal_15min` | 7411 | time | 22 | 2026-01-02 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_standbyunitreal_15min` | 7411 | time | 22 | 2026-01-02 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_testunitreal_15min` | 7411 | time | 22 | 2026-01-02 | 2026-04-08 | slightly_stale |
| `marketdata.inner_mongolia_bess_results` | 380 | created_at | 21 | 2026-03-09 | 2026-04-05 | slightly_stale |
| `marketdata.inner_mongolia_nodal_clusters` | 7988 | created_at | 21 | 2026-03-09 | 2026-04-05 | slightly_stale |
| `marketdata.md_avg_bid_price` | 1329 | data_date | 21 | 2025-01-04 | 2026-03-19 | slightly_stale |
| `public.hist_shandong_notmarketnuclearpowerforecast_15min` | 8171 | time | 13 | 2026-01-04 | 2026-04-08 | fresh |
| `public.hist_shandong_tielineloadforecast___15min` | 13586 | time | 12 | 2025-12-28 | 2026-04-08 | fresh |
| `marketdata.md_id_cleared_energy` | 25655676 | data_date | 7 | 2026-02-02 | 2026-03-11 | slightly_stale |
| `marketdata.md_id_fuel_summary` | 217935 | data_date | 7 | 2025-05-31 | 2026-04-05 | slightly_stale |
| `public.hist_mengxi_wulate_clear_15min` | 27648 | time | 7 | 2025-02-05 | 2025-02-11 | stale |
| `public.hist_shandong_localpowerplantreal_15min` | 115593 | time | 7 | 2026-01-30 | 2026-04-08 | fresh |
| `public.hist_shandong_pumpedstoragepowerreal_15min` | 115570 | time | 7 | 2026-01-30 | 2026-04-08 | fresh |
| `public.hist_shandong_negativespareforecast_15min` | 14156 | time | 6 | 2025-12-28 | 2026-04-08 | fresh |
| `public.hist_shandong_negativesparereal_15min` | 14251 | time | 6 | 2025-12-26 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_networkloadreal_15min` | 43938 | time | 6 | 2025-04-30 | 2026-04-08 | fresh |
| `public.hist_shandong_positivespareforecast_15min` | 14156 | time | 6 | 2025-12-28 | 2026-04-08 | fresh |
| `public.hist_shandong_positivesparereal_15min` | 14251 | time | 6 | 2025-12-26 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_provincerealtimeclearprice_15min` | 9121 | time | 5 | 2026-04-04 | 2026-04-08 | fresh |
| `public.hist_shandong_provincedayaheadclearprice_15min` | 67711 | time | 4 | 2026-04-05 | 2026-04-08 | fresh |
| `public.hist_shandong_standbyunitforecast_15min` | 115807 | time | 4 | 2026-01-29 | 2026-04-08 | fresh |
| `marketdata.bess_dispatch_hourly` | 458808 | created_at | 3 | 2026-02-03 | 2026-02-05 | stale |
| `marketdata.bess_monthly` | 634 | created_at | 3 | 2026-02-03 | 2026-02-05 | stale |
| `public.hist_mengxi_hubaodongrealtimepriceforecast_15min` | 128592 | time | 3 | 2022-08-02 | 2025-12-06 | stale |
| `public.hist_mengxi_hubaoxirealtimepriceforecast_15min` | 128511 | time | 3 | 2022-08-02 | 2025-11-24 | stale |
| `public.hist_shandong_loadregulationforecast_15min` | 115902 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_loadregulationreal_15min` | 116030 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_loadregulationsubtielineloadforecast_15min` | 115902 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_loadregulationsubtielineloadreal_15min` | 116030 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_networkloadforecast_15min` | 9311 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_newenergyforecast_15min` | 115902 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_newenergyreal_15min` | 116030 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_pumpedstoragepowerforecast_15min` | 117246 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_solarpowerforecast_15min` | 115902 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_solarpowerreal_15min` | 116030 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_testunitforecast_15min` | 115902 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_thermalpowerbiddingspaceforecast_15min` | 115902 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_thermalpowerbiddingspacereal_15min` | 116030 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_tielineloadforecast_15min` | 115902 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_tielineloadreal_15min` | 116030 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_windpowerforecast_15min` | 115902 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.hist_shandong_windpowerreal_15min` | 116030 | time | 3 | 2026-04-06 | 2026-04-08 | fresh |
| `public.price_d1_mengxi_hubao_east` | 599 | date | 3 | 2025-02-04 | 2025-03-12 | stale |
| `public.hist_mengxi_suyou_forecast` | 1196 | date | 2 | 2022-08-02 | 2024-12-24 | stale |
| `public.hist_mengxi_wuhai_forecast` | 1196 | date | 2 | 2022-08-02 | 2024-12-24 | stale |
| `public.hist_mengxi_wulate_forecast` | 1196 | date | 2 | 2022-08-02 | 2024-12-24 | stale |
| `public.hist_shandong_tielineloadreal___15min` | 7220 | time | 2 | 2025-12-26 | 2026-01-01 | stale |
| `marketdata.md_settlement_ref_price` | 11112 | data_date | 1 | 2025-05-31 | 2025-05-31 | slightly_stale |
| `public.hist_anhui_dingyuan` | 1249 | date | 1 | 2022-05-26 | 2022-05-26 | stale |
| `public.hist_mengxi_biddingspaceforecast_15min` | 147126 | time | 1 | 2026-03-03 | 2026-03-03 | stale |
| `public.hist_mengxi_eastwardplanforecast_15min` | 147126 | time | 1 | 2026-03-03 | 2026-03-03 | stale |
| `public.hist_mengxi_hubaodongrealtimeclearprice_15min` | 132825 | time | 1 | 2022-05-26 | 2022-05-26 | stale |
| `public.hist_mengxi_hubaoxirealtimeclearprice_15min` | 132825 | time | 1 | 2022-05-26 | 2022-05-26 | stale |
| `public.hist_mengxi_loadregulationforecast_15min` | 147126 | time | 1 | 2026-03-03 | 2026-03-03 | stale |
| `public.hist_mengxi_newenergyforecast_15min` | 147126 | time | 1 | 2026-03-03 | 2026-03-03 | stale |
| `public.hist_mengxi_notmarketpowerforecast_15min` | 147126 | time | 1 | 2026-03-03 | 2026-03-03 | stale |
| `public.hist_mengxi_provincerealtimepriceforecast_15min` | 128782 | time | 1 | 2022-08-02 | 2022-08-02 | stale |
| `public.hist_mengxi_solarpowerforecast_15min` | 147126 | time | 1 | 2026-03-03 | 2026-03-03 | stale |
| `public.hist_mengxi_suyou` | 1249 | date | 1 | 2022-05-26 | 2022-05-26 | stale |
| `public.hist_mengxi_suyou_clear` | 1249 | date | 1 | 2022-05-26 | 2022-05-26 | stale |
| `public.hist_mengxi_windpowerforecast_15min` | 147126 | time | 1 | 2026-03-03 | 2026-03-03 | stale |
| `public.hist_mengxi_wuhai` | 1250 | date | 1 | 2022-05-26 | 2022-05-26 | stale |
| `public.hist_mengxi_wuhai_clear` | 1250 | date | 1 | 2022-05-26 | 2022-05-26 | stale |
| `public.hist_mengxi_wulanchabu` | 1029 | date | 1 | 2023-03-07 | 2023-03-07 | stale |
| `public.hist_mengxi_wulanchabu_clear` | 1029 | date | 1 | 2023-03-07 | 2023-03-07 | stale |
| `public.hist_mengxi_wulanchabu_clear_15min` | 98780 | time | 1 | 2023-03-07 | 2023-03-07 | stale |
| `public.hist_mengxi_wulanchabu_forecast` | 240 | date | 1 | 2025-03-03 | 2025-03-03 | stale |
| `public.hist_mengxi_wulanchabu_forecast_15min` | 23040 | time | 1 | 2025-03-03 | 2025-03-03 | stale |
| `public.hist_mengxi_wulate` | 1249 | date | 1 | 2022-05-26 | 2022-05-26 | stale |
| `public.hist_mengxi_wulate_clear` | 1249 | date | 1 | 2022-05-26 | 2022-05-26 | stale |
| `public.pipeline_file_log` | 70 | created_at | 1 | 2026-02-27 | 2026-02-27 | stale |

## F. Tables with No Temporal Column

| table | rows |
|---|---|
| `public.Hist_MengXi_HuEast` | 1018 |
| `public._tmp_monthly_1770389974` | 3 |

## G. Schema-Level Summary

**public:** 182 tables, 7,568,410 total rows — empty: 3, fresh: 31, no_temporal_column: 2, slightly_stale: 12, stale: 134

**marketdata:** 56 tables, 68,627,084 total rows — empty: 1, slightly_stale: 12, stale: 43

### Row Count by Schema

| schema | tables | total_rows | fresh | slightly_stale | stale | empty | no_temporal | error |
|---|---|---|---|---|---|---|---|---|
| public | 182 | 7,568,410 | 31 | 12 | 134 | 3 | 2 | 0 |
| marketdata | 56 | 68,627,084 | 0 | 12 | 43 | 1 | 0 | 0 |

## H. Suspected Ingestion Candidates

### Stale > 7 days (177 tables)

| table | rows | temporal_col | min | max | stale_days | status |
|---|---|---|---|---|---|---|
| `public.hist_mengxi_wulanchabu_clear_dayahead` | 1 | date | 2023-05-01 | 2023-05-01 | 1077 | **stale** |
| `public.hist_mengxi_wulanchabu_clear_dayahead_15min` | 96 | time | 2023-05-01 | 2023-05-01 | 1077 | **stale** |
| `public.mengxi_hueast` | 487 | date | 2022-06-01 | 2023-09-30 | 925 | **stale** |
| `public.hist_anhui_day_ahead_outer_delivery_trade_plan_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_load_regulation_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_new_energy_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_new_energy_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_not_market_power_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_not_market_power_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_solar_power_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_solar_power_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_tie_line_load_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_tie_line_load_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_total_power_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_water_power_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_water_power_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_wind_power_forecast_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_wind_power_real_15min` | 96 | time | 2024-10-01 | 2024-10-01 | 558 | **stale** |
| `public.hist_anhui_dayaheadouterdeliverytradeplanforecast_15min` | 3744 | time | 2024-05-01 | 2024-12-27 | 471 | **stale** |
| `public.hourly_power_pricessuyou` | 29 | date | 2025-01-04 | 2025-02-01 | 435 | **stale** |
| `public.hist_mengxi_hueast` | 1018 | date | 2022-06-01 | 2025-03-14 | 394 | **stale** |
| `public.nominated_mengxi_wulater` | 41 | date | 2025-03-01 | 2025-04-10 | 367 | **stale** |
| `public.price_real_mengxi_hubao_east` | 598 | date | 2024-01-01 | 2025-08-20 | 235 | **stale** |
| `public.measured_mengxi_东送计划_实测_mw` | 600 | date | 2024-01-01 | 2025-08-22 | 233 | **stale** |
| `public.measured_mengxi_光伏出力_实测_mw` | 600 | date | 2024-01-01 | 2025-08-22 | 233 | **stale** |
| `public.measured_mengxi_新能源出力_实测_mw` | 600 | date | 2024-01-01 | 2025-08-22 | 233 | **stale** |
| `public.measured_mengxi_统调负荷_实测_mw` | 600 | date | 2024-01-01 | 2025-08-22 | 233 | **stale** |
| `public.measured_mengxi_非市场出力_实测_mw` | 600 | date | 2024-01-01 | 2025-08-22 | 233 | **stale** |
| `public.measured_mengxi_风电出力_实测_mw` | 600 | date | 2024-01-01 | 2025-08-22 | 233 | **stale** |
| `public.forecast_mengxi_东送计划_d_1_mw` | 602 | date | 2024-01-01 | 2025-08-24 | 231 | **stale** |
| `public.forecast_mengxi_光伏出力_d_1_mw` | 602 | date | 2024-01-01 | 2025-08-24 | 231 | **stale** |
| `public.forecast_mengxi_新能源出力_d_1_mw` | 602 | date | 2024-01-01 | 2025-08-24 | 231 | **stale** |
| `public.forecast_mengxi_统调负荷_d_1_mw` | 602 | date | 2024-01-01 | 2025-08-24 | 231 | **stale** |
| `public.forecast_mengxi_非市场出力_d_1_mw` | 602 | date | 2024-01-01 | 2025-08-24 | 231 | **stale** |
| `public.forecast_mengxi_风电出力_d_1_mw` | 602 | date | 2024-01-01 | 2025-08-24 | 231 | **stale** |
| `public.price_d1_mengxi_hubao_east` | 599 | date | 2024-01-01 | 2025-08-24 | 231 | **stale** |
| `public.actual_mengxi_suyou` | 262 | date | 2025-01-18 | 2025-10-06 | 188 | **stale** |
| `public.actual_mengxi_wulate` | 236 | date | 2025-02-13 | 2025-10-06 | 188 | **stale** |
| `public.strategy_mengxi_suyou` | 262 | date | 2025-01-18 | 2025-10-06 | 188 | **stale** |
| `public.strategy_mengxi_wulate` | 236 | date | 2025-02-13 | 2025-10-06 | 188 | **stale** |
| `public.hist_anhui_dingyuan` | 1249 | date | 2022-05-25 | 2025-10-25 | 169 | **stale** |
| `public.hist_anhui_dingyuan_clear` | 329 | date | 2024-05-01 | 2025-10-25 | 169 | **stale** |
| `public.hist_anhui_dingyuan_clear_15min` | 31584 | time | 2024-05-01 | 2025-10-25 | 169 | **stale** |
| `public.hist_mengxi_suyou` | 1249 | date | 2022-05-25 | 2025-10-25 | 169 | **stale** |
| `public.hist_mengxi_suyou_clear` | 1249 | date | 2022-05-25 | 2025-10-25 | 169 | **stale** |
| `public.hist_mengxi_suyou_clear_15min` | 28320 | time | 2025-01-04 | 2025-10-25 | 169 | **stale** |
| `public.hist_mengxi_wulate` | 1249 | date | 2022-05-25 | 2025-10-25 | 169 | **stale** |
| `public.hist_mengxi_wulate_clear` | 1249 | date | 2022-05-25 | 2025-10-25 | 169 | **stale** |
| `public.hist_mengxi_wulate_clear_15min` | 27648 | time | 2025-01-04 | 2025-10-25 | 169 | **stale** |
| `public.hist_mengxi_wuhai` | 1250 | date | 2022-05-25 | 2025-10-26 | 168 | **stale** |
| `public.hist_mengxi_wuhai_clear` | 1250 | date | 2022-05-25 | 2025-10-26 | 168 | **stale** |
| `public.hist_mengxi_wuhai_clear_15min` | 24288 | time | 2025-01-04 | 2025-10-26 | 168 | **stale** |
| `public.hist_mengxi_wulanchabu` | 1029 | date | 2023-01-01 | 2025-10-26 | 168 | **stale** |
| `public.hist_mengxi_wulanchabu_clear` | 1029 | date | 2023-01-01 | 2025-10-26 | 168 | **stale** |
| `public.hist_mengxi_wulanchabu_clear_15min` | 98780 | time | 2023-01-01 | 2025-10-26 | 168 | **stale** |
| `public.hist_anhui_dingyuan_clear_dayahead` | 332 | date | 2024-05-01 | 2025-10-27 | 167 | **stale** |
| `public.hist_anhui_dingyuan_clear_dayahead_15min` | 31872 | time | 2024-05-01 | 2025-10-27 | 167 | **stale** |
| `public.hist_mengxi_suyou_forecast` | 1196 | date | 2022-07-18 | 2025-10-27 | 167 | **stale** |
| `public.hist_mengxi_suyou_forecast_15min` | 22848 | time | 2025-03-04 | 2025-10-27 | 167 | **stale** |
| `public.hist_mengxi_wuhai_forecast` | 1196 | date | 2022-07-18 | 2025-10-27 | 167 | **stale** |
| `public.hist_mengxi_wuhai_forecast_15min` | 22848 | time | 2025-03-04 | 2025-10-27 | 167 | **stale** |
| `public.hist_mengxi_wulanchabu_forecast` | 240 | date | 2025-03-01 | 2025-10-27 | 167 | **stale** |
| `public.hist_mengxi_wulanchabu_forecast_15min` | 23040 | time | 2025-03-01 | 2025-10-27 | 167 | **stale** |
| `public.hist_mengxi_wulate_forecast` | 1196 | date | 2022-07-18 | 2025-10-27 | 167 | **stale** |
| `public.hist_mengxi_wulate_forecast_15min` | 22848 | time | 2025-03-04 | 2025-10-27 | 167 | **stale** |
| `public.hist_shandong_binzhou_clear_dayahead` | 616 | date | 2023-11-21 | 2025-10-27 | 167 | **stale** |
| `public.hist_shandong_binzhou_clear_dayahead_15min` | 59136 | time | 2023-11-21 | 2025-10-27 | 167 | **stale** |
| `public.hist_mengxi_inhouse_windforecast_15min` | 1728 | time | 2025-10-11 | 2025-10-28 | 166 | **stale** |
| `public.hist_shandong_inhouse_windforecast_15min` | 1728 | time | 2025-10-11 | 2025-10-28 | 166 | **stale** |
| `public.hist_shandong_loadregulationforecastafterclear_15min` | 81823 | time | 2023-09-01 | 2025-12-31 | 102 | **stale** |
| `public.hist_shandong_networkloadforecastafterclear_15min` | 46399 | time | 2024-09-04 | 2025-12-31 | 102 | **stale** |
| `public.hist_shandong_newenergyforecastafterclear_15min` | 5225 | time | 2025-11-07 | 2025-12-31 | 102 | **stale** |
| `public.hist_shandong_solarpowerforecastafterclear_15min` | 81823 | time | 2023-09-01 | 2025-12-31 | 102 | **stale** |
| `public.hist_shandong_tielineloadforecastafterclear_15min` | 81823 | time | 2023-09-01 | 2025-12-31 | 102 | **stale** |
| `public.hist_shandong_windpowerforecastafterclear_15min` | 81823 | time | 2023-09-01 | 2025-12-31 | 102 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773466424` | 59232 | data_date | 2026-01-03 | 2026-01-03 | 99 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773466267` | 55872 | data_date | 2026-01-04 | 2026-01-04 | 98 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773466834` | 60087 | data_date | 2026-01-05 | 2026-01-05 | 97 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773466677` | 55872 | data_date | 2026-01-06 | 2026-01-06 | 96 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773467242` | 59336 | data_date | 2026-01-06 | 2026-01-06 | 96 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773467086` | 55872 | data_date | 2026-01-07 | 2026-01-07 | 95 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773467651` | 59328 | data_date | 2026-01-07 | 2026-01-07 | 95 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773467494` | 55872 | data_date | 2026-01-08 | 2026-01-08 | 94 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773468064` | 59328 | data_date | 2026-01-08 | 2026-01-08 | 94 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773467905` | 55872 | data_date | 2026-01-09 | 2026-01-09 | 93 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773468476` | 59328 | data_date | 2026-01-09 | 2026-01-09 | 93 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773468321` | 55872 | data_date | 2026-01-10 | 2026-01-10 | 92 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773468889` | 59328 | data_date | 2026-01-10 | 2026-01-10 | 92 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773468733` | 55872 | data_date | 2026-01-11 | 2026-01-11 | 91 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773469308` | 59328 | data_date | 2026-01-11 | 2026-01-11 | 91 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773469151` | 55872 | data_date | 2026-01-12 | 2026-01-12 | 90 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773469726` | 59328 | data_date | 2026-01-12 | 2026-01-12 | 90 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773469570` | 55872 | data_date | 2026-01-13 | 2026-01-13 | 89 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773470168` | 59328 | data_date | 2026-01-13 | 2026-01-13 | 89 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773470012` | 55872 | data_date | 2026-01-14 | 2026-01-14 | 88 | **stale** |
| `public.hist_shandong_motmarketnuclearpowerforecast_15min` | 1330 | time | 2026-01-01 | 2026-01-14 | 88 | **stale** |
| `public.nominated_mengxi_suyou` | 324 | date | 2025-03-01 | 2026-01-18 | 84 | **stale** |
| `public.nominated_mengxi_wulate` | 324 | date | 2025-03-01 | 2026-01-18 | 84 | **stale** |
| `public.hist_shandong_tielineloadreal___15min` | 7220 | time | 2025-11-06 | 2026-01-22 | 80 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773470591` | 60288 | data_date | 2026-01-30 | 2026-01-30 | 72 | **stale** |
| `marketdata.bess_daily` | 19117 | date | 2025-01-01 | 2026-01-30 | 72 | **stale** |
| `marketdata.raw_timeseries` | 1841472 | ts | 2025-01-01 | 2026-01-30 | 72 | **stale** |
| `public._tmp_daily_1770389974` | 91 | date | 2025-11-01 | 2026-01-30 | 72 | **stale** |
| `public._tmp_raw_1770389972` | 17472 | ts | 2025-11-01 | 2026-01-30 | 72 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773470432` | 56160 | data_date | 2026-01-31 | 2026-01-31 | 71 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773471014` | 60288 | data_date | 2026-01-31 | 2026-01-31 | 71 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773470858` | 56160 | data_date | 2026-02-01 | 2026-02-01 | 70 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773471544` | 60288 | data_date | 2026-02-01 | 2026-02-01 | 70 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773471281` | 56160 | data_date | 2026-02-02 | 2026-02-02 | 69 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773472181` | 60288 | data_date | 2026-02-02 | 2026-02-02 | 69 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773472023` | 56160 | data_date | 2026-02-03 | 2026-02-03 | 68 | **stale** |
| `marketdata.bess_dispatch_hourly` | 458808 | created_at | 2026-02-01 | 2026-02-06 | 65 | **stale** |
| `marketdata.bess_monthly` | 634 | created_at | 2026-02-01 | 2026-02-06 | 65 | **stale** |
| `public.hist_mengxi_biddingspacereal_15min` | 144085 | time | 2022-01-01 | 2026-02-10 | 61 | **stale** |
| `public.hist_mengxi_eastwardplanreal_15min` | 144085 | time | 2022-01-01 | 2026-02-10 | 61 | **stale** |
| `public.hist_mengxi_loadregulationreal_15min` | 144085 | time | 2022-01-01 | 2026-02-10 | 61 | **stale** |
| `public.hist_mengxi_newenergyreal_15min` | 144085 | time | 2022-01-01 | 2026-02-10 | 61 | **stale** |
| `public.hist_mengxi_notmarketpowerreal_15min` | 144085 | time | 2022-01-01 | 2026-02-10 | 61 | **stale** |
| `public.hist_mengxi_solarpowerreal_15min` | 144085 | time | 2022-01-01 | 2026-02-10 | 61 | **stale** |
| `public.hist_mengxi_windpowerreal_15min` | 144085 | time | 2022-01-01 | 2026-02-10 | 61 | **stale** |
| `marketdata.bess_capture_daily` | 21938 | date | 2025-01-01 | 2026-02-19 | 52 | **stale** |
| `marketdata.spot_dispatch_hourly_rt_forecast` | 526512 | datetime | 2025-01-01 | 2026-02-19 | 52 | **stale** |
| `marketdata.spot_dispatch_hourly_theoretical` | 524952 | datetime | 2025-01-01 | 2026-02-19 | 52 | **stale** |
| `marketdata.spot_prices_hourly` | 263256 | datetime | 2025-01-01 | 2026-02-19 | 52 | **stale** |
| `marketdata.spot_prices_hourly_rt_forecast` | 263256 | datetime | 2025-01-01 | 2026-02-19 | 52 | **stale** |
| `public.hist_anhui_provincerealtimeclearprice_15min` | 21195 | time | 2024-05-01 | 2026-02-28 | 43 | **stale** |
| `public.pipeline_file_log` | 70 | created_at | 2026-02-26 | 2026-02-28 | 43 | **stale** |
| `public.pipeline_job_status` | 1 | updated_at | 2026-02-28 | 2026-02-28 | 43 | **stale** |
| `public.hist_anhui_loadregulationreal_15min` | 44323 | time | 2024-05-01 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_negativesparereal_15min` | 44323 | time | 2024-05-01 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_newenergyreal_15min` | 44228 | time | 2024-05-01 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_notmarketpowerreal_15min` | 44419 | time | 2024-05-01 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_positivesparereal_15min` | 44323 | time | 2024-05-01 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_solarpowerreal_15min` | 44228 | time | 2024-05-01 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_tielineloadreal_15min` | 41924 | time | 2024-05-01 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_tielineloadreal__5901__15min` | 30596 | time | 2025-01-20 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_tielineloadreal__5904__15min` | 30596 | time | 2025-01-20 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_tielineloadreal__5907__15min` | 30596 | time | 2025-01-20 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_tielineloadreal__5914__15min` | 30596 | time | 2025-01-20 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_tielineloadreal__5917__15min` | 30596 | time | 2025-01-20 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_tielineloadreal__5921__15min` | 30596 | time | 2025-01-20 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_tielineloadreal__5931__15min` | 30596 | time | 2025-01-20 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_tielineloadreal__i__15min` | 30596 | time | 2025-01-20 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_tielineloadreal__ii__15min` | 30596 | time | 2025-01-20 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_waterpowerreal_15min` | 44419 | time | 2024-05-01 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_windpowerreal_15min` | 44228 | time | 2024-05-01 | 2026-03-03 | 40 | **stale** |
| `public.hist_anhui_provincedayaheadclearprice_15min` | 39341 | time | 2024-05-01 | 2026-03-06 | 37 | **stale** |
| `public.hist_mengxi_hubaodongrealtimeclearprice_15min` | 132825 | time | 2022-05-25 | 2026-03-10 | 33 | **stale** |
| `public.hist_mengxi_hubaoxirealtimeclearprice_15min` | 132825 | time | 2022-05-25 | 2026-03-10 | 33 | **stale** |
| `public.hist_mengxi_provincerealtimeclearprice_15min` | 135225 | time | 2022-05-01 | 2026-03-10 | 33 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1773439587` | 60768 | data_date | 2026-03-12 | 2026-03-12 | 31 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1773439433` | 56640 | data_date | 2026-03-13 | 2026-03-13 | 30 | **stale** |
| `public.hist_mengxi_biddingspaceforecast_15min` | 147126 | time | 2022-01-01 | 2026-03-15 | 28 | **stale** |
| `public.hist_mengxi_eastwardplanforecast_15min` | 147126 | time | 2022-01-01 | 2026-03-15 | 28 | **stale** |
| `public.hist_mengxi_loadregulationforecast_15min` | 147126 | time | 2022-01-01 | 2026-03-15 | 28 | **stale** |
| `public.hist_mengxi_newenergyforecast_15min` | 147126 | time | 2022-01-01 | 2026-03-15 | 28 | **stale** |
| `public.hist_mengxi_notmarketpowerforecast_15min` | 147126 | time | 2022-01-01 | 2026-03-15 | 28 | **stale** |
| `public.hist_mengxi_solarpowerforecast_15min` | 147126 | time | 2022-01-01 | 2026-03-15 | 28 | **stale** |
| `public.hist_mengxi_windpowerforecast_15min` | 147126 | time | 2022-01-01 | 2026-03-15 | 28 | **stale** |
| `marketdata._stg_md_id_cleared_energy_1775747004` | 60768 | data_date | 2026-03-16 | 2026-03-16 | 27 | **stale** |
| `marketdata.station_master` | 33 | updated_at | 2026-03-21 | 2026-03-21 | 22 | **stale** |
| `public.hist_mengxi_hubaodongrealtimepriceforecast_15min` | 128592 | time | 2022-07-18 | 2026-03-22 | 21 | **stale** |
| `public.hist_mengxi_hubaoxirealtimepriceforecast_15min` | 128511 | time | 2022-07-18 | 2026-03-22 | 21 | **stale** |
| `public.hist_mengxi_provincerealtimepriceforecast_15min` | 128782 | time | 2022-07-18 | 2026-03-22 | 21 | **stale** |
| `marketdata._stg_md_rt_nodal_price_1775769319` | 149472 | data_date | 2026-03-27 | 2026-03-27 | 16 | **stale** |
| `marketdata._stg_md_da_cleared_energy_1775781983` | 56928 | data_date | 2026-04-03 | 2026-04-03 | 9 | **stale** |
| `public.hist_anhui_dingyuan_forecast` | 356 | date | 2024-05-02 | 2026-04-04 | 8 | **stale** |
| `public.hist_anhui_dingyuan_forecast_15min` | 34175 | time | 2024-05-02 | 2026-04-04 | 8 | **stale** |
| `public.hist_anhui_dingyuan_forecast_dayahead` | 356 | date | 2024-05-02 | 2026-04-04 | 8 | **stale** |
| `public.hist_anhui_dingyuan_forecast_dayahead_15min` | 34175 | time | 2024-05-02 | 2026-04-04 | 8 | **stale** |
| `public.hist_anhui_loadregulationforecast_15min` | 45658 | time | 2024-05-01 | 2026-04-04 | 8 | **stale** |
| `public.hist_anhui_newenergyforecast_15min` | 45754 | time | 2024-04-28 | 2026-04-04 | 8 | **stale** |
| `public.hist_anhui_notmarketpowerforecast_15min` | 45754 | time | 2024-04-28 | 2026-04-04 | 8 | **stale** |
| `public.hist_anhui_solarpowerforecast_15min` | 45754 | time | 2024-04-28 | 2026-04-04 | 8 | **stale** |
| `public.hist_anhui_tielineloadforecast_15min` | 44218 | time | 2024-04-28 | 2026-04-04 | 8 | **stale** |
| `public.hist_anhui_waterpowerforecast_15min` | 45754 | time | 2024-04-28 | 2026-04-04 | 8 | **stale** |
| `public.hist_anhui_windpowerforecast_15min` | 45754 | time | 2024-04-28 | 2026-04-04 | 8 | **stale** |

### Tables with missing dates in last 30 days (60 tables)

| table | missing_count | last_missing | status |
|---|---|---|---|
| `marketdata.inner_mongolia_bess_results` | 21 | 2026-04-05 | slightly_stale |
| `marketdata.inner_mongolia_nodal_clusters` | 21 | 2026-04-05 | slightly_stale |
| `marketdata.md_avg_bid_price` | 21 | 2026-03-19 | slightly_stale |
| `marketdata.md_da_cleared_energy` | 36 | 2026-04-03 | slightly_stale |
| `marketdata.md_da_fuel_summary` | 35 | 2026-03-25 | slightly_stale |
| `marketdata.md_id_fuel_summary` | 7 | 2026-04-05 | slightly_stale |
| `marketdata.md_rt_total_cleared_energy` | 58 | 2026-03-23 | slightly_stale |
| `public.hist_anhui_dingyuan_forecast` | 347 | 2026-04-03 | stale |
| `public.hist_anhui_dingyuan_forecast_15min` | 347 | 2026-04-03 | stale |
| `public.hist_anhui_dingyuan_forecast_dayahead` | 347 | 2026-04-03 | stale |
| `public.hist_anhui_dingyuan_forecast_dayahead_15min` | 347 | 2026-04-03 | stale |
| `public.hist_anhui_loadregulationforecast_15min` | 227 | 2026-04-03 | stale |
| `public.hist_anhui_newenergyforecast_15min` | 229 | 2026-04-03 | stale |
| `public.hist_anhui_notmarketpowerforecast_15min` | 229 | 2026-04-03 | stale |
| `public.hist_anhui_solarpowerforecast_15min` | 229 | 2026-04-03 | stale |
| `public.hist_anhui_tielineloadforecast_15min` | 245 | 2026-04-03 | stale |
| `public.hist_anhui_waterpowerforecast_15min` | 229 | 2026-04-03 | stale |
| `public.hist_anhui_windpowerforecast_15min` | 229 | 2026-04-03 | stale |
| `public.hist_shandong_binzhou` | 250 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_binzhou_clear` | 250 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_binzhou_clear_15min` | 250 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_binzhou_forecast` | 154 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_binzhou_forecast_15min` | 154 | 2026-04-08 | fresh |
| `public.hist_shandong_binzhou_forecast_dayahead` | 154 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_binzhou_forecast_dayahead_15min` | 154 | 2026-04-08 | fresh |
| `public.hist_shandong_loadregulationforecast_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_loadregulationreal_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_loadregulationsubtielineloadforecast_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_loadregulationsubtielineloadreal_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_localpowerplantforecast_15min` | 50 | 2026-04-08 | fresh |
| `public.hist_shandong_localpowerplantreal_15min` | 7 | 2026-04-08 | fresh |
| `public.hist_shandong_negativespareforecast_15min` | 6 | 2026-04-08 | fresh |
| `public.hist_shandong_negativesparereal_15min` | 6 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_networkloadforecast_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_networkloadreal_15min` | 6 | 2026-04-08 | fresh |
| `public.hist_shandong_newenergyforecast_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_newenergyreal_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_newpositivespareforecast_15min` | 112 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_notmarketnuclearpowerforecast_15min` | 13 | 2026-04-08 | fresh |
| `public.hist_shandong_notmarketnuclearpowerreal_15min` | 22 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_positivespareforecast_15min` | 6 | 2026-04-08 | fresh |
| `public.hist_shandong_positivesparereal_15min` | 6 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_provincedayaheadclearprice_15min` | 4 | 2026-04-08 | fresh |
| `public.hist_shandong_provincerealtimeclearprice_15min` | 5 | 2026-04-08 | fresh |
| `public.hist_shandong_pumpedstoragepowerforecast_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_pumpedstoragepowerreal_15min` | 7 | 2026-04-08 | fresh |
| `public.hist_shandong_solarpowerforecast_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_solarpowerreal_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_standbyunitforecast_15min` | 4 | 2026-04-08 | fresh |
| `public.hist_shandong_standbyunitreal_15min` | 22 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_testunitforecast_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_testunitreal_15min` | 22 | 2026-04-08 | slightly_stale |
| `public.hist_shandong_thermalpowerbiddingspaceforecast_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_thermalpowerbiddingspacereal_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_tielineloadforecast_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_tielineloadforecast___15min` | 12 | 2026-04-08 | fresh |
| `public.hist_shandong_tielineloadreal_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_windpowerforecast_15min` | 3 | 2026-04-08 | fresh |
| `public.hist_shandong_windpowerreal_15min` | 3 | 2026-04-08 | fresh |
| `public.spot_daily` | 362 | 2026-12-30 | fresh |

### Empty tables with production-sounding names (4 tables)

| table |
|---|
| `marketdata.bess_dispatch_hourly_rt` |
| `public.agent_request_log` |
| `public.spot_hourly` |
| `public.spotprice_ingest_log` |

## J. Methodology & Limitations

- **Temporal column selection:** priority list (business-semantic names first, then generic
  date/time names, then any column with a temporal dtype, then created_at/updated_at as fallback).
- **Freshness buckets:** fresh = max_date within 1 day of today; slightly_stale = 2–7 days;
  stale = > 7 days. Today is 2026-04-12.
- **Missing dates:** computed via generate_series between MIN and MAX date for tables with span
  ≤ 5 years. Tables with longer spans are flagged but not fully enumerated.
- **Row counts:** via COUNT(*) — exact but may be slow on very large tables.
- **Performance:** aggregate queries only; no full-scan SELECT * was used.
- **Read-only:** no data was modified. Session set to `readonly=True`.
- **Schemas:** only BASE TABLEs in schemas `public` and `marketdata`. Views, foreign tables,
  partitioned parent tables excluded.
