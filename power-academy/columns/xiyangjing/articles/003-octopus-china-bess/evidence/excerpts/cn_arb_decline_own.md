# 储能先行省份现货套利空间同比压缩（自有数据）

- source: 自有复现查询 marketdata.bess_capture_daily（LP理论最优，2h系统，模型 ols_rt_time_v1 口径下理论值与模型无关）· license: own
- retrieved_at: 2026-10-11
- query: |
    with y25 as (select province, avg(theoretical_profit_per_mwh_day) t from marketdata.bess_capture_daily
                 where duration_h=2 and date between '2025-01-01' and '2025-09-30' group by 1),
         y26 as (select province, avg(theoretical_profit_per_mwh_day) t from marketdata.bess_capture_daily
                 where duration_h=2 and date between '2026-01-01' and '2026-09-30' group by 1)
    select province, y25.t, y26.t, (y26.t-y25.t)/y25.t from y25 join y26 using (province)

> 1—9月同比（元/MWh/日，2h理论最优日均收益，2025 vs 2026）：
> 河北南网 542.5 → 267.4（−51%）；贵州 494.7 → 297.2（−40%）；河南 336.3 → 222.7（−34%）；江西 456.2 → 308.1（−32%）；江苏 111.3 → 80.2（−28%）；辽宁 612.1 → 544.1（−11%）；安徽 236.7 → 211.9（−10%）。
> 同期全国25省均值 252.2 → 341.7（+35%）——上升全部来自2025年后才转入连续运行的新开省份（山西、山东、蒙西、湖北、吉林等2025年基期接近零）；收益压缩集中发生在储能先行建成的成熟省份。
> 数据质量注记：福建2026年数据因上游采集缺陷（串流内容错误）剔除；个别省份2025年基期存在零值段，未纳入对比。
