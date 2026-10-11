# 充电侧系统运行费水平（自有数据）

- source: 自有复现查询 public.province_sysopfee_monthly · license: own
- retrieved_at: 2026-10-11
- query: |
    select province, year_month, fee_yuan_kwh from public.province_sysopfee_monthly
    where province in ('山西','陕西','河南','安徽','山东','甘肃','蒙西') and year_month >= '2026-01-01'
    order by province, year_month

> 2026年1—5月充电侧系统运行费（元/kWh）：
> 河南 0.101–0.143（五省最高）；山西 0.097–0.115（1—6月均值约0.095，与第三方测算一致）；陕西 0.076–0.108；安徽 0.069–0.110；山东 0.055–0.112；甘肃 0.029–0.094；蒙西 0.029–0.053。
> 口径对照：安徽独立储能2026年现货度电收益约0.23元/kWh（见 anhui_cap_price）；按85%综合效率折算，每放1度电需充约1.18度，充电侧系统运行费0.07–0.11元/kWh约相当于放电口径0.08–0.13元/kWh——吞噬毛价差的三到五成。
