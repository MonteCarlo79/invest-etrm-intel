import os
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(os.getenv('DB_DSN') or os.getenv('PGURL'))

q1 = text("""
WITH mapped AS (
  SELECT
    m.datetime::date AS trade_date,
    a.asset_code,
    COUNT(*) AS rows_n
  FROM marketdata.md_id_cleared_energy m
  JOIN core.asset_alias_map a
    ON a.active_flag = TRUE
   AND (
      LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.plant_name, '')))
      OR LOWER(TRIM(a.alias_value)) = LOWER(TRIM(COALESCE(m.dispatch_unit_name, '')))
   )
  WHERE a.asset_code IN ('suyou','wulate','wuhai','wulanchabu','hetao','hangjinqi','siziwangqi','gushanliang')
    AND m.cleared_price IS NOT NULL
  GROUP BY 1,2
)
SELECT trade_date, SUM(rows_n) AS mapped_rows
FROM mapped
GROUP BY 1
ORDER BY trade_date DESC
LIMIT 20
""")

q2 = text("""
SELECT trade_date, COUNT(*) AS rows_n
FROM reports.bess_asset_daily_scenario_pnl
GROUP BY trade_date
ORDER BY trade_date DESC
LIMIT 20
""")

print('=== mapped cleared_price coverage ===')
print(pd.read_sql(q1, engine).to_string(index=False))
print('=== report coverage ===')
print(pd.read_sql(q2, engine).to_string(index=False))
