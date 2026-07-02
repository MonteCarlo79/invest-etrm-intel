"""Debug script: check why nodal PF ranks are empty in the ranking PDF."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging, pandas as pd
from datetime import date, timedelta
logging.basicConfig(level=logging.INFO)

pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL")
if not pg_url:
    print("ERROR: set PGURL env var")
    sys.exit(1)

from services.hermes.mengxi_ranking_report import _latest_data_date, _read_station_master, _query_nodal_prices

latest = _latest_data_date(pg_url)
print(f"latest data date: {latest}")

plants = _read_station_master(pg_url)
plant_names = [p["plant_name"] for p in plants]
print(f"station_master plants: {len(plant_names)}")

end_excl = latest + timedelta(days=1)
prices_df = _query_nodal_prices(pg_url, plant_names, latest, end_excl)
print(f"nodal_prices_df shape: {prices_df.shape}")
if prices_df.empty:
    print("ERROR: nodal_prices_df is EMPTY - no price data found")
    sys.exit(1)

print(f"datetime dtype: {prices_df['datetime'].dtype}")
print(f"sample datetimes:\n{prices_df['datetime'].head(3).to_list()}")
print(f"unique plants with prices: {prices_df['plant_name'].nunique()}")

# test LP on first plant
first_plant = prices_df["plant_name"].iloc[0]
grp = prices_df[prices_df["plant_name"] == first_plant]
prices_s = grp.set_index("datetime")["cleared_price"].sort_index()
print(f"\nTesting LP for {first_plant}: {len(prices_s)} intervals")
print(f"index dates: {prices_s.index.normalize().unique().tolist()}")

from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices
_, profit = compute_dispatch_from_15min_prices(prices_s, power_mw=1.0, duration_h=2.0, roundtrip_eff=0.85)
print(f"profit_2h sum: {profit.sum():.2f}, n_days solved: {len(profit)}")
