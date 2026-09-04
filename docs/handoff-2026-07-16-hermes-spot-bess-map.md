# Handoff: Hermes / spot-market / bess-map — 2026-07-16

## Context

Branch: `cost-optimisation`
Last commit: `91af98a`
Repo: `MonteCarlo79/invest-etrm-intel`
Working dir: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`

ECR: `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com`
ECS cluster: `bess-platform-cluster`

---

## What was done this session

### 1. Hermes — capacity ETL filename matching fix

**Problem:** Files named `重庆装机-gpt-20260716.xlsx` / `江西装机-gpt-20260716.xlsx`
were archived but NOT ingested into `province_installed_monthly` because
`is_capacity_file()` only matched compound keywords like "储能装机", "装机容量" — not
plain "装机".

**Fix:** `services/hermes/capacity_etl.py` and `services/hermes/capacity_manual_etl.py`

```python
# capacity_etl.py — is_capacity_file()
if "装机" in filename and province_from_filename(filename) is not None:
    return True

# capacity_manual_etl.py — is_capacity_file_extended()
if "装机" in filename and _pff(filename) is not None:
    return True
```

Pattern: matches any xlsx/pdf/docx with "装机" in filename AND a recognised
Chinese province name (重庆, 江西, etc.) from `province_from_filename()`.

**Deploy:** `bess-platform-hermes` ECS service redeployed. User must **re-upload**
the 重庆装机 file via Feishu — it was never ingested before the fix.

---

### 2. spot-market — 河北 split on EOH choropleth map

**Problem:** The EOH map showed a single Hebei region (adcode 130000). User wants
separate 冀南 and 冀北 regions matching the power grid split.

**Fix:** `apps/spot-market/data/china_provinces.geojson`

Python (Shapely) split at latitude 39.5°N:
- **冀北** → fake adcode `130099` (north of 39.5°N); centroid (41.2°N, 117.0°E)
- **冀南** → fake adcode `130098` (south of 39.5°N); centroid (38.0°N, 114.5°E)

```python
# Script used (run once, result committed):
from shapely.geometry import shape, mapping, box
geom = shape(feat['geometry']).buffer(0)
jibei = geom.intersection(box(112, 39.5, 121, 44))   # adcode 130099
jinan = geom.intersection(box(112, 35, 121, 39.5))   # adcode 130098
```

`_PROV_ADCODE` in `apps/spot-market/app.py` (EOH section ~line 5494):
```python
"冀北": "130099", "冀南": "130098", "河北南网": "130098",
```

`_PROV_CENTROIDS_EOH`:
```python
"130099": (41.20, 117.00),   # 冀北
"130098": (38.04, 114.47),   # 冀南
```

`_ADCODE_TO_NAME`:
```python
130099: "冀北", 130098: "冀南",
```

---

### 3. spot-market — province deduplication in 热电缺口 chart

**Problem:** Both `河北南网` and `冀南` appeared as separate rows in the
"所有省份热电缺口" bar chart because the SQL UNION from three data sources
produces duplicate province names.

**Fix:** `apps/spot-market/app.py` ~line 5238 (after `_load_supply_data` call):

```python
_SUP_PROV_NORM = {
    '河北南网': '冀南', '冀南网': '冀南', '河北南部': '冀南',
    '冀北电网': '冀北', '国网冀北': '冀北',
    '内蒙古东': '蒙东', '内蒙古西': '蒙西',
}
_DROP_ALIASES = {'中长期', '河北'}
_sup_df = _sup_df[~_sup_df['province'].isin(_DROP_ALIASES)]
_sup_df['province'] = _sup_df['province'].map(lambda p: _SUP_PROV_NORM.get(p, p))
_sup_df = (_sup_df.sort_values('thermal_mw', ascending=False)
                  .drop_duplicates(subset=['province'], keep='first')
                  .reset_index(drop=True))
```

---

### 4. bess-map — 河北南网 灵活热电 = 0 fix

**Problem:** `province_fundamentals` stores the grid as "冀南" but bess-map demand
tab and FR sizing loop looked up "河北南网" → miss → thermal=0.

**Fix:** `apps/bess-map/app.py` — alias dicts added in both loops:

```python
# FR sizing loop (~line 2419)
_FR_FUND_ALIAS = {"河北南网": "冀南", "冀南": "河北南网"}
_pdata = _fund_all.get(_prov) or _fund_all.get(_FR_FUND_ALIAS.get(_prov, ""), {})
_mon = _monthly_all.get(_prov) or _monthly_all.get(_FR_FUND_ALIAS.get(_prov, ""))

# Demand waterfall loop (~line 2494)
_FUND_ALIAS = {"河北南网": "冀南", "冀南": "河北南网"}
_pdata = _fund_all.get(_prov) or _fund_all.get(_FUND_ALIAS.get(_prov, ""), {})
_mon = _monthly_all.get(_prov) or _monthly_all.get(_FUND_ALIAS.get(_prov, ""))
```

Thermal fallback (uses `province_installed_monthly` when annual fundamentals = 0):
```python
_thermal_mw = _thermal_wkw * 10
if _thermal_mw == 0 and _mon and _mon.get("thermal_mw"):
    _thermal_mw = float(_mon["thermal_mw"])
```

---

### 5. Database changes (applied directly via psycopg2)

All changes to `bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com`
database `marketdata`.

#### province_installed_monthly

| province | year_month | change |
|---|---|---|
| 江西 | 2026-07-01 | thermal 3200→32000, solar 3480→34800, hydro 720→7200 (unit fix: 万kW×10=MW) |
| 重庆 | 2026-07-01 | NEW row: thermal=23500, solar=9500 (风光), hydro=8800 MW |

#### province_fundamentals

| province | year | change |
|---|---|---|
| 江西 | 2025 | peak_summer_mw: 27500→40060, peak_winter_mw: 22000→33000 |
| 江西 | 2024 | thermal_gen_100gwh=1200 (estimate: 1200亿kWh) |
| 江西 | 2025 | thermal_gen_100gwh=1280 (estimate: 1280亿kWh) |
| 重庆 | 2025 | peak_summer_mw: NULL→29000, peak_winter_mw: 3028→29000, thermal_cap_10kw: NULL→2350, hydro_cap_10kw: NULL→880 |
| 重庆 | 2024 | thermal_gen_100gwh=460 (estimate: 460亿kWh) |
| 重庆 | 2025 | thermal_gen_100gwh=500 (estimate: 500亿kWh) |

**Note on thermal_gen_100gwh estimates:** These are calculated from capacity ×
estimated utilisation hours (not from official NEA stats). For 江西: ~3200h
utilisation on 37380 MW → 1200 亿kWh. For 重庆: ~2100h on 23500 MW →
460 亿kWh (low because 重庆 is a heavy importer + hydro province). Actual NEA
2024 provincial thermal generation figures should replace these when available
(check: NEA 全国电力工业统计数据 or CEC 统计快报).

---

## Pending / known issues

### Must-do

1. **Re-upload 重庆装机 file in Feishu** — the file was uploaded before the hermes
   ETL fix and was never ingested. User needs to re-upload via Feishu now that
   hermes is fixed. Province `province_installed_monthly` row for 重庆 was inserted
   manually, but re-uploading will update it with any newer values.

2. **Wrong thermal_mw=2812 in province_installed_monthly for 冀南** — a previous
   docx LLM extraction stored 2812 MW (likely a unit confusion: 2812 MW should
   be ~28120 MW if in 万kW). The supply tab now uses `province_fundamentals` as
   authoritative thermal source for EOH, so this bad row is worked around — but
   the raw data in `province_installed_monthly` is still wrong and should be
   NULLed:
   ```sql
   UPDATE marketdata.province_installed_monthly
   SET thermal_mw = NULL
   WHERE province = '冀南' AND thermal_mw = 2812;
   ```

3. **Verify thermal_gen_100gwh estimates for 江西/重庆** — current values are
   rough estimates. Replace with official NEA 2024 provincial thermal generation
   figures when available.

### Nice-to-have

4. **EOH for other non-spot provinces** — provinces like 湖北, 湖南, 广西 also
   lack `thermal_gen_100gwh` and show gray on the EOH map. Same fix pattern applies:
   update `province_fundamentals` with annual thermal generation statistics.

5. **冀北 appearing in EOH fundamentals path** — 冀北 peak loads were manually added
   to `province_fundamentals` (peak_summer=31000, peak_winter=30000 MW, year 2025).
   It now appears in the supply structure tab. If `thermal_gen_100gwh` is added for
   冀北, it will also appear on the EOH map.

---

## Key file locations

| File | Purpose |
|---|---|
| `services/hermes/capacity_etl.py` | Core capacity Excel ETL + `is_capacity_file()` |
| `services/hermes/capacity_manual_etl.py` | Manual text/PDF/URL capacity entry + `is_capacity_file_extended()` |
| `apps/spot-market/app.py` | spot-market Streamlit app; EOH section ~line 5450–5880 |
| `apps/spot-market/data/china_provinces.geojson` | Province polygons for EOH map (now has split Hebei) |
| `apps/bess-map/app.py` | bess-map Streamlit app; demand waterfall ~line 2494 |

## Key DB tables

| Table | Description |
|---|---|
| `marketdata.province_fundamentals` | Annual: capacity (万kW), generation (100GWh), peak loads (MW) |
| `marketdata.province_installed_monthly` | Monthly: capacity by fuel type (MW), from LLM ETL of uploaded xlsx |
| `marketdata.spot_fundamentals_hourly` | Hourly: load/wind/solar MW for spot-market provinces |

## Province name aliases (critical — causes many bugs)

| In code/data | Canonical | Notes |
|---|---|---|
| `河北南网` | `冀南` | spot_fundamentals_hourly uses 河北南网; province_fundamentals uses 冀南 |
| `冀北电网` | `冀北` | |
| `内蒙古东` | `蒙东` | |
| `内蒙古西` | `蒙西` | |

## Deploy commands

```bash
# ECR login (expires ~12h)
aws ecr get-login-password --region ap-southeast-1 | \
  docker login --username AWS --password-stdin \
  319383842493.dkr.ecr.ap-southeast-1.amazonaws.com

# Build + push spot-market
docker build -f apps/spot-market/Dockerfile -t spot-market .
docker tag spot-market:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets:latest
aws ecs update-service --cluster bess-platform-cluster \
  --service bess-platform-spot-markets-svc --force-new-deployment --region ap-southeast-1

# Build + push hermes
docker build -f apps/hermes-service/Dockerfile -t hermes .
docker tag hermes:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
aws ecs update-service --cluster bess-platform-cluster \
  --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1

# Build + push bess-map
docker build -f apps/bess-map/Dockerfile -t bess-map .
docker tag bess-map:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-map:latest
aws ecs update-service --cluster bess-platform-cluster \
  --service bess-platform-bess-map-svc --force-new-deployment --region ap-southeast-1
```
