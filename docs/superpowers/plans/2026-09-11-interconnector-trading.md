# Interconnector Trading Tab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an "Interconnector / 跨区通道" tab to apps/spot-market covering channel topology (S1), capacity & daily-flow analysis (S2 = A1+A5), MLT patterns + renewable MLT obligation (S3 = A4+A2), and month-ahead spread backtest (S4 = A3).

**Architecture:** New `services/interconnector/` package (registry, ingest, data, topology — all pure-python testable) + `apps/spot-market/interconnector_tab.py` render module wired into app.py's existing `st.tabs` row. Uploads go straight to staging tables via the app's cached psycopg2 connection (project pattern, no S3). Map uses streamlit-echarts with click events driving a side panel.

**Tech Stack:** Streamlit, streamlit-echarts (events API + Map class), psycopg2 (app `__conn()` pattern, autocommit), pandas, plotly (S2/S3/S4 charts), pytest.

**Spec:** `docs/superpowers/specs/2026-09-11-interconnector-trading-design.md`

## Global Constraints

- Work on branch `feat/interconnector-tab`; merge to `main` only after Task 16 local verification passes (CLAUDE.md: incomplete features don't go to main).
- DB connection: the app's cached psycopg2 conn (`PGURL`/`DATABASE_URL`/`DB_URL` env, autocommit=True). All new tables use `CREATE TABLE IF NOT EXISTS` (idempotent, applied on first use).
- In-app upload only — never S3 (CLAUDE.md data-upload rule).
- 华东 parser year: trades file has no year column; parser takes explicit `year: int` param (2026 for current file).
- No forecasts anywhere in A2 — historical actuals only; forward months = year-ago same-month, labeled 历史同期.
- MLT% runtime priority: tab input-box override (`staging.interconnector_mlt_pct_override`) > knowledge-file rule > default 80% (default visually marked).
- Multi-channel series trades: full trade volume attributes to EVERY listed channel (documented in A1 section).
- Missing data renders "无数据"/未设定 — never interpolated.
- Tests must not touch the real DB: parsers run on synthetic fixtures; DB helpers tested with fake conn/cursor capturing SQL.
- Prototype reference (visual/interaction source of truth): `debug/interconnector_topology.html`.

---

### Task 1: streamlit-echarts verification spike

**Files:**
- Modify: `apps/spot-market/requirements.txt`
- Create (throwaway, not committed): `debug/spike_st_echarts.py`

**Interfaces:**
- Produces: confirmed API usage for `st_echarts(options, map, events, height, key)` and the shape of the click return value; pinned version for requirements.

- [ ] **Step 1: Install into local venv**

```bash
~/.venvs/bess-platform/bin/pip install "streamlit-echarts>=0.5.0"
~/.venvs/bess-platform/bin/pip show streamlit-echarts | head -2
```

- [ ] **Step 2: Write the spike**

```python
# debug/spike_st_echarts.py
import json, streamlit as st
from streamlit_echarts import st_echarts, Map

geo = json.load(open("assets/geo/china_provinces.json"))  # created in Task 3; for the spike use /tmp/china_geo.json if missing
opts = {
    "geo": {"map": "china", "roam": True,
            "itemStyle": {"areaColor": "#e6ebf1", "borderColor": "#c3cfda"},
            "emphasis": {"itemStyle": {"areaColor": "#d7e0e9"}}},
    "series": [],
}
evt_js = "function(params) { return {name: params.name || null, componentType: params.componentType, seriesType: params.seriesType || null}; }"
result = st_echarts(options=opts, map=Map("china", geo),
                    events={"click": evt_js}, height="500px", key="spike")
st.write("raw result:", result)
```

Run: `~/.venvs/bess-platform/bin/streamlit run debug/spike_st_echarts.py --server.port 8599`
Expected: map renders; clicking a province shows `{'name': '山东省', 'componentType': 'geo', ...}` (or `result.chart_event` holds that dict — record which shape is real).

- [ ] **Step 3: Record findings**

Decide: (a) exact return shape → use in Task 12; (b) whether echarts renders without external network (block CDN in devtools or check component static assets) → if it needs CDN, note the fallback (selectbox + static HTML regen per spec §4) as a comment in Task 12's code.

- [ ] **Step 4: Pin and commit**

```bash
# add to apps/spot-market/requirements.txt: streamlit-echarts==<resolved version>
git add apps/spot-market/requirements.txt
git commit -m "Add streamlit-echarts for interconnector tab map (spike-verified)"
```

---

### Task 2: Channel registry module

**Files:**
- Create: `services/interconnector/__init__.py` (empty)
- Create: `services/interconnector/registry.py`
- Test: `tests/interconnector/test_registry.py`

**Interfaces:**
- Produces: `registry.CHANNELS: list[dict]`, `registry.get_channels() -> list[dict]`. Each dict: `{name, formal, send, send_prov, sc, recv, recv_prov, rc, kv, gw, commissioned, km, category, note}` — `sc`/`rc` are `[lon, lat]` floats; `kv` in `{"±100","±400","±500","±660","±800","±1100","500kV AC"}`; `gw` float; `commissioned` str; `km` int; `category` in `{"电源绑定型","大基地型","制度创新型","联网型"}`.

- [ ] **Step 1: Write the failing test**

```python
# tests/interconnector/test_registry.py
from services.interconnector.registry import CHANNELS, get_channels

KV_CLASSES = {"±100","±400","±500","±660","±800","±1100","500kV AC"}
CATEGORIES = {"电源绑定型","大基地型","制度创新型","联网型"}

def test_twenty_six_channels_with_unique_names():
    assert len(CHANNELS) == 26
    assert len({c["name"] for c in CHANNELS}) == 26

def test_required_fields_and_coords():
    for c in CHANNELS:
        assert c["kv"] in KV_CLASSES, c["name"]
        assert c["category"] in CATEGORIES, c["name"]
        assert c["gw"] > 0, c["name"]
        for key in ("send","send_prov","recv","recv_prov","formal","commissioned"):
            assert c[key], (c["name"], key)
        lon, lat = c["sc"]; assert 73 <= lon <= 136 and 17 <= lat <= 54, c["name"]
        lon, lat = c["rc"]; assert 73 <= lon <= 136 and 17 <= lat <= 54, c["name"]

def test_user_corrected_six_channels():
    by = {c["name"]: c for c in CHANNELS}
    assert by["云霄直流"]["kv"] == "±100" and by["云霄直流"]["gw"] == 2.0
    assert by["云霄直流"]["category"] == "制度创新型"
    assert by["宜华直流"]["send_prov"] == "湖北" and by["宜华直流"]["recv_prov"] == "上海"
    assert by["林枫直流"]["send"] == "团林(荆门)" and by["林枫直流"]["gw"] == 3.0
    assert by["坤渝直流"]["send_prov"] == "新疆" and by["坤渝直流"]["recv_prov"] == "重庆"
    assert by["庆东直流"]["send"] == "庆阳" and by["庆东直流"]["recv_prov"] == "山东"
    assert by["宝合直流"]["send_prov"] == "陕西" and by["宝合直流"]["recv_prov"] == "安徽"
    assert by["宝合直流"]["commissioned"] == "2026-06-30"

def test_get_channels_returns_copy():
    a = get_channels(); a[0]["gw"] = -1
    assert get_channels()[0]["gw"] > 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector/test_registry.py -q`
Expected: FAIL — `ModuleNotFoundError: services.interconnector`

- [ ] **Step 3: Write registry.py with the 26 reviewed channels**

```python
# services/interconnector/registry.py
"""Cross-region DC channel registry — mirror of knowledge/interconnectors/channel_registry.md.
User-reviewed 2026-09-11. Corrections go to the knowledge file first, then here."""
from __future__ import annotations

# (name, formal, send, send_prov, [slon,slat], recv, recv_prov, [rlon,rlat], kv, gw, commissioned, km, category, note)
_CHANNELS_RAW = [
 ("锡泰直流","锡盟—江苏泰州±800kV特高压直流","锡林浩特","内蒙古",[116.07,43.94],"泰州","江苏",[119.93,32.46],"±800",10.0,"2017",1620,"大基地型","锡盟煤电/新能源基地送江苏"),
 ("雁淮直流","雁门关—淮安±800kV特高压直流","朔州(雁门关)","山西",[112.43,39.33],"淮安","江苏",[119.02,33.61],"±800",8.0,"2017",1119,"大基地型","山西煤电/新能源送江苏"),
 ("鲁固直流","扎鲁特—青州±800kV特高压直流","扎鲁特(通辽)","内蒙古",[120.90,44.55],"青州","山东",[118.48,36.68],"±800",10.0,"2017",1234,"大基地型","蒙东煤电/新能源送山东"),
 ("灵绍直流","宁东—绍兴±800kV特高压直流","宁东","宁夏",[106.40,38.20],"绍兴","浙江",[120.58,30.03],"±800",8.0,"2016",1720,"大基地型","宁东煤电/新能源送浙江"),
 ("吉泉直流","准东—皖南±1100kV特高压直流","昌吉","新疆",[87.30,44.01],"古泉(宣城)","安徽",[118.76,30.94],"±1100",12.0,"2019",3324,"大基地型","世界电压等级最高;新疆基地送安徽"),
 ("青豫直流","青海—河南±800kV特高压直流","共和(海南州)","青海",[100.62,36.28],"驻马店","河南",[114.02,33.01],"±800",8.0,"2020-12",1587,"大基地型","纯新能源外送通道"),
 ("祁韶直流","酒泉—湖南±800kV特高压直流","酒泉","甘肃",[98.49,39.73],"湘潭","湖南",[112.94,27.83],"±800",8.0,"2017",2383,"大基地型","甘肃风光送湖南"),
 ("锦苏直流","锦屏—苏州±800kV特高压直流","锦屏(西昌)","四川",[101.80,27.90],"苏州","江苏",[120.62,31.30],"±800",7.2,"2012",2059,"电源绑定型","雅砻江水电送江苏"),
 ("复奉直流","向家坝—上海±800kV特高压直流","向家坝(宜宾)","四川",[104.40,28.60],"奉贤","上海",[121.47,30.92],"±800",6.4,"2010",1907,"电源绑定型","金沙江水电送上海"),
 ("宾金直流","溪洛渡—金华±800kV特高压直流","溪洛渡(永善)","云南",[103.60,28.20],"金华","浙江",[119.65,29.08],"±800",7.5,"2014",1680,"电源绑定型","金沙江水电送浙江;容量≈750万kW"),
 ("陕武直流","陕北—武汉±800kV特高压直流","榆林","陕西",[109.73,38.29],"武汉","湖北",[114.30,30.60],"±800",8.0,"2021-12",1129,"大基地型","陕北煤电/新能源送湖北"),
 ("中衡直流","宁夏—湖南±800kV特高压直流","中宁","宁夏",[105.68,37.49],"衡阳","湖南",[112.57,26.90],"±800",8.0,"2025-06",1634,"大基地型","沙戈荒基地送湖南"),
 ("银东直流","宁东—青岛±660kV直流","银川","宁夏",[106.23,38.49],"青岛","山东",[120.38,36.07],"±660",4.0,"2011",1335,"电源绑定型","宁东煤电送山东"),
 ("昭沂直流","上海庙—临沂±800kV特高压直流","上海庙(鄂尔多斯)","内蒙古",[106.98,38.25],"沂南(临沂)","山东",[118.35,35.05],"±800",10.0,"2019",1238,"大基地型","鄂尔多斯煤电/新能源送山东"),
 ("庆东直流","陇东—山东±800kV特高压直流","庆阳","甘肃",[107.64,35.71],"东平(泰安)","山东",[116.47,35.94],"±800",8.0,"2025",930,"大基地型","首条风光火储一体化;煤电400万+风电670万+光伏380万+储能100万kW"),
 ("德宝直流","德阳—宝鸡±500kV直流","德阳","四川",[104.40,31.13],"宝鸡","陕西",[107.14,34.37],"±500",3.0,"2009",534,"电源绑定型","四川水电送西北;丰枯双向"),
 ("高岭直流","高岭背靠背直流","高岭(绥中)","辽宁",[120.34,40.33],"华北(背靠背)","北京",[116.40,39.90],"±500",3.0,"2008",0,"联网型","东北—华北背靠背联网"),
 ("三峡直流","三峡外送直流(三常/三沪/三广)","宜昌(三峡)","湖北",[111.29,30.69],"常州","江苏",[119.97,31.78],"±500",3.0,"2003-2012",900,"电源绑定型","3回各≈3GW,以三常示意;三峡水电送华东/广东"),
 ("渝鄂直流","渝鄂背靠背直流","重庆","重庆",[106.55,29.56],"恩施","湖北",[109.49,30.27],"±500",2.5,"2019",0,"联网型","川渝—湖北背靠背联网"),
 ("柴拉直流","柴达木—拉萨±400kV直流","格尔木","青海",[94.90,36.40],"拉萨","西藏",[91.11,29.66],"±400",0.6,"2011",1038,"电源绑定型","青藏联网;西藏电力补给"),
 ("川藏断面","川藏联网500kV交流通道","甘孜","四川",[100.00,30.05],"昌都","西藏",[97.17,31.14],"500kV AC",1.5,"2014",1500,"联网型","交流断面,非直流;川西藏区联网"),
 ("云霄直流","闽粤联网±100kV直流","云霄(漳州)","福建",[117.34,23.96],"梅州","广东",[116.12,24.29],"±100",2.0,"2022-09",303,"制度创新型","国网—南网跨经营区异步互联;2026-06起输电权市场化交易试点"),
 ("宜华直流","宜都—华新±500kV直流","宜都","湖北",[111.45,30.39],"华新(青浦)","上海",[121.12,31.15],"±500",3.0,"2006-11",1049,"电源绑定型","三峡水电送上海;历史线损≈7.5%"),
 ("林枫直流","团林—枫泾±500kV直流","团林(荆门)","湖北",[112.20,30.90],"枫泾(金山)","上海",[121.01,30.89],"±500",3.0,"2011",978,"电源绑定型","三峡水电送上海"),
 ("坤渝直流","哈密—重庆±800kV特高压直流","巴里坤(哈密)","新疆",[93.00,43.60],"渝北","重庆",[106.63,29.72],"±800",8.0,"2025-06-10",2260,"大基地型","沙戈荒基地送重庆;配套1420万kW(新能源>70%)"),
 ("宝合直流","陕北—安徽±800kV特高压直流(陕电入皖)","宝塔山(延安)","陕西",[109.49,36.60],"合州(合肥)","安徽",[117.28,32.00],"±800",8.0,"2026-06-30",1055,"大基地型","十五五首个投运特高压;已开展陕西→江苏绿电交易"),
]

_KEYS = ("name","formal","send","send_prov","sc","recv","recv_prov","rc","kv","gw","commissioned","km","category","note")
CHANNELS: list[dict] = [dict(zip(_KEYS, row)) for row in _CHANNELS_RAW]

def get_channels() -> list[dict]:
    return [dict(c) for c in CHANNELS]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector/test_registry.py -q`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add services/interconnector/ tests/interconnector/test_registry.py
git commit -m "Add interconnector channel registry module (26 reviewed channels)"
```

---

### Task 3: China GeoJSON asset

**Files:**
- Create: `assets/geo/china_provinces.json`
- Test: `tests/interconnector/test_geo_asset.py`

**Interfaces:**
- Produces: repo-local GeoJSON used by `topology.py` (Task 11) and the tab (Task 12) via `Path("assets/geo/china_provinces.json")`.

- [ ] **Step 1: Write the failing test**

```python
# tests/interconnector/test_geo_asset.py
import json
from pathlib import Path

def test_china_geojson_loads_with_provinces_and_centers():
    p = Path("assets/geo/china_provinces.json")
    g = json.loads(p.read_text())
    names = {f["properties"]["name"] for f in g["features"]}
    assert len(g["features"]) >= 34
    assert {"江苏省","内蒙古自治区","新疆维吾尔自治区","上海市"} <= names
    located = sum(1 for f in g["features"]
                  if f["properties"].get("centroid") or f["properties"].get("center"))
    assert located >= 34
```

- [ ] **Step 2: Run test to verify it fails** — FAIL (file missing)

- [ ] **Step 3: Add the asset**

```bash
mkdir -p assets/geo
cp /tmp/china_geo.json assets/geo/china_provinces.json   # DataV 100000_full.json, 0.6MB
```

- [ ] **Step 4: Run test** — 1 passed

- [ ] **Step 5: Commit** `git add assets/geo tests/interconnector/test_geo_asset.py && git commit -m "Add China provinces GeoJSON asset for interconnector map"`

---

### Task 4: Ingest — DDL, 华东 parser, trades loader

**Files:**
- Create: `services/interconnector/ingest.py`
- Test: `tests/interconnector/test_ingest.py`

**Interfaces:**
- Produces:
  - `ingest.DDL: list[str]`, `ingest.ensure_tables(conn) -> None`
  - `ingest.file_fingerprint(name: str, size: int) -> str` → `"<name>:<size>"`
  - `ingest.normalize_month(raw: str, year: int) -> tuple[date, date]`
  - `ingest.anchor_for(raw: str) -> str`
  - `ingest.parse_huadong(fileobj, year: int) -> list[dict]` — row dicts: `{target_month_raw, period_type, month_start, month_end, recv_province, send_raw, send_anchor, channel_1, channel_2, channel_3, vol_pre_mwh, vol_post_mwh, send_price, land_price, jingrong_vol_mwh, jingrong_price, channel_fee}`
  - `ingest.replace_trades(conn, rows: list[dict], source_file: str) -> int` (rows written)
- Consumes: nothing from earlier tasks.

- [ ] **Step 1: Write the failing tests**

```python
# tests/interconnector/test_ingest.py
from datetime import date
import pandas as pd
import pytest
from services.interconnector import ingest

@pytest.mark.parametrize("raw,year,exp", [
    ("1月", 2026, (date(2026,1,1), date(2026,1,31))),
    ("6月 ", 2026, (date(2026,6,1), date(2026,6,30))),      # trailing space
    ("3-12月", 2026, (date(2026,3,1), date(2026,12,31))),
    ("5-8月", 2026, (date(2026,5,1), date(2026,8,31))),
    ("9-10月", 2026, (date(2026,9,1), date(2026,10,31))),
    ("1-12月", 2026, (date(2026,1,1), date(2026,12,31))),
    ("2026", 2026, (date(2026,1,1), date(2026,12,31))),
])
def test_normalize_month(raw, year, exp):
    assert ingest.normalize_month(raw, year) == exp

@pytest.mark.parametrize("raw,anchor", [
    ("蒙东&黑龙江&吉林", "蒙东"), ("锡盟", "锡盟"), ("黑龙江/吉林/辽宁/蒙东", "黑龙江"),
    ("华北/冀北/山西/蒙西", "山西"), ("坤渝配套新能源", "重庆"), ("陕武配套新能源", "陕西"),
    ("南方区域", "云南"), ("黑吉辽", "黑龙江"), ("华北锡盟二期冀北河北山西蒙西", "锡盟"),
])
def test_anchor_for(raw, anchor):
    assert ingest.anchor_for(raw) == anchor

def _fixture_xlsx(tmp_path, rows):
    f = tmp_path / "hd.xlsx"
    pd.DataFrame(rows).to_excel(f, index=False)
    return f

def test_parse_huadong_cleans_channels_and_anchors(tmp_path):
    rows = [{"标的月份":"1月","标的期间类型":"月度","华东省份-XX":"江苏","对端送出省份":"蒙东",
             "通道1":"\n鲁固直流","通道2":"雁淮直流","通道3":None,
             "成交电量（校核前）MWh":36443.61,"成交电量（校核后）MWh":36443.61,
             "上网侧均价 元/MWh":124,"落地侧均价 元/MWh":275,
             "景融成交电量 MWh":14410.11,"景融成交均价 元/MWh":275.42,"通道费":151}]
    out = ingest.parse_huadong(_fixture_xlsx(tmp_path, rows), year=2026)
    assert len(out) == 1
    r = out[0]
    assert r["channel_1"] == "鲁固直流" and r["channel_2"] == "雁淮直流" and r["channel_3"] is None
    assert r["send_anchor"] == "蒙东" and r["recv_province"] == "江苏"
    assert r["month_start"] == date(2026,1,1) and r["month_end"] == date(2026,1,31)
    assert r["vol_post_mwh"] == pytest.approx(36443.61)
    assert r["jingrong_price"] == pytest.approx(275.42)

def test_parse_huadong_annual_period_covers_year(tmp_path):
    rows = [{"标的月份":"2026","标的期间类型":"年度","华东省份-XX":"上海","对端送出省份":"四川",
             "通道1":"复奉直流","通道2":None,"通道3":None,
             "成交电量（校核前）MWh":100,"成交电量（校核后）MWh":100,
             "上网侧均价 元/MWh":300,"落地侧均价 元/MWh":400,
             "景融成交电量 MWh":None,"景融成交均价 元/MWh":None,"通道费":80}]
    out = ingest.parse_huadong(_fixture_xlsx(tmp_path, rows), year=2026)
    assert out[0]["month_start"] == date(2026,1,1) and out[0]["month_end"] == date(2026,12,31)

def test_file_fingerprint_format():
    assert ingest.file_fingerprint("华东跨省数据汇总.xlsx", 44092) == "华东跨省数据汇总.xlsx:44092"

class _Cur:
    def __init__(self): self.calls = []
    def execute(self, sql, params=None): self.calls.append((sql, params))
    def executemany(self, sql, seq): self.calls.append((sql, list(seq)))
class _Conn:
    def __init__(self): self.cur = _Cur(); self.committed = 0
    def cursor(self): return self.cur
    def commit(self): self.committed += 1

def test_replace_trades_deletes_then_inserts():
    conn = _Conn()
    rows = [{"target_month_raw":"1月","period_type":"月度","month_start":date(2026,1,1),
             "month_end":date(2026,1,31),"recv_province":"江苏","send_raw":"蒙东","send_anchor":"蒙东",
             "channel_1":"鲁固直流","channel_2":None,"channel_3":None,
             "vol_pre_mwh":1.0,"vol_post_mwh":1.0,"send_price":1,"land_price":2,
             "jingrong_vol_mwh":None,"jingrong_price":None,"channel_fee":1}]
    n = ingest.replace_trades(conn, rows, "src.xlsx:123")
    assert n == 1 and conn.committed == 1
    assert "DELETE FROM staging.interconnector_trades" in conn.cur.calls[0][0]
    assert "INSERT INTO staging.interconnector_trades" in conn.cur.calls[1][0]
    assert conn.cur.calls[1][1][0]["source_file"] == "src.xlsx:123"
```

- [ ] **Step 2: Run tests to verify they fail** — FAIL (`services.interconnector.ingest` missing)

- [ ] **Step 3: Implement ingest.py (DDL + 华东 half)**

```python
# services/interconnector/ingest.py
"""Excel → staging loaders for the interconnector tab. Upload path: in-app, no S3."""
from __future__ import annotations

import calendar
import re
from datetime import date

import pandas as pd

DDL = [
    """CREATE TABLE IF NOT EXISTS staging.interconnector_trades (
        id SERIAL PRIMARY KEY,
        target_month_raw TEXT, period_type TEXT,
        month_start DATE, month_end DATE,
        recv_province TEXT, send_raw TEXT, send_anchor TEXT,
        channel_1 TEXT, channel_2 TEXT, channel_3 TEXT,
        vol_pre_mwh NUMERIC, vol_post_mwh NUMERIC,
        send_price NUMERIC, land_price NUMERIC,
        jingrong_vol_mwh NUMERIC, jingrong_price NUMERIC,
        channel_fee NUMERIC,
        source_file TEXT, uploaded_at TIMESTAMPTZ DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS staging.interconnector_channels (
        name TEXT PRIMARY KEY, formal_name TEXT,
        send_station TEXT, send_prov TEXT, send_lon NUMERIC, send_lat NUMERIC,
        recv_station TEXT, recv_prov TEXT, recv_lon NUMERIC, recv_lat NUMERIC,
        kv TEXT, gw NUMERIC, commissioned TEXT, km NUMERIC,
        category TEXT, note TEXT, updated_at TIMESTAMPTZ DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS staging.interconnector_mlt_snapshot (
        id SERIAL PRIMARY KEY,
        snapshot_label TEXT, sheet TEXT,
        send_region TEXT, send_prov TEXT, recv_prov TEXT,
        trade_type TEXT, channel TEXT, is_subtotal BOOLEAN DEFAULT FALSE,
        volume_100m_kwh NUMERIC, landing_price NUMERIC,
        uploaded_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(snapshot_label, sheet, send_prov, recv_prov, trade_type, channel)
    )""",
    """CREATE TABLE IF NOT EXISTS staging.interconnector_mlt_pct_override (
        province TEXT PRIMARY KEY, pct NUMERIC, updated_at TIMESTAMPTZ DEFAULT NOW()
    )""",
]

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
    conn.commit()

def file_fingerprint(name: str, size: int) -> str:
    return f"{name}:{size}"

# ── 华东跨省数据汇总 ──────────────────────────────────────────────────────────
def normalize_month(raw: str, year: int) -> tuple[date, date]:
    s = str(raw).strip()
    if re.fullmatch(r"\d{4}", s):
        return date(year, 1, 1), date(year, 12, 31)
    m = re.fullmatch(r"(\d{1,2})-(\d{1,2})月", s)
    if m:
        m1, m2 = int(m.group(1)), int(m.group(2))
        return date(year, m1, 1), date(year, m2, calendar.monthrange(year, m2)[1])
    m = re.fullmatch(r"(\d{1,2})月", s)
    if m:
        m1 = int(m.group(1))
        return date(year, m1, 1), date(year, m1, calendar.monthrange(year, m1)[1])
    raise ValueError(f"unrecognized 标的月份: {raw!r}")

_ANCHOR_TOKENS = ["锡盟","蒙东","蒙西","黑吉辽","黑龙江","吉林","辽宁","山西","河北",
                  "甘肃","青海","宁夏","陕西","新疆","西藏","四川","云南","河南","湖北"]
def anchor_for(raw: str) -> str:
    s = str(raw).strip()
    first = re.split(r"[&/]", s)[0].strip()
    for tok in _ANCHOR_TOKENS:
        if first.startswith(tok):
            return {"黑吉辽": "黑龙江"}.get(tok, tok)
    if "锡盟" in s: return "锡盟"
    if "坤渝" in s: return "重庆"
    if "陕武" in s: return "陕西"
    if "南方" in s: return "云南"
    if "华北" in s: return "山西"
    raise ValueError(f"unmappable 对端送出省份: {raw!r}")

def _clean_ch(v) -> str | None:
    if not isinstance(v, str):
        return None
    v = v.replace("\n", "").strip()
    return v if v and v.lower() != "nan" else None

def _num(v):
    return float(v) if pd.notna(v) else None

_HUADONG_COLS = {"标的月份":"target_month_raw","标的期间类型":"period_type","华东省份-XX":"recv_province",
    "对端送出省份":"send_raw","成交电量（校核前）MWh":"vol_pre_mwh","成交电量（校核后）MWh":"vol_post_mwh",
    "上网侧均价 元/MWh":"send_price","落地侧均价 元/MWh":"land_price",
    "景融成交电量 MWh":"jingrong_vol_mwh","景融成交均价 元/MWh":"jingrong_price","通道费":"channel_fee"}

def parse_huadong(fileobj, year: int) -> list[dict]:
    df = pd.read_excel(fileobj)
    df.columns = [str(c).strip() for c in df.columns]
    out = []
    for _, r in df.iterrows():
        row = {dst: r[src] for src, dst in _HUADONG_COLS.items()}
        ptype = str(row["period_type"]).strip()
        if ptype == "年度":
            ms, me = date(year, 1, 1), date(year, 12, 31)
        else:
            ms, me = normalize_month(row["target_month_raw"], year)
        row.update(month_start=ms, month_end=me, period_type=ptype,
                   recv_province=str(row["recv_province"]).strip(),
                   send_raw=str(row["send_raw"]).strip(),
                   send_anchor=anchor_for(row["send_raw"]),
                   channel_1=_clean_ch(r.get("通道1")), channel_2=_clean_ch(r.get("通道2")),
                   channel_3=_clean_ch(r.get("通道3")))
        for k in ("vol_pre_mwh","vol_post_mwh","send_price","land_price",
                  "jingrong_vol_mwh","jingrong_price","channel_fee"):
            row[k] = _num(row[k])
        out.append(row)
    return out

_TRADES_INSERT = """INSERT INTO staging.interconnector_trades (
    target_month_raw, period_type, month_start, month_end,
    recv_province, send_raw, send_anchor, channel_1, channel_2, channel_3,
    vol_pre_mwh, vol_post_mwh, send_price, land_price,
    jingrong_vol_mwh, jingrong_price, channel_fee, source_file
) VALUES (%(target_month_raw)s, %(period_type)s, %(month_start)s, %(month_end)s,
    %(recv_province)s, %(send_raw)s, %(send_anchor)s, %(channel_1)s, %(channel_2)s, %(channel_3)s,
    %(vol_pre_mwh)s, %(vol_post_mwh)s, %(send_price)s, %(land_price)s,
    %(jingrong_vol_mwh)s, %(jingrong_price)s, %(channel_fee)s, %(source_file)s)"""

def replace_trades(conn, rows: list[dict], source_file: str) -> int:
    """Full replace: the 华东 file is a cumulative snapshot."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM staging.interconnector_trades")
        cur.executemany(_TRADES_INSERT, [dict(r, source_file=source_file) for r in rows])
    conn.commit()
    return len(rows)
```

- [ ] **Step 4: Run tests** — all pass

- [ ] **Step 5: Commit** `git add services/interconnector/ingest.py tests/interconnector/test_ingest.py && git commit -m "Add interconnector trades ingest: DDL, 华东 parser, full-replace loader"`

---

### Task 5: Ingest — 跨区组织 snapshot parser

**Files:**
- Modify: `services/interconnector/ingest.py` (append)
- Test: `tests/interconnector/test_ingest.py` (append)

**Interfaces:**
- Produces: `ingest.parse_mlt_snapshot(fileobj, sheet: str) -> list[dict]` — rows `{sheet, send_region, send_prov, recv_prov, trade_type, channel, is_subtotal, volume_100m_kwh, landing_price}`; `ingest.replace_snapshot(conn, label: str, rows: list[dict]) -> int`.

- [ ] **Step 1: Write the failing tests**

```python
def _snapshot_fixture(tmp_path):
    f = tmp_path / "kj.xlsx"
    rows = [
        {"送出区域":"东北","送出省份":"黑龙江","受入省份":"安徽","交易类型2":"其他市场化交易","输电通道":"锡泰直流","落地均价（亿千瓦时）":0.02,"落地均价":322.72},
        {"送出区域":"东北","送出省份":"黑龙江","受入省份":"安徽","交易类型2":"其他市场化交易 汇总","输电通道":"汇总","落地均价（亿千瓦时）":2.03,"落地均价":334.79},
        {"送出区域":"东北","送出省份":"黑龙江 汇总","受入省份":"安徽 汇总","交易类型2":"省间绿电交易（市场化交易）","输电通道":"雁淮直流","落地均价（亿千瓦时）":9.07,"落地均价":376.58},
    ]
    with pd.ExcelWriter(f) as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="中长期", index=False)
    return f

def test_parse_mlt_snapshot_flags_subtotals(tmp_path):
    out = ingest.parse_mlt_snapshot(_snapshot_fixture(tmp_path), sheet="中长期")
    assert len(out) == 3
    assert out[0]["is_subtotal"] is False
    assert out[1]["is_subtotal"] is True            # 交易类型 汇总
    assert out[2]["is_subtotal"] is True            # 省份 汇总
    assert out[0]["volume_100m_kwh"] == pytest.approx(0.02)
    assert out[0]["landing_price"] == pytest.approx(322.72)
    assert out[0]["trade_type"] == "其他市场化交易"

def test_replace_snapshot_deletes_label_then_inserts():
    conn = _Conn()
    rows = [{"sheet":"中长期","send_region":"东北","send_prov":"黑龙江","recv_prov":"安徽",
             "trade_type":"其他市场化交易","channel":"锡泰直流","is_subtotal":False,
             "volume_100m_kwh":0.02,"landing_price":322.72}]
    n = ingest.replace_snapshot(conn, "2025-full", rows)
    assert n == 1
    assert "DELETE FROM staging.interconnector_mlt_snapshot WHERE snapshot_label" in conn.cur.calls[0][0]
    assert conn.cur.calls[0][1] == ("2025-full",)
    assert conn.cur.calls[1][1][0]["snapshot_label"] == "2025-full"
```

- [ ] **Step 2: Run tests to verify they fail** — FAIL (functions missing)

- [ ] **Step 3: Implement**

```python
# ── 跨区组织交易情况 snapshot ─────────────────────────────────────────────────
def _is_subtotal(trade_type, channel, send_prov, recv_prov) -> bool:
    return any("汇总" in str(x) for x in (trade_type, channel, send_prov, recv_prov))

def parse_mlt_snapshot(fileobj, sheet: str) -> list[dict]:
    df = pd.read_excel(fileobj, sheet_name=sheet)
    df.columns = [str(c).strip() for c in df.columns]
    out = []
    for _, r in df.iterrows():
        send_prov = str(r["送出省份"]).strip()
        recv_prov = str(r["受入省份"]).strip()
        if send_prov.lower() == "nan" or recv_prov.lower() == "nan":
            continue
        tt = str(r["交易类型2"]).strip()
        ch = _clean_ch(r["输电通道"])
        out.append(dict(sheet=sheet,
            send_region=str(r["送出区域"]).strip(), send_prov=send_prov, recv_prov=recv_prov,
            trade_type=tt.replace(" 汇总", ""), channel=ch,
            is_subtotal=_is_subtotal(tt, ch, send_prov, recv_prov),
            volume_100m_kwh=_num(r["落地均价（亿千瓦时）"]), landing_price=_num(r["落地均价"])))
    return out

_SNAP_INSERT = """INSERT INTO staging.interconnector_mlt_snapshot (
    snapshot_label, sheet, send_region, send_prov, recv_prov,
    trade_type, channel, is_subtotal, volume_100m_kwh, landing_price
) VALUES (%(snapshot_label)s, %(sheet)s, %(send_region)s, %(send_prov)s, %(recv_prov)s,
    %(trade_type)s, %(channel)s, %(is_subtotal)s, %(volume_100m_kwh)s, %(landing_price)s)"""

def replace_snapshot(conn, label: str, rows: list[dict]) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM staging.interconnector_mlt_snapshot WHERE snapshot_label = %s", (label,))
        cur.executemany(_SNAP_INSERT, [dict(r, snapshot_label=label) for r in rows])
    conn.commit()
    return len(rows)
```

- [ ] **Step 4: Run tests** — all pass

- [ ] **Step 5: Commit** `git add services/interconnector/ingest.py tests/interconnector/test_ingest.py && git commit -m "Add 跨区组织 snapshot parser with subtotal flagging"`

---

### Task 6: Ingest — channels upsert + MLT% overrides

**Files:**
- Modify: `services/interconnector/ingest.py` (append)
- Test: `tests/interconnector/test_ingest.py` (append)

**Interfaces:**
- Produces: `ingest.upsert_channels(conn, channels: list[dict]) -> int`; `ingest.set_pct_override(conn, province: str, pct: float) -> None`; `ingest.get_pct_overrides(conn) -> dict[str, float]`.
- Consumes: `registry.CHANNELS` shape (Task 2).

- [ ] **Step 1: Write the failing tests**

```python
def test_upsert_channels_writes_all_fields():
    conn = _Conn()
    from services.interconnector.registry import CHANNELS
    n = ingest.upsert_channels(conn, CHANNELS[:1])
    assert n == 1
    sql, params = conn.cur.calls[0]
    assert "INSERT INTO staging.interconnector_channels" in sql
    assert "ON CONFLICT (name) DO UPDATE" in sql
    assert params["name"] == "锡泰直流" and params["send_lon"] == 116.07

def test_pct_override_roundtrip_sql():
    conn = _Conn()
    ingest.set_pct_override(conn, "蒙西", 85.0)
    sql, params = conn.cur.calls[0]
    assert "interconnector_mlt_pct_override" in sql and params == ("蒙西", 85.0)
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement**

```python
# ── channels + MLT% overrides ─────────────────────────────────────────────────
_CHANNEL_UPSERT = """INSERT INTO staging.interconnector_channels (
    name, formal_name, send_station, send_prov, send_lon, send_lat,
    recv_station, recv_prov, recv_lon, recv_lat, kv, gw, commissioned, km, category, note
) VALUES (%(name)s, %(formal)s, %(send)s, %(send_prov)s, %(send_lon)s, %(send_lat)s,
    %(recv)s, %(recv_prov)s, %(recv_lon)s, %(recv_lat)s, %(kv)s, %(gw)s, %(commissioned)s, %(km)s,
    %(category)s, %(note)s)
ON CONFLICT (name) DO UPDATE SET
    formal_name=EXCLUDED.formal_name, send_station=EXCLUDED.send_station,
    send_prov=EXCLUDED.send_prov, send_lon=EXCLUDED.send_lon, send_lat=EXCLUDED.send_lat,
    recv_station=EXCLUDED.recv_station, recv_prov=EXCLUDED.recv_prov,
    recv_lon=EXCLUDED.recv_lon, recv_lat=EXCLUDED.recv_lat, kv=EXCLUDED.kv, gw=EXCLUDED.gw,
    commissioned=EXCLUDED.commissioned, km=EXCLUDED.km, category=EXCLUDED.category,
    note=EXCLUDED.note, updated_at=NOW()"""

def upsert_channels(conn, channels: list[dict]) -> int:
    params = [dict(c, send_lon=c["sc"][0], send_lat=c["sc"][1],
                   recv_lon=c["rc"][0], recv_lat=c["rc"][1]) for c in channels]
    with conn.cursor() as cur:
        cur.executemany(_CHANNEL_UPSERT, params)
    conn.commit()
    return len(channels)

def set_pct_override(conn, province: str, pct: float) -> None:
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO staging.interconnector_mlt_pct_override (province, pct)
                       VALUES (%s, %s)
                       ON CONFLICT (province) DO UPDATE SET pct=EXCLUDED.pct, updated_at=NOW()""",
                    (province, pct))
    conn.commit()

def get_pct_overrides(conn) -> dict[str, float]:
    with conn.cursor() as cur:
        cur.execute("SELECT province, pct FROM staging.interconnector_mlt_pct_override")
        return {p: float(v) for p, v in cur.fetchall()}
```

- [ ] **Step 4: Run tests** — all pass

- [ ] **Step 5: Commit** `git add services/interconnector/ingest.py tests/interconnector/test_ingest.py && git commit -m "Add channel upsert and MLT% override helpers"`

---

### Task 7: Data — flow & channel aggregations

**Files:**
- Create: `services/interconnector/data.py`
- Test: `tests/interconnector/test_data.py`

**Interfaces:**
- Produces:
  - `data.aggregate_flows(trades: list[dict]) -> list[dict]` — per (send_anchor, recv_province): `{send, recv, vol_gwh, land, sendp, fee, trades, channels, send_raws, months}` (prototype v3 logic; VWAP = Σprice×vol/Σvol over rows with both price and vol)
  - `data.per_channel_agg(trades: list[dict]) -> dict[str, dict]` — `{channel: {vol_gwh, land, trades}}`; full volume attributed to every listed channel
  - `data.monthly_volume_by_recv(trades: list[dict]) -> pandas.DataFrame` — index month_start, columns recv provinces, values GWh
- Consumes: `parse_huadong` row shape (Task 4).

- [ ] **Step 1: Write the failing tests**

```python
# tests/interconnector/test_data.py
from datetime import date
import pytest
from services.interconnector import data

def _t(send="蒙东", recv="江苏", vol=100.0, land=300.0, sendp=150.0, fee=120.0,
       ch=("鲁固直流",), month=date(2026,1,1), raw="蒙东"):
    return dict(send_anchor=send, recv_province=recv, vol_post_mwh=vol, land_price=land,
                send_price=sendp, channel_fee=fee, channel_1=ch[0],
                channel_2=ch[1] if len(ch) > 1 else None, channel_3=None,
                month_start=month, send_raw=raw)

def test_aggregate_flows_vwap_and_volume():
    trades = [_t(vol=100, land=300), _t(vol=300, land=400), _t(recv="上海", vol=50, land=350)]
    flows = {(f["send"], f["recv"]): f for f in data.aggregate_flows(trades)}
    f = flows[("蒙东","江苏")]
    assert f["vol_gwh"] == pytest.approx(0.4)          # 400 MWh → 0.4 GWh
    assert f["land"] == 375                            # (300*100+400*300)/400
    assert f["trades"] == 2 and f["months"] == 1
    assert flows[("蒙东","上海")]["vol_gwh"] == pytest.approx(0.05)

def test_aggregate_flows_skips_nan_price_in_vwap():
    trades = [_t(vol=100, land=300), _t(vol=100, land=None)]
    f = data.aggregate_flows(trades)[0]
    assert f["land"] == 300 and f["vol_gwh"] == pytest.approx(0.2)

def test_per_channel_agg_full_volume_each_channel():
    trades = [_t(vol=100, land=300, ch=("高岭直流","锡泰直流"))]
    agg = data.per_channel_agg(trades)
    assert agg["高岭直流"]["vol_gwh"] == pytest.approx(0.1)
    assert agg["锡泰直流"]["vol_gwh"] == pytest.approx(0.1)   # full vol on each (series path)
    assert agg["锡泰直流"]["trades"] == 1

def test_monthly_volume_by_recv():
    trades = [_t(recv="江苏", vol=100, month=date(2026,1,1)),
              _t(recv="江苏", vol=200, month=date(2026,2,1)),
              _t(recv="上海", vol=50, month=date(2026,1,1))]
    pv = data.monthly_volume_by_recv(trades)
    assert pv.loc[date(2026,1,1), "江苏"] == pytest.approx(0.1)
    assert pv.loc[date(2026,2,1), "江苏"] == pytest.approx(0.2)
    assert pv.loc[date(2026,1,1), "上海"] == pytest.approx(0.05)
```

- [ ] **Step 2: Run tests to verify they fail** — FAIL (`data` missing)

- [ ] **Step 3: Implement data.py (aggregation half)**

```python
# services/interconnector/data.py
"""Aggregations for the interconnector tab. Pure functions over row dicts — no DB here."""
from __future__ import annotations

import pandas as pd

def aggregate_flows(trades: list[dict]) -> list[dict]:
    agg = {}
    for r in trades:
        k = (r["send_anchor"], r["recv_province"])
        a = agg.setdefault(k, dict(send=r["send_anchor"], recv=r["recv_province"],
                                   vol=0.0, lv=0.0, sv=0.0, fv=0.0, n=0,
                                   channels=set(), send_raws=set(), months=set()))
        vol = r["vol_post_mwh"] or 0.0
        a["vol"] += vol
        if r["land_price"] is not None and vol: a["lv"] += r["land_price"] * vol
        if r["send_price"] is not None and vol: a["sv"] += r["send_price"] * vol
        if r["channel_fee"] is not None and vol: a["fv"] += r["channel_fee"] * vol
        a["n"] += 1
        a["channels"].update(c for c in (r["channel_1"], r["channel_2"], r["channel_3"]) if c)
        a["send_raws"].add(r["send_raw"]); a["months"].add(r["month_start"])
    out = []
    for a in agg.values():
        v = a["vol"] or 1.0
        out.append(dict(send=a["send"], recv=a["recv"], vol_gwh=round(a["vol"]/1000, 1),
            land=round(a["lv"]/v) if a["lv"] else None, sendp=round(a["sv"]/v) if a["sv"] else None,
            fee=round(a["fv"]/v) if a["fv"] else None, trades=a["n"],
            channels=sorted(a["channels"]), send_raws=sorted(a["send_raws"]), months=len(a["months"])))
    return sorted(out, key=lambda x: -x["vol_gwh"])

def per_channel_agg(trades: list[dict]) -> dict[str, dict]:
    """Full trade volume attributed to EVERY listed channel (series path wheeling)."""
    agg: dict[str, dict] = {}
    for r in trades:
        vol = r["vol_post_mwh"] or 0.0
        for ch in {c for c in (r["channel_1"], r["channel_2"], r["channel_3"]) if c}:
            a = agg.setdefault(ch, dict(vol=0.0, lv=0.0, n=0))
            a["vol"] += vol; a["n"] += 1
            if r["land_price"] is not None and vol: a["lv"] += r["land_price"] * vol
    return {ch: dict(vol_gwh=round(a["vol"]/1000, 1),
                     land=round(a["lv"]/a["vol"]) if a["lv"] and a["vol"] else None,
                     trades=a["n"]) for ch, a in agg.items()}

def monthly_volume_by_recv(trades: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame([{"m": r["month_start"], "recv": r["recv_province"],
                        "gwh": (r["vol_post_mwh"] or 0)/1000} for r in trades])
    if df.empty:
        return pd.DataFrame()
    return df.pivot_table(index="m", columns="recv", values="gwh", aggfunc="sum").fillna(0)
```

- [ ] **Step 4: Run tests** — all pass

- [ ] **Step 5: Commit** `git add services/interconnector/data.py tests/interconnector/test_data.py && git commit -m "Add flow and per-channel aggregations"`

---

### Task 8: Data — A1 balance-of-year

**Files:**
- Modify: `services/interconnector/data.py` (append)
- Test: `tests/interconnector/test_data.py` (append)

**Interfaces:**
- Produces: `data.balance_of_year(channels: list[dict], channel_agg: dict, as_of: date) -> list[dict]` — per channel: `{name, gw, capability_gwh, traded_gwh, utilization_pct, remaining_capability_gwh, remaining_hours}` where `capability_gwh = gw*8760`, `utilization_pct = 100*traded/capability`, `remaining_capability_gwh = gw*remaining_hours` (hours from `as_of` to Dec 31 24:00), `traded_gwh` from `channel_agg` (0 if absent).
- Consumes: `registry.CHANNELS` (Task 2), `per_channel_agg` (Task 7).

- [ ] **Step 1: Write the failing tests**

```python
def test_balance_of_year_math():
    channels = [dict(name="锡泰直流", gw=10.0), dict(name="云霄直流", gw=2.0)]
    agg = {"锡泰直流": {"vol_gwh": 6139.0, "land": 346, "trades": 93}}
    out = {c["name"]: c for c in data.balance_of_year(channels, agg, date(2026,10,1))}
    xt = out["锡泰直流"]
    assert xt["capability_gwh"] == pytest.approx(10*8760)
    assert xt["traded_gwh"] == pytest.approx(6139.0)
    assert xt["utilization_pct"] == pytest.approx(100*6139/(10*8760), rel=1e-3)
    # remaining hours Oct 1 → Dec 31 = (31+30+31)*24 = 2208
    assert xt["remaining_hours"] == 2208
    assert xt["remaining_capability_gwh"] == pytest.approx(10*2208)
    yx = out["云霄直流"]
    assert yx["traded_gwh"] == 0 and yx["utilization_pct"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement**

```python
def balance_of_year(channels: list[dict], channel_agg: dict, as_of: date) -> list[dict]:
    """A1: capability vs YTD traded (traded-volume proxy — NOT physical flow)."""
    year_end = date(as_of.year, 12, 31)
    remaining_days = (year_end - as_of).days + 1
    remaining_hours = remaining_days * 24
    out = []
    for c in channels:
        cap = c["gw"] * 8760
        traded = channel_agg.get(c["name"], {}).get("vol_gwh", 0.0)
        out.append(dict(name=c["name"], gw=c["gw"], capability_gwh=round(cap, 1),
            traded_gwh=traded,
            utilization_pct=round(100*traded/cap, 1) if cap else 0.0,
            remaining_hours=remaining_hours,
            remaining_capability_gwh=round(c["gw"]*remaining_hours, 1)))
    return sorted(out, key=lambda x: -x["gw"])
```

- [ ] **Step 4: Run tests** — pass

- [ ] **Step 5: Commit** `git add services/interconnector/data.py tests/interconnector/test_data.py && git commit -m "Add A1 balance-of-year computation"`

---

### Task 9: Data — A2 MLT obligation & speculation

**Files:**
- Modify: `services/interconnector/data.py` (append)
- Test: `tests/interconnector/test_data.py` (append)

**Interfaces:**
- Produces:
  - `data.resolve_mlt_pct(province: str, rules: dict[str, float], overrides: dict[str, float], default: float = 80.0) -> tuple[float, str]` — returns `(pct, source)`; source ∈ `{"override","rule","default"}`
  - `data.year_ago_estimate(monthly: dict[str, float], month: date) -> float | None` — `monthly` keys are first-of-month dates; returns value at same month one year earlier, else None
  - `data.recycle_gap(required_gwh: float, within_gwh: float, exported_gwh: float, mlt_price: float | None, spot_price: float | None) -> float | None` — `max(0, required-within-exported) * abs(mlt-spot)`; None if either price missing
- Consumes: nothing new.

- [ ] **Step 1: Write the failing tests**

```python
def test_resolve_mlt_pct_priority():
    assert data.resolve_mlt_pct("蒙西", {"蒙西": 85.0}, {"蒙西": 90.0}) == (90.0, "override")
    assert data.resolve_mlt_pct("蒙西", {"蒙西": 85.0}, {}) == (85.0, "rule")
    assert data.resolve_mlt_pct("青海", {}, {}) == (80.0, "default")

def test_year_ago_estimate():
    monthly = {date(2025,7,1): 12000.0, date(2026,7,1): 13500.0}
    assert data.year_ago_estimate(monthly, date(2026,7,1)) == 12000.0
    assert data.year_ago_estimate(monthly, date(2026,3,1)) is None

def test_recycle_gap_math():
    assert data.recycle_gap(1000, 600, 200, 300, 250) == pytest.approx(200*50)
    assert data.recycle_gap(1000, 900, 200, 300, 250) == 0        # no shortfall → 0
    assert data.recycle_gap(1000, 0, 0, None, 250) is None        # missing price → None
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement**

```python
def resolve_mlt_pct(province: str, rules: dict[str, float], overrides: dict[str, float],
                    default: float = 80.0) -> tuple[float, str]:
    if province in overrides: return overrides[province], "override"
    if province in rules: return rules[province], "rule"
    return default, "default"

def year_ago_estimate(monthly: dict, month: date) -> float | None:
    """Forward-month estimate = year-ago same-month actual (历史同期). Never forecast."""
    try:
        prev = date(month.year - 1, month.month, 1)
    except ValueError:
        return None
    return monthly.get(prev)

def recycle_gap(required_gwh: float, within_gwh: float, exported_gwh: float,
                mlt_price: float | None, spot_price: float | None) -> float | None:
    if mlt_price is None or spot_price is None:
        return None
    shortfall = max(0.0, required_gwh - within_gwh - exported_gwh)
    return shortfall * abs(mlt_price - spot_price)
```

- [ ] **Step 4: Run tests** — pass

- [ ] **Step 5: Commit** `git add services/interconnector/data.py tests/interconnector/test_data.py && git commit -m "Add A2 MLT obligation and recycle-gap computations"`

---

### Task 10: Data — A3 backtest + A4 pattern comparison

**Files:**
- Modify: `services/interconnector/data.py` (append)
- Test: `tests/interconnector/test_data.py` (append)

**Interfaces:**
- Produces:
  - `data.backtest_rows(pairs: list[tuple[str,str]], delivery_months: list[date], month_ahead: dict, spot_monthly: dict, trades: list[dict]) -> list[dict]` — per (send,recv,M): `{send, recv, month, send_ahead, send_spot, recv_ahead, recv_spot, landing, premium_over_recv_spot, realized_spread}`; `month_ahead[(prov, M)]` = price from report M−1 (caller builds; this function only aligns); landing = VWAP of trades covering M for the pair (trade covers M if month_start ≤ M ≤ month_end); premium = landing − recv_spot; realized_spread = recv_spot − send_spot − fee (fee = pair VWAP channel_fee); rows with any critical value missing keep None fields, never interpolated
  - `data.green_premium(snapshot_rows: list[dict]) -> list[dict]` — per (send_prov, recv_prov, channel): `{send, recv, channel, green_price, other_price, premium}` from snapshot where same key has both 省间绿电交易（市场化交易） and 其他市场化交易
- Consumes: trade row shape (Task 4), snapshot row shape (Task 5).

- [ ] **Step 1: Write the failing tests**

```python
def test_backtest_rows_alignment_and_none_policy():
    trades = [_t(send="山西", recv="江苏", vol=100, land=380, fee=70,
                 month=date(2026,7,1))]
    trades[0]["month_end"] = date(2026,7,31)
    month_ahead = {("山西", date(2026,7,1)): 330.0, ("江苏", date(2026,7,1)): 400.0}
    spot = {("山西", date(2026,7,1)): 310.0, ("江苏", date(2026,7,1)): 420.0}
    rows = data.backtest_rows([("山西","江苏")], [date(2026,7,1)], month_ahead, spot, trades)
    r = rows[0]
    assert r["landing"] == 380 and r["premium_over_recv_spot"] == 380-420
    assert r["realized_spread"] == 420-310-70
    # month with no spot → None fields, row still present
    rows2 = data.backtest_rows([("山西","江苏")], [date(2026,8,1)], month_ahead, spot, trades)
    assert rows2[0]["recv_spot"] is None and rows2[0]["realized_spread"] is None

def test_green_premium():
    snap = [
        dict(send_prov="黑龙江", recv_prov="安徽", channel="雁淮直流",
             trade_type="省间绿电交易（市场化交易）", is_subtotal=False,
             volume_100m_kwh=9.07, landing_price=376.58),
        dict(send_prov="黑龙江", recv_prov="安徽", channel="雁淮直流",
             trade_type="其他市场化交易", is_subtotal=False,
             volume_100m_kwh=2.01, landing_price=334.89),
    ]
    out = data.green_premium(snap)
    assert out == [dict(send="黑龙江", recv="安徽", channel="雁淮直流",
                        green_price=376.58, other_price=334.89,
                        premium=pytest.approx(41.69))] or abs(out[0]["premium"] - 41.69) < 0.01
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement**

```python
def _pair_vwap(trades, send, recv, price_key):
    num = den = 0.0
    for r in trades:
        if r["send_anchor"] == send and r["recv_province"] == recv and r[price_key] is not None and r["vol_post_mwh"]:
            num += r[price_key] * r["vol_post_mwh"]; den += r["vol_post_mwh"]
    return num/den if den else None

def backtest_rows(pairs, delivery_months, month_ahead, spot_monthly, trades):
    out = []
    for send, recv in pairs:
        fee = _pair_vwap(trades, send, recv, "channel_fee")
        for m in delivery_months:
            covering = [r for r in trades if r["send_anchor"] == send
                        and r["recv_province"] == recv
                        and r["month_start"] <= m <= r["month_end"]]
            land = _pair_vwap(covering, send, recv, "land_price") if covering else None
            ss, rs = spot_monthly.get((send, m)), spot_monthly.get((recv, m))
            out.append(dict(send=send, recv=recv, month=m,
                send_ahead=month_ahead.get((send, m)), send_spot=ss,
                recv_ahead=month_ahead.get((recv, m)), recv_spot=rs,
                landing=land,
                premium_over_recv_spot=(land - rs) if (land is not None and rs is not None) else None,
                realized_spread=(rs - ss - (fee or 0)) if (ss is not None and rs is not None) else None))
    return out

def green_premium(snapshot_rows: list[dict]) -> list[dict]:
    by: dict[tuple, dict] = {}
    for r in snapshot_rows:
        if r["is_subtotal"] or r["landing_price"] is None:
            continue
        k = (r["send_prov"], r["recv_prov"], r["channel"])
        e = by.setdefault(k, {})
        if r["trade_type"] == "省间绿电交易（市场化交易）": e["green"] = r["landing_price"]
        elif r["trade_type"] == "其他市场化交易": e["other"] = r["landing_price"]
    return [dict(send=k[0], recv=k[1], channel=k[2], green_price=v["green"],
                 other_price=v["other"], premium=round(v["green"]-v["other"], 2))
            for k, v in by.items() if "green" in v and "other" in v]
```

- [ ] **Step 4: Run tests** — pass

- [ ] **Step 5: Commit** `git add services/interconnector/data.py tests/interconnector/test_data.py && git commit -m "Add A3 backtest alignment and A4 green-premium computations"`

---

### Task 11: Topology — echarts option builders

**Files:**
- Create: `services/interconnector/topology.py`
- Test: `tests/interconnector/test_topology.py`

**Interfaces:**
- Produces:
  - `topology.KV_ORDER: list[str]`, `topology.KV_COLORS: dict[str, str]` (light theme, validated ramp: `{"±100":"#f0f5fc","±400":"#dbe7f7","±500":"#a8c4ea","±660":"#6a99dd","±800":"#2563eb","±1100":"#0f2b6e","500kV AC":"#9aa6b5"}`)
  - `topology.RECV_COLORS: dict[str, str]` — `{"江苏":"#2563eb","上海":"#e11d48","浙江":"#0d9488","福建":"#d97706"}`
  - `topology.physical_options(channels: list[dict], channel_agg: dict, selected_prov: str | None = None) -> dict` — one `lines` series per kV class (legend-togglable), per-channel `lineStyle.width = 1 + 5.5*sqrt(gw/maxGw)`, opacity 0.85 linked / 0.07 dimmed when `selected_prov` set, `data[i].meta` = the channel dict + trade stats; plus a `stations` scatter series
  - `topology.flows_options(flows: list[dict], coords: dict[str, list[float]]) -> dict` — one `lines` series per receiving province, `width = 0.8 + 6.2*sqrt(vol/maxVol)`, `data[i].meta` = flow dict; plus `senders`/`receivers` scatter series
- Consumes: `registry.CHANNELS` (Task 2), `per_channel_agg`/`aggregate_flows` (Task 7).

- [ ] **Step 1: Write the failing tests**

```python
# tests/interconnector/test_topology.py
import pytest
from services.interconnector import topology

_CHANNELS = [
    dict(name="锡泰直流", send="锡林浩特", send_prov="内蒙古", sc=[116.07,43.94],
         recv="泰州", recv_prov="江苏", rc=[119.93,32.46], kv="±800", gw=10.0,
         category="大基地型", formal="f", commissioned="2017", km=1, note=""),
    dict(name="云霄直流", send="云霄", send_prov="福建", sc=[117.34,23.96],
         recv="梅州", recv_prov="广东", rc=[116.12,24.29], kv="±100", gw=2.0,
         category="制度创新型", formal="f", commissioned="2022", km=1, note=""),
]

def test_physical_options_series_per_kv_class_and_widths():
    opts = topology.physical_options(_CHANNELS, {}, None)
    line_series = [s for s in opts["series"] if s["type"] == "lines"]
    assert {s["name"] for s in line_series} == {"±800", "±100"}
    s800 = next(s for s in line_series if s["name"] == "±800")
    assert s800["data"][0]["lineStyle"]["width"] == pytest.approx(1 + 5.5)  # max gw
    assert s800["data"][0]["meta"]["name"] == "锡泰直流"
    assert opts["legend"]["data"] == topology.KV_ORDER

def test_physical_options_dim_when_province_selected():
    opts = topology.physical_options(_CHANNELS, {}, "福建")
    s800 = next(s for s in opts["series"] if s.get("name") == "±800")
    s100 = next(s for s in opts["series"] if s.get("name") == "±100")
    assert s800["data"][0]["lineStyle"]["opacity"] == pytest.approx(0.07)   # not linked
    assert s100["data"][0]["lineStyle"]["opacity"] == pytest.approx(0.85)   # linked

def test_flows_options_widths_and_meta():
    flows = [dict(send="蒙东", recv="江苏", vol_gwh=100.0, land=300, sendp=150, fee=120,
                  trades=2, channels=["鲁固直流"], send_raws=["蒙东"], months=1),
             dict(send="山西", recv="江苏", vol_gwh=25.0, land=380, sendp=None, fee=70,
                  trades=1, channels=["雁淮直流"], send_raws=["山西"], months=1)]
    coords = {"蒙东":[122.24,43.62], "山西":[112.55,37.87], "江苏":[119.78,32.96]}
    opts = topology.flows_options(flows, coords)
    s = next(x for x in opts["series"] if x["name"] == "江苏")
    w_big = s["data"][0]["lineStyle"]["width"]
    assert w_big == pytest.approx(0.8 + 6.2)     # max vol flow
    assert s["data"][1]["lineStyle"]["width"] < w_big
    assert s["data"][0]["meta"]["channels"] == ["鲁固直流"]
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement topology.py**

```python
# services/interconnector/topology.py
"""ECharts option builders for S1. Pure dict builders — rendering lives in the tab."""
from __future__ import annotations

import math

KV_ORDER = ["±100","±400","±500","±660","±800","±1100","500kV AC"]
KV_COLORS = {"±100":"#f0f5fc","±400":"#dbe7f7","±500":"#a8c4ea","±660":"#6a99dd",
             "±800":"#2563eb","±1100":"#0f2b6e","500kV AC":"#9aa6b5"}
RECV_COLORS = {"江苏":"#2563eb","上海":"#e11d48","浙江":"#0d9488","福建":"#d97706"}
MUTED = "#5b6675"
LAND = "#e6ebf1"; BORDER = "#c3cfda"; EMPH = "#d7e0e9"

def _geo() -> dict:
    return {"map": "china", "roam": True, "scaleLimit": {"min": 0.8, "max": 6},
            "layoutCenter": ["50%", "52%"], "layoutSize": "102%", "silent": False,
            "itemStyle": {"areaColor": LAND, "borderColor": BORDER, "borderWidth": 0.7},
            "emphasis": {"itemStyle": {"areaColor": EMPH}, "label": {"show": False}}}

def physical_options(channels: list[dict], channel_agg: dict,
                     selected_prov: str | None = None) -> dict:
    max_gw = max((c["gw"] for c in channels), default=1.0) or 1.0
    series = []
    for kv in KV_ORDER:
        data = []
        for c in channels:
            if c["kv"] != kv:
                continue
            linked = (selected_prov is None or c["send_prov"] == selected_prov
                      or c["recv_prov"] == selected_prov)
            agg = channel_agg.get(c["name"], {})
            data.append({"coords": [c["sc"], c["rc"]],
                "lineStyle": {"color": KV_COLORS[kv], "width": 1 + 5.5*math.sqrt(c["gw"]/max_gw),
                              "opacity": 0.85 if linked else 0.07,
                              "type": "dashed" if kv == "500kV AC" else "solid"},
                "meta": dict(c, vol_gwh=agg.get("vol_gwh", 0), land=agg.get("land"),
                             trades=agg.get("trades", 0))})
        series.append({"name": kv, "type": "lines", "coordinateSystem": "geo",
                       "zlevel": 2, "lineStyle": {"curveness": 0.08}, "data": data})
    stations = []
    for c in channels:
        stations.append({"name": c["send"], "value": c["sc"], "itemStyle": {"color": MUTED}})
        stations.append({"name": c["recv"], "value": c["rc"], "itemStyle": {"color": KV_COLORS[c["kv"]]}})
    series.append({"name": "stations", "type": "scatter", "coordinateSystem": "geo",
                   "zlevel": 3, "symbolSize": 3.5, "data": stations})
    return {"backgroundColor": "transparent",
            "color": [KV_COLORS[k] for k in KV_ORDER],
            "legend": {"top": 6, "left": "center", "icon": "roundRect", "data": KV_ORDER},
            "geo": _geo(), "series": series}

def flows_options(flows: list[dict], coords: dict) -> dict:
    max_vol = max((f["vol_gwh"] for f in flows), default=1.0) or 1.0
    series = []
    for rp, color in RECV_COLORS.items():
        data = [{"coords": [coords[f["send"]], coords[f["recv"]]],
                 "lineStyle": {"color": color,
                               "width": 0.8 + 6.2*math.sqrt(f["vol_gwh"]/max_vol),
                               "opacity": 0.72},
                 "meta": f} for f in flows if f["recv"] == rp]
        series.append({"name": rp, "type": "lines", "coordinateSystem": "geo",
                       "zlevel": 2, "lineStyle": {"curveness": 0.22}, "data": data})
    send_names = sorted({f["send"] for f in flows})
    senders = [{"name": s,
                "value": [*coords[s], sum(f["vol_gwh"] for f in flows if f["send"] == s)],
                "symbolSize": 5 + 9*math.sqrt(sum(f["vol_gwh"] for f in flows if f["send"] == s)/max_vol),
                "itemStyle": {"color": MUTED}} for s in send_names]
    receivers = [{"name": rp,
                  "value": [*coords[rp], sum(f["vol_gwh"] for f in flows if f["recv"] == rp)],
                  "symbolSize": 7 + 11*math.sqrt(sum(f["vol_gwh"] for f in flows if f["recv"] == rp)/max_vol),
                  "itemStyle": {"color": c, "borderWidth": 2},
                  "label": {"show": True, "formatter": rp, "fontWeight": 700, "fontSize": 12}}
                 for rp, c in RECV_COLORS.items()]
    series += [{"name": "senders", "type": "scatter", "coordinateSystem": "geo",
                "zlevel": 3, "data": senders},
               {"name": "receivers", "type": "scatter", "coordinateSystem": "geo",
                "zlevel": 4, "data": receivers}]
    return {"backgroundColor": "transparent",
            "color": list(RECV_COLORS.values()),
            "legend": {"top": 6, "left": "center", "icon": "roundRect",
                       "data": list(RECV_COLORS)},
            "geo": _geo(), "series": series}

# province centroid coords for the flows view (from GeoJSON + sub-province anchors)
FLOW_COORDS = {"蒙东": [122.24, 43.62], "锡盟": [116.07, 43.94]}
```

- [ ] **Step 4: Run tests** — pass

- [ ] **Step 5: Commit** `git add services/interconnector/topology.py tests/interconnector/test_topology.py && git commit -m "Add echarts option builders for physical and flows views"`

---

### Task 12: Tab module — S1 + uploaders + app wiring

**Files:**
- Create: `apps/spot-market/interconnector_tab.py`
- Modify: `apps/spot-market/app.py` (i18n dicts ~line 95/~406; tabs row ~line 1710)
- Modify: `apps/spot-market/requirements.txt` (Task 1 already added streamlit-echarts)

**Interfaces:**
- Consumes: `registry.get_channels`, `ingest.*` (Tasks 4–6), `data.*` (Tasks 7–10), `topology.*` (Task 11), `assets/geo/china_provinces.json` (Task 3), app `__conn()` psycopg2 conn.
- Produces: `interconnector_tab.render(conn) -> None`, called from app.py inside the new tab.

- [ ] **Step 1: Write the tab module**

```python
# apps/spot-market/interconnector_tab.py
"""Interconnector tab (S1 topology + S2/S3/S4 sections). Rendered from app.py."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_echarts import Map, st_echarts

from services.interconnector import data as ic_data
from services.interconnector import ingest as ic_ingest
from services.interconnector import topology as ic_topo
from services.interconnector.registry import get_channels

_GEO_PATH = Path(__file__).resolve().parents[2] / "assets" / "geo" / "china_provinces.json"
_CLICK_JS = ("function(params) { return {name: params.name || null, "
             "componentType: params.componentType, seriesType: params.seriesType || null, "
             "meta: (params.data && params.data.meta) ? params.data.meta : null}; }")

# province centroids for flows view — loaded from GeoJSON + sub-province anchors
def _flow_coords() -> dict:
    g = json.loads(_GEO_PATH.read_text())
    coords = {}
    for f in g["features"]:
        p = f["properties"]
        short = (p["name"].replace("省","").replace("市","").replace("壮族自治区","")
                 .replace("回族自治区","").replace("维吾尔自治区","")
                 .replace("自治区","").replace("特别行政区",""))
        c = p.get("centroid") or p.get("center")
        if c: coords[short] = c
    coords.update(ic_topo.FLOW_COORDS)
    return coords

@st.cache_data(ttl=300)
def _trades(_conn) -> list[dict]:
    df = pd.read_sql("SELECT * FROM staging.interconnector_trades", _conn)
    return df.to_dict("records")

@st.cache_data(ttl=300)
def _channels(_conn) -> list[dict]:
    df = pd.read_sql("SELECT * FROM staging.interconnector_channels", _conn)
    if df.empty:
        return []
    return [dict(r, sc=[r["send_lon"], r["send_lat"]], rc=[r["recv_lon"], r["recv_lat"]])
            for r in df.to_dict("records")]

def _upload_section(conn) -> None:
    with st.expander("数据上传 (Data Upload)", expanded=False):
        f1 = st.file_uploader("华东跨省数据汇总.xlsx (2026)", type=["xlsx"], key="ic_up_hd")
        if f1 and st.button("导入华东汇总", key="ic_btn_hd"):
            fp = ic_ingest.file_fingerprint(f1.name, f1.size)
            if st.session_state.get("ic_fp_hd") == fp:
                st.info("同一文件已导入过（指纹一致），跳过。")
            else:
                rows = ic_ingest.parse_huadong(f1, year=2026)
                n = ic_ingest.replace_trades(conn, rows, fp)
                st.session_state["ic_fp_hd"] = fp
                st.cache_data.clear()
                st.success(f"已导入 {n} 笔交易（全量替换）。")
        f2 = st.file_uploader("跨区组织交易情况.xlsx (2025全年)", type=["xlsx"], key="ic_up_kj")
        label = st.text_input("快照标签 snapshot label", value="2025-full", key="ic_snap_label")
        if f2 and st.button("导入跨区组织", key="ic_btn_kj"):
            rows = []
            for sheet in ("中长期", "上海"):
                rows += ic_ingest.parse_mlt_snapshot(f2, sheet=sheet)
            n = ic_ingest.replace_snapshot(conn, label, rows)
            st.cache_data.clear()
            st.success(f"已导入 {n} 行到快照 {label}。")
        if st.button("重建通道注册表 (reseed channels)", key="ic_btn_seed"):
            n = ic_ingest.upsert_channels(conn, get_channels())
            st.cache_data.clear()
            st.success(f"通道注册表已重建：{n} 回。")

def _panel(selected: str | None, channels: list[dict], agg: dict) -> None:
    if not selected:
        st.caption("点击省份查看其连接的通道与容量；点击通道线查看明细。")
        top = sorted(channels, key=lambda c: -c["gw"])[:8]
        for c in top:
            a = agg.get(c["name"], {})
            st.markdown(f"**{c['kv']} {c['name']}** — {c['gw']:g} GW  ")
            st.caption(f"{c['send']}({c['send_prov']}) → {c['recv']}({c['recv_prov']}) · {c['category']} · "
                       f"2026成交 {a.get('vol_gwh', 0):,.0f} GWh"
                       + (f" · 落地 {a['land']:,.0f} 元/MWh" if a.get("land") else ""))
        return
    out = [c for c in channels if c["send_prov"] == selected]
    inc = [c for c in channels if c["recv_prov"] == selected]
    if st.button("返回 (reset)", key="ic_panel_reset"):
        st.session_state["ic_sel_prov"] = None
        st.rerun()
    st.markdown(f"**{selected}** — 送出 {len(out)} 回 · 受入 {len(inc)} 回 · "
                f"合计 {sum(c['gw'] for c in out+inc):.1f} GW")
    for c in out + inc:
        a = agg.get(c["name"], {})
        st.markdown(f"**{c['kv']} {c['name']}** — {c['gw']:g} GW · {c['category']}  ")
        st.caption(f"{c['send']} → {c['recv']} · {c['commissioned']}投运 · "
                   f"2026成交 {a.get('vol_gwh', 0):,.0f} GWh"
                   + (f" · 落地 {a['land']:,.0f} 元/MWh" if a.get("land") else ""))

def render(conn) -> None:
    st.title("跨区通道 Interconnector Trading")
    ic_ingest.ensure_tables(conn)
    _upload_section(conn)

    channels = _channels(conn)
    if not channels:                      # first run: seed from registry
        ic_ingest.upsert_channels(conn, get_channels())
        st.cache_data.clear()
        channels = _channels(conn)
    trades = _trades(conn)
    agg = ic_data.per_channel_agg(trades) if trades else {}

    st.header("1 · 通道拓扑 Channel Topology")
    view = st.radio("视图", ["物理通道", "交易流向"], horizontal=True, key="ic_view")
    geo = json.loads(_GEO_PATH.read_text())
    col_map, col_panel = st.columns([2, 1])
    with col_map:
        if view == "物理通道":
            opts = ic_topo.physical_options(channels, agg, st.session_state.get("ic_sel_prov"))
        else:
            if not trades:
                st.info("尚未导入华东跨省数据汇总 — 请在上方数据上传区导入。")
                opts = ic_topo.physical_options(channels, agg, None)
            else:
                opts = ic_topo.flows_options(ic_data.aggregate_flows(trades), _flow_coords())
        result = st_echarts(options=opts, map=Map("china", geo),
                            events={"click": _CLICK_JS}, height="640px", key="ic_map")
        evt = result.get("chart_event") if isinstance(result, dict) else getattr(result, "chart_event", None)
        if evt and evt.get("componentType") == "geo" and evt.get("name"):
            prov = (evt["name"].replace("省","").replace("市","")
                    .replace("壮族自治区","").replace("回族自治区","")
                    .replace("维吾尔自治区","").replace("自治区",""))
            if prov != st.session_state.get("ic_sel_prov"):
                st.session_state["ic_sel_prov"] = prov
                st.rerun()
    with col_panel:
        _panel(st.session_state.get("ic_sel_prov"), channels, agg)

    st.subheader("通道明细 (by capacity)")
    st.dataframe(pd.DataFrame([{
        "通道": c["name"], "送出": f"{c['send']} ({c['send_prov']})",
        "受入": f"{c['recv']} ({c['recv_prov']})", "类别": c["category"],
        "电压": c["kv"], "容量GW": c["gw"], "投运": c["commissioned"],
        "2026成交GWh": agg.get(c["name"], {}).get("vol_gwh", 0),
        "加权落地价": agg.get(c["name"], {}).get("land"),
        "备注": c["note"]} for c in sorted(channels, key=lambda x: -x["gw"])]),
        use_container_width=True, hide_index=True)

    # S2/S3/S4 sections added by Tasks 13-15 — they append below.
```

- [ ] **Step 2: Wire into app.py — i18n dicts**

In `_T_EN` (after `"tab_interprov": "Inter-Provincial Flow",`):
```python
        "tab_ic":               "Interconnector",
```
In `_T_CN` (same position):
```python
        "tab_ic":               "跨区通道",
```

- [ ] **Step 3: Wire into app.py — tabs row**

```python
# before:
tab_interprov, tab_fundamentals, tab_agent, tab_news, tab_library, tab_jizhi, tab_supply, \
tab_forecast, tab_mgmt = st.tabs([
    _t("tab_overview"), _t("tab_spread"), _t("tab_heatmap"), _t("tab_intraday"),
    _t("tab_province"), _t("tab_dist"), _t("tab_geo"),
    _t("tab_interprov"), _t("tab_fundamentals"), _t("tab_agent"), _t("tab_news"),
    "Library", "机制竞价", "供需结构", "价格预测", _t("tab_mgmt"),
])
# after:
tab_interprov, tab_ic, tab_fundamentals, tab_agent, tab_news, tab_library, tab_jizhi, tab_supply, \
tab_forecast, tab_mgmt = st.tabs([
    _t("tab_overview"), _t("tab_spread"), _t("tab_heatmap"), _t("tab_intraday"),
    _t("tab_province"), _t("tab_dist"), _t("tab_geo"),
    _t("tab_interprov"), _t("tab_ic"), _t("tab_fundamentals"), _t("tab_agent"), _t("tab_news"),
    "Library", "机制竞价", "供需结构", "价格预测", _t("tab_mgmt"),
])
```
Note: the variable tuple on the line above (tab_overview…tab_geo) is unchanged; insert `tab_ic` right after `tab_interprov` in BOTH the unpacking and the labels list. Then at the end of the interprov tab's `with` block add:

```python
with tab_ic:
    import interconnector_tab
    interconnector_tab.render(__conn())
```
(Place adjacent to the other `with tab_*:` blocks; `import` inside the block matches the app's lazy style for heavy tabs.)

- [ ] **Step 4: Local verification (manual gate)**

```bash
~/.venvs/bess-platform/bin/streamlit run apps/spot-market/app.py --server.port 8505
```
Checklist: 跨区通道 tab renders; reseed button populates channels; upload 华东 file → 234 rows; physical view renders + province click → panel + dim; flows view renders after upload; channel table sorted by GW. Fix any st_echarts API drift found here (Task 1 spike notes).

- [ ] **Step 5: Commit**

```bash
git add apps/spot-market/interconnector_tab.py apps/spot-market/app.py
git commit -m "Add Interconnector tab S1 topology with upload and province-click panel"
```

---

### Task 13: Tab — S2 通道裕度 (A1 + A5)

**Files:**
- Modify: `apps/spot-market/interconnector_tab.py` (append to `render`)
- Test: `tests/interconnector/test_data.py` (add A5 aggregation test below)

**Interfaces:**
- Consumes: `ic_data.balance_of_year` (Task 8); `staging.spot_interprov_flow` columns `report_date, direction, metric_type, province_cn, price_yuan_kwh, total_vol_100gwh` (existing table).
- Produces: S2 section in the tab; `data.daily_interprov_trend(rows: list[dict]) -> pandas.DataFrame` (per report_date+direction: avg price, total vol) — new function in `data.py`.

- [ ] **Step 1: Failing test for the A5 helper**

```python
def test_daily_interprov_trend():
    rows = [
        dict(report_date=date(2026,8,1), direction="送出", price_yuan_kwh=0.30, total_vol_100gwh=2.0),
        dict(report_date=date(2026,8,1), direction="受入", price_yuan_kwh=0.35, total_vol_100gwh=2.0),
        dict(report_date=date(2026,8,2), direction="送出", price_yuan_kwh=0.32, total_vol_100gwh=1.0),
    ]
    df = ic_data.daily_interprov_trend(rows)
    s = df[(df.report_date == date(2026,8,1)) & (df.direction == "送出")].iloc[0]
    assert s["price_yuan_kwh"] == pytest.approx(0.30) and s["total_vol_100gwh"] == pytest.approx(2.0)
    assert len(df) == 3
```

Implement:
```python
def daily_interprov_trend(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return (df.groupby(["report_date", "direction"], as_index=False)
              .agg(price_yuan_kwh=("price_yuan_kwh", "mean"),
                   total_vol_100gwh=("total_vol_100gwh", "sum"))
              .sort_values("report_date"))
```

- [ ] **Step 2: Run test → fail; implement → pass; commit helper with S2 UI**

- [ ] **Step 3: Append S2 to `render()`**

```python
    st.header("2 · 通道裕度 Capacity & Balance of Year")
    st.caption("成交量口径：通道成交电量（华东跨省数据汇总）为交易代理，非调度口径物理潮流；"
               "物理容量 ≠ 可交易容量（ATC 需扣除配套优先/中长期/保供/安全约束）。")
    boy = ic_data.balance_of_year(channels, agg, date.today())
    st.dataframe(pd.DataFrame([{
        "通道": b["name"], "容量GW": b["gw"], "年能力GWh": b["capability_gwh"],
        "2026成交GWh": b["traded_gwh"], "利用率%": b["utilization_pct"],
        "剩余小时": b["remaining_hours"], "剩余能力GWh": b["remaining_capability_gwh"]}
        for b in boy]), use_container_width=True, hide_index=True)

    st.subheader("省间现货日报趋势 (A5)")
    dr = st.date_input("日期范围", value=(date(2026,1,1), date.today()), key="ic_a5_range")
    if isinstance(dr, tuple) and len(dr) == 2:
        rows = pd.read_sql(
            "SELECT report_date, direction, price_yuan_kwh, total_vol_100gwh "
            "FROM staging.spot_interprov_flow WHERE report_date BETWEEN %s AND %s",
            conn, params=(dr[0], dr[1])).to_dict("records")
        trend = ic_data.daily_interprov_trend(rows)
        if trend.empty:
            st.info("所选时段无省间现货数据。")
        else:
            import plotly.express as px
            fig = px.line(trend, x="report_date", y="price_yuan_kwh", color="direction",
                          labels={"report_date":"", "price_yuan_kwh":"均价 元/kWh", "direction":""})
            st.plotly_chart(fig, use_container_width=True)
            fig2 = px.bar(trend, x="report_date", y="total_vol_100gwh", color="direction",
                          labels={"report_date":"", "total_vol_100gwh":"总电量 亿kWh", "direction":""})
            st.plotly_chart(fig2, use_container_width=True)
```

- [ ] **Step 4: Local verification** — S2 renders; numbers match hand-check of 锡泰直流 row (capability 87,600 GWh; traded from upload).

- [ ] **Step 5: Commit** `git add apps/spot-market/interconnector_tab.py services/interconnector/data.py tests/interconnector/test_data.py && git commit -m "Add S2 capacity balance and daily interprov trend"`

---

### Task 14: Tab — S3 MLT patterns + A2 obligation table

**Files:**
- Modify: `apps/spot-market/interconnector_tab.py` (append to `render`)
- Create: `knowledge/interconnectors/mlt_contract_requirements.md` (stub with `{}` defaults — Task 17 fills it)

**Interfaces:**
- Consumes: `ic_data.green_premium`, `resolve_mlt_pct`, `year_ago_estimate`, `recycle_gap`, `monthly_volume_by_recv`; snapshot table; `staging.exchange_monthly_metrics` columns `province, report_month, medium_longterm_volume_gwh, contract_avg_price_yuan_mwh, wind_volume_gwh, solar_volume_gwh` (existing).
- Produces: S3 section; MLT% rules loader `data.load_mlt_rules(path) -> dict[str, float]` in `data.py`.

- [ ] **Step 1: Failing test for rules loader**

```python
def test_load_mlt_rules(tmp_path):
    f = tmp_path / "rules.md"
    f.write_text("# MLT rules\n\n- 蒙西: 85%\n- 甘肃: 80%\n")
    rules = ic_data.load_mlt_rules(f)
    assert rules == {"蒙西": 85.0, "甘肃": 80.0}
```
Implement (parse `- 省: NN%` lines only; ignore everything else):
```python
def load_mlt_rules(path) -> dict[str, float]:
    import re
    rules = {}
    p = Path(path)
    if not p.exists():
        return rules
    for line in p.read_text().splitlines():
        m = re.match(r"\s*-\s*([一-鿿]{2,4})[:：]\s*(\d+(?:\.\d+)?)\s*%", line)
        if m:
            rules[m.group(1)] = float(m.group(2))
    return rules
```

- [ ] **Step 2: Run test → fail; implement → pass**

- [ ] **Step 3: Append S3 to `render()`**

```python
    st.header("3 · 中长期交易 MLT Patterns & 新能源中长期义务")
    snap_label = st.text_input("历史快照标签", value="2025-full", key="ic_snap_read")
    snap = pd.read_sql("SELECT * FROM staging.interconnector_mlt_snapshot WHERE snapshot_label = %s",
                       conn, params=(snap_label,)).to_dict("records")
    if snap:
        gp = ic_data.green_premium(snap)
        st.subheader("绿电溢价 (2025快照, 市场化绿电 vs 其他市场化)")
        st.dataframe(pd.DataFrame(gp), use_container_width=True, hide_index=True)
    else:
        st.info(f"快照 {snap_label} 无数据 — 请在数据上传区导入跨区组织交易情况。")

    st.subheader("A2 新能源中长期义务与出口信号")
    st.caption("义务为省级口径：省内中长期 + 外送中长期 均可履约；"
               "回收风险 = max(0, 要求 − 省内 − 外送) × |中长期价 − 现货价|。"
               "新能源电量为历史实际（远期月份=历史同期估计）；% 可在此修改并保存。")
    rules = ic_data.load_mlt_rules(
        Path(__file__).resolve().parents[2] / "knowledge" / "interconnectors" / "mlt_contract_requirements.md")
    overrides = ic_ingest.get_pct_overrides(conn)
    emm = pd.read_sql(
        "SELECT province, report_month, medium_longterm_volume_gwh, contract_avg_price_yuan_mwh,"
        " wind_volume_gwh, solar_volume_gwh FROM staging.exchange_monthly_metrics", conn)
    emm["report_month"] = pd.to_datetime(emm["report_month"]).dt.date
    senders = sorted({t["send_anchor"] for t in trades}) if trades else []
    rows = []
    for prov in senders:
        pct, src = ic_data.resolve_mlt_pct(prov, rules, overrides)
        pe = emm[emm["province"] == prov]
        renewable_gwh = float((pe["wind_volume_gwh"].fillna(0) + pe["solar_volume_gwh"].fillna(0)).tail(12).sum()) if not pe.empty else None
        exported_gwh = sum((t["vol_post_mwh"] or 0) for t in trades if t["send_anchor"] == prov) / 1000
        within_gwh = float(pe["medium_longterm_volume_gwh"].fillna(0).tail(12).sum()) if not pe.empty else None
        rows.append(dict(prov=prov, pct=pct, src=src, renewable_gwh=renewable_gwh,
                         within_gwh=within_gwh, exported_gwh=round(exported_gwh, 1),
                         required_gwh=round(renewable_gwh * pct / 100, 1) if renewable_gwh else None))
    for r in rows:
        c1, c2 = st.columns([3, 1])
        label = f"{r['prov']} — MLT% ({'默认80%' if r['src']=='default' else ('规则库' if r['src']=='rule' else '已覆盖')})"
        new_pct = c2.number_input(label, 0.0, 100.0, float(r["pct"]), 0.5,
                                  key=f"ic_pct_{r['prov']}", label_visibility="visible")
        if new_pct != r["pct"]:
            ic_ingest.set_pct_override(conn, r["prov"], new_pct)
            st.rerun()
        c1.write(f"可再生(T12M): {r['renewable_gwh'] or '无数据':>12} GWh · 要求: {r['required_gwh'] or '—'} GWh · "
                 f"省内中长期: {r['within_gwh'] or '—'} GWh · 外送: {r['exported_gwh']} GWh")
```

- [ ] **Step 4: Local verification** — S3 renders; % edit persists across rerun (check `staging.interconnector_mlt_pct_override` row); renewable numbers sane vs exchange reports.

- [ ] **Step 5: Commit** `git add apps/spot-market/interconnector_tab.py services/interconnector/data.py tests/interconnector/test_data.py knowledge/interconnectors/mlt_contract_requirements.md && git commit -m "Add S3 MLT patterns, green premium, A2 obligation table with % override"`

---

### Task 15: Tab — S4 价差回测 (A3)

**Files:**
- Modify: `apps/spot-market/interconnector_tab.py` (append to `render`)

**Interfaces:**
- Consumes: `ic_data.backtest_rows` (Task 10); `staging.exchange_monthly_metrics.contract_avg_price_yuan_mwh` (month-ahead proxy, report M−1); `marketdata.spot_prices_hourly` (delivery-month spot monthly avg, DA); trades for landing VWAP.

- [ ] **Step 1: Append S4 to `render()`**

```python
    st.header("4 · 价差回测 Month-Ahead Spread Backtest")
    st.caption("月前价代理 = 交易月前一月披露月报中的合约均价 (contract_avg_price)；"
               "交割月现货 = LingFeng 日前月度均价；落地价 = 该省对当月覆盖交易加权。"
               "缺口月份显示 无数据，不插值。")
    if trades:
        pairs = sorted({(t["send_anchor"], t["recv_province"]) for t in trades})
        pair = st.selectbox("省对", pairs, format_func=lambda p: f"{p[0]} → {p[1]}", key="ic_a3_pair")
        months = sorted({t["month_start"] for t in trades})
        msel = st.multiselect("交割月", months, default=months[-3:], key="ic_a3_months")
        # month-ahead proxy: contract_avg_price at report month M-1 → keyed to delivery month M
        emm2 = pd.read_sql(
            "SELECT province, report_month, contract_avg_price_yuan_mwh"
            " FROM staging.exchange_monthly_metrics", conn)
        emm2["report_month"] = pd.to_datetime(emm2["report_month"])
        month_ahead = {}
        for _, r in emm2.iterrows():
            if pd.notna(r["contract_avg_price_yuan_mwh"]):
                dm = (r["report_month"] + pd.offsets.MonthBegin(1)).date()
                month_ahead[(r["province"], dm)] = float(r["contract_avg_price_yuan_mwh"])
        spot = pd.read_sql(
            "SELECT province, date_trunc('month', metric_date::timestamp)::date AS m,"
            " AVG(da_price) AS p FROM marketdata.spot_prices_hourly"
            " GROUP BY 1, 2", conn)
        spot_monthly = {(r["province"], r["m"]): float(r["p"]) for _, r in spot.iterrows()
                        if pd.notna(r["p"])}
        rows = ic_data.backtest_rows([pair], msel, month_ahead, spot_monthly, trades)
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        hits = [r for r in rows if r["premium_over_recv_spot"] is not None]
        if hits:
            beat = sum(1 for r in hits if r["premium_over_recv_spot"] < 0)
            st.metric("落地价低于受端现货的月份占比", f"{100*beat/len(hits):.0f}% ({beat}/{len(hits)})")
    else:
        st.info("尚未导入华东跨省数据汇总。")
```
Note: `spot_prices_hourly` column names (`metric_date`, `da_price`, `province`) must be checked against the app's existing queries (app.py:872 area) and adjusted — the app uses its own column names; reuse exactly what app.py's spot queries use.

- [ ] **Step 2: Local verification** — pick 山西→江苏, verify one month's numbers by hand against source data.

- [ ] **Step 3: Commit** `git add apps/spot-market/interconnector_tab.py && git commit -m "Add S4 month-ahead spread backtest"`

---

### Task 16: Full-suite green + merge

**Files:** none new.

- [ ] **Step 1: Run the complete test suites**

```bash
~/.venvs/bess-platform/bin/python -m pytest tests/interconnector/ tests/spot_ingest/ -q
~/.venvs/bess-platform/bin/python -m pytest tests/ -q --ignore=tests/hermes --ignore=tests/knowledge_pool
```
Expected: all green (hermes/knowledge_pool suites have external deps; run them too if quick and note pre-existing failures — do not fix unrelated suites here).

- [ ] **Step 2: Final local run** — full pass through S1–S4 on localhost:8505 with both files uploaded.

- [ ] **Step 3: Merge**

```bash
git checkout main && git merge feat/interconnector-tab && git push origin main
```

- [ ] **Step 4: Deploy prep** — run `/deploy` for spot-market (v32 → v33). **Deploy itself needs the user's explicit in-session yes.**

---

### Task 17: MLT% rules extraction (research task, post-MVP)

**Files:**
- Create/Update: `knowledge/interconnectors/mlt_contract_requirements.md`

- [ ] **Step 1: Search policy sources** — KB (`staging.spot_knowledge_docs` via `services/knowledge_pool/retrieval.py`) + `data/policies/` for provincial 新能源中长期签约/交易比例 rules for the sender provinces (蒙西/甘肃/宁夏/新疆/青海/山西/黑龙江/吉林/辽宁/陕西/四川/云南). Note: RDS must be reachable — if the Mac's rds-sg rule is still missing, run this from an environment that has access.

- [ ] **Step 2: Draft the knowledge file** — format the loader accepts (`- 省份: NN%` lines) plus source quotes + effective dates as comments.

- [ ] **Step 3: User review gate** — present the file to the user for correction before the tab relies on it (spec §7). Until then the 80% default applies (marked).

- [ ] **Step 4: Commit** with the reviewed values.

---

## Self-Review Notes

- **Spec coverage:** S1→Task 11+12, S2(A1+A5)→Task 8+13, S3(A4+A2)→Task 9+14 (+17), S4(A3)→Task 10+15; uploads→Tasks 4–6+12; registry→2+3; streamlit-echarts gate→Task 1; i18n/app wiring→Task 12; deploy→Task 16 step 4 (confirmation-gated).
- **Type consistency:** trade row keys identical across Tasks 4/7/10/12 (parse_huadong output); channel dict keys identical across Tasks 2/6/11/12; `channel_agg` shape `{name:{vol_gwh,land,trades}}` produced Task 7, consumed Tasks 8/11/12; `backtest_rows` signature Task 10 consumed Task 15.
- **Known open point:** Task 15's `spot_prices_hourly` column names must be matched to app.py's existing spot queries during implementation (marked in-task).
