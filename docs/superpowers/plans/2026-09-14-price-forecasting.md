# Price Forecasting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-province day-ahead hourly RT price forecasting in bess-map: merit-order stack model (coal/gas fuel costs + priced interconnector imports) for price level, PCA shape model for curve shape, hybrid combination, shown in a new "Price Forecast" analysis tab with backtests.

**Architecture:** New pure-computation package `services/bess_map/price_lab/` (no DB/I/O — testable), a new tab module `apps/bess-map/price_forecast_tab.py`, and Hermes auto-extraction of fuel/fleet data (`fuel_fleet_screener.py` + `fuel_fleet_etl.py`) following the existing capcomp pattern with `draft`→`confirmed` human review.

**Tech Stack:** Python 3.11, numpy (eigendecomposition + closed-form ridge — NO sklearn; not in requirements), pandas, plotly, Streamlit, psycopg2. Hermes: Claude via `shared.anthropic_client.make_client`, model `claude-sonnet-4-6`.

**Spec:** `docs/superpowers/specs/2026-09-14-price-forecasting-design.md`

## Global Constraints

- Pure-computation modules in `price_lab/` must not import streamlit, psycopg2, or sqlalchemy — DB access lives in `price_forecast_tab.py` loaders only.
- Ridge regression is closed-form numpy (`β = (XᵀX + λI)⁻¹Xᵀy`), deterministic — do NOT add sklearn to requirements.
- New table only: `marketdata.province_fuel_fleet`. No changes to existing tables/views.
- Only `status='confirmed'` fuel/fleet rows feed the model; `draft` rows are review-only.
- i18n: every tab string added to BOTH dicts in `apps/bess-map/app.py` (EN block ~line 99, ZH block ~line 356).
- AppTest headless smoke test against real DB before any deploy (repo pre-deploy standard).
- Deploy uses the Aliyun-mirror Dockerfile trick (ERRORS.md 2026-09-13); hermes deploys via jq-swap from the service's CURRENT tdArn with env-count guard (≥31).
- Commit after every task. Commit messages: imperative, one line, ending with `Co-Authored-By: Claude Code <noreply@anthropic.com>`.

---

### Task 1: `province_fuel_fleet` table + ETL with conflict detection

**Files:**
- Create: `services/hermes/fuel_fleet_etl.py`
- Test: `tests/hermes/test_fuel_fleet_etl.py`

**Interfaces:**
- Produces:
  - `ensure_table(cur) -> None` — idempotent DDL
  - `upsert_fuel_fleet_rows(rows: list[dict], pg_url: str, source: str) -> dict` — returns `{"upserted": int, "conflicts": int, "errors": list[str]}`
  - Row shape: `{"province": str, "effective_date": "YYYY-MM-DD", "coal_price_yuan_t": float|None, "gas_price_yuan_m3": float|None, "fleet_segments": list[dict], "notes": str|None}`
  - fleet_segment shape: `{"fuel": "coal"|"gas", "capacity_mw": float, "heat_rate_kj_kwh": float, "vom_yuan_mwh": float, "label": str}`
  - `resolve_fuel_fleet_conflict(row_id_keep: int, row_id_drop: int, pg_url: str) -> None`
- Consumed by: Task 2 (screener), Task 8 (tab loader reads confirmed rows), Task 12 (review UI).

- [ ] **Step 1: Write the failing test**

```python
# tests/hermes/test_fuel_fleet_etl.py
from unittest.mock import MagicMock, patch
import services.hermes.fuel_fleet_etl as etl


def _conn_mock():
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn, cur


_SEG = [{"fuel": "coal", "capacity_mw": 40000, "heat_rate_kj_kwh": 8200,
         "vom_yuan_mwh": 12, "label": "coal_main"}]


def test_upsert_inserts_valid_row(monkeypatch):
    conn, cur = _conn_mock()
    cur.fetchall.return_value = []  # no existing confirmed row
    monkeypatch.setattr(etl.psycopg2, "connect", lambda *a, **k: conn)
    out = etl.upsert_fuel_fleet_rows(
        [{"province": "山东", "effective_date": "2026-09-01",
          "coal_price_yuan_t": 850.0, "gas_price_yuan_m3": 3.1,
          "fleet_segments": _SEG}],
        "postgresql://x", "test")
    assert out["upserted"] == 1 and out["errors"] == []
    assert any("province_fuel_fleet" in str(c.args[0]) for c in cur.execute.call_args_list)


def test_upsert_rejects_bad_rows(monkeypatch):
    conn, cur = _conn_mock()
    monkeypatch.setattr(etl.psycopg2, "connect", lambda *a, **k: conn)
    out = etl.upsert_fuel_fleet_rows(
        [{"province": "", "effective_date": "2026-09-01", "fleet_segments": _SEG},
         {"province": "山东", "effective_date": "not-a-date", "fleet_segments": _SEG},
         {"province": "山西", "effective_date": "2026-09-01", "fleet_segments": []}],
        "postgresql://x", "test")
    assert out["upserted"] == 0 and len(out["errors"]) == 3


def test_conflict_flagged_on_large_diff(monkeypatch):
    conn, cur = _conn_mock()
    # existing confirmed row id=7 with coal 800; new row coal 1000 (>5% diff)
    cur.fetchall.return_value = [(7, 800.0)]
    monkeypatch.setattr(etl.psycopg2, "connect", lambda *a, **k: conn)
    out = etl.upsert_fuel_fleet_rows(
        [{"province": "山东", "effective_date": "2026-09-01",
          "coal_price_yuan_t": 1000.0, "gas_price_yuan_m3": None,
          "fleet_segments": _SEG}],
        "postgresql://x", "test")
    assert out["conflicts"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/hermes/test_fuel_fleet_etl.py -q`
Expected: FAIL (`ModuleNotFoundError: services.hermes.fuel_fleet_etl`)

- [ ] **Step 3: Implement `services/hermes/fuel_fleet_etl.py`**

Follow `services/hermes/capcomp_etl.py` structure exactly (same DDL-in-code, same `_parse_date`, same 5% `_CONFLICT_THRESHOLD`, same upsert-with-`ON CONFLICT ... COALESCE(source,'')`). Key content:

```python
"""Fuel & fleet ETL — upsert + conflict detection for marketdata.province_fuel_fleet.

Entry points:
    ensure_table(cur)                                 — idempotent DDL
    upsert_fuel_fleet_rows(rows, pg_url, source)      — upsert extracted rows
    resolve_fuel_fleet_conflict(row_id_keep, row_id_drop, pg_url)
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Optional

import psycopg2

logger = logging.getLogger(__name__)

_ENSURE_SQL = """
CREATE TABLE IF NOT EXISTS marketdata.province_fuel_fleet (
    id                  SERIAL PRIMARY KEY,
    province            TEXT        NOT NULL,
    effective_date      DATE        NOT NULL,
    coal_price_yuan_t   NUMERIC,
    gas_price_yuan_m3   NUMERIC,
    fleet_segments      JSONB       NOT NULL,
    source              TEXT,
    status              TEXT        NOT NULL DEFAULT 'draft',
    notes               TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pff_prov_date_src
    ON marketdata.province_fuel_fleet (province, effective_date, COALESCE(source, ''));
CREATE INDEX IF NOT EXISTS idx_pff_prov_date
    ON marketdata.province_fuel_fleet (province, effective_date DESC);
"""

_INSERT_SQL = """
INSERT INTO marketdata.province_fuel_fleet
    (province, effective_date, coal_price_yuan_t, gas_price_yuan_m3,
     fleet_segments, source, status, notes)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (province, effective_date, COALESCE(source, '')) DO UPDATE SET
    coal_price_yuan_t = EXCLUDED.coal_price_yuan_t,
    gas_price_yuan_m3 = EXCLUDED.gas_price_yuan_m3,
    fleet_segments    = EXCLUDED.fleet_segments,
    notes             = EXCLUDED.notes,
    ingested_at       = NOW()
RETURNING id
"""

_FETCH_CONFIRMED_SQL = """
SELECT id, coal_price_yuan_t FROM marketdata.province_fuel_fleet
WHERE province = %s AND effective_date = %s AND status = 'confirmed'
ORDER BY ingested_at DESC LIMIT 1
"""

_SET_STATUS_SQL = "UPDATE marketdata.province_fuel_fleet SET status = %s, ingested_at = NOW() WHERE id = %s"

_CONFLICT_THRESHOLD = 0.05


def ensure_table(cur) -> None:
    cur.execute(_ENSURE_SQL)


def _parse_date(d) -> Optional[date]:
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        try:
            return date.fromisoformat(d[:10])
        except ValueError:
            return None
    return None


def _valid_segments(segs) -> bool:
    if not isinstance(segs, list) or not segs:
        return False
    for s in segs:
        if not isinstance(s, dict):
            return False
        if s.get("fuel") not in ("coal", "gas"):
            return False
        try:
            float(s["capacity_mw"]); float(s["heat_rate_kj_kwh"]); float(s["vom_yuan_mwh"])
        except (KeyError, TypeError, ValueError):
            return False
    return True


def _values_conflict(existing: float, new_val: float) -> bool:
    if existing == 0:
        return new_val != 0
    return abs(new_val - existing) / abs(existing) > _CONFLICT_THRESHOLD


def upsert_fuel_fleet_rows(rows: list[dict], pg_url: str, source: str) -> dict:
    upserted, conflicts, errors = 0, 0, []
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            for row in rows:
                province = str(row.get("province", "")).strip()
                if not province:
                    errors.append("missing province"); continue
                eff = _parse_date(row.get("effective_date"))
                if not eff:
                    errors.append(f"{province}: invalid effective_date"); continue
                segs = row.get("fleet_segments")
                if not _valid_segments(segs):
                    errors.append(f"{province}: invalid fleet_segments"); continue
                coal = row.get("coal_price_yuan_t")
                gas = row.get("gas_price_yuan_m3")
                # conflict check on coal price vs existing confirmed
                cur.execute(_FETCH_CONFIRMED_SQL, (province, eff))
                existing = cur.fetchall()
                status = "draft"
                if existing and coal is not None and existing[0][1] is not None:
                    if _values_conflict(float(existing[0][1]), float(coal)):
                        cur.execute(_SET_STATUS_SQL, ("conflict", existing[0][0]))
                        status = "conflict"
                        conflicts += 1
                cur.execute(_INSERT_SQL, (
                    province, eff, coal, gas,
                    json.dumps(segs, ensure_ascii=False), source, status,
                    row.get("notes")))
                upserted += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"upserted": upserted, "conflicts": conflicts, "errors": errors}


def resolve_fuel_fleet_conflict(row_id_keep: int, row_id_drop: int, pg_url: str) -> None:
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_SET_STATUS_SQL, ("confirmed", row_id_keep))
            cur.execute(_SET_STATUS_SQL, ("superseded", row_id_drop))
        conn.commit()
    finally:
        conn.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/hermes/test_fuel_fleet_etl.py -q`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add services/hermes/fuel_fleet_etl.py tests/hermes/test_fuel_fleet_etl.py
git commit -m "Add fuel_fleet ETL with conflict detection

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: Hermes `fuel_fleet_screener.py` — KB/web extraction

**Files:**
- Create: `services/hermes/fuel_fleet_screener.py`
- Test: `tests/hermes/test_fuel_fleet_screener.py`
- Modify: `services/hermes/app.py` (register `/fuelfleet` chat command next to the `/capcomp` command; wire a monthly cron beside `_screen_capcomp` — day 5, hour 12:30 UTC)

**Interfaces:**
- Consumes: `upsert_fuel_fleet_rows` from Task 1; `_search_kb`/`_claude_extract` conventions from `services/hermes/capcomp_screener.py`.
- Produces:
  - `screen_fuel_fleet(pg_url: str, api_key: str, feishu=None, owner_open_id: str = "", year: int | None = None, provinces: list[str] | None = None) -> dict` — returns `{"scanned": int, "extracted": int, "upserted": int, "errors": list[str]}`
  - `EXTRACTION_PROMPT: str` — the Claude prompt template (for tests).

- [ ] **Step 1: Write the failing test**

```python
# tests/hermes/test_fuel_fleet_screener.py
from unittest.mock import MagicMock, patch
import services.hermes.fuel_fleet_screener as sc


def test_extract_json_from_fenced_block():
    text = '```json\n{"coal_price_yuan_t": 850, "gas_price_yuan_m3": 3.1, "fleet_segments": []}\n```'
    data = sc._extract_json(text)
    assert data["coal_price_yuan_t"] == 850


def test_screen_upserts_extraction(monkeypatch):
    fake_data = {"coal_price_yuan_t": 850.0, "gas_price_yuan_m3": 3.1,
                 "fleet_segments": [{"fuel": "coal", "capacity_mw": 40000,
                                     "heat_rate_kj_kwh": 8200, "vom_yuan_mwh": 12,
                                     "label": "coal_main"}],
                 "confidence": "high", "source_url": "kb:doc1"}
    monkeypatch.setattr(sc, "_search_kb", lambda *a, **k: [("chunk text", "doc1.pdf")])
    monkeypatch.setattr(sc, "_claude_extract", lambda *a, **k: fake_data)
    captured = {}

    def fake_upsert(rows, pg_url, source):
        captured["rows"] = rows
        return {"upserted": len(rows), "conflicts": 0, "errors": []}

    monkeypatch.setattr(sc, "upsert_fuel_fleet_rows", fake_upsert)
    out = sc.screen_fuel_fleet("postgresql://x", "key", provinces=["山东"])
    assert out["extracted"] == 1 and out["upserted"] == 1
    row = captured["rows"][0]
    assert row["province"] == "山东" and row["coal_price_yuan_t"] == 850.0
    assert row["source"].startswith("kb:") or "doc1" in row["source"]


def test_screen_skips_failed_extraction(monkeypatch):
    monkeypatch.setattr(sc, "_search_kb", lambda *a, **k: [])
    monkeypatch.setattr(sc, "_claude_extract", lambda *a, **k: None)
    monkeypatch.setattr(sc, "upsert_fuel_fleet_rows",
                        lambda *a, **k: {"upserted": 0, "conflicts": 0, "errors": []})
    out = sc.screen_fuel_fleet("postgresql://x", "key", provinces=["山东"])
    assert out["extracted"] == 0 and out["upserted"] == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/hermes/test_fuel_fleet_screener.py -q`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `services/hermes/fuel_fleet_screener.py`**

Clone the structure of `capcomp_screener.py`: `_SEARCH_PROVINCES` (default list = 山东, 山西, 蒙西, 广东, 甘肃, 江苏, 浙江, 河北南网, 冀北, 河南, 新疆), `_search_kb` (same FTS query against `staging.spot_knowledge_chunks` joined to docs — copy the SQL verbatim from capcomp_screener and swap keywords), `_claude_extract` (same client call via `shared.anthropic_client.make_client`, `model="claude-sonnet-4-6"`, `max_tokens=1024`), `_extract_json` (copy verbatim — it handles fenced/unfenced JSON), and `screen_fuel_fleet` orchestrator. The only genuinely new content is the prompt:

```python
EXTRACTION_PROMPT = """你是电力市场数据提取助手。目标省份：{province}，年份：{year}。

从下面的资料中提取该省的：
1. 动力煤标杆/指数价（元/吨，5500大卡为准；若只有其他热值，按热值比例折算并说明）
2. 天然气门站价（元/立方米；取发电用气价格，非居民用气）
3. 煤电/气电装机结构，拆成 2–4 个效率段，每段给出：
   fuel（coal 或 gas）、capacity_mw、heat_rate_kj_kwh（供电煤耗×3600≈kJ/kWh；超超临界≈8200、超临界≈8700、亚临界≈9500、CCGT≈6400、OCGT≈9000 可作先验）、vom_yuan_mwh（变动运维费，煤≈12、气≈8 可作先验）、label

只输出一个 JSON 对象（不要 markdown）：
{{"coal_price_yuan_t": float|null, "gas_price_yuan_m3": float|null,
  "fleet_segments": [...], "confidence": "high|medium|low", "source_url": "来源文件名或URL"}}

资料（{n_chunks} 段）：
{context}"""
```

Orchestrator logic per province: `kb_rows = _search_kb(province, ["动力煤", "煤价", "天然气门站价", "装机结构"], pg_url)` → `data = _claude_extract(...)` → if data and `_valid_segments(data["fleet_segments"])` (import from fuel_fleet_etl): build row `{province, effective_date: f"{year}-01-01" if data has no explicit date else parsed, coal_price_yuan_t, gas_price_yuan_m3, fleet_segments, notes: f"confidence={data.get('confidence')}"}` → `upsert_fuel_fleet_rows(rows, pg_url, source=f"fuel_fleet_screener:{data.get('source_url','')[:200]}")`. Accumulate counters; wrap per-province in try/except so one province's failure doesn't abort the scan (append to errors).

Feishu summary at the end (same style as capcomp): if feishu and owner_open_id, `feishu.send_text(open_id=owner_open_id, text=f"⛽ 燃料装机扫描完成：{extracted}/{scanned} 提取，{upserted} 入库（draft，待确认）")`.

- [ ] **Step 4: Wire into `services/hermes/app.py`**

Two small edits, both mirroring the capcomp registration:
- Chat command (next to the `/capcomp` handler): match `r'^/?(?:fuelfleet|燃料扫描)$'` → reply "⛽ 正在扫描燃料与装机数据…" → background thread `screen_fuel_fleet(pg_url, api_key, feishu, msg.sender_id)`.
- Cron (next to the `_screen_capcomp` add_job): `scheduler.add_job(_screen_fuel_fleet_wrapper, "cron", day=5, hour=12, minute=30, kwargs={...})` — wrapper mirrors `_screen_capcomp` kwargs construction (`_mengxi_pg_url`, `ANTHROPIC_API_KEY`, feishu, owner open id).

- [ ] **Step 5: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/hermes/test_fuel_fleet_screener.py -q`
Expected: 3 PASS

- [ ] **Step 6: Commit**

```bash
git add services/hermes/fuel_fleet_screener.py tests/hermes/test_fuel_fleet_screener.py services/hermes/app.py
git commit -m "Add fuel/fleet Hermes screener with /fuelfleet command and monthly cron

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: `price_lab/merit_order.py` — stack builder + marginal solver

**Files:**
- Create: `services/bess_map/price_lab/__init__.py` (empty)
- Create: `services/bess_map/price_lab/merit_order.py`
- Test: `tests/services/price_lab/test_merit_order.py`

**Interfaces:**
- Produces:
  - `build_stack(fleet_segments: list[dict], coal_price_yuan_t: float, gas_price_yuan_m3: float, import_blocks: list[dict] | None = None, renewable_mw: float = 0.0) -> list[dict]` — returns segments sorted by variable cost; each `{label, vc_yuan_mwh, capacity_mw, is_import: bool}`
  - `marginal_price(stack: list[dict], residual_demand_mw: float) -> float` — variable cost (¥/MWh) of the marginal segment; `nan` if demand exceeds stack
  - Import block shape: `{"label": str, "price_yuan_mwh": float, "capacity_mw": float}`
  - Fuel price conversion constants: `COAL_KG_PER_T = 1000.0`, `GAS_KJ_PER_M3 = 38931.0` (≈9300 kcal/m³)
- Consumed by: Task 4 (markup), Task 8 (tab loader), Task 9 (explorer chart).

- [ ] **Step 1: Write the failing test**

```python
# tests/services/price_lab/test_merit_order.py
import math
from services.bess_map.price_lab.merit_order import build_stack, marginal_price

_SEGS = [
    {"fuel": "coal", "capacity_mw": 10000, "heat_rate_kj_kwh": 9500, "vom_yuan_mwh": 12, "label": "coal_low_eff"},
    {"fuel": "coal", "capacity_mw": 30000, "heat_rate_kj_kwh": 8200, "vom_yuan_mwh": 12, "label": "coal_hi_eff"},
    {"fuel": "gas",  "capacity_mw": 5000,  "heat_rate_kj_kwh": 6400, "vom_yuan_mwh": 8,  "label": "ccgt"},
]

def test_stack_sorted_by_variable_cost():
    stack = build_stack(_SEGS, coal_price_yuan_t=850.0, gas_price_yuan_m3=3.1)
    vcs = [s["vc_yuan_mwh"] for s in stack]
    assert vcs == sorted(vcs)
    # hi-eff coal: kg/kWh = 8200/29307 LHV → vc = 850/1000 × (8200/29307) × 1000 + 12
    hi = [s for s in stack if s["label"] == "coal_hi_eff"][0]
    assert abs(hi["vc_yuan_mwh"] - (850 / 1000 * 8200 / 29307 * 1000 + 12)) < 0.01

def test_renewable_first_in_stack():
    stack = build_stack(_SEGS, 850.0, 3.1, renewable_mw=8000)
    assert stack[0]["label"] == "renewable" and stack[0]["vc_yuan_mwh"] == 0.0

def test_marginal_price_walks_the_stack():
    stack = build_stack(_SEGS, 850.0, 3.1, renewable_mw=5000)
    p1 = marginal_price(stack, 3000)    # inside renewable
    assert p1 == 0.0
    p2 = marginal_price(stack, 30000)   # past renewable, inside cheapest thermal
    cheapest = stack[1]
    assert p2 == cheapest["vc_yuan_mwh"]
    p3 = marginal_price(stack, 44000)   # into second thermal
    assert p3 == stack[2]["vc_yuan_mwh"]

def test_import_block_placed_by_price():
    imp = [{"label": "锦苏直流", "price_yuan_mwh": 300.0, "capacity_mw": 2000}]
    stack = build_stack(_SEGS, 850.0, 3.1, import_blocks=imp)
    assert any(s["is_import"] for s in stack)
    imp_seg = [s for s in stack if s["is_import"]][0]
    assert imp_seg["vc_yuan_mwh"] == 300.0 and imp_seg["capacity_mw"] == 2000

def test_demand_above_stack_returns_nan():
    stack = build_stack(_SEGS, 850.0, 3.1)
    assert math.isnan(marginal_price(stack, 999999))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_merit_order.py -q`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `services/bess_map/price_lab/merit_order.py`**

```python
"""Merit-order stack model — pure computation, no DB/I/O.

Variable cost (¥/MWh):
  coal: kg/kWh  = heat_rate_kj_kwh / COAL_LHV_KJ_KG
        vc = (coal_price_yuan_t / 1000) × kg/kWh × 1000 + VOM
  gas:  m³/kWh  = heat_rate_kj_kwh / GAS_LHV_KJ_M3
        vc = gas_price_yuan_m3 × m³/kWh × 1000 + VOM
"""
from __future__ import annotations

import math

COAL_LHV_KJ_KG = 29307.0   # ~7000 kcal/kg standard coal equivalent
GAS_LHV_KJ_M3 = 38931.0    # ~9300 kcal/m³


def _vc_yuan_mwh(fuel: str, fuel_price: float, heat_rate_kj_kwh: float, vom: float) -> float:
    if fuel == "coal":
        kg_per_kwh = heat_rate_kj_kwh / COAL_LHV_KJ_KG
        return fuel_price / 1000.0 * kg_per_kwh * 1000.0 + vom
    if fuel == "gas":
        m3_per_kwh = heat_rate_kj_kwh / GAS_LHV_KJ_M3
        return fuel_price * m3_per_kwh * 1000.0 + vom
    raise ValueError(f"unknown fuel {fuel!r}")


def build_stack(fleet_segments, coal_price_yuan_t, gas_price_yuan_m3,
                import_blocks=None, renewable_mw=0.0):
    stack = []
    if renewable_mw and renewable_mw > 0:
        stack.append({"label": "renewable", "vc_yuan_mwh": 0.0,
                      "capacity_mw": float(renewable_mw), "is_import": False})
    for s in fleet_segments:
        price = coal_price_yuan_t if s["fuel"] == "coal" else gas_price_yuan_m3
        stack.append({"label": s.get("label", s["fuel"]),
                      "vc_yuan_mwh": _vc_yuan_mwh(s["fuel"], price,
                                                  float(s["heat_rate_kj_kwh"]),
                                                  float(s["vom_yuan_mwh"])),
                      "capacity_mw": float(s["capacity_mw"]),
                      "is_import": False})
    for b in import_blocks or []:
        stack.append({"label": b["label"], "vc_yuan_mwh": float(b["price_yuan_mwh"]),
                      "capacity_mw": float(b["capacity_mw"]), "is_import": True})
    stack.sort(key=lambda s: s["vc_yuan_mwh"])
    return stack


def marginal_price(stack, residual_demand_mw):
    remaining = float(residual_demand_mw)
    if remaining < 0:
        return 0.0
    for seg in stack:
        if remaining <= seg["capacity_mw"]:
            return seg["vc_yuan_mwh"]
        remaining -= seg["capacity_mw"]
    return math.nan
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_merit_order.py -q`
Expected: 5 PASS

- [ ] **Step 5: Commit**

```bash
git add services/bess_map/price_lab/__init__.py services/bess_map/price_lab/merit_order.py tests/services/price_lab/test_merit_order.py
git commit -m "Add merit-order stack builder and marginal price solver

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: Markup calibration (`fit_markup` / `apply_markup`)

**Files:**
- Modify: `services/bess_map/price_lab/merit_order.py` (append)
- Test: `tests/services/price_lab/test_merit_order.py` (append)

**Interfaces:**
- Produces:
  - `fit_markup(observed: "pd.Series", structural: "pd.Series", tightness: "pd.Series", n_bins: int = 5) -> list[tuple[float, float]]` — returns monotone non-decreasing `[(tightness_threshold, markup)]`, markup ≥ 1.0; inputs are aligned pandas Series (observed RT price, merit-order vc, tightness 0–1.5)
  - `apply_markup(vc: float, tightness: float, curve: list[tuple[float, float]]) -> float` — piecewise-linear interpolation of markup curve applied to vc
- Consumed by: Task 8 (weekly-refit loader), Task 9 (display).

- [ ] **Step 1: Write the failing test (append to test_merit_order.py)**

```python
import numpy as np
import pandas as pd
from services.bess_map.price_lab.merit_order import fit_markup, apply_markup


def test_fit_markup_monotone_and_floored():
    rng = np.random.default_rng(0)
    n = 500
    tight = rng.uniform(0.1, 1.2, n)
    structural = np.full(n, 250.0)
    observed = 250.0 * (1.0 + np.maximum(tight - 0.6, 0) * 2) + rng.normal(0, 5, n)
    curve = fit_markup(pd.Series(observed), pd.Series(structural), pd.Series(tight), n_bins=4)
    markups = [m for _, m in curve]
    assert all(m >= 1.0 for m in markups)
    assert markups == sorted(markups)
    # tight hours should price above base
    assert apply_markup(250.0, 1.1, curve) > apply_markup(250.0, 0.2, curve)


def test_apply_markup_interpolates():
    curve = [(0.0, 1.0), (1.0, 2.0)]
    assert apply_markup(100.0, 0.5, curve) == 150.0
    assert apply_markup(100.0, -1.0, curve) == 100.0   # clamp low
    assert apply_markup(100.0, 5.0, curve) == 200.0    # clamp high


def test_fit_markup_empty_returns_flat():
    curve = fit_markup(pd.Series([], dtype=float), pd.Series([], dtype=float),
                       pd.Series([], dtype=float))
    assert curve == [(0.0, 1.0)]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_merit_order.py -q -k markup`
Expected: FAIL (`ImportError: cannot import name 'fit_markup'`)

- [ ] **Step 3: Implement (append to merit_order.py)**

```python
# ── Scarcity markup calibration ─────────────────────────────────────────────

def fit_markup(observed, structural, tightness, n_bins=5):
    """Median observed/structural ratio per tightness bin, floored at 1.0,
    enforced monotone non-decreasing (isotonic via cummax)."""
    import numpy as np
    obs = np.asarray(observed, dtype=float)
    st = np.asarray(structural, dtype=float)
    ti = np.asarray(tightness, dtype=float)
    mask = np.isfinite(obs) & np.isfinite(st) & np.isfinite(ti) & (st > 0)
    if mask.sum() < 20:
        return [(0.0, 1.0)]
    obs, st, ti = obs[mask], st[mask], ti[mask]
    edges = np.quantile(ti, np.linspace(0, 1, n_bins + 1))
    edges[0], edges[-1] = -np.inf, np.inf
    pts = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        m = (ti >= lo) & (ti < hi)
        ratio = np.median(obs[m] / st[m]) if m.sum() else 1.0
        pts.append((float((lo + hi) / 2) if np.isfinite(lo + hi) else float(ti.mean()),
                    max(1.0, float(ratio))))
    # monotone enforcement + keep representative tightness values
    out, best = [], 1.0
    for t, mk in pts:
        best = max(best, mk)
        out.append((t, best))
    return out


def apply_markup(vc, tightness, curve):
    if not curve:
        return vc
    xs = [p[0] for p in curve]
    ys = [p[1] for p in curve]
    t = min(max(tightness, xs[0]), xs[-1])
    for i in range(len(xs) - 1):
        if xs[i] <= t <= xs[i + 1]:
            if xs[i + 1] == xs[i]:
                return vc * ys[i]
            frac = (t - xs[i]) / (xs[i + 1] - xs[i])
            return vc * (ys[i] + frac * (ys[i + 1] - ys[i]))
    return vc * ys[-1]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_merit_order.py -q`
Expected: 8 PASS

- [ ] **Step 5: Commit**

```bash
git add services/bess_map/price_lab/merit_order.py tests/services/price_lab/test_merit_order.py
git commit -m "Add scarcity markup calibration to merit-order model

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5: `price_lab/pca_shapes.py` — extract `compute_pca` from app.py + deviation matrix builder

**Files:**
- Create: `services/bess_map/price_lab/pca_shapes.py`
- Modify: `apps/bess-map/app.py:1051-1082` (replace the `compute_pca` body with a delegating import — keep the public name so the existing PCA tab keeps working unchanged)
- Test: `tests/services/price_lab/test_pca_shapes.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces:
  - `compute_pca(price_matrix: pd.DataFrame, n_pcs: int = 4) -> dict` — IDENTICAL signature/return as the current `app.py:1051` implementation (`loadings, eigenvalues, variance_explained, mean_profile, n_days, scores, dates`)
  - `build_deviation_matrix(hourly_prices: pd.DataFrame) -> pd.DataFrame` — input: DatetimeIndex hourly df with `rt_price`; output: (days × 24) matrix of `rt_price − daily_mean`, hours as columns 0–23, days with < 24 hours dropped
- app.py's `compute_pca` becomes `from services.bess_map.price_lab.pca_shapes import compute_pca` re-export.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/price_lab/test_pca_shapes.py
import numpy as np
import pandas as pd
from services.bess_map.price_lab.pca_shapes import build_deviation_matrix, compute_pca


def _toy_days(n_days=60, seed=1):
    rng = np.random.default_rng(seed)
    hours = np.arange(24)
    days = []
    for d in range(n_days):
        level = 300 + rng.normal(0, 30)
        duck = -80 * np.exp(-((hours - 12) ** 2) / 8) * (1 + 0.2 * rng.normal())
        peak = 120 * np.exp(-((hours - 19) ** 2) / 6) * (1 + 0.2 * rng.normal())
        days.append(level + duck + peak + rng.normal(0, 5, 24))
    idx = pd.date_range("2026-01-01", periods=n_days * 24, freq="h")
    vals = np.repeat(np.arange(n_days), 24)
    df = pd.DataFrame({"rt_price": np.array(days).ravel()}, index=idx)
    df["_day"] = vals
    return df


def test_deviation_matrix_shape_and_zero_mean():
    df = _toy_days(30)
    mat = build_deviation_matrix(df)
    assert mat.shape == (30, 24)
    assert np.allclose(mat.mean(axis=1).values, 0.0, atol=1e-8)


def test_deviation_matrix_drops_short_days():
    df = _toy_days(10).iloc[:-5]   # last day has 19 hours
    mat = build_deviation_matrix(df)
    assert mat.shape[0] == 9


def test_compute_pca_recovers_dominant_shape():
    df = _toy_days(60)
    mat = build_deviation_matrix(df)
    out = compute_pca(mat, n_pcs=3)
    assert len(out["loadings"]) == 3
    assert out["variance_explained"][0] > 40.0     # duck+peak dominate
    assert out["scores"].shape == (60, 3)
    # reconstruction with all 3 pcs gets reasonably close
    recon = np.array(out["scores"]) @ np.array([v / (sum(v) / 24) if abs(sum(v)) > 1e-10 else v for v in out["loadings"]]).T \
        if False else None  # reconstruction correctness is Task 6's concern
```

NOTE for the implementer: the last assertion block above is intentionally inert — reconstruction is tested in Task 6. Keep only the first three assertions of `test_compute_pca_recovers_dominant_shape`.

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_pca_shapes.py -q`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `pca_shapes.py`**

Move the `compute_pca` implementation VERBATIM from `apps/bess-map/app.py:1051-1082` (eigendecomposition with sum=24 loading normalisation — do not change the math; the existing PCA tab's visuals depend on it), **with one addition**: the returned dict gains `"_raw_eigvecs": eig_vecs[:, :n_keep]` (ndarray, 24 × n_keep — the un-normalised eigenvectors used for the score projection; Task 6 needs them for exact reconstruction; the existing tab only reads the documented keys and is unaffected).

Add:

```python
def build_deviation_matrix(hourly_prices: pd.DataFrame) -> pd.DataFrame:
    """(days × 24) matrix of rt_price − daily mean. Days with <24 hours dropped."""
    df = hourly_prices.copy()
    df["_day"] = df.index.date
    df["_hour"] = df.index.hour
    counts = df.groupby("_day")["_hour"].count()
    full_days = counts[counts == 24].index
    df = df[df["_day"].isin(set(full_days))]
    mat = df.pivot_table(index="_day", columns="_hour", values="rt_price")
    mat = mat.sort_index().reindex(columns=list(range(24)))
    return mat.sub(mat.mean(axis=1), axis=0)
```

Then in `apps/bess-map/app.py`: delete the moved function body, replace with `from services.bess_map.price_lab.pca_shapes import compute_pca` at module scope near the other service imports, keeping the name available for the existing tab.

- [ ] **Step 4: Run tests to verify they pass + regression-test the existing PCA tab code path**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/ apps/bess-map/tests/ -q`
Expected: all PASS (the def-order AST guards in `apps/bess-map/tests/test_app_def_order.py` must stay green — the import line must sit with the other service imports, not after first use)

- [ ] **Step 5: Commit**

```bash
git add services/bess_map/price_lab/pca_shapes.py apps/bess-map/app.py tests/services/price_lab/test_pca_shapes.py
git commit -m "Extract compute_pca into price_lab and add deviation matrix builder

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6: Score forecasting (closed-form ridge) + curve reconstruction

**Files:**
- Modify: `services/bess_map/price_lab/pca_shapes.py` (append)
- Test: `tests/services/price_lab/test_pca_shapes.py` (append)

**Interfaces:**
- Consumes: `compute_pca` output dict from Task 5.
- Produces:
  - `fit_score_models(scores: np.ndarray, features: pd.DataFrame, lam: float = 1.0) -> np.ndarray` — ridge weights, shape (n_features+1, n_pcs), first row = intercept; features standardized inside (returns also `feat_mean`, `feat_std` — see below; return `(weights, feat_mean, feat_std)` tuple)
  - `predict_scores(weights: np.ndarray, feat_mean: pd.Series, feat_std: pd.Series, features: pd.DataFrame) -> np.ndarray` — (n_days, n_pcs)
  - `reconstruct_shape(loadings: list[np.ndarray], scores_row: np.ndarray) -> np.ndarray` — (24,) deviation curve, using the RAW eigenvector projection convention consistent with `compute_pca`'s scores (see implementation note)
  - `FEATURE_COLUMNS = ["load_d1_mw", "renewable_total_d1_mw", "bidding_space_d1_mw", "wind_d1_mw", "solar_d1_mw", "net_import_share", "landing_price", "dow", "month"]`

- [ ] **Step 1: Write the failing test**

```python
def test_ridge_recovers_linear_scores():
    import numpy as np, pandas as pd
    from services.bess_map.price_lab.pca_shapes import fit_score_models, predict_scores
    rng = np.random.default_rng(2)
    n, k = 200, 2
    X = pd.DataFrame({"load_d1_mw": rng.normal(25000, 3000, n),
                      "bidding_space_d1_mw": rng.normal(15000, 2000, n)})
    true_w = np.array([[10.0, -5.0], [0.02, 0.0], [0.0, 0.03]])
    Xs = (X - X.mean()) / X.std(ddof=0)
    scores = np.column_stack([np.ones(n)] + [Xs.values]) @ true_w + rng.normal(0, 0.5, (n, k))
    w, mu, sd = fit_score_models(scores, X, lam=1.0)
    pred = predict_scores(w, mu, sd, X)
    assert np.corrcoef(pred[:, 0], scores[:, 0])[0, 1] > 0.95
    assert np.corrcoef(pred[:, 1], scores[:, 1])[0, 1] > 0.95


def test_reconstruct_shape_matches_projection():
    import numpy as np
    from services.bess_map.price_lab.pca_shapes import reconstruct_shape, compute_pca
    rng = np.random.default_rng(3)
    mat = pd.DataFrame(rng.normal(0, 50, (40, 24)))
    out = compute_pca(mat, n_pcs=4)
    # reconstruct day 0 from its scores must match the raw eigenvector projection
    recon = reconstruct_shape(out["loadings"], out["scores"][0], out.get("_raw_eigvecs"))
    centered = mat.values[0] - out["mean_profile"]
    raw_proj = centered @ out["_raw_eigvecs"] @ out["_raw_eigvecs"].T
    assert np.allclose(recon, raw_proj, atol=1e-6)
```

IMPLEMENTATION NOTE: `compute_pca` returns normalised loadings (sum=24) for display but computes scores with the RAW eigenvectors, which it also returns as `"_raw_eigvecs"` (added in Task 5). Exact reconstruction must therefore use the raw eigenvectors, not the display loadings: `reconstruct_shape(loadings, scores_row, raw_eigvecs)` returns `scores_row @ raw_eigvecs.T`. The `loadings` parameter is kept for API clarity but is not used in the computation.

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_pca_shapes.py -q -k "ridge or reconstruct"`
Expected: FAIL (names not defined)

- [ ] **Step 3: Implement (append to pca_shapes.py)**

```python
FEATURE_COLUMNS = ["load_d1_mw", "renewable_total_d1_mw", "bidding_space_d1_mw",
                   "wind_d1_mw", "solar_d1_mw", "net_import_share",
                   "landing_price", "dow", "month"]


def fit_score_models(scores, features, lam=1.0):
    import numpy as np
    X = features.astype(float)
    mu, sd = X.mean(), X.std(ddof=0).replace(0, 1.0)
    Xs = ((X - mu) / sd).values
    Xd = np.column_stack([np.ones(len(Xs)), Xs])
    I = np.eye(Xd.shape[1]); I[0, 0] = 0.0   # don't penalise intercept
    w = np.linalg.solve(Xd.T @ Xd + lam * I, Xd.T @ np.asarray(scores))
    return w, mu, sd


def predict_scores(weights, feat_mean, feat_std, features):
    import numpy as np
    Xs = ((features.astype(float) - feat_mean) / feat_std).values
    return np.column_stack([np.ones(len(Xs)), Xs]) @ weights


def reconstruct_shape(loadings, scores_row, raw_eigvecs):
    import numpy as np
    return np.asarray(scores_row, dtype=float) @ np.asarray(raw_eigvecs).T
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/ -q`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add services/bess_map/price_lab/pca_shapes.py tests/services/price_lab/test_pca_shapes.py
git commit -m "Add ridge score forecasting and shape reconstruction to pca_shapes

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 7: `price_lab/hybrid.py` — level + shape combination

**Files:**
- Create: `services/bess_map/price_lab/hybrid.py`
- Test: `tests/services/price_lab/test_hybrid.py`

**Interfaces:**
- Consumes: merit-order level series (Task 3/4), PCA shape series (Task 6).
- Produces:
  - `combine(level: np.ndarray, shape: np.ndarray, clip_lo: float, clip_hi: float) -> np.ndarray` — elementwise `clip(level + shape, clip_lo, clip_hi)`; `nan` propagates from either input
  - `clip_bounds(prices: pd.Series) -> tuple[float, float]` — (0.1%, 99.9%) quantiles of the series; `(0.0, 1500.0)` when empty

- [ ] **Step 1: Write the failing test**

```python
# tests/services/price_lab/test_hybrid.py
import numpy as np
import pandas as pd
from services.bess_map.price_lab.hybrid import combine, clip_bounds


def test_combine_adds_and_clips():
    level = np.array([300.0, 300.0, 300.0])
    shape = np.array([-400.0, 0.0, 400.0])
    out = combine(level, shape, clip_lo=-80.0, clip_hi=1500.0)
    assert out[0] == -80.0 and out[1] == 300.0 and out[2] == 700.0


def test_combine_nan_propagates():
    out = combine(np.array([np.nan, 300.0]), np.array([10.0, np.nan]), -80.0, 1500.0)
    assert np.isnan(out[0]) and np.isnan(out[1])


def test_clip_bounds_quantiles_and_empty():
    s = pd.Series(np.concatenate([np.full(50, -500.0), np.full(895, 400.0), np.full(55, 3000.0)]))
    lo, hi = clip_bounds(s)
    assert lo <= 400.0 <= hi and lo < 0.0
    assert clip_bounds(pd.Series([], dtype=float)) == (0.0, 1500.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_hybrid.py -q`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement `hybrid.py`**

```python
"""Hybrid price forecast = structural level (merit-order) + statistical shape (PCA)."""
from __future__ import annotations

import numpy as np
import pandas as pd


def combine(level, shape, clip_lo, clip_hi):
    level = np.asarray(level, dtype=float)
    shape = np.asarray(shape, dtype=float)
    return np.clip(level + shape, clip_lo, clip_hi)


def clip_bounds(prices: pd.Series) -> tuple[float, float]:
    s = pd.to_numeric(prices, errors="coerce").dropna()
    if s.empty:
        return (0.0, 1500.0)
    return (float(s.quantile(0.001)), float(s.quantile(0.999)))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/ -q`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add services/bess_map/price_lab/hybrid.py tests/services/price_lab/test_hybrid.py
git commit -m "Add hybrid level+shape price forecast combiner

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 8: Tab data loaders (`price_forecast_tab.py` — DB layer)

**Files:**
- Create: `apps/bess-map/price_forecast_tab.py`
- Test: `tests/services/price_lab/test_tab_loaders.py` (loaders tested with mocked `pd.read_sql`)

**Interfaces:**
- Consumes: `marketdata.province_fuel_fleet` (Task 1), `staging.interconnector_trades` + `staging.interconnector_channels`, `marketdata.spot_fundamentals_hourly`, `marketdata.spot_prices_hourly`.
- Produces (all plain functions taking a SQLAlchemy engine; caching applied by the caller):
  - `load_confirmed_fuel_fleet(eng, province: str) -> dict | None` — latest confirmed row; `{"coal_price_yuan_t": float|None, "gas_price_yuan_m3": float|None, "fleet_segments": list[dict], "effective_date": date}`
  - `load_import_blocks(eng, recv_province: str) -> list[dict]` — per active channel: `{"label", "price_yuan_mwh", "capacity_mw"}`; price = latest month's `land_price × 1000` (¥/kWh→¥/MWh) from `interconnector_trades` for that recv_province, capacity = `gw × 1000` from `interconnector_channels` where `recv_prov` matches
  - `load_fundamentals_d1(eng, province: str, start: date, end: date) -> pd.DataFrame` — hourly: `load_d1_mw, renewable_total_d1_mw, bidding_space_d1_mw, wind_d1_mw, solar_d1_mw, net_export_d1_mw` (fall back to realized `*_mw` columns where the `_d1` variant is NULL, using `COALESCE`)
  - `load_rt_prices(eng, province: str, start: date, end: date) -> pd.DataFrame` — DatetimeIndex hourly, column `rt_price`
  - `compute_net_import_share(fund_df: pd.DataFrame) -> pd.Series` — `clip(-net_export_d1_mw / load_d1_mw, -1, 1)` (positive = importing)
  - `load_landing_price_latest(eng, recv_province: str) -> float | None` — volume-weighted mean `land_price` of the latest month in `interconnector_trades`

- [ ] **Step 1: Write the failing test**

```python
# tests/services/price_lab/test_tab_loaders.py
from unittest.mock import patch
import pandas as pd
import apps.bess_map.price_forecast_tab as pft  # see note on import path below


def test_load_confirmed_fuel_fleet_returns_latest():
    df = pd.DataFrame({
        "coal_price_yuan_t": [850.0], "gas_price_yuan_m3": [3.1],
        "fleet_segments": [[{"fuel": "coal", "capacity_mw": 40000,
                             "heat_rate_kj_kwh": 8200, "vom_yuan_mwh": 12, "label": "c1"}]],
        "effective_date": [pd.Timestamp("2026-01-01").date()],
    })
    with patch("pandas.read_sql", return_value=df):
        out = pft.load_confirmed_fuel_fleet(None, "山东")
    assert out["coal_price_yuan_t"] == 850.0
    assert out["fleet_segments"][0]["label"] == "c1"


def test_load_confirmed_fuel_fleet_none_when_empty():
    with patch("pandas.read_sql", return_value=pd.DataFrame()):
        assert pft.load_confirmed_fuel_fleet(None, "山东") is None


def test_import_blocks_convert_price_and_capacity():
    trades = pd.DataFrame({"channel_1": ["锦苏直流"], "land_price": [0.30], "month_start": [pd.Timestamp("2026-08-01")]})
    channels = pd.DataFrame({"name": ["锦苏直流"], "gw": [7.2]})
    def fake_read_sql(sql, eng, params=None):
        return trades if "trades" in str(sql) else channels
    with patch("pandas.read_sql", side_effect=fake_read_sql):
        blocks = pft.load_import_blocks(None, "江苏")
    assert blocks[0]["price_yuan_mwh"] == 300.0
    assert blocks[0]["capacity_mw"] == 7200.0
    assert blocks[0]["label"] == "锦苏直流"


def test_net_import_share_sign():
    fund = pd.DataFrame({"net_export_d1_mw": [-2000.0, 1000.0], "load_d1_mw": [20000.0, 20000.0]})
    share = pft.compute_net_import_share(fund)
    assert share.iloc[0] == 0.1 and share.iloc[1] == -0.05
```

IMPORT-PATH NOTE: `apps/bess-map` contains a hyphen and is not a package. Tests must do what `apps/bess-map/tests/test_irr_helpers.py` already does — check that file's sys.path shim and copy it verbatim (it inserts `apps/bess-map` into `sys.path` and imports the module by filename). If no such shim exists there yet, use:

```python
import sys, importlib.util
from pathlib import Path
def _load(name):
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).resolve().parents[3] / "apps" / "bess-map" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
pft = _load("price_forecast_tab")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_tab_loaders.py -q`
Expected: FAIL (module/file not found)

- [ ] **Step 3: Implement `apps/bess-map/price_forecast_tab.py` loader half**

```python
"""Price Forecast tab — DB loaders (top) + Streamlit UI (bottom, Tasks 9-11).

Only this module touches the DB for the price-forecast feature. All loaders
take a SQLAlchemy engine; Streamlit caching is applied in the UI layer.
"""
from __future__ import annotations

import json
from datetime import date
import pandas as pd


def load_confirmed_fuel_fleet(eng, province: str):
    df = pd.read_sql(
        """SELECT coal_price_yuan_t, gas_price_yuan_m3, fleet_segments, effective_date
           FROM marketdata.province_fuel_fleet
           WHERE province = %(p)s AND status = 'confirmed'
           ORDER BY effective_date DESC, ingested_at DESC LIMIT 1""",
        eng, params={"p": province})
    if df.empty:
        return None
    r = df.iloc[0]
    segs = r["fleet_segments"]
    if isinstance(segs, str):
        segs = json.loads(segs)
    return {"coal_price_yuan_t": r["coal_price_yuan_t"],
            "gas_price_yuan_m3": r["gas_price_yuan_m3"],
            "fleet_segments": segs,
            "effective_date": r["effective_date"]}


def load_import_blocks(eng, recv_province: str) -> list[dict]:
    trades = pd.read_sql(
        """SELECT DISTINCT ON (channel_1) channel_1, land_price
           FROM staging.interconnector_trades
           WHERE recv_province = %(p)s AND channel_1 IS NOT NULL AND land_price IS NOT NULL
           ORDER BY channel_1, month_start DESC""",
        eng, params={"p": recv_province})
    if trades.empty:
        return []
    channels = pd.read_sql("SELECT name, gw FROM staging.interconnector_channels", eng)
    cap = dict(zip(channels["name"], channels["gw"]))
    blocks = []
    for _, r in trades.iterrows():
        gw = cap.get(r["channel_1"])
        blocks.append({"label": r["channel_1"],
                       "price_yuan_mwh": float(r["land_price"]) * 1000.0,
                       "capacity_mw": float(gw) * 1000.0 if gw else 1000.0})
    return blocks


def load_fundamentals_d1(eng, province: str, start: date, end: date) -> pd.DataFrame:
    df = pd.read_sql(
        """SELECT datetime,
                  COALESCE(load_d1_mw, load_mw)                       AS load_d1_mw,
                  COALESCE(renewable_total_d1_mw, renewable_total_mw) AS renewable_total_d1_mw,
                  COALESCE(bidding_space_d1_mw, bidding_space_mw)     AS bidding_space_d1_mw,
                  COALESCE(wind_d1_mw, wind_mw)                       AS wind_d1_mw,
                  COALESCE(solar_d1_mw, solar_mw)                     AS solar_d1_mw,
                  COALESCE(net_export_d1_mw, net_export_mw)           AS net_export_d1_mw
           FROM marketdata.spot_fundamentals_hourly
           WHERE province = %(p)s AND datetime BETWEEN %(s)s AND %(e)s
           ORDER BY datetime""",
        eng, params={"p": province, "s": start, "e": end})
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.set_index("datetime")


def load_rt_prices(eng, province: str, start: date, end: date) -> pd.DataFrame:
    df = pd.read_sql(
        """SELECT datetime, rt_price FROM marketdata.spot_prices_hourly
           WHERE province = %(p)s AND datetime BETWEEN %(s)s AND %(e)s
           ORDER BY datetime""",
        eng, params={"p": province, "s": start, "e": end})
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.set_index("datetime")


def compute_net_import_share(fund_df: pd.DataFrame) -> pd.Series:
    share = -fund_df["net_export_d1_mw"] / fund_df["load_d1_mw"]
    return share.clip(-1, 1).fillna(0.0)


def load_landing_price_latest(eng, recv_province: str):
    df = pd.read_sql(
        """SELECT SUM(vol_post_mwh * land_price) / NULLIF(SUM(vol_post_mwh), 0) AS wavg
           FROM staging.interconnector_trades
           WHERE recv_province = %(p)s AND land_price IS NOT NULL
             AND month_start = (SELECT MAX(month_start) FROM staging.interconnector_trades
                                WHERE recv_province = %(p)s)""",
        eng, params={"p": recv_province})
    if df.empty or df.iloc[0]["wavg"] is None:
        return None
    return float(df.iloc[0]["wavg"]) * 1000.0   # ¥/kWh → ¥/MWh
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_tab_loaders.py -q`
Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add apps/bess-map/price_forecast_tab.py tests/services/price_lab/test_tab_loaders.py
git commit -m "Add price forecast tab DB loaders

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 9: Tab section ① — merit-order explorer UI

**Files:**
- Modify: `apps/bess-map/price_forecast_tab.py` (append `render_merit_order_explorer`)
- Test: `tests/services/price_lab/test_explorer_logic.py` (UI-free logic only)

**Interfaces:**
- Consumes: Tasks 1–4 (fuel fleet, stack, markup), Task 8 loaders.
- Produces:
  - `compute_daily_stack_series(stack: list[dict], residual_demand: pd.Series, markup_curve: list[tuple[float,float]], total_capacity_mw: float) -> pd.Series` — per-hour marginal price incl. markup; tightness = residual / total_capacity
  - `render_merit_order_explorer(st, eng, provinces: list[str]) -> None` — Streamlit section

- [ ] **Step 1: Write the failing test**

```python
# tests/services/price_lab/test_explorer_logic.py
import numpy as np
import pandas as pd
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "price_forecast_tab",
    Path(__file__).resolve().parents[3] / "apps" / "bess-map" / "price_forecast_tab.py")
pft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pft)


def test_daily_stack_series_applies_markup():
    stack = [{"label": "c1", "vc_yuan_mwh": 250.0, "capacity_mw": 10000, "is_import": False}]
    demand = pd.Series([5000.0, 9500.0], index=pd.date_range("2026-09-01", periods=2, freq="h"))
    curve = [(0.0, 1.0), (1.0, 2.0)]
    out = pft.compute_daily_stack_series(stack, demand, curve, total_capacity_mw=10000)
    assert out.iloc[0] == 250.0 * 1.0         # tightness 0.5 → markup 1.0
    assert abs(out.iloc[1] - 250.0 * 1.9) < 1e-6  # tightness 0.95 → markup ≈1.9
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_explorer_logic.py -q`
Expected: FAIL (attribute missing)

- [ ] **Step 3: Implement (append to price_forecast_tab.py)**

```python
def compute_daily_stack_series(stack, residual_demand, markup_curve, total_capacity_mw):
    from services.bess_map.price_lab.merit_order import marginal_price, apply_markup
    out = {}
    for ts, dem in residual_demand.items():
        vc = marginal_price(stack, dem)
        if vc != vc:                      # nan — demand above stack
            out[ts] = float("nan")
            continue
        tightness = dem / total_capacity_mw if total_capacity_mw else 0.0
        out[ts] = apply_markup(vc, tightness, markup_curve)
    return pd.Series(out)


def render_merit_order_explorer(st, eng, provinces):
    """Section ①: stack chart + marginal price marker + what-if fuel inputs."""
    from services.bess_map.price_lab.merit_order import build_stack
    import plotly.graph_objects as go

    prov = st.selectbox("省份 / Province", provinces, key="pf_mo_prov")
    ff = load_confirmed_fuel_fleet(eng, prov)
    if ff is None:
        st.info("该省份暂无已确认的燃料/装机数据（Hermes 扫描结果为 draft，待确认）。")
        return
    c1, c2 = st.columns(2)
    coal = c1.number_input("煤价 (¥/t)", value=float(ff["coal_price_yuan_t"] or 850.0), key="pf_coal")
    gas = c2.number_input("气价 (¥/m³)", value=float(ff["gas_price_yuan_m3"] or 3.0), key="pf_gas")
    day = st.date_input("日期", key="pf_mo_day")

    fund = load_fundamentals_d1(eng, prov, day, day)
    if fund.empty:
        st.info("该日无电网预测数据。")
        return
    renewable_mw = float(fund["renewable_total_d1_mw"].mean())
    imports = load_import_blocks(eng, prov)
    stack = build_stack(ff["fleet_segments"], coal, gas,
                        import_blocks=imports, renewable_mw=renewable_mw)

    # step chart
    fig = go.Figure()
    x = 0.0
    for seg in stack:
        fig.add_trace(go.Bar(x=[seg["capacity_mw"]], y=[seg["vc_yuan_mwh"]],
                             base=x, name=seg["label"], width=1.0,
                             marker_color="#d62728" if seg["is_import"] else None))
        x += seg["capacity_mw"]
    fig.update_layout(barmode="stack", barnorm=None, xaxis_title="累计容量 (MW)",
                      yaxis_title="变动成本 (¥/MWh)", height=420, showlegend=True)
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"进口/通道块以红色显示；数据源：province_fuel_fleet "
               f"({ff['effective_date']}) + interconnector_trades 最新月落地价。")
```

NOTE: the step chart is intentionally simple (stacked bar with base offsets). Polish (hover, residual-demand marker line) is acceptable to add during implementation; do not add scenario persistence.

- [ ] **Step 4: Run test + AppTest smoke on the tab import**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_explorer_logic.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add apps/bess-map/price_forecast_tab.py tests/services/price_lab/test_explorer_logic.py
git commit -m "Add merit-order explorer section to price forecast tab

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 10: Tab section ② — PCA decomposition + score forecast chart

**Files:**
- Modify: `apps/bess-map/price_forecast_tab.py` (append `render_pca_section`)
- Test: `tests/services/price_lab/test_pca_section_logic.py`

**Interfaces:**
- Consumes: Tasks 5–6 (compute_pca, fit/predict scores, FEATURE_COLUMNS), Task 8 loaders.
- Produces:
  - `build_feature_frame(fund_df: pd.DataFrame, net_import_share: pd.Series, landing_price: float | None) -> pd.DataFrame` — hourly frame with exactly `FEATURE_COLUMNS` columns (`dow`, `month` derived from index); missing landing_price → column filled with 0.0
  - `render_pca_section(st, eng, provinces) -> None`

- [ ] **Step 1: Write the failing test**

```python
# tests/services/price_lab/test_pca_section_logic.py
import pandas as pd
import numpy as np
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "price_forecast_tab",
    Path(__file__).resolve().parents[3] / "apps" / "bess-map" / "price_forecast_tab.py")
pft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pft)

from services.bess_map.price_lab.pca_shapes import FEATURE_COLUMNS


def test_feature_frame_columns_and_calendar():
    idx = pd.date_range("2026-09-01", periods=48, freq="h")
    fund = pd.DataFrame({"load_d1_mw": 20000.0, "renewable_total_d1_mw": 5000.0,
                         "bidding_space_d1_mw": 15000.0, "wind_d1_mw": 3000.0,
                         "solar_d1_mw": 2000.0, "net_export_d1_mw": -1000.0}, index=idx)
    share = pft.compute_net_import_share(fund)
    ff = pft.build_feature_frame(fund, share, landing_price=300.0)
    assert list(ff.columns) == FEATURE_COLUMNS
    assert ff["dow"].iloc[0] == idx[0].dayofweek
    assert ff["month"].iloc[0] == 9
    assert (ff["landing_price"] == 300.0).all()


def test_feature_frame_none_landing_fills_zero():
    idx = pd.date_range("2026-09-01", periods=24, freq="h")
    fund = pd.DataFrame({"load_d1_mw": 20000.0, "renewable_total_d1_mw": 5000.0,
                         "bidding_space_d1_mw": 15000.0, "wind_d1_mw": 3000.0,
                         "solar_d1_mw": 2000.0, "net_export_d1_mw": -1000.0}, index=idx)
    ff = pft.build_feature_frame(fund, pft.compute_net_import_share(fund), None)
    assert (ff["landing_price"] == 0.0).all()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_pca_section_logic.py -q`
Expected: FAIL

- [ ] **Step 3: Implement (append to price_forecast_tab.py)**

```python
def build_feature_frame(fund_df, net_import_share, landing_price):
    from services.bess_map.price_lab.pca_shapes import FEATURE_COLUMNS
    ff = fund_df[["load_d1_mw", "renewable_total_d1_mw", "bidding_space_d1_mw",
                  "wind_d1_mw", "solar_d1_mw"]].copy()
    ff["net_import_share"] = net_import_share
    ff["landing_price"] = float(landing_price) if landing_price is not None else 0.0
    ff["dow"] = ff.index.dayofweek
    ff["month"] = ff.index.month
    return ff[FEATURE_COLUMNS]


def render_pca_section(st, eng, provinces):
    """Section ②: scree + component shapes + score-vs-driver history."""
    from services.bess_map.price_lab.pca_shapes import (
        build_deviation_matrix, compute_pca)
    import plotly.graph_objects as go

    prov = st.selectbox("省份 / Province", provinces, key="pf_pca_prov")
    days = st.slider("训练窗口 (天)", 90, 365, 365, key="pf_pca_days")
    end = pd.Timestamp.now().date()
    start = end - pd.Timedelta(days=days)
    prices = load_rt_prices(eng, prov, start, end)
    if prices.empty:
        st.info("无价格数据。")
        return
    mat = build_deviation_matrix(prices)
    if len(mat) < 30:
        st.info(f"完整日数据不足 ({len(mat)} 天 < 30)。")
        return
    n_pcs = st.slider("主成分个数", 2, 6, 4, key="pf_pca_k")
    out = compute_pca(mat, n_pcs=n_pcs)

    var = out["variance_explained"][:n_pcs]
    fig_scree = go.Figure(go.Bar(x=[f"PC{i+1}" for i in range(n_pcs)], y=var))
    fig_scree.update_layout(title="方差解释率 (%)", height=260)
    st.plotly_chart(fig_scree, use_container_width=True)

    fig_load = go.Figure()
    for i, vec in enumerate(out["loadings"]):
        fig_load.add_trace(go.Scatter(x=list(range(24)), y=vec, mode="lines", name=f"PC{i+1}"))
    fig_load.update_layout(title="主成分形状 (sum=24 归一)", height=320)
    st.plotly_chart(fig_load, use_container_width=True)
    st.caption("得分为原始特征向量投影；形状预测模型见 ③。")
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_pca_section_logic.py -q`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add apps/bess-map/price_forecast_tab.py tests/services/price_lab/test_pca_section_logic.py
git commit -m "Add PCA decomposition section to price forecast tab

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 11: Tab section ③ — forecast vs actual + backtest table

**Files:**
- Modify: `apps/bess-map/price_forecast_tab.py` (append `run_hybrid_forecast`, `run_backtest`, `render_forecast_section`)
- Test: `tests/services/price_lab/test_backtest.py`

**Interfaces:**
- Consumes: everything from Tasks 3–10.
- Produces:
  - `run_hybrid_forecast(eng, province: str, target_date: date, train_days: int = 365) -> pd.DataFrame` — hourly frame for target_date with columns `level, shape, forecast, actual` (actual = realized RT; may be empty for future dates)
  - `run_backtest(eng, province: str, n_days: int = 90) -> dict` — `{"hybrid": (mae, smape), "merit_only": (mae, smape), "pca_only": (mae, smape), "naive_lag1": (mae, smape)}`
  - `smape(actual: np.ndarray, pred: np.ndarray) -> float`

- [ ] **Step 1: Write the failing test**

```python
# tests/services/price_lab/test_backtest.py
import numpy as np
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "price_forecast_tab",
    Path(__file__).resolve().parents[3] / "apps" / "bess-map" / "price_forecast_tab.py")
pft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pft)


def test_smape_definition():
    a = np.array([100.0, 200.0])
    p = np.array([110.0, 190.0])
    v = pft.smape(a, p)
    expect = np.mean([10/105, 10/195]) * 100
    assert abs(v - expect) < 1e-9


def test_smape_skips_zero_pairs():
    a = np.array([0.0, 200.0])
    p = np.array([0.0, 210.0])
    v = pft.smape(a, p)
    assert abs(v - (10/205*100)) < 1e-9
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_backtest.py -q`
Expected: FAIL

- [ ] **Step 3: Implement (append to price_forecast_tab.py)**

```python
def smape(actual, pred):
    import numpy as np
    a, p = np.asarray(actual, float), np.asarray(pred, float)
    denom = (np.abs(a) + np.abs(p)) / 2.0
    mask = np.isfinite(a) & np.isfinite(p) & (denom > 0)
    if not mask.any():
        return float("nan")
    return float(np.mean(np.abs(a[mask] - p[mask]) / denom[mask]) * 100.0)


def run_hybrid_forecast(eng, province, target_date, train_days=365):
    """D-1 hybrid forecast for target_date: merit-order level + PCA shape."""
    from services.bess_map.price_lab.merit_order import build_stack, fit_markup, marginal_price, apply_markup
    from services.bess_map.price_lab.pca_shapes import (
        build_deviation_matrix, compute_pca, fit_score_models, predict_scores,
        reconstruct_shape)
    from services.bess_map.price_lab.hybrid import combine, clip_bounds

    train_start = target_date - pd.Timedelta(days=train_days)
    hist_start = target_date - pd.Timedelta(days=90)   # markup window
    prices = load_rt_prices(eng, province, train_start, target_date - pd.Timedelta(days=1))
    fund_train = load_fundamentals_d1(eng, province, train_start, target_date)
    ff = load_confirmed_fuel_fleet(eng, province)
    if prices.empty or fund_train.empty or ff is None:
        return pd.DataFrame()

    # --- level: merit order on target day, markup fit on last 90d
    coal = ff["coal_price_yuan_t"] or 850.0
    gas = ff["gas_price_yuan_m3"] or 3.0
    imports = load_import_blocks(eng, province)
    fund_target = fund_train[fund_train.index.date == target_date]
    if fund_target.empty:
        return pd.DataFrame()
    renewable_mw = float(fund_target["renewable_total_d1_mw"].mean())
    stack = build_stack(ff["fleet_segments"], coal, gas, imports, renewable_mw)
    total_cap = sum(s["capacity_mw"] for s in stack)

    hist = fund_train[fund_train.index < fund_target.index[0]].tail(90 * 24)
    if not hist.empty:
        vc_hist = np.array([marginal_price(build_stack(ff["fleet_segments"], coal, gas, imports,
                                                       renewable_mw=float(row["renewable_total_d1_mw"])),
                                           row["load_d1_mw"] - row["renewable_total_d1_mw"] - row["net_export_d1_mw"])
                            for _, row in hist.iterrows()])
        tight_hist = (hist["load_d1_mw"] - hist["renewable_total_d1_mw"] - hist["net_export_d1_mw"]) / total_cap
        actual_hist = prices.reindex(hist.index)["rt_price"]
        curve = fit_markup(actual_hist, pd.Series(vc_hist, index=hist.index), tight_hist)
    else:
        curve = [(0.0, 1.0)]

    level = []
    for ts, row in fund_target.iterrows():
        dem = row["load_d1_mw"] - row["renewable_total_d1_mw"] - row["net_export_d1_mw"]
        vc = marginal_price(stack, dem)
        level.append(apply_markup(vc, dem / total_cap, curve) if vc == vc else float("nan"))
    level = np.array(level)

    # --- shape: PCA on deviation matrix + ridge score forecast
    mat = build_deviation_matrix(prices)
    out = compute_pca(mat, n_pcs=4)
    fund_hist = fund_train.loc[mat.index[0]:mat.index[-1]] if len(mat) else fund_train.iloc[0:0]
    share_hist = compute_net_import_share(fund_hist)
    landing = load_landing_price_latest(eng, province)
    X_hist = build_feature_frame(fund_hist, share_hist, landing)
    # daily-aggregate features to match scores' daily rows
    X_daily = X_hist.groupby(X_hist.index.date).mean()
    common = min(len(X_daily), len(out["scores"]))
    w, mu, sd = fit_score_models(out["scores"][-common:], X_daily.iloc[-common:])
    share_t = compute_net_import_share(fund_target)
    X_t = build_feature_frame(fund_target, share_t, landing)
    X_t_daily = X_t.groupby(X_t.index.date).mean()
    score_pred = predict_scores(w, mu, sd, X_t_daily)[0]
    shape = reconstruct_shape(out["loadings"], score_pred, out["_raw_eigvecs"])

    lo, hi = clip_bounds(prices["rt_price"])
    fc = combine(level, shape, lo, hi)
    actual = load_rt_prices(eng, province, target_date, target_date)["rt_price"].reindex(fund_target.index)
    return pd.DataFrame({"level": level, "shape": shape, "forecast": fc,
                         "actual": actual.values}, index=fund_target.index)


def run_backtest(eng, province, n_days=90):
    end = pd.Timestamp.now().date() - pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=n_days)
    res = {"hybrid": [], "merit_only": [], "pca_only": [], "naive_lag1": []}
    prices = load_rt_prices(eng, province, start - pd.Timedelta(days=7), end)["rt_price"]
    for d in pd.date_range(start, end):
        r = run_hybrid_forecast(eng, province, d.date())
        if r.empty:
            continue
        a = r["actual"].values
        res["hybrid"].append((a, r["forecast"].values))
        res["merit_only"].append((a, r["level"].values))
        res["pca_only"].append((a, (np.nanmean(r["level"]) + r["shape"]).values))
        naive = prices.shift(24).reindex(r.index).values
        res["naive_lag1"].append((a, naive))
    out = {}
    for k, pairs in res.items():
        if not pairs:
            out[k] = (float("nan"), float("nan"))
            continue
        a = np.concatenate([p[0] for p in pairs])
        p = np.concatenate([p[1] for p in pairs])
        out[k] = (float(np.nanmean(np.abs(a - p))), smape(a, p))
    return out
```

`render_forecast_section(st, eng, provinces)`: date picker → `run_hybrid_forecast` → plotly line chart with `level`, `shape+level_mean`, `forecast`, `actual` traces → `run_backtest` (behind a button; ~90 × per-day full refits — cache with `@st.cache_data(ttl=3600)` keyed on province) → st.dataframe of the 4-model MAE/sMAPE table. Include the "无燃料数据 / 无电网预测" info guards.

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_backtest.py -q`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add apps/bess-map/price_forecast_tab.py tests/services/price_lab/test_backtest.py
git commit -m "Add hybrid forecast and backtest engine to price forecast tab

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 12: Data Management review section for fuel_fleet drafts

**Files:**
- Modify: `apps/bess-map/app.py` (Data Management tab — add a "燃料与装机数据确认" section next to the existing capcomp gap-fill area ~line 3152)
- Test: `tests/services/price_lab/test_fuel_fleet_review.py`

**Interfaces:**
- Consumes: `marketdata.province_fuel_fleet` (Task 1), `resolve_fuel_fleet_conflict` (Task 1).
- Produces:
  - `list_pending_fuel_fleet(eng) -> pd.DataFrame` — rows with `status IN ('draft','conflict')`, newest first
  - `confirm_fuel_fleet(eng, row_id: int) -> None` — set `status='confirmed'`
  - `dismiss_fuel_fleet(eng, row_id: int) -> None` — set `status='superseded'`

These three helpers live in `apps/bess-map/price_forecast_tab.py` (single DB module for the feature).

- [ ] **Step 1: Write the failing test**

```python
# tests/services/price_lab/test_fuel_fleet_review.py
from unittest.mock import patch, MagicMock
import pandas as pd
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "price_forecast_tab",
    Path(__file__).resolve().parents[3] / "apps" / "bess-map" / "price_forecast_tab.py")
pft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pft)


def test_confirm_sets_status():
    eng = MagicMock()
    pft.confirm_fuel_fleet(eng, 42)
    sql = str(eng.execute.call_args.args[0] if eng.execute.called else
              eng.begin.return_value.__enter__.return_value.execute.call_args.args[0])
    assert "confirmed" in sql and "province_fuel_fleet" in sql
```

IMPLEMENTATION NOTE: write `confirm_fuel_fleet`/`dismiss_fuel_fleet` with `with eng.begin() as conn: conn.execute(text(...), {"id": row_id})` so the mock assertion above works against `eng.begin.return_value.__enter__.return_value.execute`.

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/test_fuel_fleet_review.py -q`
Expected: FAIL

- [ ] **Step 3: Implement helpers + UI section**

Helpers in `price_forecast_tab.py`:

```python
def list_pending_fuel_fleet(eng):
    return pd.read_sql(
        """SELECT id, province, effective_date, coal_price_yuan_t, gas_price_yuan_m3,
                  status, source, notes, ingested_at
           FROM marketdata.province_fuel_fleet
           WHERE status IN ('draft', 'conflict')
           ORDER BY ingested_at DESC LIMIT 100""", eng)


def confirm_fuel_fleet(eng, row_id):
    from sqlalchemy import text
    with eng.begin() as conn:
        conn.execute(text(
            "UPDATE marketdata.province_fuel_fleet SET status = 'confirmed', "
            "ingested_at = NOW() WHERE id = :id"), {"id": row_id})


def dismiss_fuel_fleet(eng, row_id):
    from sqlalchemy import text
    with eng.begin() as conn:
        conn.execute(text(
            "UPDATE marketdata.province_fuel_fleet SET status = 'superseded', "
            "ingested_at = NOW() WHERE id = :id"), {"id": row_id})
```

UI in `apps/bess-map/app.py` Data Management tab: import the three helpers from `price_forecast_tab`, render `list_pending_fuel_fleet` in a dataframe, and per-row 确认/忽略 buttons calling confirm/dismiss with `st.rerun()`. Follow the existing capcomp review section's layout conventions (same expander + per-row button pattern used at ~line 3152).

- [ ] **Step 4: Run tests + full bess-map unit suite**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/ apps/bess-map/tests/ -q`
Expected: all PASS incl. def-order AST guards

- [ ] **Step 5: Commit**

```bash
git add apps/bess-map/app.py apps/bess-map/price_forecast_tab.py tests/services/price_lab/test_fuel_fleet_review.py
git commit -m "Add fuel/fleet draft review section to Data Management tab

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 13: Wire tab into app.py + i18n + smoke test + deploy

**Files:**
- Modify: `apps/bess-map/app.py` (tab declaration ~line 1537, i18n dicts ~lines 99/356, tab render block)

**Interfaces:**
- Consumes: all previous tasks.
- Produces: visible "Price Forecast / 价格预测" tab between `tab_pca` and `tab_demand`.

- [ ] **Step 1: i18n keys (add to BOTH dicts)**

EN: `"tab_price_forecast": "Price Forecast"`, `"pf_caption": "Merit-order stack + PCA shape hybrid — structural level, statistical shape, D-1 horizon."`
ZH: `"tab_price_forecast": "价格预测"`, `"pf_caption": " merit-order 燃料成本栈 + PCA 形状混合预测 — 结构定水平，统计定形状，D-1 时域。"`

- [ ] **Step 2: Tab declaration (line 1537 area)**

```python
tab_ranking, tab_geo, tab_pca, tab_pf, tab_demand, tab_sysopfee, tab_aux, tab_dispatch, tab_irr, tab_mgmt, tab_agent = st.tabs([
    _t("tab_ranking"), _t("tab_geo"), _t("tab_pca"), _t("tab_price_forecast"), _t("tab_demand"),
    _t("tab_sysopfee"), _t("tab_aux"), _t("tab_dispatch"), _t("tab_irr"), _t("tab_mgmt"), _t("tab_agent"),
])
```

- [ ] **Step 3: Render block (insert between `with tab_pca:` and `with tab_demand:`)**

```python
with tab_pf:
    st.subheader(_t("tab_price_forecast"))
    st.caption(_t("pf_caption"))
    from apps.bess_map import price_forecast_tab as _pft  # see import note
    _provs_pf = load_province_list(_ENG_KEY)
    _sec = st.radio("Section", ["① Merit-Order", "② PCA", "③ Forecast & Backtest"],
                    horizontal=True, key="pf_section")
    if _sec.startswith("①"):
        _pft.render_merit_order_explorer(st, _eng(), _provs_pf)
    elif _sec.startswith("②"):
        _pft.render_pca_section(st, _eng(), _provs_pf)
    else:
        _pft.render_forecast_section(st, _eng(), _provs_pf)
```

IMPORT NOTE: `apps/bess-map` is not a package (hyphen). `app.py` already solves this for its own helpers — check how `irr_helpers` is imported in app.py (line ~35 area) and copy that exact mechanism for `price_forecast_tab` (likely `from apps.bess_map` via sys.path shim or plain `import price_forecast_tab` if app.py's directory is on sys.path). Use the identical pattern; do not invent a new one.

- [ ] **Step 4: Run full test suite**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/price_lab/ apps/bess-map/tests/ tests/hermes/test_fuel_fleet_etl.py tests/hermes/test_fuel_fleet_screener.py -q`
Expected: all PASS

- [ ] **Step 5: AppTest headless smoke (repo pre-deploy standard)**

```bash
~/.venvs/bess-platform/bin/python - <<'EOF'
import os
for line in open("config/.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
os.environ.setdefault("AUTH_MODE", "dev")
from streamlit.testing.v1 import AppTest
at = AppTest.from_file("apps/bess-map/app.py", default_timeout=240)
at.run()
print("exception:", bool(at.exception))
for e in at.exception:
    print(e)
EOF
```
Expected: `exception: False`. DB connectivity required — if it times out, check for VPN interference first (ERRORS.md 2026-09-13).

- [ ] **Step 6: Commit**

```bash
git add apps/bess-map/app.py
git commit -m "Add Price Forecast tab to bess-map

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

- [ ] **Step 7: Deploy (requires explicit in-session confirmation from the user)**

1. bess-map v65: Aliyun-mirror Dockerfile trick (ERRORS.md 2026-09-13) → `linux/amd64` manifest check → push v65 + latest → tfvars `image_bess_map` bump → `terraform plan/apply -target=aws_ecs_task_definition.bess_map -target=aws_ecs_service.bess_map` → verify rollout + bump CLAUDE.md service table.
2. hermes image (screener): rebuild with mirror Dockerfile → push date tag + latest → jq-swap from CURRENT service tdArn (env-count ≥ 31 guard) → update-service → verify `run_patrol`/`/fuelfleet` registered in logs → bump CLAUDE.md pinned-td note.

---

## Execution Handoff

**"Plan complete and saved to `docs/superpowers/plans/2026-09-14-price-forecasting.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?"**
