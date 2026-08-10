# Hermes Spot Monthly Report Recognition — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Hermes recognise `电力现货市场价格与运行月报（YYYY年M月）.pdf` files (Feishu + Telegram), ingest them into the Strategist knowledge base, and parse national + per-province monthly data into two new DB tables, with a backfill CLI.

**Architecture:** New module `services/spot_ingest/monthly_report.py` (recognizer + pdfplumber text extraction + Claude JSON structuring + validation + upsert) beside the existing daily pipeline; thin branches in Hermes `app.py` placed before the exchange-report check (which currently misroutes these files). Spec: `docs/superpowers/specs/2026-08-10-hermes-spot-monthly-report-design.md`.

**Tech Stack:** Python 3, pdfplumber, psycopg2, `shared.anthropic_client.make_client` (Claude sonnet-4-6), pytest + unittest.mock. Venv: `~/.venvs/bess-platform/bin/python`.

**Branch:** `feat/hermes-spot-monthly-report` (based on `feat/deal-structurer-bedrock-migration` — the de-facto trunk; `main` is 614 commits behind and lacks `services/hermes/` entirely).

## Global Constraints

- Model for extraction: `claude-sonnet-4-6` — haiku-4-5 requires a use-case form on this Bedrock account (see `services/hermes/capacity_etl.py:141`).
- Upsert convention: `ON CONFLICT ... DO UPDATE SET col = COALESCE(EXCLUDED.col, table.col)` (see `services/spot_ingest/db_upsert.py`).
- DB access: `from services.knowledge_pool.db import get_conn`, `with get_conn() as conn: with conn.cursor() as cur: ...` then `conn.commit()` — imported lazily inside functions (house style).
- Units: prices 元/kWh (= yuan/kWh); volumes 亿kWh as printed; MoM/coverage as percent numbers (`4.82` = 4.82%). `/` or missing → NULL, never 0.
- Yearless filename → skip and ask for rename; never stamp current year (commit 1064925 precedent).
- Commit messages: imperative, one line, ends with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- **Never commit the sample PDF** (`电力现货市场价格与运行月报（2026年6月）.pdf`, 21.5 MB, repo root). The extracted text fixture under `tests/` IS committed.
- **No deploy, no RDS schema change without explicit in-session "yes".** DDL application is gated in Task 8.
- Surgical edits only: in `app.py` touch only the import block + the two new branches + the two exchange-guard lines.

---

### Task 1: DDL for the two new tables

**Files:**
- Create: `db/ddl/public/spot_monthly.sql`

**Interfaces:**
- Produces: tables `spot_monthly_national` (PK `report_month`) and `spot_monthly_province` (PK `(report_month, province_en)`), consumed by `upsert_monthly_rows` in Task 5 and by future Strategist tooling.

- [ ] **Step 1: Write the DDL file**

```sql
-- National + per-province monthly spot market data from 电力现货市场价格与运行月报.
-- Prices: 元/kWh (= yuan/kWh, same convention as spot_daily).
-- Volumes: 亿kWh as printed in the report. Percent columns store percent numbers (4.82 = 4.82%).

CREATE TABLE IF NOT EXISTS spot_monthly_national (
    report_month               DATE PRIMARY KEY,
    rt_total_volume_yi_kwh     NUMERIC,
    rt_avg_price               NUMERIC,
    da_total_volume_yi_kwh     NUMERIC,
    da_avg_price               NUMERIC,
    mlt_coverage_volume_yi_kwh NUMERIC,
    mlt_coverage_pct           NUMERIC,
    mlt_avg_price              NUMERIC,
    source_file                TEXT,
    ingested_at                TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS spot_monthly_province (
    report_month       DATE NOT NULL,
    province_en        TEXT NOT NULL,
    province_cn        TEXT,
    run_status         TEXT,
    mlt_volume_yi_kwh  NUMERIC,
    mlt_avg_price      NUMERIC,
    mlt_coverage_pct   NUMERIC,
    rt_volume_yi_kwh   NUMERIC,
    rt_avg_price       NUMERIC,
    rt_mom_pct         NUMERIC,
    da_volume_yi_kwh   NUMERIC,
    da_avg_price       NUMERIC,
    da_mom_pct         NUMERIC,
    source_file        TEXT,
    ingested_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (report_month, province_en)
);
```

- [ ] **Step 2: Commit (no DB application yet — that is gated in Task 8)**

```bash
git add db/ddl/public/spot_monthly.sql
git commit -m "Add DDL for spot_monthly_national and spot_monthly_province tables
Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Shared province map module

**Files:**
- Create: `services/spot_ingest/provinces.py`
- Modify: `services/hermes/spot_ingest_bridge.py:11-47` (delete local map, import instead)
- Test: `tests/spot_ingest/test_provinces.py`

**Interfaces:**
- Produces: `PROVINCES_MAP: dict[str, str]` (province_cn → province_en), imported by `monthly_report.py` (Task 3) and `spot_ingest_bridge.py`.

- [ ] **Step 1: Write the failing test**

Create `tests/spot_ingest/__init__.py` (empty) and `tests/spot_ingest/test_provinces.py`:

```python
from services.spot_ingest.provinces import PROVINCES_MAP


def test_map_has_core_provinces():
    assert PROVINCES_MAP["山东"] == "Shandong"
    assert PROVINCES_MAP["河北南网"] == "Hebei-South"
    assert PROVINCES_MAP["蒙西"] == "Mengxi"
    assert PROVINCES_MAP["蒙东"] == "Mengdong"


def test_map_size_unchanged():
    # 35 entries — same as the copy previously in spot_ingest_bridge.py
    assert len(PROVINCES_MAP) == 35


def test_bridge_imports_shared_map():
    from services.hermes import spot_ingest_bridge
    assert spot_ingest_bridge.PROVINCES_MAP is PROVINCES_MAP
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_provinces.py -v`
Expected: FAIL — `ModuleNotFoundError: services.spot_ingest.provinces`

- [ ] **Step 3: Create `services/spot_ingest/provinces.py`**

Copy the `PROVINCES_MAP` dict verbatim from `services/hermes/spot_ingest_bridge.py` lines 11–47 (35 entries), under a module docstring:

```python
"""Province CN → EN name map for spot market ingest pipelines.

Single source of truth — previously duplicated in services/hermes/spot_ingest_bridge.py
and apps/spot-watcher/pipeline.py (the latter keeps its own copy, out of scope).
"""
from __future__ import annotations

PROVINCES_MAP: dict[str, str] = {
    "山东": "Shandong",
    # ... all 35 entries, verbatim from spot_ingest_bridge.py ...
    "天津": "Tianjin",
}
```

- [ ] **Step 4: Update `spot_ingest_bridge.py`**

Replace lines 11–47 (the whole `PROVINCES_MAP` dict) with:

```python
from services.spot_ingest.provinces import PROVINCES_MAP
```

(Keep the name imported at module level so existing references `PROVINCES_MAP` inside the bridge resolve unchanged.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_provinces.py -v`
Expected: 3 PASS

- [ ] **Step 6: Commit**

```bash
git add services/spot_ingest/provinces.py services/hermes/spot_ingest_bridge.py tests/spot_ingest/
git commit -m "Move PROVINCES_MAP to services/spot_ingest/provinces.py (single source)
Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Recognizer + month inference

**Files:**
- Create: `services/spot_ingest/monthly_report.py` (first part — recognizer functions)
- Test: `tests/spot_ingest/test_monthly_report.py`

**Interfaces:**
- Produces:
  - `is_spot_monthly_pdf(filename: str) -> bool`
  - `infer_report_month(filename: str) -> Optional[datetime.date]`
- Consumed by: Hermes `app.py` branches (Task 8), `ingest_monthly_report` (Task 6), backfill CLI (Task 7).

- [ ] **Step 1: Write the failing tests**

`tests/spot_ingest/test_monthly_report.py`:

```python
import datetime as dt

import pytest

from services.spot_ingest.monthly_report import is_spot_monthly_pdf, infer_report_month


def test_matches_monthly_report():
    assert is_spot_monthly_pdf("电力现货市场价格与运行月报（2026年6月）.pdf") is True


def test_rejects_daily_report():
    # 日报 must NOT match — it has its own pipeline (is_spot_pdf)
    assert is_spot_monthly_pdf("电力现货市场价格与运行日报2026-06-01.pdf") is False


def test_rejects_exchange_monthly():
    # provincial exchange 月报 must NOT match — handled by is_exchange_report
    assert is_spot_monthly_pdf("山东电力交易中心2026年6月月报.pdf") is False


def test_requires_pdf_extension():
    assert is_spot_monthly_pdf("电力现货市场价格与运行月报（2026年6月）.xlsx") is False


def test_infer_month_full_width_parens():
    assert infer_report_month("电力现货市场价格与运行月报（2026年6月）.pdf") == dt.date(2026, 6, 1)


def test_infer_month_zero_padded():
    assert infer_report_month("电力现货市场价格与运行月报（2026年06月）.pdf") == dt.date(2026, 6, 1)


def test_infer_month_yearless_returns_none():
    assert infer_report_month("电力现货市场价格与运行月报（6月）.pdf") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_monthly_report.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement recognizer + inference**

`services/spot_ingest/monthly_report.py`:

```python
"""Recognise and ingest 电力现货市场价格与运行月报 (national spot monthly report) PDFs.

Separate from the daily pipeline (spot_ingest_bridge / is_spot_pdf), which only
handles 日报. Monthly files previously fell through to is_exchange_report and were
misrouted into staging.exchange_monthly_reports — this module takes precedence.
"""
from __future__ import annotations

import datetime as dt
import logging
import re
from typing import Optional

from services.spot_ingest.provinces import PROVINCES_MAP

logger = logging.getLogger(__name__)

# Filename patterns that identify the national spot monthly report (case-insensitive)
SPOT_MONTHLY_PATTERNS = ["电力现货市场价格与运行月报"]


def is_spot_monthly_pdf(filename: str) -> bool:
    name_lower = filename.lower()
    return name_lower.endswith(".pdf") and any(
        p.lower() in name_lower for p in SPOT_MONTHLY_PATTERNS
    )


def infer_report_month(filename: str) -> Optional[dt.date]:
    """Infer report month (first of month) from filename, e.g. （2026年6月）.

    Returns None if no explicit year+month — never stamps the current year
    (same rule as settlement ingest, commit 1064925).
    """
    m = re.search(r"(\d{4})年(\d{1,2})月", filename)
    if not m:
        return None
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), 1)
    except ValueError:
        return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_monthly_report.py -v`
Expected: 7 PASS

- [ ] **Step 5: Commit**

```bash
git add services/spot_ingest/monthly_report.py tests/spot_ingest/test_monthly_report.py
git commit -m "Add spot monthly report recognizer and month inference
Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Text extraction + Claude JSON structuring + validation

**Files:**
- Modify: `services/spot_ingest/monthly_report.py` (append)
- Test: `tests/spot_ingest/test_monthly_report.py` (append)
- Fixture: `tests/spot_ingest/fixtures/monthly_2026_06_pages.txt` (generated in Step 6)

**Interfaces:**
- Produces:
  - `extract_pages_text(pdf_path: Path, max_pages: int = 10) -> str` — pdfplumber, `layout=True` (column positions matter for 表2)
  - `extract_monthly_json(text: str, report_month: dt.date, api_key: str) -> dict` — returns `{"national": {...}, "provinces": [...]}`; raises `ValueError` on unparseable Claude output
  - `validate_monthly_data(data: dict) -> list[str]` — returns warnings; raises `ValueError` on hard failure; nulls out-of-range values in place
  - National dict keys: `rt_total_volume_yi_kwh, rt_avg_price, da_total_volume_yi_kwh, da_avg_price, mlt_coverage_volume_yi_kwh, mlt_coverage_pct, mlt_avg_price`
  - Province dict keys: `province_cn, run_status, mlt_volume_yi_kwh, mlt_avg_price, mlt_coverage_pct, rt_volume_yi_kwh, rt_avg_price, rt_mom_pct, da_volume_yi_kwh, da_avg_price, da_mom_pct`

- [ ] **Step 1: Write the failing tests (append to `test_monthly_report.py`)**

```python
from unittest.mock import MagicMock, patch

from services.spot_ingest.monthly_report import extract_monthly_json, validate_monthly_data


def _province(**over):
    base = {
        "province_cn": "山东", "run_status": "正式运行",
        "mlt_volume_yi_kwh": 164.63, "mlt_avg_price": 0.344, "mlt_coverage_pct": 62.58,
        "rt_volume_yi_kwh": None, "rt_avg_price": 0.346, "rt_mom_pct": -12.42,
        "da_volume_yi_kwh": None, "da_avg_price": 0.315, "da_mom_pct": 4.41,
    }
    base.update(over)
    return base


def _data(n=25, **nat_over):
    national = {
        "rt_total_volume_yi_kwh": 4469.26, "rt_avg_price": 0.291,
        "da_total_volume_yi_kwh": 4493.13, "da_avg_price": 0.294,
        "mlt_coverage_volume_yi_kwh": 12951.62, "mlt_coverage_pct": 66.04,
        "mlt_avg_price": 0.313,
    }
    national.update(nat_over)
    return {"national": national, "provinces": [_province() for _ in range(n)]}


def test_validate_clean_data_no_warnings():
    assert validate_monthly_data(_data()) == []


def test_validate_few_provinces_warns():
    warnings = validate_monthly_data(_data(n=5))
    assert any("省份" in w for w in warnings)


def test_validate_zero_provinces_raises():
    with pytest.raises(ValueError):
        validate_monthly_data({"national": {}, "provinces": []})


def test_validate_price_out_of_range_nulled():
    data = _data()
    data["provinces"][0]["rt_avg_price"] = 5.0
    warnings = validate_monthly_data(data)
    assert data["provinces"][0]["rt_avg_price"] is None
    assert any("rt_avg_price" in w for w in warnings)


def test_validate_coverage_pct_out_of_range_nulled():
    data = _data()
    data["provinces"][0]["mlt_coverage_pct"] = 150.0
    warnings = validate_monthly_data(data)
    assert data["provinces"][0]["mlt_coverage_pct"] is None
    assert warnings


def test_validate_unknown_province_dropped():
    data = _data()
    data["provinces"].append(_province(province_cn="亚特兰蒂斯"))
    warnings = validate_monthly_data(data)
    assert len(data["provinces"]) == 25
    assert any("亚特兰蒂斯" in w for w in warnings)


def test_extract_monthly_json_parses_claude_output():
    payload = '{"national": {"rt_avg_price": 0.291}, "provinces": []}'
    fake_resp = MagicMock()
    fake_resp.content = [MagicMock(text=f"Here is the data:\n{payload}")]
    fake_client = MagicMock()
    fake_client.messages.create.return_value = fake_resp
    with patch("shared.anthropic_client.make_client", return_value=fake_client):
        result = extract_monthly_json("page text", dt.date(2026, 6, 1), "key")
    assert result["national"]["rt_avg_price"] == 0.291


def test_extract_monthly_json_raises_on_garbage():
    fake_resp = MagicMock()
    fake_resp.content = [MagicMock(text="no json here")]
    fake_client = MagicMock()
    fake_client.messages.create.return_value = fake_resp
    with patch("shared.anthropic_client.make_client", return_value=fake_client):
        with pytest.raises(ValueError):
            extract_monthly_json("page text", dt.date(2026, 6, 1), "key")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_monthly_report.py -v`
Expected: FAIL — `ImportError: cannot import name 'extract_monthly_json'`

- [ ] **Step 3: Implement extraction + validation (append to `monthly_report.py`)**

```python
def extract_pages_text(pdf_path, max_pages: int = 10) -> str:
    """Extract text from the first max_pages pages. layout=True preserves column
    positions, which the wrapped multi-line headers of 表2 require."""
    import pdfplumber  # lazy — keeps recognizer tests free of the dependency

    parts = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages[:max_pages]):
            text = page.extract_text(layout=True) or ""
            parts.append(f"=== page {i + 1} ===\n{text}")
    return "\n\n".join(parts)


_EXTRACT_SYSTEM = """You are a data extraction assistant for China electricity market reports.
The user provides text from 《电力现货市场价格与运行月报》 (national spot market monthly report), first pages only.

Extract TWO things:
1. 总体情况 (section 一, national summary): RT/DA total cleared volume & avg price, 中长期合约覆盖电量/占比/成交均价.
2. 表2 连续运行地区运行情况一览表: one row per province/region.

Return ONLY valid JSON, no markdown:
{
  "national": {
    "rt_total_volume_yi_kwh": number|null, "rt_avg_price": number|null,
    "da_total_volume_yi_kwh": number|null, "da_avg_price": number|null,
    "mlt_coverage_volume_yi_kwh": number|null, "mlt_coverage_pct": number|null,
    "mlt_avg_price": number|null
  },
  "provinces": [
    {
      "province_cn": "地区名(按原文)", "run_status": "正式运行/试运行|null",
      "mlt_volume_yi_kwh": number|null, "mlt_avg_price": number|null,
      "mlt_coverage_pct": number|null,
      "rt_volume_yi_kwh": number|null, "rt_avg_price": number|null, "rt_mom_pct": number|null,
      "da_volume_yi_kwh": number|null, "da_avg_price": number|null, "da_mom_pct": number|null
    }
  ]
}

Rules:
- Volumes in 亿千瓦时, prices in 元/千瓦时, percentages as numbers (4.82 for 4.82%). Negative MoM keeps its sign.
- mlt_volume_yi_kwh = 中长期市场成交电量 (NOT 合约覆盖电量); mlt_coverage_pct = 中长期合约覆盖电量占比.
- "/" or unreadable/missing → null, never 0.
- SKIP 表1 省间现货市场 (四川主网/灵绍配套电源/天中配套电源 etc. belong to 表1 — exclude them).
- SKIP chart captions (图 N …) and narrative paragraphs.
- provinces come ONLY from 表2 (连续运行地区)."""


def extract_monthly_json(text: str, report_month: dt.date, api_key: str) -> dict:
    """Structure extracted page text via Claude. Raises ValueError on bad output."""
    import json

    from shared.anthropic_client import make_client  # lazy, house style

    client = make_client(api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-6",  # haiku-4-5 requires use-case form on this Bedrock account
        max_tokens=4000,
        system=_EXTRACT_SYSTEM,
        messages=[{
            "role": "user",
            "content": (
                f"Report month: {report_month.strftime('%Y年%m月')} "
                f"(all data is for {report_month.year}-{report_month.month:02d}).\n\n"
                f"Content:\n{text[:30000]}"
            ),
        }],
    )
    raw = resp.content[0].text.strip()
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if not m:
        raise ValueError(f"Claude returned no JSON: {raw[:200]}")
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Claude returned invalid JSON: {exc}: {raw[:200]}") from exc
    if "provinces" not in data or "national" not in data:
        raise ValueError(f"Claude JSON missing required keys: {list(data.keys())}")
    return data


_PRICE_FIELDS = ("rt_avg_price", "da_avg_price", "mlt_avg_price")
_VOLUME_FIELDS = (
    "rt_volume_yi_kwh", "da_volume_yi_kwh", "mlt_volume_yi_kwh",
    "rt_total_volume_yi_kwh", "da_total_volume_yi_kwh", "mlt_coverage_volume_yi_kwh",
)
_PCT_FIELDS = ("rt_mom_pct", "da_mom_pct")


def _clean_number(record: dict, field: str, lo: float, hi: float, label: str, warnings: list[str]) -> None:
    val = record.get(field)
    if val is None:
        return
    try:
        val = float(val)
    except (TypeError, ValueError):
        record[field] = None
        warnings.append(f"{label}: {field} 非数值已置空")
        return
    if not (lo <= val <= hi):
        record[field] = None
        warnings.append(f"{label}: {field}={val} 超出范围[{lo},{hi}]已置空")
    else:
        record[field] = val


def validate_monthly_data(data: dict) -> list[str]:
    """Validate in place. Returns warnings. Raises ValueError on hard failure."""
    warnings: list[str] = []
    provinces = data.get("provinces") or []
    if not provinces:
        raise ValueError("未提取到任何省份数据")
    if len(provinces) < 20:
        warnings.append(f"省份数量仅 {len(provinces)}（预期 ≥20）")

    national = data.get("national") or {}
    for f in _PRICE_FIELDS:
        _clean_number(national, f, 0.0, 2.0, "全国", warnings)
    for f in _VOLUME_FIELDS:
        _clean_number(national, f, 0.0, 20000.0, "全国", warnings)
    _clean_number(national, "mlt_coverage_pct", 0.0, 100.0, "全国", warnings)

    kept = []
    for row in provinces:
        cn = (row.get("province_cn") or "").strip()
        if cn not in PROVINCES_MAP:
            warnings.append(f"无法识别地区「{cn}」，该行已丢弃")
            continue
        for f in _PRICE_FIELDS:
            _clean_number(row, f, 0.0, 2.0, cn, warnings)
        for f in _VOLUME_FIELDS:
            if f in row:
                _clean_number(row, f, 0.0, 20000.0, cn, warnings)
        for f in _PCT_FIELDS:
            _clean_number(row, f, -1000.0, 1000.0, cn, warnings)
        _clean_number(row, "mlt_coverage_pct", 0.0, 100.0, cn, warnings)
        kept.append(row)
    data["provinces"] = kept
    if not kept:
        raise ValueError("所有省份行均无法识别")
    return warnings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_monthly_report.py -v`
Expected: 15 PASS (7 from Task 3 + 8 new)

- [ ] **Step 5: Commit**

```bash
git add services/spot_ingest/monthly_report.py tests/spot_ingest/test_monthly_report.py
git commit -m "Add monthly report text extraction, Claude structuring, validation
Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 6: Generate the text fixture from the sample PDF (for Task 9 and future regression runs)**

```bash
~/.venvs/bess-platform/bin/python -c "
from pathlib import Path
from services.spot_ingest.monthly_report import extract_pages_text
text = extract_pages_text(Path('电力现货市场价格与运行月报（2026年6月）.pdf'))
Path('tests/spot_ingest/fixtures').mkdir(parents=True, exist_ok=True)
Path('tests/spot_ingest/fixtures/monthly_2026_06_pages.txt').write_text(text, encoding='utf-8')
print(len(text), 'chars')
"
git add tests/spot_ingest/fixtures/monthly_2026_06_pages.txt
git commit -m "Add extracted text fixture for 2026-06 monthly report
Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: DB upsert

**Files:**
- Modify: `services/spot_ingest/monthly_report.py` (append)
- Test: `tests/spot_ingest/test_monthly_report.py` (append)

**Interfaces:**
- Produces: `upsert_monthly_rows(national: dict, provinces: list[dict], report_month: dt.date, source_file: str) -> dict` — returns `{"national_written": bool, "provinces_upserted": int}`. Maps `province_cn` → `province_en` via `PROVINCES_MAP` (caller guarantees `province_cn` is known — validation in Task 4 dropped unknowns).

- [ ] **Step 1: Write the failing test (append)**

```python
from services.spot_ingest.monthly_report import upsert_monthly_rows


def test_upsert_writes_national_and_provinces():
    data = _data(n=2)
    executed = []

    fake_cur = MagicMock()
    fake_cur.execute.side_effect = lambda sql, params: executed.append(params)
    fake_conn = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur
    fake_get_conn = MagicMock()
    fake_get_conn.return_value.__enter__.return_value = fake_conn

    with patch("services.knowledge_pool.db.get_conn", fake_get_conn):
        result = upsert_monthly_rows(data["national"], data["provinces"], dt.date(2026, 6, 1), "test.pdf")

    assert result == {"national_written": True, "provinces_upserted": 2}
    assert len(executed) == 3  # 1 national + 2 provinces
    assert executed[0]["report_month"] == dt.date(2026, 6, 1)
    assert executed[0]["source_file"] == "test.pdf"
    assert executed[1]["province_en"] == "Shandong"
    fake_conn.commit.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_monthly_report.py::test_upsert_writes_national_and_provinces -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement upsert (append to `monthly_report.py`)**

```python
_UPSERT_NATIONAL_SQL = """
INSERT INTO spot_monthly_national (
    report_month, rt_total_volume_yi_kwh, rt_avg_price,
    da_total_volume_yi_kwh, da_avg_price,
    mlt_coverage_volume_yi_kwh, mlt_coverage_pct, mlt_avg_price, source_file
) VALUES (
    %(report_month)s, %(rt_total_volume_yi_kwh)s, %(rt_avg_price)s,
    %(da_total_volume_yi_kwh)s, %(da_avg_price)s,
    %(mlt_coverage_volume_yi_kwh)s, %(mlt_coverage_pct)s, %(mlt_avg_price)s, %(source_file)s
)
ON CONFLICT (report_month) DO UPDATE SET
    rt_total_volume_yi_kwh     = COALESCE(EXCLUDED.rt_total_volume_yi_kwh, spot_monthly_national.rt_total_volume_yi_kwh),
    rt_avg_price               = COALESCE(EXCLUDED.rt_avg_price, spot_monthly_national.rt_avg_price),
    da_total_volume_yi_kwh     = COALESCE(EXCLUDED.da_total_volume_yi_kwh, spot_monthly_national.da_total_volume_yi_kwh),
    da_avg_price               = COALESCE(EXCLUDED.da_avg_price, spot_monthly_national.da_avg_price),
    mlt_coverage_volume_yi_kwh = COALESCE(EXCLUDED.mlt_coverage_volume_yi_kwh, spot_monthly_national.mlt_coverage_volume_yi_kwh),
    mlt_coverage_pct           = COALESCE(EXCLUDED.mlt_coverage_pct, spot_monthly_national.mlt_coverage_pct),
    mlt_avg_price              = COALESCE(EXCLUDED.mlt_avg_price, spot_monthly_national.mlt_avg_price),
    source_file                = EXCLUDED.source_file,
    ingested_at                = now();
"""

_UPSERT_PROVINCE_SQL = """
INSERT INTO spot_monthly_province (
    report_month, province_en, province_cn, run_status,
    mlt_volume_yi_kwh, mlt_avg_price, mlt_coverage_pct,
    rt_volume_yi_kwh, rt_avg_price, rt_mom_pct,
    da_volume_yi_kwh, da_avg_price, da_mom_pct, source_file
) VALUES (
    %(report_month)s, %(province_en)s, %(province_cn)s, %(run_status)s,
    %(mlt_volume_yi_kwh)s, %(mlt_avg_price)s, %(mlt_coverage_pct)s,
    %(rt_volume_yi_kwh)s, %(rt_avg_price)s, %(rt_mom_pct)s,
    %(da_volume_yi_kwh)s, %(da_avg_price)s, %(da_mom_pct)s, %(source_file)s
)
ON CONFLICT (report_month, province_en) DO UPDATE SET
    province_cn       = EXCLUDED.province_cn,
    run_status        = COALESCE(EXCLUDED.run_status, spot_monthly_province.run_status),
    mlt_volume_yi_kwh = COALESCE(EXCLUDED.mlt_volume_yi_kwh, spot_monthly_province.mlt_volume_yi_kwh),
    mlt_avg_price     = COALESCE(EXCLUDED.mlt_avg_price, spot_monthly_province.mlt_avg_price),
    mlt_coverage_pct  = COALESCE(EXCLUDED.mlt_coverage_pct, spot_monthly_province.mlt_coverage_pct),
    rt_volume_yi_kwh  = COALESCE(EXCLUDED.rt_volume_yi_kwh, spot_monthly_province.rt_volume_yi_kwh),
    rt_avg_price      = COALESCE(EXCLUDED.rt_avg_price, spot_monthly_province.rt_avg_price),
    rt_mom_pct        = COALESCE(EXCLUDED.rt_mom_pct, spot_monthly_province.rt_mom_pct),
    da_volume_yi_kwh  = COALESCE(EXCLUDED.da_volume_yi_kwh, spot_monthly_province.da_volume_yi_kwh),
    da_avg_price      = COALESCE(EXCLUDED.da_avg_price, spot_monthly_province.da_avg_price),
    da_mom_pct        = COALESCE(EXCLUDED.da_mom_pct, spot_monthly_province.da_mom_pct),
    source_file       = EXCLUDED.source_file,
    ingested_at       = now();
"""


def upsert_monthly_rows(national: dict, provinces: list[dict], report_month: dt.date, source_file: str) -> dict:
    """Upsert national + province monthly rows in a single transaction."""
    from services.knowledge_pool.db import get_conn  # lazy, house style

    national_params = {
        "report_month": report_month,
        "rt_total_volume_yi_kwh": national.get("rt_total_volume_yi_kwh"),
        "rt_avg_price": national.get("rt_avg_price"),
        "da_total_volume_yi_kwh": national.get("da_total_volume_yi_kwh"),
        "da_avg_price": national.get("da_avg_price"),
        "mlt_coverage_volume_yi_kwh": national.get("mlt_coverage_volume_yi_kwh"),
        "mlt_coverage_pct": national.get("mlt_coverage_pct"),
        "mlt_avg_price": national.get("mlt_avg_price"),
        "source_file": source_file,
    }
    province_params = []
    for row in provinces:
        params = {k: row.get(k) for k in (
            "run_status", "mlt_volume_yi_kwh", "mlt_avg_price", "mlt_coverage_pct",
            "rt_volume_yi_kwh", "rt_avg_price", "rt_mom_pct",
            "da_volume_yi_kwh", "da_avg_price", "da_mom_pct",
        )}
        params["report_month"] = report_month
        params["province_cn"] = row["province_cn"]
        params["province_en"] = PROVINCES_MAP[row["province_cn"]]
        params["source_file"] = source_file
        province_params.append(params)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_UPSERT_NATIONAL_SQL, national_params)
            for params in province_params:
                cur.execute(_UPSERT_PROVINCE_SQL, params)
        conn.commit()
    return {"national_written": True, "provinces_upserted": len(province_params)}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_monthly_report.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add services/spot_ingest/monthly_report.py tests/spot_ingest/test_monthly_report.py
git commit -m "Add upsert for spot monthly national and province tables
Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: `ingest_monthly_report` orchestrator

**Files:**
- Modify: `services/spot_ingest/monthly_report.py` (append)
- Test: `tests/spot_ingest/test_monthly_report.py` (append)

**Interfaces:**
- Produces: `ingest_monthly_report(filename: str, pdf_bytes: bytes, api_key: str) -> dict` returning `{"month": str, "n_provinces": int, "national_rt_avg": float|None, "provinces_upserted": int, "warnings": list[str]}`. Raises `ValueError` if the filename lacks year+month (message tells the user to rename). Consumed by Hermes branches (Task 8) and CLI (Task 7).

- [ ] **Step 1: Write the failing tests (append)**

```python
from services.spot_ingest.monthly_report import ingest_monthly_report


def test_ingest_yearless_raises_with_rename_hint():
    with pytest.raises(ValueError, match="重命名"):
        ingest_monthly_report("电力现货市场价格与运行月报（6月）.pdf", b"%PDF", "key")


def test_ingest_full_flow():
    data = _data()
    with patch("services.spot_ingest.monthly_report.extract_pages_text", return_value="text"), \
         patch("services.spot_ingest.monthly_report.extract_monthly_json", return_value=data), \
         patch("services.spot_ingest.monthly_report.upsert_monthly_rows",
               return_value={"national_written": True, "provinces_upserted": 25}) as mock_up:
        result = ingest_monthly_report("电力现货市场价格与运行月报（2026年6月）.pdf", b"%PDF-bytes", "key")
    assert result["month"] == "2026-06"
    assert result["n_provinces"] == 25
    assert result["national_rt_avg"] == 0.291
    assert result["warnings"] == []
    assert mock_up.call_args[0][2] == dt.date(2026, 6, 1)
    assert mock_up.call_args[0][3] == "电力现货市场价格与运行月报（2026年6月）.pdf"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_monthly_report.py -k ingest -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement orchestrator (append to `monthly_report.py`)**

```python
def ingest_monthly_report(filename: str, pdf_bytes: bytes, api_key: str) -> dict:
    """Parse a spot monthly report PDF and upsert into DB. Single entry point
    used by the Hermes handlers and the backfill CLI.

    Raises ValueError if the filename has no explicit year+month.
    """
    import tempfile
    from pathlib import Path

    report_month = infer_report_month(filename)
    if report_month is None:
        raise ValueError(
            f"无法从文件名推断报告月份：{filename}。"
            "请重命名为包含年份和月份的形式（如「（2026年6月）」）后重发。"
        )

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = Path(tmp.name)
    try:
        text = extract_pages_text(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    data = extract_monthly_json(text, report_month, api_key)
    warnings = validate_monthly_data(data)
    result = upsert_monthly_rows(data["national"], data["provinces"], report_month, filename)
    logger.info(
        "spot monthly: %s → %s, %d provinces, %d warnings",
        filename, report_month, result["provinces_upserted"], len(warnings),
    )
    return {
        "month": report_month.strftime("%Y-%m"),
        "n_provinces": result["provinces_upserted"],
        "national_rt_avg": data["national"].get("rt_avg_price"),
        "provinces_upserted": result["provinces_upserted"],
        "warnings": warnings,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_monthly_report.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add services/spot_ingest/monthly_report.py tests/spot_ingest/test_monthly_report.py
git commit -m "Add ingest_monthly_report orchestrator
Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Backfill CLI

**Files:**
- Create: `services/spot_ingest/run_monthly_ingest.py`
- Test: `tests/spot_ingest/test_run_monthly_ingest.py`

**Interfaces:**
- Consumes: `is_spot_monthly_pdf`, `infer_report_month`, `ingest_monthly_report`, `extract_pages_text`, `extract_monthly_json`, `validate_monthly_data` from `services.spot_ingest.monthly_report`.
- Produces: CLI `python -m services.spot_ingest.run_monthly_ingest --dir <folder> [--dry-run]`; function `find_monthly_pdfs(folder: Path) -> list[Path]`.

- [ ] **Step 1: Write the failing test**

`tests/spot_ingest/test_run_monthly_ingest.py`:

```python
from pathlib import Path
from unittest.mock import patch

from services.spot_ingest.run_monthly_ingest import find_monthly_pdfs


def test_find_monthly_pdfs_filters(tmp_path: Path):
    (tmp_path / "电力现货市场价格与运行月报（2026年6月）.pdf").write_bytes(b"x")
    (tmp_path / "电力现货市场价格与运行日报2026-06-01.pdf").write_bytes(b"x")
    (tmp_path / "山东电力交易中心2026年6月月报.pdf").write_bytes(b"x")
    (tmp_path / "notes.txt").write_bytes(b"x")
    found = find_monthly_pdfs(tmp_path)
    assert [p.name for p in found] == ["电力现货市场价格与运行月报（2026年6月）.pdf"]


def test_find_monthly_pdfs_skips_yearless(tmp_path: Path):
    (tmp_path / "电力现货市场价格与运行月报（6月）.pdf").write_bytes(b"x")
    assert find_monthly_pdfs(tmp_path) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_run_monthly_ingest.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement the CLI**

```python
"""Backfill national spot monthly reports into spot_monthly_* tables.

Usage:
    python -m services.spot_ingest.run_monthly_ingest --dir /path/to/folder [--dry-run]

--dry-run parses and validates but skips the DB write (prints warnings).
KB backfill is separate: scripts/ingest_knowledge_bulk.py.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_repo_root))

try:
    from dotenv import load_dotenv
    load_dotenv(_repo_root / "config" / ".env", override=False)
except ImportError:
    pass

from services.spot_ingest.monthly_report import (  # noqa: E402
    infer_report_month, ingest_monthly_report, is_spot_monthly_pdf,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def find_monthly_pdfs(folder: Path) -> list[Path]:
    """PDFs in folder matching the monthly pattern with an inferable month."""
    return [
        p for p in sorted(folder.glob("*.pdf"))
        if is_spot_monthly_pdf(p.name) and infer_report_month(p.name) is not None
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill spot monthly report PDFs")
    ap.add_argument("--dir", required=True, help="Folder containing monthly report PDFs")
    ap.add_argument("--dry-run", action="store_true", help="Parse + validate only, no DB write")
    args = ap.parse_args()

    files = find_monthly_pdfs(Path(args.dir))
    if not files:
        print("No matching monthly report PDFs found.")
        return 0

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    ok = failed = 0
    for path in files:
        try:
            if args.dry_run:
                from services.spot_ingest.monthly_report import (
                    extract_monthly_json, extract_pages_text, validate_monthly_data,
                )
                text = extract_pages_text(path)
                data = extract_monthly_json(text, infer_report_month(path.name), api_key)
                warnings = validate_monthly_data(data)
                print(f"DRY  {path.name}: {len(data['provinces'])} provinces, "
                      f"{len(warnings)} warnings")
                for w in warnings:
                    print(f"     ⚠️ {w}")
            else:
                result = ingest_monthly_report(path.name, path.read_bytes(), api_key)
                print(f"OK   {path.name}: {result['month']}, "
                      f"{result['n_provinces']} provinces, "
                      f"{len(result['warnings'])} warnings")
            ok += 1
        except Exception as exc:
            failed += 1
            print(f"FAIL {path.name}: {exc}")
    print(f"\nDone: {ok} ok, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/test_run_monthly_ingest.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add services/spot_ingest/run_monthly_ingest.py tests/spot_ingest/test_run_monthly_ingest.py
git commit -m "Add backfill CLI for spot monthly reports
Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Hermes wiring (Feishu + Telegram handlers)

**Files:**
- Modify: `services/hermes/app.py` — import block (near line 62), Feishu handler (insert before line 2535, guard line 2536), Telegram handler (insert before line 2740, guard line 2741)

**Interfaces:**
- Consumes: `is_spot_monthly_pdf`, `infer_report_month`, `ingest_monthly_report` from `services.spot_ingest.monthly_report`; `agent.ingest_file_to_kb(filename, file_bytes, category=...)` (existing, `services/hermes/agent.py:896`).
- Note: handlers are sequential `if` blocks — the new branch computes `_is_spot_monthly` / `_is_spot_monthly_tg` which the exchange-report `if` then guards against.

- [ ] **Step 1: Add the import (near line 62, beside the other spot/capacity imports)**

```python
from services.spot_ingest.monthly_report import (
    infer_report_month, ingest_monthly_report, is_spot_monthly_pdf,
)
```

- [ ] **Step 2: Feishu branch — insert immediately BEFORE the `# Auto ETL: exchange monthly report → shared KB` block (line 2535)**

```python
    # Spot monthly report (电力现货市场价格与运行月报): KB ingest + DB parse.
    # Must run BEFORE the exchange-report check below — these filenames also
    # match the exchange 月报 keyword regex and were previously misrouted.
    _is_spot_monthly = is_spot_monthly_pdf(filename) and resource_type == "file"
    if _is_spot_monthly:
        try:
            if infer_report_month(filename) is None:
                feishu.send_text(open_id=sender_id, text=(
                    "⚠️ 现货月报文件名需包含年份和月份（如「（2026年6月）」），请重命名后重发。"
                ))
            else:
                api_key = os.environ.get("ANTHROPIC_API_KEY", "")
                kb_reply = agent.ingest_file_to_kb(filename, file_bytes, category="research_report")
                feishu.send_text(open_id=sender_id, text=kb_reply)
                _sm = ingest_monthly_report(filename, file_bytes, api_key)
                _sm_msg = (
                    f"📊 现货月报已入库（{_sm['month']}）\n"
                    f"{_sm['n_provinces']} 省 + 全国汇总"
                )
                if _sm.get("national_rt_avg") is not None:
                    _sm_msg += f"，全国实时均价 {_sm['national_rt_avg']:.3f} 元/kWh"
                if _sm["warnings"]:
                    _sm_msg += f"\n⚠️ 校验提示：{'；'.join(_sm['warnings'][:3])}"
                feishu.send_text(open_id=sender_id, text=_sm_msg)
        except Exception as exc:
            logger.error("Spot monthly ingest failed: %s", exc, exc_info=True)
            feishu.send_text(open_id=sender_id, text=f"⚠️ 现货月报入库失败：{exc}")
```

- [ ] **Step 3: Guard the Feishu exchange-report check — change line 2536**

From:
```python
    _exchange_province = is_exchange_report(filename)
    if _exchange_province:
```
To:
```python
    _exchange_province = is_exchange_report(filename)
    if _exchange_province and not _is_spot_monthly:
```

- [ ] **Step 4: Telegram branch — insert immediately BEFORE the `# Auto ETL: exchange monthly report → shared KB (Telegram)` block (line 2740)**

Identical logic, Telegram send style (note: Telegram handler has no `resource_type`; photos arrive as `.jpg` filenames so the pattern+`.pdf` check is sufficient):

```python
    # Spot monthly report (电力现货市场价格与运行月报): KB ingest + DB parse (Telegram)
    _is_spot_monthly_tg = is_spot_monthly_pdf(filename)
    if _is_spot_monthly_tg:
        try:
            if infer_report_month(filename) is None:
                telegram.send_text(chat_id,
                    "⚠️ 现货月报文件名需包含年份和月份（如「（2026年6月）」），请重命名后重发。")
            else:
                api_key = os.environ.get("ANTHROPIC_API_KEY", "")
                kb_reply = agent.ingest_file_to_kb(filename, file_bytes, category="research_report")
                telegram.send_text(chat_id, kb_reply)
                _sm_tg = ingest_monthly_report(filename, file_bytes, api_key)
                _sm_tg_msg = (
                    f"📊 现货月报已入库（{_sm_tg['month']}）\n"
                    f"{_sm_tg['n_provinces']} 省 + 全国汇总"
                )
                if _sm_tg.get("national_rt_avg") is not None:
                    _sm_tg_msg += f"，全国实时均价 {_sm_tg['national_rt_avg']:.3f} 元/kWh"
                if _sm_tg["warnings"]:
                    _sm_tg_msg += f"\n⚠️ 校验提示：{'；'.join(_sm_tg['warnings'][:3])}"
                telegram.send_text(chat_id, _sm_tg_msg)
        except Exception as exc:
            logger.error("Spot monthly ingest (Telegram) failed: %s", exc, exc_info=True)
            telegram.send_text(chat_id, f"⚠️ 现货月报入库失败：{exc}")
```

- [ ] **Step 5: Guard the Telegram exchange-report check — change line 2742**

From:
```python
    if _exchange_province_tg:
```
To:
```python
    if _exchange_province_tg and not _is_spot_monthly_tg:
```

- [ ] **Step 6: Syntax + import check (no unit tests exist for the 200KB handler file — house reality)**

```bash
~/.venvs/bess-platform/bin/python -c "import ast; ast.parse(open('services/hermes/app.py').read()); print('syntax ok')"
~/.venvs/bess-platform/bin/python -c "
import sys; sys.path.insert(0, '.')
from services.spot_ingest.monthly_report import is_spot_monthly_pdf, infer_report_month, ingest_monthly_report
print('imports ok')
"
```

- [ ] **Step 7: Run the full new test suite once more**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/spot_ingest/ -v`
Expected: all PASS

- [ ] **Step 8: Commit**

```bash
git add services/hermes/app.py
git commit -m "Route spot monthly reports in Hermes before exchange-report misroute
Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: Local end-to-end verification

**Files:** none (verification only)

- [ ] **Step 1: Apply DDL — REQUIRES EXPLICIT USER CONFIRMATION (live RDS). Ask first.**

```bash
source config/.env  # provides PGURL
psql "$PGURL" -f db/ddl/public/spot_monthly.sql
psql "$PGURL" -c "\d spot_monthly_province" -c "\d spot_monthly_national"
```

Expected: both tables described with the columns from Task 1.

- [ ] **Step 2: Dry-run the parser on the real sample PDF**

```bash
~/.venvs/bess-platform/bin/python -m services.spot_ingest.run_monthly_ingest --dir . --dry-run
```

Expected: `DRY 电力现货市场价格与运行月报（2026年6月）.pdf: ~25 provinces, few warnings`. Eyeball warnings for plausibility.

- [ ] **Step 3: Real ingest of the sample**

```bash
~/.venvs/bess-platform/bin/python -m services.spot_ingest.run_monthly_ingest --dir .
```

Expected: `OK ... 2026-06, 25 provinces`.

- [ ] **Step 4: Spot-check DB values against the PDF**

```bash
psql "$PGURL" -c "SELECT province_cn, rt_avg_price, da_avg_price, mlt_avg_price, mlt_coverage_pct FROM spot_monthly_province WHERE report_month='2026-06-01' AND province_cn IN ('山东','吉林','四川','湖南') ORDER BY province_cn;"
psql "$PGURL" -c "SELECT rt_total_volume_yi_kwh, rt_avg_price, da_total_volume_yi_kwh, da_avg_price, mlt_coverage_pct FROM spot_monthly_national WHERE report_month='2026-06-01';"
```

Expected (from the PDF itself): national RT volume 4469.26 亿kWh, RT avg 0.291, DA volume 4493.13, DA avg 0.294, coverage 66.04%. 山东 rt_avg ≈ 0.346. 吉林 highest RT avg (0.506 per narrative), 四川 lowest (0.111), 湖南 lowest DA (0.118). Tolerance: exact or within rounding of printed values.

- [ ] **Step 5: Check for previously misrouted rows**

```bash
psql "$PGURL" -c "SELECT id, province, report_month, file_name, created_at FROM staging.exchange_monthly_reports WHERE file_name LIKE '%电力现货市场价格与运行月报%';"
```

If rows exist: report them to the user. **Do not delete without explicit confirmation.**

- [ ] **Step 6: Idempotency check — re-run Step 3, confirm no error and same row counts**

```bash
psql "$PGURL" -c "SELECT count(*) FROM spot_monthly_province WHERE report_month='2026-06-01';"
```

Expected: same count as Step 4 (25), no duplicates.

- [ ] **Step 7: Final commit of any fixture/verification notes; push branch**

```bash
git push -u origin feat/hermes-spot-monthly-report
```

---

## Self-review notes (author)

- **Spec coverage:** recognizer (T3), month inference + yearless rule (T3/T6), pdfplumber+Claude parsing (T4), validation (T4), tables (T1), COALESCE upsert (T5), Feishu+Telegram branches before exchange check (T8), KB ingest in both branches (T8), backfill CLI (T7), misrouted-row check (T9 Step 5), chart pages skipped (T4 max_pages=10). Out-of-scope items remain out.
- **Type consistency:** `ingest_monthly_report` return keys (`month`, `n_provinces`, `national_rt_avg`, `provinces_upserted`, `warnings`) match between T6 implementation, T8 handler usage. `upsert_monthly_rows` return keys (`national_written`, `provinces_upserted`) match T5/T6. JSON field names consistent across T4 prompt/validation and T5 SQL params.
- **Follow-up for user (not in plan):** `main` is 614 commits behind `feat/deal-structurer-bedrock-migration` (which has 2 unpushed commits); deal with trunk hygiene separately. Deployment of Hermes image is a separate confirmation-gated step after this plan merges.
