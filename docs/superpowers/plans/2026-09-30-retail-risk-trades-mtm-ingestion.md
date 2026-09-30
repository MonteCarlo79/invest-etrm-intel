# Retail Risk — Trades / Settlement / MTM Ingestion (P1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Feed by-province retail trades, wholesale invoices, MTM workbooks and market benchmarks into `marketdata.rm_*` tables, and light up three analytics in the retail-risk app: trade-vs-invoice reconciliation, 复盘-aligned P&L breakdown (批零价差 / channel alpha / trader-sales attribution), and per-book scenario MtM.

**Architecture:** Per-province parser modules emit canonical DataFrames; a single `loader.py` is the only DB writer (batch delete+insert for positions, ON CONFLICT upserts elsewhere, file-hash dedup for invoices). Three pure-logic engines (`reconcile`, `pnl_bridge`, `mtm`) sit between the tables and four Streamlit tabs. Asset-risk code and asset books are never touched.

**Tech Stack:** Python 3.13, pandas, SQLAlchemy (psycopg2), openpyxl, pdfplumber, Streamlit, plotly, pytest. Venv: `source ~/.venvs/bess-platform/bin/activate` (all pytest/run commands assume it).

**Spec:** `docs/superpowers/specs/2026-09-30-retail-risk-trades-mtm-ingestion-design.md`

## Global Constraints

- **Never touch** `apps/asset_risk/`, `services/settlement_ingest/`, `services/asset_risk/`, or asset book rows (`rm_books.book_type='asset'`, ids 1–20).
- Load books: `景融售电-{province}`, `book_type='load'`, `asset_id=NULL`, provinces 冀南/浙江/山东/安徽/福建/广东/江苏/上海.
- Spot joins use `marketdata.spot_prices_hourly(province, datetime, rt_price, da_price)` with alias `冀南 → 河北南网`.
- All non-test DB writes need explicit in-session user confirmation (CLAUDE.md). The Task-1 migration is **written only**, applied in Task 20 after confirmation.
- No S3. No real business data committed to git: test fixtures are tiny generated files or short captured text snippets.
- Province terms → `(channel, instrument_type)` per spec D-table; 绿电 keeps tenor channel with `counterparty='绿电'`.
- `rm_positions` grain: day × channel × direction × counterparty (Σ volume, VWAP price). Hour-level fidelity goes to `rm_position_volumes`.
- Commit after every task: `Add <what> for retail-risk ingestion` (+ `Co-Authored-By: Claude Code <noreply@anthropic.com>`). Stage explicit paths only; never stage `infra/terraform/terraform.tfvars`.

## Review Focus

1. **Watermark-contaminated PDF text** （安徽统推 has diagonal stamp text interleaved as single-char lines) — a parser that silently misreads amounts corrupts every downstream number. Expect: cleaning step + invoice total cross-check; mismatch → file written with `status='flagged'`, never silently "processed". (Task 11 test.)
2. **Negative-price lines** （河北 合同转让 cleared at −60.718 元/MWh) — sign must survive parsing and VWAP rollup. Expect: negative amounts/prices preserved; VWAP uses signed sums. (Tasks 2, 10 tests.)
3. **山东 daily→hourly spread conservation** — if 分月分时比例 rows don't sum to 1, spreading inflates/destroys volume. Expect: ratios normalised per month; daily Σ spread volume == daily input volume within 0.1%. (Task 5 test.)
4. **月度竞价 hourly-block semantics** （冀南 月度竞价 clears N MWh per hour-block — interpreted as per-day profile across the delivery month) — wrong interpretation inflates volume ×30. Expect: parser emits per-day rows; Task 20 recon against invoice 中长期电量 is the verification gate. (Tasks 3, 20.)
5. **Scenario filename parsing** (`【分月分时-10】` / `【分月-不分月分时+10】` / base with no suffix) — a mis-mapped scenario silently overwrites another product's curves. Expect: suffix→product map unit-tested for all three filename shapes incl. the 广东 double-dash variant. (Task 7 test.)

---

### Task 1: `schemas.py` + DDL migration file

**Files:**
- Create: `services/retail_risk/schemas.py`
- Create: `db/migrations/2026-09-30_rm_retail_categories_benchmarks.sql`
- Modify: `db/ddl/marketdata/rm_settlements.sql` (category CHECK widened, keep in sync)
- Create: `db/ddl/marketdata/rm_market_benchmarks.sql`
- Test: `tests/services/retail_risk/test_schemas.py`

**Interfaces:**
- Consumes: nothing.
- Produces (used by every later task):
  - `LOAD_BOOK_PROVINCES: list[str]`, `book_name(province) -> str`, `spot_province(province) -> str`
  - `TERM_CHANNEL_INSTRUMENT: dict[str, tuple[str, str]]` — province term → (channel, instrument_type)
  - `JINAN_CATEGORY_RULES / ZHEJIANG_CATEGORY_RULES / ANHUI_CATEGORY_RULES: list[tuple[str, str]]` (prefix, category), `category_for_code(code: str, rules: list[tuple[str,str]]) -> str` (longest prefix wins, default `'other'`)
  - `TRADES_COLS / VOLUMES_COLS / CURVES_COLS / CONTRACTS_COLS / BENCH_COLS: list[str]`
  - `InvoiceItem = TypedDict('InvoiceItem', {category: str, label_cn: str, volume_mwh: float|None, price_cny_mwh: float|None, amount_cny: float, delivery_date: datetime.date|None, notes: str|None})`
  - `@dataclass InvoiceDoc: settlement_month: datetime.date; items: list[InvoiceItem]; total_amount_cny: float|None`

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_schemas.py
import datetime
from services.retail_risk import schemas as s


def test_book_name_and_spot_alias():
    assert s.book_name("冀南") == "景融售电-冀南"
    assert s.spot_province("冀南") == "河北南网"
    assert s.spot_province("山东") == "山东"


def test_term_channel_instrument_map():
    assert s.TERM_CHANNEL_INSTRUMENT["年度双边"] == ("annual", "bilateral")
    assert s.TERM_CHANNEL_INSTRUMENT["月度竞价"] == ("monthly_auction", "forward")
    assert s.TERM_CHANNEL_INSTRUMENT["月度挂牌"] == ("monthly_listed", "forward")
    assert s.TERM_CHANNEL_INSTRUMENT["日滚动"] == ("intramonth_match", "forward")


def test_category_longest_prefix():
    # 河北: 0202030002 偏差收益回收 -> imbalance, although 0202 -> market_redistribution
    assert s.category_for_code("0202030002", s.JINAN_CATEGORY_RULES) == "imbalance"
    assert s.category_for_code("0101", s.JINAN_CATEGORY_RULES) == "midlong_energy"
    assert s.category_for_code("01020203", s.JINAN_CATEGORY_RULES) == "spot_energy"
    assert s.category_for_code("9999", s.JINAN_CATEGORY_RULES) == "other"
    # 安徽 uses different code for 中长期
    assert s.category_for_code("01010201", s.ANHUI_CATEGORY_RULES) == "midlong_energy"


def test_frame_columns():
    assert s.TRADES_COLS == ["delivery_date", "hour", "channel", "instrument_type",
                             "direction", "volume_mwh", "price_cny_mwh",
                             "counterparty", "source_term", "source_file"]
    assert "estimated" in s.VOLUMES_COLS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_schemas.py -v`
Expected: FAIL — `ModuleNotFoundError: services.retail_risk.schemas`

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/schemas.py
"""Canonical frames and vocabulary maps for retail-risk ingestion.

Every parser emits plain DataFrames with these columns; loader.py is the only DB writer.
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import TypedDict

LOAD_BOOK_PROVINCES = ["冀南", "浙江", "山东", "安徽", "福建", "广东", "江苏", "上海"]

_SPOT_ALIAS = {"冀南": "河北南网"}


def book_name(province: str) -> str:
    return f"景融售电-{province}"


def spot_province(province: str) -> str:
    """Province name as used in marketdata.spot_prices_hourly."""
    return _SPOT_ALIAS.get(province, province)


# Province term -> (rm_positions.channel, rm_positions.instrument_type)
TERM_CHANNEL_INSTRUMENT: dict[str, tuple[str, str]] = {
    "年度双边": ("annual", "bilateral"),
    "年度竞价": ("annual", "forward"),
    "年度挂牌": ("annual", "forward"),
    "月度双边": ("monthly_auction", "bilateral"),
    "月度竞价": ("monthly_auction", "forward"),
    "月度挂牌": ("monthly_listed", "forward"),
    "日滚动": ("intramonth_match", "forward"),
    "滚撮": ("intramonth_match", "forward"),
    "月内": ("intramonth_match", "forward"),
    "10日竞价": ("intramonth_match", "forward"),
    "绿电": ("annual", "forward"),  # tenor overridden by source; counterparty='绿电'
}

TRADES_COLS = ["delivery_date", "hour", "channel", "instrument_type",
               "direction", "volume_mwh", "price_cny_mwh",
               "counterparty", "source_term", "source_file"]

VOLUMES_COLS = ["delivery_date", "hour", "channel", "volume_mwh",
                "vwap_cny_mwh", "nominated_mwh", "settled_mwh", "estimated"]

CURVES_COLS = ["province", "product", "delivery_date", "delivery_hour",
               "price_cny_kwh", "curve_date"]

CONTRACTS_COLS = ["customer_name", "contract_ref", "package_name", "package_class",
                  "contract_type", "price_cny_mwh", "share_ratio",
                  "start_date", "end_date", "annual_mwh", "monthly_mwh"]

BENCH_COLS = ["province", "channel", "month", "avg_price_cny_mwh", "volume_mwh",
              "source", "source_file"]

# Benchmark (exchange-stat) channel -> ours, for trader-alpha joins
BENCH_CHANNEL_MAP = {
    "双边协商交易": ["annual", "monthly_auction"],   # bilateral side joins annual + monthly bilateral rows
    "集中竞价交易": ["monthly_auction"],
    "挂牌交易": ["monthly_listed"],
    "月内集中竞价": ["intramonth_match"],
}


class InvoiceItem(TypedDict, total=False):
    category: str
    label_cn: str
    volume_mwh: float | None
    price_cny_mwh: float | None
    amount_cny: float
    delivery_date: datetime.date | None
    notes: str | None


@dataclass
class InvoiceDoc:
    settlement_month: datetime.date          # 1st of month
    items: list[InvoiceItem] = field(default_factory=list)
    total_amount_cny: float | None = None    # printed 售电公司收益/合计, for cross-check


# --- Settlement subject-code -> rm_settlement_items.category -----------------
# Ordered longest-prefix-first matching via category_for_code(). Codes come from
# each exchange's 结算科目编码 hierarchy (see data/exchange-annual-reports/2026年政策/).

JINAN_CATEGORY_RULES: list[tuple[str, str]] = [  # 河北 HEPX
    ("0101", "midlong_energy"),
    ("0102", "spot_energy"),
    ("0201", "green_premium"),
    ("0202030001", "imbalance"), ("0202030002", "imbalance"), ("0202030003", "imbalance"),
    ("0202030010", "market_redistribution"),
    ("0202", "market_redistribution"),
    ("0204", "imbalance"),
    ("021103", "rule_charges"),
]

ZHEJIANG_CATEGORY_RULES: list[tuple[str, str]] = [
    ("0101", "midlong_energy"),
    ("0102", "spot_energy"),
    ("0201", "green_premium"),
    ("02020300", "imbalance"),
    ("0202", "market_redistribution"),
    ("0204", "imbalance"),
    ("021103", "rule_charges"),
]

ANHUI_CATEGORY_RULES: list[tuple[str, str]] = [
    ("01010201", "midlong_energy"),
    ("0101", "midlong_energy"),
    ("0102", "spot_energy"),
    ("0201", "green_premium"),
    ("02020300", "imbalance"),
    ("0202", "market_redistribution"),
    ("0204", "imbalance"),
    ("021103", "rule_charges"),
]

PACKAGE_CLASS_TO_CONTRACT_TYPE = {
    "联动": "indexed",
    "固定": "fixed",
    "分时": "peak_offpeak",
    "峰谷": "peak_offpeak",
}


def contract_type_for(package_class: str) -> str:
    for key, ctype in PACKAGE_CLASS_TO_CONTRACT_TYPE.items():
        if key in (package_class or ""):
            return ctype
    return "indexed_band"


def category_for_code(code: str, rules: list[tuple[str, str]]) -> str:
    """Longest matching subject-code prefix wins; default 'other'."""
    best, best_len = "other", -1
    for prefix, cat in rules:
        if code.startswith(prefix) and len(prefix) > best_len:
            best, best_len = cat, len(prefix)
    return best
```

Migration file (written, NOT applied — Task 20 applies after confirmation):

```sql
-- db/migrations/2026-09-30_rm_retail_categories_benchmarks.sql
-- Widen rm_settlement_items categories for retail wholesale invoices + add benchmark table.
-- Rollback: DELETE rows using the 4 new categories, then reverse the ALTER; DROP TABLE rm_market_benchmarks.

BEGIN;

ALTER TABLE marketdata.rm_settlement_items DROP CONSTRAINT rm_settlement_items_category_check;
ALTER TABLE marketdata.rm_settlement_items ADD CONSTRAINT rm_settlement_items_category_check
  CHECK (category IN ('charge_energy','discharge_energy','generation_revenue',
    'capacity_compensation','bilateral_energy','transmission','govt_surcharges',
    'system_operation','coal_capacity_charge','basic_fee','curtailment','flex_fees',
    'imbalance','market_redistribution','rule_charges','frequency','penalty','rebate',
    'subsidy','other',
    'spot_energy','midlong_energy','retail_revenue','green_premium'));

CREATE TABLE IF NOT EXISTS marketdata.rm_market_benchmarks (
    id                SERIAL PRIMARY KEY,
    province          TEXT NOT NULL,
    channel           TEXT NOT NULL,           -- exchange stat label: 双边协商交易/集中竞价交易/挂牌交易/月内集中竞价
    month             DATE NOT NULL,           -- 1st of delivery month
    avg_price_cny_mwh NUMERIC(10,4) NOT NULL,
    volume_mwh        NUMERIC(14,4),
    source            TEXT NOT NULL DEFAULT 'infohub',
    source_file       TEXT,
    uploaded_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (province, channel, month, source)
);

COMMIT;
```

`db/ddl/marketdata/rm_market_benchmarks.sql`: same CREATE TABLE block (DDL library copy). `db/ddl/marketdata/rm_settlements.sql`: update the CHECK list to match the migration.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_schemas.py -v`
Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/schemas.py tests/services/retail_risk/test_schemas.py \
        db/migrations/2026-09-30_rm_retail_categories_benchmarks.sql \
        db/ddl/marketdata/rm_market_benchmarks.sql db/ddl/marketdata/rm_settlements.sql
git commit -m "Add retail-risk ingestion schemas and DDL migration"
```

---

### Task 2: `loader.py` — the only DB writer

**Files:**
- Create: `services/retail_risk/loader.py`
- Test: `tests/services/retail_risk/test_loader.py`

**Interfaces:**
- Consumes: `services.retail_risk.schemas` (book_name, frame COLS).
- Produces (used by run_backfill, tabs, engines):
  - `get_engine() -> sqlalchemy.Engine` (PGURL or DB_DSN, dotenv `config/.env`)
  - `get_or_create_book(conn, province: str) -> int`
  - `write_trades(conn, book_id: int, df: pd.DataFrame, batch_id: str) -> int` — day-grain rollup, delete-batch-then-insert
  - `write_volumes(conn, book_id: int, df: pd.DataFrame, batch_id: str) -> int` — upsert on (book_id, delivery_date, hour)
  - `write_curves(conn, df: pd.DataFrame) -> int` — upsert
  - `write_contracts(conn, province: str, df: pd.DataFrame) -> tuple[int, int]` — (customers, contracts) counts
  - `write_invoice(conn, book_id: int, doc: InvoiceDoc, file_name: str, file_hash: str) -> int | None` — None when hash already ingested
  - `write_benchmarks(conn, df: pd.DataFrame) -> int` — upsert
  - `file_sha256(path: str) -> str`

- [ ] **Step 1: Write the failing test**

DB-free unit tests using a mock conn that records statements (live-PG test gated separately):

```python
# tests/services/retail_risk/test_loader.py
import datetime
from unittest.mock import MagicMock
import pandas as pd
import pytest
from services.retail_risk import loader, schemas


def _mock_conn(book_id=42):
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = book_id
    return conn


def test_rollup_vwap_signed():
    df = pd.DataFrame([
        ["2026-03-01", 8, "monthly_auction", "forward", "buy", 10.0, 400.0, None, "月度竞价", "f1"],
        ["2026-03-01", 9, "monthly_auction", "forward", "buy", 30.0, 300.0, None, "月度竞价", "f1"],
        ["2026-03-01", 10, "monthly_auction", "forward", "buy", 20.0, -50.0, None, "合同转让", "f2"],
    ], columns=schemas.TRADES_COLS)
    rolled = loader.rollup_trades_day(df)
    row = rolled[(rolled.channel == "monthly_auction")].iloc[0]
    # (10*400 + 30*300 - 20*50) / 60 = 12000/60 = 200.0  (negative price preserved)
    assert row["volume_mwh"] == 60.0
    assert row["price_cny_mwh"] == pytest.approx(200.0)


def test_rollup_status_open_closed():
    today = datetime.date.today()
    past = (today - datetime.timedelta(days=5)).isoformat()
    future = (today + datetime.timedelta(days=5)).isoformat()
    df = pd.DataFrame([
        [past, 8, "annual", "bilateral", "buy", 1.0, 350.0, None, "年度双边", "f"],
        [future, 8, "annual", "bilateral", "buy", 1.0, 350.0, None, "年度双边", "f"],
    ], columns=schemas.TRADES_COLS)
    rolled = loader.rollup_trades_day(df)
    assert rolled.loc[rolled.delivery_date == past, "status"].iloc[0] == "closed"
    assert rolled.loc[rolled.delivery_date == future, "status"].iloc[0] == "open"


def test_write_trades_deletes_batch_first():
    conn = _mock_conn()
    df = pd.DataFrame([["2026-03-01", 8, "monthly_auction", "forward", "buy",
                        1.0, 400.0, None, "月度竞价", "f"]], columns=schemas.TRADES_COLS)
    loader.write_trades(conn, 42, df, "jinan_202603_trades", province="冀南")
    statements = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any("DELETE FROM marketdata.rm_positions" in s and "upload_batch_id" in s for s in statements)
    assert any("INSERT INTO marketdata.rm_positions" in s for s in statements)


def test_write_invoice_dedup_by_hash():
    conn = _mock_conn()
    # first execute (hash check) returns an existing id -> skip
    conn.execute.return_value.scalar.return_value = 7
    doc = schemas.InvoiceDoc(settlement_month=datetime.date(2026, 3, 1), items=[], total_amount_cny=1.0)
    assert loader.write_invoice(conn, 42, doc, "f.pdf", "deadbeef") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_loader.py -v`
Expected: FAIL — `ModuleNotFoundError: services.retail_risk.loader`

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/loader.py
"""Single DB writer for retail-risk ingestion. Parsers never touch the DB."""
from __future__ import annotations

import datetime
import hashlib
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from services.retail_risk import schemas


def get_engine():
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"), override=False)
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise RuntimeError("PGURL or DB_DSN not configured")
    return create_engine(url, pool_pre_ping=True)


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def get_or_create_book(conn, province: str) -> int:
    name = schemas.book_name(province)
    bid = conn.execute(text(
        "SELECT id FROM marketdata.rm_books WHERE name = :n AND book_type = 'load'"
    ), {"n": name}).scalar()
    if bid is not None:
        return bid
    return conn.execute(text(
        "INSERT INTO marketdata.rm_books (name, book_type, asset_id, description) "
        "VALUES (:n, 'load', NULL, :d) RETURNING id"
    ), {"n": name, "d": f"Retail load book for {province} (景融绿色能源售电)"}).scalar()


def rollup_trades_day(df: pd.DataFrame) -> pd.DataFrame:
    """Hour-level trades -> day × channel × direction × counterparty, VWAP price.

    Signed VWAP: negative prices (合同转让 credits) preserved. Status open/closed
    by delivery_date vs today.
    """
    if df.empty:
        return df
    df = df.copy()
    df["delivery_date"] = pd.to_datetime(df["delivery_date"]).dt.date
    df["pv"] = df["volume_mwh"] * df["price_cny_mwh"]
    g = df.groupby(["delivery_date", "channel", "instrument_type", "direction",
                    "counterparty"], dropna=False, as_index=False)
    rolled = g.agg(volume_mwh=("volume_mwh", "sum"), pv=("pv", "sum"))
    rolled["price_cny_mwh"] = rolled["pv"] / rolled["volume_mwh"].replace(0, float("nan"))
    today = datetime.date.today()
    rolled["status"] = rolled["delivery_date"].apply(lambda d: "closed" if d < today else "open")
    return rolled.drop(columns=["pv"])


def write_trades(conn, book_id: int, df: pd.DataFrame, batch_id: str, province: str) -> int:
    rolled = rollup_trades_day(df)
    rolled = rolled[rolled["volume_mwh"] > 0]          # drop zero/NaN-price artefacts
    conn.execute(text(
        "DELETE FROM marketdata.rm_positions WHERE book_id = :b AND upload_batch_id = :bid"
    ), {"b": book_id, "bid": batch_id})
    n = 0
    for r in rolled.itertuples(index=False):
        px = None if pd.isna(r.price_cny_mwh) else r.price_cny_mwh
        conn.execute(text("""
            INSERT INTO marketdata.rm_positions
              (book_id, instrument_type, province, channel, direction, volume_mwh,
               price_cny_mwh, start_date, end_date, counterparty, status, upload_batch_id)
            VALUES (:b, :it, :prov, :ch, :di, :vol, :px, :d, :d, :cp, :st, :bid)
        """), {"b": book_id, "it": r.instrument_type, "prov": province, "ch": r.channel,
               "di": r.direction, "vol": r.volume_mwh, "px": px,
               "d": r.delivery_date, "cp": r.counterparty, "st": r.status, "bid": batch_id})
        n += 1
    return n
```

(Final interface: `write_trades(conn, book_id, df, batch_id, province)` — the mock test passes `province="冀南"`.)

```python
def write_volumes(conn, book_id: int, df: pd.DataFrame, batch_id: str) -> int:
    n = 0
    for r in df.itertuples(index=False):
        conn.execute(text("""
            INSERT INTO marketdata.rm_position_volumes
              (book_id, delivery_date, hour, annual_volume_mwh, annual_price_cny_mwh,
               monthly_auction_volume_mwh, monthly_auction_price_cny_mwh,
               monthly_listed_volume_mwh, monthly_listed_price_cny_mwh,
               intramonth_match_volume_mwh, intramonth_match_price_cny_mwh,
               nominated_mwh, settled_mwh, upload_batch_id)
            VALUES (:b, :d, :h, :av, :ap, :mv, :mp, :lv, :lp, :iv, :ip, :nom, :set, :bid)
            ON CONFLICT (book_id, delivery_date, hour) DO UPDATE SET
              annual_volume_mwh = COALESCE(EXCLUDED.annual_volume_mwh, rm_position_volumes.annual_volume_mwh),
              annual_price_cny_mwh = COALESCE(EXCLUDED.annual_price_cny_mwh, rm_position_volumes.annual_price_cny_mwh),
              monthly_auction_volume_mwh = COALESCE(EXCLUDED.monthly_auction_volume_mwh, rm_position_volumes.monthly_auction_volume_mwh),
              monthly_auction_price_cny_mwh = COALESCE(EXCLUDED.monthly_auction_price_cny_mwh, rm_position_volumes.monthly_auction_price_cny_mwh),
              monthly_listed_volume_mwh = COALESCE(EXCLUDED.monthly_listed_volume_mwh, rm_position_volumes.monthly_listed_volume_mwh),
              monthly_listed_price_cny_mwh = COALESCE(EXCLUDED.monthly_listed_price_cny_mwh, rm_position_volumes.monthly_listed_price_cny_mwh),
              intramonth_match_volume_mwh = COALESCE(EXCLUDED.intramonth_match_volume_mwh, rm_position_volumes.intramonth_match_volume_mwh),
              intramonth_match_price_cny_mwh = COALESCE(EXCLUDED.intramonth_match_price_cny_mwh, rm_position_volumes.intramonth_match_price_cny_mwh),
              nominated_mwh = COALESCE(EXCLUDED.nominated_mwh, rm_position_volumes.nominated_mwh),
              settled_mwh = COALESCE(EXCLUDED.settled_mwh, rm_position_volumes.settled_mwh),
              upload_batch_id = EXCLUDED.upload_batch_id
        """), _volume_params(book_id, r, batch_id))
        n += 1
    return n


def _volume_params(book_id, r, batch_id):
    """Map one VOLUMES_COLS row to per-channel column params (channel -> its columns).
    NaN -> None for every numeric (psycopg2 cannot adapt float('nan'))."""
    def _clean(v):
        return None if v is None or pd.isna(v) else v
    p = {"b": book_id, "d": r.delivery_date, "h": int(r.hour), "bid": batch_id,
         "av": None, "ap": None, "mv": None, "mp": None, "lv": None, "lp": None,
         "iv": None, "ip": None,
         "nom": _clean(getattr(r, "nominated_mwh", None)),
         "set": _clean(getattr(r, "settled_mwh", None))}
    prefix = {"annual": "a", "monthly_auction": "m", "monthly_listed": "l",
              "intramonth_match": "i"}.get(r.channel)
    if prefix:
        p[f"{prefix}v"] = _clean(r.volume_mwh)
        p[f"{prefix}p"] = _clean(r.vwap_cny_mwh)
    return p
```

```python
def write_curves(conn, df: pd.DataFrame) -> int:
    n = 0
    for r in df.itertuples(index=False):
        conn.execute(text("""
            INSERT INTO marketdata.rm_forward_curves
              (province, product, curve_date, delivery_date, delivery_hour, price_cny_kwh, source)
            VALUES (:p, :pr, :cd, :dd, :dh, :px, 'manual')
            ON CONFLICT (province, product, curve_date, delivery_date, delivery_hour, source)
            DO UPDATE SET price_cny_kwh = EXCLUDED.price_cny_kwh
        """), {"p": r.province, "pr": r.product, "cd": r.curve_date,
               "dd": r.delivery_date, "dh": int(r.delivery_hour), "px": r.price_cny_kwh})
        n += 1
    return n


def write_contracts(conn, province: str, df: pd.DataFrame) -> tuple[int, int]:
    n_cust, n_ctr = 0, 0
    for r in df.itertuples(index=False):
        cid = conn.execute(text(
            "SELECT id FROM marketdata.rm_customers WHERE name = :n AND province = :p"
        ), {"n": r.customer_name, "p": province}).scalar()
        if cid is None:
            cid = conn.execute(text("""
                INSERT INTO marketdata.rm_customers (name, province, revenue_share_ratio, status)
                VALUES (:n, :p, :sr, 'active') RETURNING id
            """), {"n": r.customer_name, "p": province,
                   "sr": r.share_ratio if pd.notna(r.share_ratio) else None}).scalar()
            n_cust += 1
        existing = conn.execute(text(
            "SELECT id FROM marketdata.rm_customer_contracts WHERE contract_ref = :cr"
        ), {"cr": r.contract_ref}).scalar()
        if existing is not None:
            continue
        conn.execute(text("""
            INSERT INTO marketdata.rm_customer_contracts
              (customer_id, contract_ref, contract_type, price_cny_mwh,
               start_date, end_date, annual_forecast_mwh, monthly_forecast, contract_status)
            VALUES (:cid, :cr, :ct, :px, :sd, :ed, :am, CAST(:mf AS jsonb), 'active')
        """), {"cid": cid, "cr": r.contract_ref, "ct": r.contract_type,
               "px": r.price_cny_mwh if pd.notna(r.price_cny_mwh) else None,
               "sd": r.start_date, "ed": r.end_date,
               "am": r.annual_mwh if pd.notna(r.annual_mwh) else None,
               "mf": r.monthly_mwh if isinstance(r.monthly_mwh, str) else "{}"})
        n_ctr += 1
    return n_cust, n_ctr


def write_invoice(conn, book_id: int, doc: schemas.InvoiceDoc,
                  file_name: str, file_hash: str) -> int | None:
    """Insert settlement + items. Returns settlement id, or None if hash already ingested.

    Cross-check: Σitems vs doc.total_amount_cny; mismatch >1% -> status='flagged'.
    """
    dup = conn.execute(text(
        "SELECT id FROM marketdata.rm_settlements WHERE raw_data->>'file_hash' = :h"
    ), {"h": file_hash}).scalar()
    if dup is not None:
        return None
    items_sum = sum(i["amount_cny"] for i in doc.items)
    status = "processed"
    if doc.total_amount_cny is not None and doc.items:
        if abs(items_sum - doc.total_amount_cny) > max(0.01 * abs(doc.total_amount_cny), 1.0):
            status = "flagged"
    sid = conn.execute(text("""
        INSERT INTO marketdata.rm_settlements
          (book_id, settlement_month, file_name, file_type, status, total_amount_cny, raw_data)
        VALUES (:b, :m, :f, :ft, :st, :tot, CAST(:raw AS jsonb)) RETURNING id
    """), {"b": book_id, "m": doc.settlement_month, "f": file_name,
           "ft": "pdf" if file_name.lower().endswith(".pdf") else "excel",
           "st": status, "tot": doc.total_amount_cny,
           "raw": '{"file_hash": "' + file_hash + '"}'}).scalar()
    for i in doc.items:
        conn.execute(text("""
            INSERT INTO marketdata.rm_settlement_items
              (settlement_id, category, delivery_date, volume_mwh, price_cny_kwh, amount_cny, notes)
            VALUES (:sid, :cat, :dd, :vol, :px, :amt, :notes)
        """), {"sid": sid, "cat": i["category"], "dd": i.get("delivery_date"),
               "vol": i.get("volume_mwh"),
               "px": (i["price_cny_mwh"] / 1000.0) if i.get("price_cny_mwh") is not None else None,
               "amt": i["amount_cny"],
               "notes": (i.get("label_cn") or "") + ((" | " + i["notes"]) if i.get("notes") else "")})
    return sid


def write_benchmarks(conn, df: pd.DataFrame) -> int:
    n = 0
    for r in df.itertuples(index=False):
        conn.execute(text("""
            INSERT INTO marketdata.rm_market_benchmarks
              (province, channel, month, avg_price_cny_mwh, volume_mwh, source, source_file)
            VALUES (:p, :c, :m, :px, :vol, 'infohub', :sf)
            ON CONFLICT (province, channel, month, source)
            DO UPDATE SET avg_price_cny_mwh = EXCLUDED.avg_price_cny_mwh,
                          volume_mwh = EXCLUDED.volume_mwh,
                          source_file = EXCLUDED.source_file
        """), {"p": r.province, "c": r.channel, "m": r.month, "px": r.avg_price_cny_mwh,
               "vol": r.volume_mwh if pd.notna(r.volume_mwh) else None, "sf": r.source_file})
        n += 1
    return n
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_loader.py -v`
Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/loader.py tests/services/retail_risk/test_loader.py
git commit -m "Add retail-risk ingestion loader"
```

---

### Task 3: `parsers/trades_jinan.py` — 冀南 我的交易结果

**Files:**
- Create: `services/retail_risk/parsers/__init__.py`
- Create: `services/retail_risk/parsers/trades_jinan.py`
- Test: `tests/services/retail_risk/test_trades_jinan.py`

**Interfaces:**
- Consumes: `schemas.TERM_CHANNEL_INSTRUMENT`, `schemas.TRADES_COLS`.
- Produces: `parse_jinan_trades(root: str | Path) -> pd.DataFrame` (TRADES_COLS, hour-level). Used by run_backfill and tab_data_upload.

Format notes (from real files): `中长期交易结果/2026年X月/*.xlsx` and `中长期交易结果/年度/{年度双边,年度竞价}/*.xlsx`, sheet `我的交易结果`, columns `[分时段类型, 交易单元, 买卖方向, 成交电量, 成交均价]`, 分时段类型 like `00:00-01:00`. 月度竞价 files → per-day profile across the whole delivery month (Review Focus #4); 日滚动交易 files carry the delivery date in the filename `(2026-03-28)`; 年度 folder files → whole-year per-day profile.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_trades_jinan.py
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk.parsers.trades_jinan import parse_jinan_trades


def _make_result_xlsx(path: Path, rows):
    """Tiny 我的交易结果 sheet: [分时段类型, 交易单元, 买卖方向, 成交电量, 成交均价]."""
    df = pd.DataFrame(rows, columns=["分时段类型", "交易单元", "买卖方向", "成交电量", "成交均价"])
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="我的交易结果", index=False)


def test_monthly_auction_expands_per_day(tmp_path):
    d = tmp_path / "中长期交易结果" / "2026年3月"
    d.mkdir(parents=True)
    _make_result_xlsx(d / "2026年3月月度竞价.xlsx",
                      [["00:00-01:00", "景融绿色能源售电", "买入", 1.0, 450.0],
                       ["01:00-02:00", "景融绿色能源售电", "买入", 2.0, 419.9]])
    out = parse_jinan_trades(tmp_path)
    mar = out[out.channel == "monthly_auction"]
    assert set(mar.hour) == {0, 1}
    assert mar.delivery_date.nunique() == 31          # per-day across delivery month
    assert set(mar.direction) == {"buy"}
    day0 = mar[(mar.delivery_date == "2026-03-01") & (mar.hour == 0)]
    assert day0.volume_mwh.iloc[0] == 1.0 and day0.price_cny_mwh.iloc[0] == 450.0


def test_daily_rolling_uses_filename_date(tmp_path):
    d = tmp_path / "中长期交易结果" / "2026年3月"
    d.mkdir(parents=True)
    _make_result_xlsx(d / "2026年3月25日日滚动交易(2026-03-28).xlsx",
                      [["00:00-01:00", "景融绿色能源售电", "卖出", 5.0, 380.0]])
    out = parse_jinan_trades(tmp_path)
    row = out.iloc[0]
    assert row.channel == "intramonth_match" and row.direction == "sell"
    assert str(row.delivery_date) == "2026-03-28"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_trades_jinan.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/parsers/__init__.py
"""Province parsers. Registry maps province -> trades parser callable."""
from services.retail_risk.parsers import trades_jinan  # noqa: F401
```

```python
# services/retail_risk/parsers/trades_jinan.py
"""冀南 trades: 中长期交易结果/**/我的交易结果 sheets -> TRADES_COLS frame.

Monthly files (月度竞价/年度双边/年度竞价) are per-day hour-block profiles applied to
every day of the delivery period (Review Focus #4). 日滚动交易 files carry the
delivery date in the filename.
"""
from __future__ import annotations

import calendar
import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_FILE_KIND = [
    ("日滚动交易", ("intramonth_match", "forward")),
    ("月度竞价", ("monthly_auction", "forward")),
    ("月度挂牌", ("monthly_listed", "forward")),
    ("年度双边", ("annual", "bilateral")),
    ("年度竞价", ("annual", "forward")),
]
_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月")
_DATE_RE = re.compile(r"\((\d{4}-\d{2}-\d{2})\)")


def _kind_for(fname: str):
    for key, ci in _FILE_KIND:
        if key in fname:
            return key, ci
    return None, None


def _delivery_dates(path: Path, year: int, month: int) -> list[pd.Timestamp]:
    """月度/年度 files -> all days of delivery period; 日滚动 -> [filename date]."""
    m = _DATE_RE.search(path.name)
    if m:
        return [pd.Timestamp(m.group(1))]
    if "年度" in str(path):
        start, end = pd.Timestamp(year, 1, 1), pd.Timestamp(year, 12, 31)
    else:
        ndays = calendar.monthrange(year, month)[1]
        start, end = pd.Timestamp(year, month, 1), pd.Timestamp(year, month, ndays)
    return list(pd.date_range(start, end, freq="D"))


def _parse_file(path: Path, year: int, month: int) -> pd.DataFrame:
    term, ci = _kind_for(path.name)
    if ci is None:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    try:
        df = pd.read_excel(path, sheet_name="我的交易结果")
    except ValueError:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    df = df.dropna(subset=["成交电量"])
    rows = []
    for r in df.itertuples(index=False):
        block = str(r._0 if hasattr(r, "_0") else r[0])  # 分时段类型 first col
        m = re.match(r"(\d{2}):00-(\d{2}):00", block.replace(" ", ""))
        if not m:
            continue
        hour = int(m.group(1))
        direction = "buy" if "买" in str(r.买卖方向) else "sell"
        for d in _delivery_dates(path, year, month):
            rows.append([d.date(), hour, ci[0], ci[1], direction,
                         float(r.成交电量), float(r.成交均价) if pd.notna(r.成交均价) else None,
                         None, term, path.name])
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def parse_jinan_trades(root: str | Path) -> pd.DataFrame:
    base = Path(root) / "冀南" / "中长期交易结果"
    frames = []
    for path in sorted(base.rglob("*.xlsx")):
        m = _MONTH_RE.search(str(path))
        if m:
            frames.append(_parse_file(path, int(m.group(1)), int(m.group(2))))
        elif "年度" in str(path):
            frames.append(_parse_file(path, 2026, 1))
    if not frames:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    return pd.concat(frames, ignore_index=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_trades_jinan.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/parsers/__init__.py services/retail_risk/parsers/trades_jinan.py \
        tests/services/retail_risk/test_trades_jinan.py
git commit -m "Add Jinan trades parser for retail-risk ingestion"
```

---

### Task 4: `parsers/trades_zhejiang.py` — 浙江 持仓 CSV

**Files:**
- Create: `services/retail_risk/parsers/trades_zhejiang.py`
- Test: `tests/services/retail_risk/test_trades_zhejiang.py`

**Interfaces:**
- Consumes: `schemas.TRADES_COLS`.
- Produces: `parse_zhejiang_trades(root: str | Path) -> pd.DataFrame` (TRADES_COLS, hour-level, buy-side).

Format notes: `浙江/交易记录/景融浙江持仓_20260101_20260831.csv` — `start_time` like `2026/4/1 0:00`; per-channel pairs `position_{yr_sb,yr_jj,yr_gp,month_sb,month_jj,month_gp,10days_jj,cm,green_total}_{volume,price}`; `volume_pre`/`volume_rt` are forecast/actual load → carried as nominated/settled on the volumes frame via a second output.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_trades_zhejiang.py
from pathlib import Path
import pandas as pd
from services.retail_risk.parsers.trades_zhejiang import (
    parse_zhejiang_trades, parse_zhejiang_load,
)


def _make_csv(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([{
        "start_time": "2026/4/1 0:00", "end_time": "2026/4/1 1:00",
        "volume_pre": 42.2, "volume_rt": 38.7,
        "position_yr_sb_volume": 2.44, "position_yr_sb_price": 344.86,
        "position_yr_jj_volume": 8.41, "position_yr_jj_price": 344.842,
        "position_month_gp_volume": 1.5, "position_month_gp_price": 350.0,
        "position_green_total_volume": 0.5, "position_green_total_price": 360.0,
        "position_total_volume": 12.85, "position_total_price": 344.9,
    }])
    for c in ["position_yr_gp_volume","position_yr_gp_price","position_month_sb_volume",
              "position_month_sb_price","position_month_jj_volume","position_month_jj_price",
              "position_10days_jj_volume","position_10days_jj_price",
              "position_cm_volume","position_cm_price"]:
        df[c] = float("nan")
    df.to_csv(path, index=False)


def test_channels_melted(tmp_path):
    f = tmp_path / "浙江" / "交易记录" / "景融浙江持仓_test.csv"
    _make_csv(f)
    out = parse_zhejiang_trades(tmp_path)
    # annual rows: yr_sb (bilateral) and yr_jj (forward) are separate rows
    annual = out[out.channel == "annual"]
    assert set(annual.instrument_type) >= {"bilateral", "forward"}
    assert sorted(annual.volume_mwh) == [2.44, 8.41]
    listed = out[out.channel == "monthly_listed"].iloc[0]
    assert listed.volume_mwh == 1.5 and listed.price_cny_mwh == 350.0
    green = out[out.counterparty == "绿电"].iloc[0]
    assert green.volume_mwh == 0.5
    assert set(out.direction) == {"buy"}


def test_load_frame(tmp_path):
    f = tmp_path / "浙江" / "交易记录" / "景融浙江持仓_test.csv"
    _make_csv(f)
    load = parse_zhejiang_load(tmp_path)
    row = load.iloc[0]
    assert row.nominated_mwh == 42.2 and row.settled_mwh == 38.7 and row.hour == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_trades_zhejiang.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/parsers/trades_zhejiang.py
"""浙江 trades: 景融浙江持仓 CSV -> TRADES_COLS frame + load frame.

CSV has per-channel volume/price pairs per hour. All positions are buy-side
(retailer procurement). 绿电 rows keep channel='annual', counterparty='绿电'.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_CHANNEL_COLS = {  # csv prefix -> (channel, instrument_type, counterparty)
    "yr_sb": ("annual", "bilateral", None),
    "yr_jj": ("annual", "forward", None),
    "yr_gp": ("annual", "forward", None),
    "month_sb": ("monthly_auction", "bilateral", None),
    "month_jj": ("monthly_auction", "forward", None),
    "month_gp": ("monthly_listed", "forward", None),
    "10days_jj": ("intramonth_match", "forward", None),
    "cm": ("intramonth_match", "forward", None),
    "green_total": ("annual", "forward", "绿电"),
}


def _find_csv(root: Path) -> Path | None:
    d = root / "浙江" / "交易记录"
    hits = sorted(d.glob("景融浙江持仓_*.csv"))
    return hits[-1] if hits else None


def parse_zhejiang_trades(root: str | Path) -> pd.DataFrame:
    root = Path(root)
    csv = _find_csv(root)
    if csv is None:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    df = pd.read_csv(csv, parse_dates=["start_time"])
    rows = []
    for r in df.itertuples(index=False):
        d = r.start_time.date()
        h = r.start_time.hour
        for prefix, (ch, inst, cp) in _CHANNEL_COLS.items():
            vol = getattr(r, f"position_{prefix}_volume", None)
            px = getattr(r, f"position_{prefix}_price", None)
            if vol is None or pd.isna(vol) or float(vol) == 0:
                continue
            rows.append([d, h, ch, inst, "buy", float(vol),
                         float(px) if pd.notna(px) else None,
                         cp, prefix, csv.name])
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def parse_zhejiang_load(root: str | Path) -> pd.DataFrame:
    """Forecast/actual load -> (delivery_date, hour, nominated_mwh, settled_mwh)."""
    root = Path(root)
    csv = _find_csv(root)
    if csv is None:
        return pd.DataFrame(columns=["delivery_date", "hour", "nominated_mwh", "settled_mwh"])
    df = pd.read_csv(csv, parse_dates=["start_time"])
    out = pd.DataFrame({
        "delivery_date": df["start_time"].dt.date,
        "hour": df["start_time"].dt.hour,
        "nominated_mwh": df["volume_pre"],
        "settled_mwh": df["volume_rt"],
    })
    return out.dropna(how="all", subset=["nominated_mwh", "settled_mwh"])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_trades_zhejiang.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/parsers/trades_zhejiang.py tests/services/retail_risk/test_trades_zhejiang.py
git commit -m "Add Zhejiang trades parser for retail-risk ingestion"
```

---

### Task 5: `parsers/trades_shandong.py` — 山东 持仓明细 (daily → hourly spread)

**Files:**
- Create: `services/retail_risk/parsers/trades_shandong.py`
- Test: `tests/services/retail_risk/test_trades_shandong.py`

**Interfaces:**
- Consumes: `schemas.TRADES_COLS`; ratio frame from `parsers.mtm_workbook.load_hourly_ratios(path)` (Task 7 provides it; this task mocks it in tests).
- Produces: `parse_shandong_trades(root: str | Path, ratio_df: pd.DataFrame) -> pd.DataFrame` (TRADES_COLS with `estimated` marker via source_term suffix); `parse_shandong_load(root) -> pd.DataFrame` (nominated/settled). `ratio_df` columns: `month:int, hour:int, ratio:float` (rows sum to 1 per month — normalised inside Task 7's loader).

Format notes: `山东/2026XX/景融/2026年X月持仓明细.xlsx`, sheet `月前中长期持仓`: col0 = date, then `预估用电量, 实际用电量, 绿电, 年度双边, 年度竞价, 年度挂牌, 月度双边, 月度竞价, 月度挂牌1, 月度挂牌2` daily MWh. Sheet `月前中长期电价`: same layout with prices (verify at implementation; if layout differs, adapt `_parse_detail` and keep tests on the fixture).

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_trades_shandong.py
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk.parsers.trades_shandong import parse_shandong_trades


def _make_detail(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([
        ["2026-03-01", 7061, 7546.9, 138.7, 0, 0, 0, 0, 2000, 535.7, 98.4],
        ["2026-03-02", 7061, 7662.2, 138.7, 0, 0, 0, 0, 2000, 535.7, 98.4],
    ], columns=["日期", "预估用电量", "实际用电量", "绿电", "年度双边", "年度竞价",
                "年度挂牌", "月度双边", "月度竞价", "月度挂牌1", "月度挂牌2"])
    px = df.copy()
    for c in px.columns[3:]:
        px[c] = 350.0
    px["月度竞价"] = 400.0
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="月前中长期持仓", index=False)
        px.to_excel(w, sheet_name="月前中长期电价", index=False)


def _ratios() -> pd.DataFrame:
    return pd.DataFrame({"month": [3] * 24, "hour": list(range(24)),
                         "ratio": [1 / 24] * 24})


def test_daily_spread_conserves_volume(tmp_path):
    _make_detail(tmp_path / "山东" / "202603" / "景融" / "2026年3月持仓明细.xlsx")
    out = parse_shandong_trades(tmp_path, _ratios())
    ma = out[out.channel == "monthly_auction"]
    day1 = ma[ma.delivery_date.astype(str) == "2026-03-01"]
    assert day1.volume_mwh.sum() == pytest.approx(2000.0, rel=1e-3)   # conservation
    assert set(day1.hour) == set(range(24))
    assert day1.price_cny_mwh.iloc[0] == 400.0
    assert "est" in day1.source_term.iloc[0]                          # estimated flag


def test_green_counterparty(tmp_path):
    _make_detail(tmp_path / "山东" / "202603" / "景融" / "2026年3月持仓明细.xlsx")
    out = parse_shandong_trades(tmp_path, _ratios())
    green = out[out.counterparty == "绿电"]
    assert not green.empty and green.volume_mwh.sum() == pytest.approx(138.7 * 2, rel=1e-3)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_trades_shandong.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/parsers/trades_shandong.py
"""山东 trades: 持仓明细 daily × channel -> hour-level TRADES_COLS via 分月分时比例.

Daily channel volumes are spread to 24 hours by the MTM workbook's 分月分时比例
(estimated rows: source_term gets ' (est)'). Prices come from the 月前中长期电价
sheet (same layout). 预估/实际用电量 -> load frame (nominated/settled).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_CHANNEL_COLS = {
    "绿电": ("annual", "forward", "绿电"),
    "年度双边": ("annual", "bilateral", None),
    "年度竞价": ("annual", "forward", None),
    "年度挂牌": ("annual", "forward", None),
    "月度双边": ("monthly_auction", "bilateral", None),
    "月度竞价": ("monthly_auction", "forward", None),
    "月度挂牌1": ("monthly_listed", "forward", None),
    "月度挂牌2": ("monthly_listed", "forward", None),
}


def _detail_files(root: Path) -> list[Path]:
    return sorted((root / "山东").glob("2026*/景融/*持仓明细.xlsx"))


def _spread_day(date, vols: dict, pxs: dict, ratio_df: pd.DataFrame) -> list:
    rows = []
    r = ratio_df[ratio_df.month == date.month]
    if r.empty:
        return rows
    for col, (ch, inst, cp) in _CHANNEL_COLS.items():
        vol = vols.get(col)
        if vol is None or pd.isna(vol) or float(vol) == 0:
            continue
        px = pxs.get(col)
        for rr in r.itertuples(index=False):
            rows.append([date, int(rr.hour), ch, inst, "buy",
                         float(vol) * float(rr.ratio),
                         float(px) if px is not None and pd.notna(px) else None,
                         cp, f"{col} (est)", None])
    return rows


def parse_shandong_trades(root: str | Path, ratio_df: pd.DataFrame) -> pd.DataFrame:
    root = Path(root)
    rows = []
    for f in _detail_files(root):
        vols = pd.read_excel(f, sheet_name="月前中长期持仓")
        try:
            pxs = pd.read_excel(f, sheet_name="月前中长期电价")
        except ValueError:
            pxs = pd.DataFrame()
        vols = vols.rename(columns={vols.columns[0]: "日期"})
        pxs = pxs.rename(columns={pxs.columns[0]: "日期"}) if not pxs.empty else pxs
        vols["日期"] = pd.to_datetime(vols["日期"]).dt.date
        if not pxs.empty:
            pxs["日期"] = pd.to_datetime(pxs["日期"]).dt.date
        for r in vols.itertuples(index=False):
            v = {c: getattr(r, c, None) for c in _CHANNEL_COLS}
            p = {}
            if not pxs.empty:
                prow = pxs[pxs["日期"] == r.日期]
                if not prow.empty:
                    p = {c: prow.iloc[0][c] for c in _CHANNEL_COLS if c in prow.columns}
            rows.extend(_spread_day(r.日期, v, p, ratio_df))
    out = pd.DataFrame(rows, columns=schemas.TRADES_COLS)
    if not out.empty:
        out["source_file"] = "持仓明细"
    return out


def parse_shandong_load(root: str | Path) -> pd.DataFrame:
    root = Path(root)
    frames = []
    for f in _detail_files(root):
        vols = pd.read_excel(f, sheet_name="月前中长期持仓")
        vols = vols.rename(columns={vols.columns[0]: "日期"})
        frames.append(pd.DataFrame({
            "delivery_date": pd.to_datetime(vols["日期"]).dt.date,
            "nominated_mwh": vols["预估用电量"],
            "settled_mwh": vols["实际用电量"],
        }))
    if not frames:
        return pd.DataFrame(columns=["delivery_date", "nominated_mwh", "settled_mwh"])
    return pd.concat(frames, ignore_index=True)
```

Note: 山东 load is daily-grain; when writing volumes for 山东， `run_backfill` spreads load the same way (ratio frame) and sets `nominated_mwh`/`settled_mwh` per hour. That logic lives in run_backfill (Task 15), not the loader.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_trades_shandong.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/parsers/trades_shandong.py tests/services/retail_risk/test_trades_shandong.py
git commit -m "Add Shandong trades parser for retail-risk ingestion"
```

---

### Task 6: `parsers/trades_anhui.py` — 安徽 滚撮 + 中长期合同批次

**Files:**
- Create: `services/retail_risk/parsers/trades_anhui.py`
- Test: `tests/services/retail_risk/test_trades_anhui.py`

**Interfaces:**
- Consumes: `schemas.TRADES_COLS`.
- Produces: `parse_anhui_trades(root: str | Path) -> pd.DataFrame`.

Format notes:
(a) `安徽/交易记录/X月-中长期交易.xlsx` sheet `滚搓市场成交结果`: `[申报日, 标的日, 交易类型, 平均, 1点…24点]`; rows alternate 电量/电价 per （申报日， 标的日） pair.
(b) `安徽/交易记录/安徽中长期合同2026-月度多月度.xlsx` sheets like `9-12 批次1`: col2=company, col3=`2026-09-01-2026-09-01` (delivery range), col4=`段9:08:00-09:00` (hour segment), col5=volume, col6=price. Channel for batch sheets: `monthly_auction` — **confirm with user at implementation** (spec ambiguity: 批次 = monthly trading batches).

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_trades_anhui.py
from pathlib import Path
import pandas as pd
from services.retail_risk.parsers.trades_anhui import parse_anhui_trades


def _make_guncuo(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["申报日", "标的日", "交易类型", "平均"] + [f"{h}点" for h in range(1, 25)]
    vol = ["2026-02-27", "2026-03-01", "电量", None] + [100.0] * 24
    px = ["2026-02-27", "2026-03-01", "电价", 293.3] + [266.8] * 24
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame([vol, px], columns=cols).to_excel(w, sheet_name="滚搓市场成交结果", index=False)


def _make_contracts(path: Path):
    rows = [[1, None, "景融绿色能源科技有限公司", "2026-09-01-2026-09-01",
             "段9:08:00-09:00", 9.0, 175.0, "景融绿色能源科技有限公司"],
            [2, None, "景融绿色能源科技有限公司", "2026-09-02-2026-09-02",
             "段10:09:00-10:00", 9.5, 175.0, "景融绿色能源科技有限公司"]]
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="9-12 批次1", index=False, header=False)


def test_guncuo_hourly_pairs(tmp_path):
    _make_guncuo(tmp_path / "安徽" / "交易记录" / "3月-中长期交易.xlsx")
    out = parse_anhui_trades(tmp_path)
    g = out[out.channel == "intramonth_match"]
    assert len(g) == 24
    row = g[g.hour == 0].iloc[0]
    assert row.volume_mwh == 100.0 and row.price_cny_mwh == 266.8
    assert str(row.delivery_date) == "2026-03-01"


def test_contract_batches(tmp_path):
    _make_contracts(tmp_path / "安徽" / "交易记录" / "安徽中长期合同2026-月度多月度.xlsx")
    out = parse_anhui_trades(tmp_path)
    b = out[out.source_term.str.contains("批次", na=False)]
    assert len(b) == 2
    assert set(b.channel) == {"monthly_auction"}
    assert b.iloc[0].volume_mwh == 9.0 and b.iloc[0].price_cny_mwh == 175.0
    assert b.iloc[0].hour == 8      # 段9 = 08:00-09:00 -> hour 8 (0-indexed)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_trades_anhui.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/parsers/trades_anhui.py
"""安徽 trades: 滚搓市场成交结果 (hourly cleared vol/price) + 中长期合同批次 sheets."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_HOUR_RE = re.compile(r"段\d+:(\d{2}):00-(\d{2}):00")


def _parse_guncuo(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_excel(path, sheet_name="滚搓市场成交结果")
    except ValueError:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    hour_cols = [c for c in df.columns if re.fullmatch(r"\d{1,2}点", str(c))]
    rows = []
    i = 0
    while i < len(df) - 1:
        vol_row, px_row = df.iloc[i], df.iloc[i + 1]
        if str(vol_row.get("交易类型")) == "电量" and str(px_row.get("交易类型")) == "电价":
            delivery = pd.to_datetime(vol_row["标的日"]).date()
            for c in hour_cols:
                h = int(str(c).replace("点", "")) - 1       # 1点 = hour 0
                vol, px = vol_row[c], px_row[c]
                if pd.notna(vol) and float(vol) != 0:
                    rows.append([delivery, h, "intramonth_match", "forward", "buy",
                                 float(vol), float(px) if pd.notna(px) else None,
                                 None, "滚撮", path.name])
            i += 2
        else:
            i += 1
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def _parse_contracts(path: Path) -> pd.DataFrame:
    rows = []
    for sheet in pd.ExcelFile(path).sheet_names:
        df = pd.read_excel(path, sheet_name=sheet, header=None)
        for r in df.itertuples(index=False):
            try:
                date_range, seg = str(r[3]), str(r[4])
                m = _HOUR_RE.search(seg.replace(" ", ""))
                if not m:
                    continue
                delivery = pd.to_datetime("-".join(date_range.split("-")[:3])).date()
                rows.append([delivery, int(m.group(1)), "monthly_auction", "forward", "buy",
                             float(r[5]), float(r[6]) if pd.notna(r[6]) else None,
                             None, f"批次:{sheet}", path.name])
            except (ValueError, TypeError, IndexError):
                continue
    return pd.DataFrame(rows, columns=schemas.TRADES_COLS)


def parse_anhui_trades(root: str | Path) -> pd.DataFrame:
    d = Path(root) / "安徽" / "交易记录"
    frames = []
    for f in sorted(d.glob("*月-中长期交易.xlsx")):
        frames.append(_parse_guncuo(f))
    for f in sorted(d.glob("安徽中长期合同*.xlsx")):
        frames.append(_parse_contracts(f))
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=schemas.TRADES_COLS)
    return pd.concat(frames, ignore_index=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_trades_anhui.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/parsers/trades_anhui.py tests/services/retail_risk/test_trades_anhui.py
git commit -m "Add Anhui trades parser for retail-risk ingestion"
```

---

### Task 7: `parsers/mtm_workbook.py` — curves + contracts + hourly ratios

**Files:**
- Create: `services/retail_risk/parsers/mtm_workbook.py`
- Test: `tests/services/retail_risk/test_mtm_workbook.py`

**Interfaces:**
- Consumes: `schemas.CURVES_COLS`, `schemas.CONTRACTS_COLS`, `schemas.contract_type_for`.
- Produces:
  - `parse_mtm_workbook(path: str | Path) -> dict` with keys `"curves"` (CURVES_COLS frame), `"contracts"` (CONTRACTS_COLS frame; only from base-scenario file), `"province"`, `"product"`
  - `load_hourly_ratios(path: str | Path) -> pd.DataFrame` (`month:int, hour:int, ratio:float`, normalised per month) — consumed by trades_shandong and run_backfill 山东 load spread
  - `scenario_for_filename(name: str) -> str` (`spot_base|spot_p10|spot_m10`)
  - `mtm_files(root) -> list[Path]` — the 24 测算 workbooks under `台账/mtm/` (excludes 信息汇总 and 【0】汇总）

Format notes: filename shapes `1 【广东测算】零售合同Mark to Market利润测算【分月-不分月分时-10】.xlsx`, `8 【山东测算】...【分月分时】.xlsx` (no suffix = base), `4 【江苏测算-更新】2026生效用户-利润测算&电量统计-汇总+10.xlsx` (suffix at end). Price sheet: header cell `现货价格（中价假设）`, col0 = `1月`…`12月`, cols 1..24 = hours 0..23, values CNY/MWh. Contract sheet name contains `合约` and `总表`. Ratio sheet name contains `分月分时比例`.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_mtm_workbook.py
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk.parsers import mtm_workbook as m


def test_scenario_for_filename():
    assert m.scenario_for_filename("8 【山东测算】零售合同Mark to Market利润测算【分月分时-10】.xlsx") == "spot_m10"
    assert m.scenario_for_filename("1 【广东测算】零售合同Mark to Market利润测算【分月-不分月分时+10】.xlsx") == "spot_p10"
    assert m.scenario_for_filename("8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx") == "spot_base"
    assert m.scenario_for_filename("4 【江苏测算-更新】2026生效用户-利润测算&电量统计-汇总-10.xlsx") == "spot_m10"


def test_province_for_filename():
    assert m.province_for_filename("8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx") == "山东"
    assert m.province_for_filename("4 【江苏测算-更新】2026生效用户-利润测算&电量统计-汇总.xlsx") == "江苏"


def _make_workbook(path: Path):
    price = pd.DataFrame([["1月"] + [300.0 + h for h in range(24)],
                          ["2月"] + [310.0 + h for h in range(24)]],
                         columns=["现货价格（中价假设）"] + list(range(24)))
    contracts = pd.DataFrame([
        [1, "测试用户A", "单元1", "2025-12-22", "2026-01-01", "2026-03-31",
         "景融参考价格联动类0792", "联动+上浮", "已生效", 6.0, 100.0, 200.0, 300.0, 600.0, "渠道X", 0.9],
    ], columns=["序号", "零售用户名称", "交易单元名称", "建立时间", "生效时间", "失效时间",
                "套餐名称", "套餐类别", "状态", "套餐价格", "1月电量", "2月电量", "3月电量",
                "年度电量/万度（匹配原始台账）", "渠道归属", "渠道分成比例"])
    ratio = pd.DataFrame([["1月"] + [1 / 24] * 24], columns=["分月分时比例"] + list(range(24)))
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        price.to_excel(w, sheet_name="山东模型价格预测", index=False)
        contracts.to_excel(w, sheet_name="新山东零售合约-总表", index=False)
        ratio.to_excel(w, sheet_name="山东分月分时比例", index=False)


def test_curves_expand_month_hour_to_days(tmp_path):
    f = tmp_path / "8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_workbook(f)
    out = m.parse_mtm_workbook(f)
    assert out["province"] == "山东" and out["product"] == "spot_base"
    curves = out["curves"]
    jan = curves[(curves.delivery_date.astype(str).str.startswith("2026-01"))]
    assert jan.delivery_date.nunique() == 31 and set(jan.delivery_hour) == set(range(24))
    h0 = jan[(jan.delivery_date.astype(str) == "2026-01-01") & (jan.delivery_hour == 0)]
    assert h0.price_cny_kwh.iloc[0] == pytest.approx(0.300)   # CNY/MWh -> /1000


def test_contracts_parsed(tmp_path):
    f = tmp_path / "8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_workbook(f)
    out = m.parse_mtm_workbook(f)
    c = out["contracts"].iloc[0]
    assert c.customer_name == "测试用户A" and c.contract_type == "indexed"
    assert c.price_cny_mwh == 6.0 and c.share_ratio == 0.9
    assert c.annual_mwh == 6000.0          # 万度 -> MWh ×10
    assert '"1": 1000.0' in c.monthly_mwh  # JSON string, 万度 -> MWh


def test_ratios_normalised(tmp_path):
    f = tmp_path / "8 【山东测算】零售合同Mark to Market利润测算【分月分时】.xlsx"
    _make_workbook(f)
    r = m.load_hourly_ratios(f)
    assert r[r.month == 1].ratio.sum() == pytest.approx(1.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_mtm_workbook.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/parsers/mtm_workbook.py
"""MTM 测算 workbooks (台账/mtm/): forward curves, retail contracts, hourly ratios."""
from __future__ import annotations

import calendar
import json
import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_PROVINCE_RE = re.compile(r"【(.+?)测算")
_SCENARIO_SUFFIX = {"-10": "spot_m10", "+10": "spot_p10"}


def scenario_for_filename(name: str) -> str:
    stem = name.rsplit(".", 1)[0]
    for suffix, product in _SCENARIO_SUFFIX.items():
        if stem.endswith(suffix + "】") or stem.endswith(suffix):
            return product
    return "spot_base"


def province_for_filename(name: str) -> str:
    m = _PROVINCE_RE.search(name)
    if not m:
        raise ValueError(f"Cannot parse province from {name}")
    return m.group(1)


def mtm_files(root: str | Path) -> list[Path]:
    d = Path(root) / "台账" / "mtm"
    return [p for p in sorted(d.glob("*.xlsx"))
            if "测算" in p.name and "信息汇总" not in p.name]


def _find_sheet(xl: pd.ExcelFile, *keywords: str) -> str | None:
    for s in xl.sheet_names:
        if all(k in s for k in keywords):
            return s
    return None


def _month_hour_grid(df: pd.DataFrame) -> pd.DataFrame:
    """[month label col0, hour cols 1..24] -> long frame (month, hour, value)."""
    df = df.rename(columns={df.columns[0]: "月份"})
    df = df[df["月份"].astype(str).str.contains("月", na=False)]
    df["month"] = df["月份"].astype(str).str.extract(r"(\d{1,2})").astype(int)
    long = df.melt(id_vars=["month"], var_name="hour", value_name="value")
    long["hour"] = long["hour"].astype(int)
    return long.dropna(subset=["value"])


def parse_mtm_workbook(path: str | Path) -> dict:
    path = Path(path)
    province = province_for_filename(path.name)
    product = scenario_for_filename(path.name)
    xl = pd.ExcelFile(path)
    curve_date = pd.Timestamp(path.stat().st_mtime, unit="s").date()

    curves = pd.DataFrame(columns=schemas.CURVES_COLS)
    price_sheet = _find_sheet(xl, "模型价格预测") or _find_sheet(xl, "价格预测")
    if price_sheet:
        grid = _month_hour_grid(xl.parse(price_sheet))
        rows = []
        for r in grid.itertuples(index=False):
            ndays = calendar.monthrange(2026, r.month)[1]
            for day in range(1, ndays + 1):
                rows.append([province, product, f"2026-{r.month:02d}-{day:02d}",
                             int(r.hour), float(r.value) / 1000.0, curve_date])
        curves = pd.DataFrame(rows, columns=schemas.CURVES_COLS)

    contracts = pd.DataFrame(columns=schemas.CONTRACTS_COLS)
    c_sheet = _find_sheet(xl, "合约", "总表")
    if c_sheet and product == "spot_base":   # contracts identical across scenarios
        cdf = xl.parse(c_sheet)
        month_cols = [c for c in cdf.columns if re.fullmatch(r"\d{1,2}月(/\d{1,2}月)?电量", str(c))]
        annual_col = next((c for c in cdf.columns if "年度电量" in str(c)), None)
        rows = []
        for r in cdf.itertuples(index=False):
            if pd.isna(getattr(r, "零售用户名称", None)):
                continue
            monthly = {}
            for c in month_cols:
                mnum = re.match(r"(\d{1,2})月", str(c)).group(1)
                v = cdf.loc[r.Index, c]
                if pd.notna(v):
                    monthly[mnum] = float(v) * 10.0      # 万度 -> MWh
            rows.append([
                str(r.零售用户名称), str(r.序号), str(getattr(r, "套餐名称", "")),
                str(getattr(r, "套餐类别", "")),
                schemas.contract_type_for(str(getattr(r, "套餐类别", ""))),
                float(r.套餐价格) if pd.notna(getattr(r, "套餐价格", None)) else None,
                float(r.渠道分成比例) if pd.notna(getattr(r, "渠道分成比例", None)) else None,
                pd.to_datetime(r.生效时间).date(), pd.to_datetime(r.失效时间).date(),
                float(cdf.loc[r.Index, annual_col]) * 10.0
                if annual_col and pd.notna(cdf.loc[r.Index, annual_col]) else None,
                json.dumps(monthly),
            ])
        contracts = pd.DataFrame(rows, columns=schemas.CONTRACTS_COLS)

    return {"province": province, "product": product,
            "curves": curves, "contracts": contracts}


def load_hourly_ratios(path: str | Path) -> pd.DataFrame:
    """分月分时比例 sheet -> (month, hour, ratio), normalised to sum 1 per month."""
    path = Path(path)
    xl = pd.ExcelFile(path)
    sheet = _find_sheet(xl, "分月分时比例")
    if sheet is None:
        return pd.DataFrame(columns=["month", "hour", "ratio"])
    grid = _month_hour_grid(xl.parse(sheet))
    grid["ratio"] = grid["value"] / grid.groupby("month")["value"].transform("sum")
    return grid[["month", "hour", "ratio"]]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_mtm_workbook.py -v`
Expected: 5 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/parsers/mtm_workbook.py tests/services/retail_risk/test_mtm_workbook.py
git commit -m "Add MTM workbook parser for retail-risk ingestion"
```

---

### Task 8: `parsers/benchmark_infohub.py` — 信息汇总 中长期价格

**Files:**
- Create: `services/retail_risk/parsers/benchmark_infohub.py`
- Test: `tests/services/retail_risk/test_benchmark_infohub.py`

**Interfaces:**
- Consumes: `schemas.BENCH_COLS`.
- Produces: `parse_infohub_benchmarks(path: str | Path) -> pd.DataFrame` (BENCH_COLS); `infohub_files(root) -> list[Path]`.

Format notes （山东 file, verified): sheet `中长期价格` has TWO blocks. Right block (starts ~row 40 col 8): channel headers `双边协商交易 / 集中竞价交易 / 月内集中竞价 / 挂牌交易` each spanning 2 sub-columns (`净合约量`, `加权均价`), with a `月份` column; rows are monthly series (2024–2026 YTD), ending with a `合计` row (excluded). The parser must locate the block by finding the header row containing `加权均价`, not by fixed coordinates.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_benchmark_infohub.py
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk.parsers.benchmark_infohub import parse_infohub_benchmarks


def _make_infohub(path: Path):
    # Right block at rows 2+, col D: 月份 | 双边协商交易(净合约量,加权均价) | 集中竞价交易(...)
    header1 = [None, None, None, "月份", "双边协商交易", None, "集中竞价交易", None]
    header2 = [None, None, None, None, "净合约量", "加权均价", "净合约量", "加权均价"]
    rows = [
        [None, None, None, "1 月", 72.1, 372.21, 18.6, 371.63],
        [None, None, None, "2 月", 76.1, 372.16, 17.3, 371.0],
        [None, None, None, "合计", 803.1, 372.11, 204.4, 373.08],
    ]
    pad = [[None] * 8, [None] * 8]
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame(pad + [header1, header2] + rows).to_excel(
            w, sheet_name="中长期价格", index=False, header=False)


def test_right_block_extracted(tmp_path):
    f = tmp_path / "山东电力市场信息汇总by20260131.xlsx"
    _make_infohub(f)
    out = parse_infohub_benchmarks(f)
    assert set(out.channel) == {"双边协商交易", "集中竞价交易"}
    assert len(out[out.channel == "双边协商交易"]) == 2          # 合计 row excluded
    row = out[(out.channel == "双边协商交易") & (out.month.astype(str) == "2026-01-01")]
    assert row.avg_price_cny_mwh.iloc[0] == pytest.approx(372.21)
    assert row.volume_mwh.iloc[0] == pytest.approx(72.1 * 10000)  # 亿度 -> MWh
    assert set(out.province) == {"山东"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_benchmark_infohub.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/parsers/benchmark_infohub.py
"""信息汇总 workbooks: 中长期价格 sheet right block -> market benchmarks.

Right block layout: a header row whose cells contain channel names (双边协商交易,
集中竞价交易, 挂牌交易, 月内集中竞价), each spanning two columns (净合约量, 加权均价),
plus a 月份 column. Located by anchors, never by fixed coordinates. Volumes are 亿度
(x10,000 -> MWh). Years inferred from file rows (year column or 'by<date>' fallback
for single-year blocks); rows labelled 合计 are excluded.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_CHANNELS = ["双边协商交易", "集中竞价交易", "挂牌交易", "月内集中竞价"]
_PROVINCE_RE = re.compile(r"^(.+?)电力市场信息汇总")
_YEAR_RE = re.compile(r"by(\d{4})\d{4}")


def infohub_files(root: str | Path) -> list[Path]:
    d = Path(root) / "台账" / "mtm"
    return sorted(d.glob("*电力市场信息汇总*.xlsx"))


def parse_infohub_benchmarks(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    pm = _PROVINCE_RE.match(path.name)
    if not pm:
        raise ValueError(f"Cannot parse province from {path.name}")
    province = pm.group(1)
    default_year = int(_YEAR_RE.search(path.name).group(1)) if _YEAR_RE.search(path.name) else 2026

    df = pd.read_excel(path, sheet_name="中长期价格", header=None)
    # locate the header row containing 加权均价
    hdr_idx = None
    for i in range(len(df)):
        if any("加权均价" in str(v) for v in df.iloc[i]):
            hdr_idx = i
            break
    if hdr_idx is None:
        return pd.DataFrame(columns=schemas.BENCH_COLS)

    chan_row = df.iloc[hdr_idx - 1]     # channel names sit one row above 净合约量/加权均价
    month_col = next(j for j, v in enumerate(chan_row) if "月份" in str(v))
    chan_cols = []                       # (channel, vol_col, price_col)
    for j, v in enumerate(chan_row):
        v = str(v)
        if v in _CHANNELS:
            chan_cols.append((v, j, j + 1))

    rows = []
    for i in range(hdr_idx + 1, len(df)):
        label = str(df.iloc[i, month_col]).replace(" ", "")
        if "合计" in label or "月" not in label:
            continue
        mnum = int(re.match(r"(\d{1,2})月", label).group(1))
        year = default_year
        for j in range(month_col):       # a year marker left of 月份, if present
            yv = df.iloc[i, j]
            if pd.notna(yv) and re.fullmatch(r"20\d{2}(\.0)?", str(yv)):
                year = int(float(yv))
        for chan, vc, pc in chan_cols:
            px, vol = df.iloc[i, pc], df.iloc[i, vc]
            if pd.isna(px):
                continue
            rows.append([province, chan, f"{year}-{mnum:02d}-01", float(px),
                         float(vol) * 10000.0 if pd.notna(vol) else None,
                         "infohub", path.name])
    return pd.DataFrame(rows, columns=schemas.BENCH_COLS)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_benchmark_infohub.py -v`
Expected: 1 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/parsers/benchmark_infohub.py tests/services/retail_risk/test_benchmark_infohub.py
git commit -m "Add market benchmark parser for retail-risk ingestion"
```

---

### Task 9: `parsers/invoice_common.py` + `invoice_shandong.py`

**Files:**
- Create: `services/retail_risk/parsers/invoice_common.py`
- Create: `services/retail_risk/parsers/invoice_shandong.py`
- Test: `tests/services/retail_risk/test_invoice_common.py`

**Interfaces:**
- Consumes: `schemas.InvoiceDoc`, `schemas.InvoiceItem`, `schemas.category_for_code`.
- Produces:
  - `parse_subject_lines(text: str, rules: list[tuple[str,str]]) -> list[InvoiceItem]` — 科目编码 line parser shared by all PDF invoices
  - `extract_pdf_text(path: str | Path) -> str` (pdfplumber, all pages)
  - `parse_total_from_summary(text: str) -> float | None` （售电公司收益 / 结算电费 from the summary block)
  - `parse_shandong_invoice(path) -> InvoiceDoc` (7021 Excel: 结算依据 daily RT rows)
- Consumed by Tasks 10–11 (province PDF parsers) and run_backfill.

Format notes （科目编码 lines, 河北/浙江 verified): `0101020302 现货日其他电力直接交易 16338.000 16338.000 372.205 6081078.57` — code, name (may contain CJK and parentheses), then up to 4 numerics （分月交易计划电量， 结算电量， 结算均价， 结算电费）, missing values shown as `-`. Category codes are hierarchical (longest-prefix rules from schemas). 山东 7021 Excel 结算依据： header rows 0–5, then daily rows `[结算单元, 日期, 实时用电量, 实时市场电价, 实时电能量电费, 日前出清电量, 日前电价, 日前电费]`, last row `合计`.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_invoice_common.py
from pathlib import Path
import pandas as pd
import pytest
from services.retail_risk import schemas
from services.retail_risk.parsers.invoice_common import (
    parse_subject_lines, parse_total_from_summary,
)
from services.retail_risk.parsers.invoice_shandong import parse_shandong_invoice

JINAN_TEXT = """
结算单编号：HEPX-2026-03-SD0100
本月 175251.68
结算科目编码 结算科目 分月交易计划电量 结算电量 结算均价 结算电费 备注
01 电量清分 17770.000 21906.460 331.580 7263741.37
0101 中长期交易 17770.000 17770.000 337.685 6000657.47
010105 合同交易 1418.946 1418.946 -60.718 -86155.80
0102 现货交易 - 4136.460 305.354 1263083.90
0202030002 中长期偏差收益回收（差额） - - - 10383.71
0211030001 零售市场超额收益费用 - - - 193111.28
第1页，共6页
"""


def test_subject_lines_categories_and_signs():
    items = parse_subject_lines(JINAN_TEXT, schemas.JINAN_CATEGORY_RULES)
    by_code = {i["notes"].split(" ")[0]: i for i in items}
    ml = [i for i in items if i["label_cn"] == "中长期交易"][0]
    assert ml["category"] == "midlong_energy" and ml["amount_cny"] == 6000657.47
    neg = [i for i in items if i["label_cn"] == "合同交易"][0]
    assert neg["price_cny_mwh"] == -60.718 and neg["amount_cny"] == -86155.80
    spot = [i for i in items if i["label_cn"] == "现货交易"][0]
    assert spot["category"] == "spot_energy"
    dev = [i for i in items if "偏差收益回收" in i["label_cn"]][0]
    assert dev["category"] == "imbalance"
    claw = [i for i in items if "超额收益" in i["label_cn"]][0]
    assert claw["category"] == "rule_charges"


def test_total_from_summary():
    assert parse_total_from_summary(JINAN_TEXT) == 175251.68


def test_shandong_7021_excel(tmp_path):
    rows = [["2026年3月月清算临时结果单"] + [None] * 7,
            [None] * 8, [None] * 8, [None] * 8,
            ["结算单元名称", "日期", "省内实时市场结算", None, None, "省内日前市场结算", None, None],
            [None, None, "实时用电量", "实时市场电价", "实时电能量电费", "日前出清电量", "日前市场出清电价", "日前电能量电费"],
            ["景融绿色能源科技有限公司", "2026-03-01", 7546.9, 436.69, 3295685.07, 0, 0, 0],
            [None, "2026-03-02", 7662.2, 375.92, 2880362.44, 0, 0, 0],
            ["合计", None, 15209.1, None, 6176047.51, 0, 0, 0]]
    f = tmp_path / "7021-2026-03景融绿色能源科技有限公司结算单.xlsx"
    with pd.ExcelWriter(f, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="结算依据", index=False, header=False)
    doc = parse_shandong_invoice(f)
    assert doc.settlement_month.isoformat() == "2026-03-01"
    assert len(doc.items) == 2
    assert all(i["category"] == "spot_energy" for i in doc.items)
    assert doc.items[0].delivery_date.isoformat() == "2026-03-01"
    assert doc.total_amount_cny == pytest.approx(6176047.51)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_invoice_common.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/parsers/invoice_common.py
"""Shared invoice parsing: 科目编码 subject lines -> InvoiceItem list.

Line shape: <code> <name> <plan_vol> <settle_vol> <avg_price> <amount> [备注]
Numerics may be '-' (missing). Category via longest-prefix rules (schemas).
"""
from __future__ import annotations

import re
from pathlib import Path

import pdfplumber

from services.retail_risk import schemas

_LINE_RE = re.compile(
    r"^(?P<code>\d{4,10})\s+(?P<name>.+?)\s+"
    r"(?P<v1>[\-\d.,]+)\s+(?P<v2>[\-\d.,]+)\s+(?P<v3>[\-\d.,]+)\s+(?P<v4>[\-\d.,]+)"
    r"(?:\s+(?P<note>.*))?$"
)
_TOTAL_RE = re.compile(r"本月\s+([\d,]+\.\d{2})")


def _num(s: str) -> float | None:
    s = s.replace(",", "").strip()
    if s in ("-", "—", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def extract_pdf_text(path: str | Path) -> str:
    with pdfplumber.open(str(path)) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages)


def parse_subject_lines(text: str, rules: list[tuple[str, str]]) -> list[schemas.InvoiceItem]:
    items = []
    for line in text.splitlines():
        m = _LINE_RE.match(line.strip())
        if not m:
            continue
        code = m.group("code")
        amount = _num(m.group("v4"))
        if amount is None:
            continue                     # header rows without amounts: skip
        items.append(schemas.InvoiceItem(
            category=schemas.category_for_code(code, rules),
            label_cn=m.group("name").strip(),
            volume_mwh=_num(m.group("v2")),
            price_cny_mwh=_num(m.group("v3")),
            amount_cny=amount,
            delivery_date=None,
            notes=code,
        ))
    return items


def parse_total_from_summary(text: str) -> float | None:
    m = _TOTAL_RE.search(text)
    return float(m.group(1).replace(",", "")) if m else None
```

```python
# services/retail_risk/parsers/invoice_shandong.py
"""山东 wholesale invoice: 7021-*结算单.xlsx 结算依据 sheet (daily RT settlement)."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_MONTH_RE = re.compile(r"7021-(\d{4})-(\d{2})")


def parse_shandong_invoice(path: str | Path) -> schemas.InvoiceDoc:
    path = Path(path)
    ym = _MONTH_RE.search(path.name)
    month = f"{ym.group(1)}-{ym.group(2)}-01" if ym else None
    df = pd.read_excel(path, sheet_name="结算依据", header=None)
    # data starts after the 2-row header (row containing 实时用电量), ends before 合计
    start = next(i for i in range(len(df)) if "实时用电量" in str(df.iloc[i, 2])) + 1
    items: list[schemas.InvoiceItem] = []
    total = None
    for i in range(start, len(df)):
        label = str(df.iloc[i, 0])
        if "合计" in label:
            total = float(df.iloc[i, 4]) if pd.notna(df.iloc[i, 4]) else None
            continue
        date_v = df.iloc[i, 1]
        if pd.isna(date_v):
            continue
        delivery = pd.to_datetime(date_v).date()
        rt_vol, rt_px, rt_amt = df.iloc[i, 2], df.iloc[i, 3], df.iloc[i, 4]
        if pd.notna(rt_amt) and float(rt_amt) != 0:
            items.append(schemas.InvoiceItem(
                category="spot_energy", label_cn="实时电能量电费",
                volume_mwh=float(rt_vol) if pd.notna(rt_vol) else None,
                price_cny_mwh=float(rt_px) if pd.notna(rt_px) else None,
                amount_cny=float(rt_amt), delivery_date=delivery, notes="RT"))
        da_vol, da_px, da_amt = df.iloc[i, 5], df.iloc[i, 6], df.iloc[i, 7]
        if pd.notna(da_amt) and float(da_amt) != 0:
            items.append(schemas.InvoiceItem(
                category="spot_energy", label_cn="日前电能量电费",
                volume_mwh=float(da_vol) if pd.notna(da_vol) else None,
                price_cny_mwh=float(da_px) if pd.notna(da_px) else None,
                amount_cny=float(da_amt), delivery_date=delivery, notes="DA"))
    return schemas.InvoiceDoc(settlement_month=pd.to_datetime(month).date(),
                              items=items, total_amount_cny=total)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_invoice_common.py -v`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/parsers/invoice_common.py services/retail_risk/parsers/invoice_shandong.py \
        tests/services/retail_risk/test_invoice_common.py
git commit -m "Add invoice common parser and Shandong 7021 parser"
```

---

### Task 10: `parsers/invoice_jinan.py` + `invoice_zhejiang.py`

**Files:**
- Create: `services/retail_risk/parsers/invoice_jinan.py`
- Create: `services/retail_risk/parsers/invoice_zhejiang.py`
- Test: `tests/services/retail_risk/test_invoice_pdfs.py`

**Interfaces:**
- Consumes: `invoice_common.parse_subject_lines / parse_total_from_summary / extract_pdf_text`, `schemas.JINAN_CATEGORY_RULES / ZHEJIANG_CATEGORY_RULES`.
- Produces: `parse_jinan_invoice(path) -> InvoiceDoc`, `parse_zhejiang_invoice(path) -> InvoiceDoc`. Month parsed from filename `2026年03月` / `2026年3月`.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_invoice_pdfs.py
from pathlib import Path
from unittest.mock import patch
import pytest
from services.retail_risk.parsers.invoice_jinan import parse_jinan_invoice
from services.retail_risk.parsers.invoice_zhejiang import parse_zhejiang_invoice

JINAN_TEXT = """
结算单编号：HEPX-2026-03-SD0100
本月 175251.68
01 电量清分 17770.000 21906.460 331.580 7263741.37
0101 中长期交易 17770.000 17770.000 337.685 6000657.47
0102 现货交易 - 4136.460 305.354 1263083.90
0202 市场运营费用 - - - 176.41
"""

ZHEJIANG_TEXT = """
结算单编号：
本月 9163096.47
01 电量清分 24069.407 27007.772 337.848 9124509.97
0101 中长期交易 24069.407 - - 1395926.90
010104 省间送受电交易 3661.792 - - 111114.51
0102 现货交易 - 27007.772 286.161 7728583.07
0201 权益和凭证交易 - 4955.000 3.989 19763.99
"""


def test_jinan_invoice(tmp_path):
    f = tmp_path / "景融绿色能源科技有限公司2026年03月现货月结算.pdf"
    f.write_bytes(b"%PDF-1.4 fake")
    with patch("services.retail_risk.parsers.invoice_jinan.extract_pdf_text",
               return_value=JINAN_TEXT):
        doc = parse_jinan_invoice(f)
    assert doc.settlement_month.isoformat() == "2026-03-01"
    assert doc.total_amount_cny == 175251.68
    cats = {i["label_cn"]: i["category"] for i in doc.items}
    assert cats["中长期交易"] == "midlong_energy"
    assert cats["现货交易"] == "spot_energy"
    assert cats["市场运营费用"] == "market_redistribution"


def test_zhejiang_invoice(tmp_path):
    f = tmp_path / "景融绿色能源科技有限公司2026年03月结算单-25年现货月依据.pdf"
    f.write_bytes(b"%PDF-1.4 fake")
    with patch("services.retail_risk.parsers.invoice_zhejiang.extract_pdf_text",
               return_value=ZHEJIANG_TEXT):
        doc = parse_zhejiang_invoice(f)
    cats = {i["label_cn"]: i["category"] for i in doc.items}
    assert cats["省间送受电交易"] == "midlong_energy"
    assert cats["权益和凭证交易"] == "green_premium"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_invoice_pdfs.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/parsers/invoice_jinan.py
"""冀南 (河北 HEPX) monthly invoice PDF -> InvoiceDoc."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas
from services.retail_risk.parsers.invoice_common import (
    extract_pdf_text, parse_subject_lines, parse_total_from_summary,
)

_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月")


def parse_jinan_invoice(path: str | Path) -> schemas.InvoiceDoc:
    path = Path(path)
    ym = _MONTH_RE.search(path.name)
    text = extract_pdf_text(path)
    return schemas.InvoiceDoc(
        settlement_month=pd.to_datetime(f"{ym.group(1)}-{int(ym.group(2)):02d}-01").date(),
        items=parse_subject_lines(text, schemas.JINAN_CATEGORY_RULES),
        total_amount_cny=parse_total_from_summary(text),
    )
```

```python
# services/retail_risk/parsers/invoice_zhejiang.py
"""浙江 monthly invoice PDF -> InvoiceDoc."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas
from services.retail_risk.parsers.invoice_common import (
    extract_pdf_text, parse_subject_lines, parse_total_from_summary,
)

_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月")


def parse_zhejiang_invoice(path: str | Path) -> schemas.InvoiceDoc:
    path = Path(path)
    ym = _MONTH_RE.search(path.name)
    text = extract_pdf_text(path)
    return schemas.InvoiceDoc(
        settlement_month=pd.to_datetime(f"{ym.group(1)}-{int(ym.group(2)):02d}-01").date(),
        items=parse_subject_lines(text, schemas.ZHEJIANG_CATEGORY_RULES),
        total_amount_cny=parse_total_from_summary(text),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_invoice_pdfs.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/parsers/invoice_jinan.py services/retail_risk/parsers/invoice_zhejiang.py \
        tests/services/retail_risk/test_invoice_pdfs.py
git commit -m "Add Jinan and Zhejiang invoice PDF parsers"
```

---

### Task 11: `parsers/invoice_anhui.py` — watermark cleaning

**Files:**
- Create: `services/retail_risk/parsers/invoice_anhui.py`
- Test: `tests/services/retail_risk/test_invoice_anhui.py`

**Interfaces:**
- Consumes: `invoice_common`, `schemas.ANHUI_CATEGORY_RULES`.
- Produces: `parse_anhui_invoice(path) -> InvoiceDoc`; `clean_anhui_watermark(text: str) -> str` (exported for tests).

Format notes （安徽统推， verified): diagonal stamp watermark breaks into single/double-char lines (`景`, `融`, `绿`, `色`, `能`, `源`, `科`, `技`, `有`, `限`, `公`, `司`, `年4`, `月14`, `15:58:36`, `2026`) interleaved with real lines. Real subject lines always start with a ≥4-digit code. Cleaning rule: drop lines that (a) are ≤3 chars after strip, or (b) fully match `^(20\d{2}|年\d+|月\d+|\d{2}:\d{2}:\d{2}|[景融绿色能源科技有限司公]+)$`. Amount safety: the loader's Σ-items-vs-total cross-check (Task 2) flags the file if cleaning ate a real line.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_invoice_anhui.py
from pathlib import Path
from unittest.mock import patch
import pytest
from services.retail_risk.parsers.invoice_anhui import (
    clean_anhui_watermark, parse_anhui_invoice,
)

RAW = """司
公
源
科
技
有
限
日
15:58:36
售电公司交易结算单
结算单编号：AHPX-2026-3-R0615
景
融 2026
单位：兆瓦时、元 年4
本月 462471.22
结算科目编码 结算科目 分月交易计划电量 结算电量/容量 结算电价/均价 结算电费 备注
购电侧
01 电量清分
01010201 中长期交易 38101.323 38101.323 347.902 13255508.85
0102 现货交易 - 19411.918 288.5 5601534.2
色
能 月14
绿 年4
"""


def test_clean_watermark():
    cleaned = clean_anhui_watermark(RAW)
    assert "AHPX-2026-3-R0615" in cleaned
    assert "15:58:36" not in cleaned
    for frag in ["司", "公", "源", "技"]:
        assert frag not in cleaned.splitlines()


def test_parse_anhui_invoice(tmp_path):
    f = tmp_path / "景融绿色能源科技有限公司2026年03月统推售电公司结算单结算单 (1).pdf"
    f.write_bytes(b"%PDF-1.4 fake")
    with patch("services.retail_risk.parsers.invoice_anhui.extract_pdf_text",
               return_value=RAW):
        doc = parse_anhui_invoice(f)
    assert doc.settlement_month.isoformat() == "2026-03-01"
    assert doc.total_amount_cny == 462471.22
    cats = {i["label_cn"]: i["category"] for i in doc.items}
    assert cats["中长期交易"] == "midlong_energy"
    assert cats["现货交易"] == "spot_energy"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_invoice_anhui.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/parsers/invoice_anhui.py
"""安徽统推 invoice PDF -> InvoiceDoc, with diagonal-stamp watermark cleaning."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas
from services.retail_risk.parsers.invoice_common import (
    extract_pdf_text, parse_subject_lines, parse_total_from_summary,
)

_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月")
_WATERMARK_RE = re.compile(
    r"^(20\d{2}|年\d+|月\d+|\d{2}:\d{2}:\d{2}|[景融绿色能源科技有限司公日\s]+)$"
)


def clean_anhui_watermark(text: str) -> str:
    """Drop stamp-watermark fragments. Real subject lines start with >=4-digit codes
    and are never short; short CJK fragments and date/time stamp pieces are noise."""
    keep = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if len(s) <= 3 and not re.match(r"^\d{4}", s):
            continue
        if _WATERMARK_RE.match(s) and not re.match(r"^\d{4}\s", s):
            continue
        keep.append(line)
    return "\n".join(keep)


def parse_anhui_invoice(path: str | Path) -> schemas.InvoiceDoc:
    path = Path(path)
    ym = _MONTH_RE.search(path.name)
    text = clean_anhui_watermark(extract_pdf_text(path))
    return schemas.InvoiceDoc(
        settlement_month=pd.to_datetime(f"{ym.group(1)}-{int(ym.group(2)):02d}-01").date(),
        items=parse_subject_lines(text, schemas.ANHUI_CATEGORY_RULES),
        total_amount_cny=parse_total_from_summary(text),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_invoice_anhui.py -v`
Expected: 2 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/parsers/invoice_anhui.py tests/services/retail_risk/test_invoice_anhui.py
git commit -m "Add Anhui invoice parser with watermark cleaning"
```

---

### Task 12: `reconcile.py` — Goal 1 engine

**Files:**
- Create: `services/retail_risk/reconcile.py`
- Test: `tests/services/retail_risk/test_reconcile.py`

**Interfaces:**
- Consumes: DB tables `rm_positions`, `rm_settlement_items` (via `rm_settlements`), `rm_position_volumes`, `spot_prices_hourly`; `schemas.spot_province`.
- Produces:
  - `@dataclass ReconLine: name: str; expected_cny: float | None; invoice_cny: float | None; delta_cny: float | None; note: str`
  - `@dataclass ReconResult: book_id: int; month: date; status: str; lines: list[ReconLine]; tolerance_cny: float`
  - `reconcile_month(conn, book_id: int, month: date, tolerance_pct: float = 0.005, tolerance_floor: float = 5000.0) -> ReconResult`
  - Pure helpers (DB-free, unit-tested): `spot_vwap_series(df_prices) -> float`, `judge(lines, tol) -> str`, `expected_spot_cost(exposure_mwh, rt_vwap) -> float`

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_reconcile.py
import datetime
import pandas as pd
import pytest
from services.retail_risk import reconcile as rc


def test_spot_vwap_simple_mean():
    df = pd.DataFrame({"rt_price": [300.0, 400.0, 500.0]})
    assert rc.spot_vwap_series(df) == pytest.approx(400.0)


def test_expected_spot_cost():
    assert rc.expected_spot_cost(1000.0, 350.0) == 350000.0


def test_judge_status():
    L = rc.ReconLine
    ok = [L("a", 100.0, 100.5, 0.5, ""), L("b", 200.0, 199.0, -1.0, "")]
    assert rc.judge(ok, tol=2.0) == "matched"
    assert rc.judge(ok, tol=0.5) == "flagged"
    # explained: midlong/spot lines match, residual == itemised fees
    exp = [L("midlong", 100.0, 100.0, 0.0, ""), L("spot", 50.0, 50.0, 0.0, ""),
           L("residual", 30.0, 30.0, 0.0, "fees")]
    assert rc.judge(exp, tol=1.0) == "explained"


def test_tolerance():
    assert rc.tolerance_for(2_000_000.0, 0.005, 5000.0) == 10000.0
    assert rc.tolerance_for(500_000.0, 0.005, 5000.0) == 5000.0


def test_settle_volume_shortest_code_wins():
    items = pd.DataFrame([
        {"volume_mwh": 21906.46, "category": "other", "notes": "01"},
        {"volume_mwh": 17770.0, "category": "midlong_energy", "notes": "0101"},
        {"volume_mwh": 4136.46, "category": "spot_energy", "notes": "0102"},
    ])
    assert rc.settle_volume(items) == 21906.46      # top line, not the double-counted sum


def test_settle_volume_fallback_spot_sum():
    items = pd.DataFrame([
        {"volume_mwh": 7546.9, "category": "spot_energy", "notes": "RT"},
        {"volume_mwh": 7662.2, "category": "spot_energy", "notes": "RT"},
    ])
    assert rc.settle_volume(items) == 15209.1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_reconcile.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/reconcile.py
"""Goal 1: reconcile trades with settlement invoices, per book x month.

Checks:
  1. midlong:  Σ rm_positions cost (month)  vs  Σ items[midlong_energy]
  2. spot:     exposure x RT VWAP           vs  Σ items[spot_energy]
               exposure = invoice settled volume - cleared midlong volume
  3. residual: invoice total - (midlong+spot) vs itemised fees (imbalance/penalty/
               surcharges/redistribution/rule_charges/other/green_premium)
Status: matched | explained | flagged.
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import text

from services.retail_risk import schemas

FEE_CATEGORIES = ["imbalance", "penalty", "govt_surcharges", "market_redistribution",
                  "rule_charges", "other", "green_premium"]


@dataclass
class ReconLine:
    name: str
    expected_cny: float | None
    invoice_cny: float | None
    delta_cny: float | None
    note: str = ""


@dataclass
class ReconResult:
    book_id: int
    month: datetime.date
    status: str
    lines: list[ReconLine] = field(default_factory=list)
    tolerance_cny: float = 0.0


def spot_vwap_series(df_prices: pd.DataFrame) -> float:
    return float(df_prices["rt_price"].dropna().mean())


def expected_spot_cost(exposure_mwh: float, rt_vwap: float) -> float:
    return exposure_mwh * rt_vwap


def tolerance_for(invoice_total_cny: float, pct: float, floor: float) -> float:
    return max(pct * abs(invoice_total_cny), floor)


def judge(lines: list[ReconLine], tol: float) -> str:
    core = [l for l in lines if l.name in ("midlong", "spot", "total")]
    residual = next((l for l in lines if l.name == "residual"), None)
    if all(l.delta_cny is not None and abs(l.delta_cny) <= tol for l in core):
        return "matched"
    core12 = [l for l in lines if l.name in ("midlong", "spot")]
    if (all(l.delta_cny is not None and abs(l.delta_cny) <= tol for l in core12)
            and residual is not None and residual.delta_cny is not None
            and abs(residual.delta_cny) <= tol):
        return "explained"
    return "flagged"


def _invoice_by_category(conn, book_id: int, month: datetime.date) -> dict[str, float]:
    df = pd.read_sql(text("""
        SELECT si.category, SUM(si.amount_cny) AS amt
        FROM marketdata.rm_settlement_items si
        JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
        WHERE s.book_id = :b AND s.settlement_month = :m
        GROUP BY si.category
    """), conn, params={"b": book_id, "m": month})
    return dict(zip(df["category"], df["amt"]))


def _invoice_totals(conn, book_id: int, month: datetime.date) -> dict:
    """Invoice total + settled volume.

    Settled volume must NOT be Σ all item volumes — hierarchical subject lines
    (01 > 0101 > 010102…) double/triple count. Rule: the volume of the item with
    the SHORTEST subject code (top line '01 电量清分' = total settled); fallback
    Σ spot_energy volumes (山东 7021 daily RT rows carry actual load)."""
    df = pd.read_sql(text("""
        SELECT s.total_amount_cny, si.volume_mwh, si.category, si.notes
        FROM marketdata.rm_settlements s
        LEFT JOIN marketdata.rm_settlement_items si ON si.settlement_id = s.id
        WHERE s.book_id = :b AND s.settlement_month = :m
    """), conn, params={"b": book_id, "m": month})
    total = df["total_amount_cny"].dropna().sum() or None
    return {"total": total, "settled_vol": settle_volume(df)}


def settle_volume(items: pd.DataFrame) -> float | None:
    if items.empty:
        return None
    codes = items["notes"].fillna("").str.extract(r"^(\d+)")[0]
    if codes.notna().any():
        top = items.loc[codes[codes.notna()].str.len().idxmin()]
        if pd.notna(top["volume_mwh"]):
            return float(top["volume_mwh"])
    spot = items[items["category"] == "spot_energy"]["volume_mwh"].dropna()
    return float(spot.sum()) if not spot.empty else None


def _positions_cost(conn, book_id: int, month: datetime.date) -> tuple[float, float]:
    row = pd.read_sql(text("""
        SELECT SUM(volume_mwh * price_cny_mwh) AS cost, SUM(volume_mwh) AS vol
        FROM marketdata.rm_positions
        WHERE book_id = :b AND start_date >= :m
          AND start_date < (:m::date + INTERVAL '1 month')::date
    """), conn, params={"b": book_id, "m": month}).iloc[0]
    return (float(row["cost"]) if row["cost"] is not None else 0.0,
            float(row["vol"]) if row["vol"] is not None else 0.0)


def _rt_vwap(conn, province: str, month: datetime.date) -> float | None:
    df = pd.read_sql(text("""
        SELECT rt_price FROM marketdata.spot_prices_hourly
        WHERE province = :p AND datetime >= :m
          AND datetime < (:m::date + INTERVAL '1 month')::date
    """), conn, params={"p": schemas.spot_province(province), "m": month})
    return spot_vwap_series(df) if not df.empty else None


def reconcile_month(conn, book_id: int, month: datetime.date,
                    tolerance_pct: float = 0.005,
                    tolerance_floor: float = 5000.0) -> ReconResult:
    province = conn.execute(text(
        "SELECT province FROM marketdata.rm_positions WHERE book_id = :b LIMIT 1"
    ), {"b": book_id}).scalar() or ""
    by_cat = _invoice_by_category(conn, book_id, month)
    totals = _invoice_totals(conn, book_id, month)
    pos_cost, pos_vol = _positions_cost(conn, book_id, month)

    inv_midlong = by_cat.get("midlong_energy", 0.0)
    inv_spot = by_cat.get("spot_energy", 0.0)
    inv_fees = sum(by_cat.get(c, 0.0) for c in FEE_CATEGORIES)
    inv_total = float(totals["total"]) if totals["total"] is not None else None

    lines: list[ReconLine] = []
    lines.append(ReconLine("midlong", pos_cost, inv_midlong, pos_cost - inv_midlong,
                           f"trades vol {pos_vol:,.0f} MWh"))
    rt = _rt_vwap(conn, province, month) if province else None
    exp_spot = None
    if rt is not None and totals["settled_vol"] is not None and pos_vol:
        exposure = float(totals["settled_vol"]) - pos_vol
        exp_spot = expected_spot_cost(exposure, rt)
    lines.append(ReconLine("spot", exp_spot, inv_spot,
                           (exp_spot - inv_spot) if exp_spot is not None else None,
                           f"RT VWAP {rt:.1f}" if rt else "no spot data"))
    if inv_total is not None:
        exp_total = pos_cost + (exp_spot or 0.0) + inv_fees
        lines.append(ReconLine("total", exp_total, inv_total, exp_total - inv_total, ""))
        residual = inv_total - inv_midlong - inv_spot
        lines.append(ReconLine("residual", inv_fees, residual, inv_fees - residual, "fees"))
        tol = tolerance_for(inv_total, tolerance_pct, tolerance_floor)
    else:
        tol = tolerance_floor
    return ReconResult(book_id=book_id, month=month,
                       status=judge(lines, tol), lines=lines, tolerance_cny=tol)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_reconcile.py -v`
Expected: 6 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/reconcile.py tests/services/retail_risk/test_reconcile.py
git commit -m "Add reconciliation engine for retail-risk"
```

---

### Task 13: `pnl_bridge.py` — Goal 2 engine (bridge + channel alpha + trader/sales attribution)

**Files:**
- Create: `services/retail_risk/pnl_bridge.py`
- Test: `tests/services/retail_risk/test_pnl_bridge.py`

**Interfaces:**
- Consumes: DB tables (positions, settlement items, spot, benchmarks, contracts); `reconcile._invoice_by_category`; `schemas.spot_province`, `schemas.BENCH_CHANNEL_MAP`.
- Produces:
  - `@dataclass BridgeResult: retail_revenue, channel_costs: dict[str,float], spot_cost, deviation, other, net, retail_avg_price, wholesale_avg_cost, spread, coverage_pct`
  - `bridge_month(conn, book_id, month) -> BridgeResult`
  - `channel_alpha(conn, book_id, month) -> pd.DataFrame` (channel, volume_mwh, vwap, spot_vwap, alpha_cny, alpha_cny_mwh)
  - `attribution_month(conn, book_id, month) -> dict` (`trader_by_channel` frame, `sales_alpha_cny`, `identity_residual_cny`)
  - `persist_bridge_snapshot(conn, book_id, month, bridge, midlong_alpha_total) -> None`
  - Pure helpers: `alpha_rows(positions_df, spot_vwap)`, `trader_alpha(our_df, bench_df)`, `sales_alpha(retail_rev, channel_fee, blended_benchmark, retail_vol)`

Formulas (spec §7.2 + D10): channel alpha = (spot_VWAP − channel_VWAP) × volume. Trader alpha per channel = (mkt_avg − our_VWAP) × vol, mkt_avg from `rm_market_benchmarks` joined via `BENCH_CHANNEL_MAP`. Sales alpha = retail_revenue − 渠道费用 − blended_benchmark × retail_vol, blended = Σ(bench_c × vol_c)/Σvol_c. 渠道费用 = Σ over contracts of (contract retail revenue × (1 − share_ratio)) when share_ratio present, else 0 (documented; D10 pins exact formula at implementation against workbook).

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_pnl_bridge.py
import pandas as pd
import pytest
from services.retail_risk import pnl_bridge as pb


def test_alpha_rows():
    pos = pd.DataFrame([
        {"channel": "annual", "volume_mwh": 100.0, "pv": 35000.0},       # VWAP 350
        {"channel": "monthly_auction", "volume_mwh": 50.0, "pv": 20000.0},  # VWAP 400
    ])
    out = pb.alpha_rows(pos, spot_vwap=380.0)
    a = out.set_index("channel")
    assert a.loc["annual", "alpha_cny"] == pytest.approx((380 - 350) * 100)
    assert a.loc["monthly_auction", "alpha_cny_mwh"] == pytest.approx(-20.0)


def test_trader_alpha():
    our = pd.DataFrame([{"channel": "monthly_auction", "volume_mwh": 10.0, "vwap": 370.0}])
    bench = pd.DataFrame([{"channel": "集中竞价交易", "avg_price_cny_mwh": 372.0}])
    out = pb.trader_alpha(our, bench)
    row = out.iloc[0]
    assert row["mkt_avg"] == 372.0 and row["trader_alpha_cny"] == pytest.approx(20.0)


def test_sales_alpha_and_identity():
    # retail 400, channel fee 1000 on 100 MWh, blended benchmark 370
    alpha = pb.sales_alpha(retail_revenue_cny=40000.0, channel_fee_cny=1000.0,
                           blended_benchmark=370.0, retail_vol_mwh=100.0)
    assert alpha == pytest.approx(40000 - 1000 - 37000)
    # identity: trader + sales = retail - fee - our_cost
    trader = (372.0 - 370.0) * 100.0
    identity = trader + alpha
    assert identity == pytest.approx(40000 - 1000 - 37000 + 200)


def test_channel_fee():
    contracts = pd.DataFrame([
        {"annual_mwh": 1000.0, "price_cny_mwh": 400.0, "share_ratio": 0.9},
        {"annual_mwh": 500.0, "price_cny_mwh": 380.0, "share_ratio": None},
    ])
    # fee = revenue x (1 - share) where share present: 1000*400*0.1 = 40000
    assert pb.channel_fee(contracts) == pytest.approx(40000.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_pnl_bridge.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/pnl_bridge.py
"""Goal 2: P&L bridge, channel alpha vs spot, trader/sales attribution (spec §7.2, D10)."""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import text

from services.retail_risk import schemas
from services.retail_risk.reconcile import _invoice_by_category

MIDLONG_CHANNELS = ["annual", "monthly_auction", "monthly_listed", "intramonth_match"]


@dataclass
class BridgeResult:
    retail_revenue: float = 0.0
    channel_costs: dict[str, float] = field(default_factory=dict)
    spot_cost: float = 0.0
    deviation: float = 0.0
    other: float = 0.0
    net: float = 0.0                       # == printed 售电公司收益 (identity, cross-check)
    retail_avg_price: float | None = None
    wholesale_avg_cost: float | None = None
    spread: float | None = None
    coverage_pct: float | None = None
    settled_vol_mwh: float | None = None


def alpha_rows(positions_pv: pd.DataFrame, spot_vwap: float) -> pd.DataFrame:
    """positions_pv: (channel, volume_mwh, pv=Σvol*price). alpha = (spot - vwap) x vol."""
    df = positions_pv.copy()
    df["vwap"] = df["pv"] / df["volume_mwh"].replace(0, float("nan"))
    df["spot_vwap"] = spot_vwap
    df["alpha_cny_mwh"] = spot_vwap - df["vwap"]
    df["alpha_cny"] = df["alpha_cny_mwh"] * df["volume_mwh"]
    return df[["channel", "volume_mwh", "vwap", "spot_vwap", "alpha_cny_mwh", "alpha_cny"]]


def trader_alpha(our_df: pd.DataFrame, bench_df: pd.DataFrame) -> pd.DataFrame:
    """our_df: (channel, volume_mwh, vwap). bench_df: rm_market_benchmarks rows.
    Join via schemas.BENCH_CHANNEL_MAP (benchmark channel covers several of ours).
    Positive = bought below market = trader gain."""
    rows = []
    for r in our_df.itertuples(index=False):
        mkt = None
        for b in bench_df.itertuples(index=False):
            if r.channel in schemas.BENCH_CHANNEL_MAP.get(b.channel, []):
                mkt = b.avg_price_cny_mwh
                break
        rows.append({"channel": r.channel, "volume_mwh": r.volume_mwh, "vwap": r.vwap,
                     "mkt_avg": mkt,
                     "trader_alpha_cny": (mkt - r.vwap) * r.volume_mwh
                     if mkt is not None and pd.notna(r.vwap) else None})
    return pd.DataFrame(rows)


def sales_alpha(retail_revenue_cny: float, channel_fee_cny: float,
                blended_benchmark: float, retail_vol_mwh: float) -> float:
    return retail_revenue_cny - channel_fee_cny - blended_benchmark * retail_vol_mwh


def channel_fee(contracts: pd.DataFrame) -> float:
    """渠道费用 = Σ contract revenue x (1 - 渠道分成比例); share missing -> 0 (D10)."""
    fee = 0.0
    for r in contracts.itertuples(index=False):
        if pd.notna(getattr(r, "share_ratio", None)) and pd.notna(getattr(r, "price_cny_mwh", None)) \
                and pd.notna(getattr(r, "annual_mwh", None)):
            fee += r.annual_mwh * r.price_cny_mwh * (1.0 - r.share_ratio)
    return fee


def bridge_month(conn, book_id: int, month: datetime.date) -> BridgeResult:
    """P&L bridge per book x month.

    P1 invoices are 购电侧-only: retail revenue is NOT an invoice line. Per spec D7
    fallback, retail_revenue = Σ wholesale cost categories + printed 售电公司收益 —
    which makes `net == 收益` an exact identity and a built-in cross-check against
    the invoice's printed margin (复盘 批零价差 derives from the same numbers)."""
    from services.retail_risk.reconcile import _invoice_totals
    by_cat = _invoice_by_category(conn, book_id, month)
    totals = _invoice_totals(conn, book_id, month)
    res = BridgeResult()
    res.spot_cost = by_cat.get("spot_energy", 0.0)
    res.deviation = by_cat.get("imbalance", 0.0) + by_cat.get("penalty", 0.0)
    res.other = sum(by_cat.get(c, 0.0) for c in
                    ["govt_surcharges", "market_redistribution", "rule_charges", "other"])
    midlong = by_cat.get("midlong_energy", 0.0)
    res.channel_costs = {"midlong": midlong, "green_premium": by_cat.get("green_premium", 0.0)}
    total_costs = midlong + res.spot_cost + res.deviation + res.other \
        + res.channel_costs["green_premium"]
    margin = float(totals["total"]) if totals["total"] is not None else 0.0
    res.retail_revenue = total_costs + margin
    res.net = margin
    vols = totals["settled_vol"]
    if vols:
        res.wholesale_avg_cost = total_costs / float(vols)
        res.retail_avg_price = res.retail_revenue / float(vols)
        res.spread = res.retail_avg_price - res.wholesale_avg_cost
    res.settled_vol_mwh = vols
    return res


def channel_alpha(conn, book_id: int, month: datetime.date) -> pd.DataFrame:
    pos = pd.read_sql(text("""
        SELECT channel, SUM(volume_mwh) AS volume_mwh,
               SUM(volume_mwh * price_cny_mwh) AS pv
        FROM marketdata.rm_positions
        WHERE book_id = :b AND start_date >= :m
          AND start_date < (:m::date + INTERVAL '1 month')::date
          AND direction = 'buy'
        GROUP BY channel
    """), conn, params={"b": book_id, "m": month})
    if pos.empty:
        return pd.DataFrame(columns=["channel", "volume_mwh", "vwap", "spot_vwap",
                                     "alpha_cny_mwh", "alpha_cny"])
    province = conn.execute(text(
        "SELECT province FROM marketdata.rm_positions WHERE book_id = :b LIMIT 1"
    ), {"b": book_id}).scalar()
    from services.retail_risk.reconcile import _rt_vwap
    rt = _rt_vwap(conn, province, month)
    if rt is None:
        return pd.DataFrame(columns=["channel", "volume_mwh", "vwap", "spot_vwap",
                                     "alpha_cny_mwh", "alpha_cny"])
    return alpha_rows(pos, rt)


def attribution_month(conn, book_id: int, month: datetime.date) -> dict:
    """Trader alpha per channel (vs 全网 benchmark) + sales alpha + identity residual.

    sales_alpha = retail_revenue - 渠道费用 - blended_benchmark x settled_vol
    identity: trader_total + sales_alpha ≈ bridge.net - 渠道费用 (residual shown, not hidden).
    """
    alpha = channel_alpha(conn, book_id, month)
    bridge = bridge_month(conn, book_id, month)
    bench = pd.read_sql(text("""
        SELECT channel, avg_price_cny_mwh FROM marketdata.rm_market_benchmarks
        WHERE month = :m AND source = 'infohub'
    """), conn, params={"m": month})
    our = alpha.dropna(subset=["vwap"]) if not alpha.empty else alpha
    t = trader_alpha(our, bench) if not our.empty else pd.DataFrame(
        columns=["channel", "volume_mwh", "vwap", "mkt_avg", "trader_alpha_cny"])
    blended = None
    if not t.empty and t["mkt_avg"].notna().any():
        tt = t.dropna(subset=["mkt_avg"])
        blended = float((tt["mkt_avg"] * tt["volume_mwh"]).sum() / tt["volume_mwh"].sum())
    province = conn.execute(text(
        "SELECT name FROM marketdata.rm_books WHERE id = :b"
    ), {"b": book_id}).scalar().split("-", 1)[1]
    contracts = pd.read_sql(text("""
        SELECT cc.annual_forecast_mwh AS annual_mwh, cc.price_cny_mwh,
               c.revenue_share_ratio AS share_ratio
        FROM marketdata.rm_customer_contracts cc
        JOIN marketdata.rm_customers c ON c.id = cc.customer_id
        WHERE c.province = :p AND cc.contract_status = 'active'
    """), conn, params={"p": province})
    fee = channel_fee(contracts) if not contracts.empty else 0.0
    sales = None
    if blended is not None and bridge.settled_vol_mwh:
        sales = sales_alpha(bridge.retail_revenue, fee, blended, bridge.settled_vol_mwh)
    trader_total = float(t["trader_alpha_cny"].dropna().sum()) if not t.empty else None
    identity_residual = None
    if sales is not None and trader_total is not None:
        identity_residual = (trader_total + sales) - (bridge.net - fee)
    return {"trader_by_channel": t, "blended_benchmark": blended,
            "trader_alpha_total": trader_total,
            "channel_fee_cny": fee, "sales_alpha_cny": sales,
            "identity_residual_cny": identity_residual}


def persist_bridge_snapshot(conn, book_id: int, month: datetime.date,
                            bridge: BridgeResult, midlong_alpha_total: float | None) -> None:
    conn.execute(text("""
        INSERT INTO marketdata.rm_pnl_snapshots
          (book_id, snapshot_date, realized_cny, bilateral_pnl_cny, spot_pnl_cny,
           deviation_pnl_cny, other_pnl_cny)
        VALUES (:b, :d, :r, :bil, :spot, :dev, :oth)
        ON CONFLICT (book_id, snapshot_date) DO UPDATE SET
          realized_cny = EXCLUDED.realized_cny,
          bilateral_pnl_cny = EXCLUDED.bilateral_pnl_cny,
          spot_pnl_cny = EXCLUDED.spot_pnl_cny,
          deviation_pnl_cny = EXCLUDED.deviation_pnl_cny,
          other_pnl_cny = EXCLUDED.other_pnl_cny
    """), {"b": book_id, "d": month, "r": bridge.net, "bil": midlong_alpha_total,
           "spot": -bridge.spot_cost, "dev": -bridge.deviation, "oth": -bridge.other})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_pnl_bridge.py -v`
Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/pnl_bridge.py tests/services/retail_risk/test_pnl_bridge.py
git commit -m "Add P&L bridge and attribution engine for retail-risk"
```

---

### Task 14: `mtm.py` — Goal 3 engine

**Files:**
- Create: `services/retail_risk/mtm.py`
- Test: `tests/services/retail_risk/test_mtm.py`

**Interfaces:**
- Consumes: `rm_positions` (open), `rm_forward_curves`, `rm_customer_contracts` + `rm_customers` (province), `libs.risk.mtm.compute_mtm`.
- Produces:
  - `@dataclass MtmResult: book_id, scenario, as_of, procurement_mtm_cny, retail_mtm_cny, total_mtm_cny, positions_df, contracts_df`
  - `book_mtm(conn, book_id, scenario: str = "spot_base", as_of: datetime.date | None = None) -> MtmResult`
  - `persist_mtm(conn, mtm_result) -> None` (upsert `rm_pnl_snapshots.unrealized_mtm_cny`)
  - Pure: `retail_contract_mtm(contracts_df, forward_price: float) -> pd.DataFrame` — fixed: (price − fwd) × remaining_vol; indexed: uplift(price field) × remaining_vol; remaining_vol from `monthly_forecast` JSON months ≥ as_of month, fallback `annual_forecast_mwh × remaining_months/total_months`.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_mtm.py
import datetime
import pandas as pd
import pytest
from services.retail_risk import mtm as mm


def _contracts():
    return pd.DataFrame([
        # fixed 380 vs fwd 400, 100 MWh remaining in Oct-Dec
        {"contract_type": "fixed", "price_cny_mwh": 380.0,
         "monthly_forecast": '{"10": 30.0, "11": 30.0, "12": 40.0}',
         "annual_forecast_mwh": 1200.0},
        # indexed 联动+上浮 6 元/MWh uplift, 100 MWh remaining
        {"contract_type": "indexed", "price_cny_mwh": 6.0,
         "monthly_forecast": '{"10": 30.0, "11": 30.0, "12": 40.0}',
         "annual_forecast_mwh": 1200.0},
    ])


def test_remaining_volume_from_monthly_json():
    vol = mm.remaining_volume(_contracts().iloc[0], as_of=datetime.date(2026, 9, 30))
    assert vol == pytest.approx(100.0)


def test_retail_mtm_fixed_vs_indexed():
    out = mm.retail_contract_mtm(_contracts(), forward_price=400.0,
                                 as_of=datetime.date(2026, 9, 30))
    fixed = out[out.contract_type == "fixed"].iloc[0]
    assert fixed["mtm_cny"] == pytest.approx((380.0 - 400.0) * 100.0)   # -2000
    indexed = out[out.contract_type == "indexed"].iloc[0]
    assert indexed["mtm_cny"] == pytest.approx(6.0 * 100.0)             # +600 service margin


def test_procurement_mtm_delegates():
    positions = [{"direction": "buy", "volume_mwh": 50.0, "price_cny_mwh": 350.0,
                  "province": "山东", "start_date": "2026-10-01", "end_date": "2026-10-31"}]
    res = mm.procurement_mtm(positions, {"山东": 400.0})
    assert res[0]["unrealized_pnl_cny"] == pytest.approx((400.0 - 350.0) * 50.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_mtm.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/mtm.py
"""Goal 3: per-book MtM = procurement MtM (open positions x scenario curve)
+ retail-contract MtM (fixed: price spread; indexed: locked service uplift)."""
from __future__ import annotations

import datetime
import json
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import text

from libs.risk.mtm import compute_mtm
from services.retail_risk import schemas


@dataclass
class MtmResult:
    book_id: int
    scenario: str
    as_of: datetime.date
    procurement_mtm_cny: float = 0.0
    retail_mtm_cny: float = 0.0
    total_mtm_cny: float = 0.0
    positions_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    contracts_df: pd.DataFrame = field(default_factory=pd.DataFrame)


def procurement_mtm(positions: list[dict], forward_prices: dict[str, float]) -> list[dict]:
    return compute_mtm(positions, forward_prices)


def remaining_volume(contract_row, as_of: datetime.date) -> float:
    """Remaining MWh from monthly_forecast JSON (months >= as_of month);
    fallback: annual x remaining_months/12."""
    mf = contract_row.get("monthly_forecast")
    if isinstance(mf, str) and mf and mf != "{}":
        months = json.loads(mf)
        vol = sum(float(v) for k, v in months.items() if int(k) >= as_of.month)
        if vol:
            return vol
    annual = contract_row.get("annual_forecast_mwh")
    if annual and pd.notna(annual):
        return float(annual) * (12 - as_of.month + 1) / 12.0
    return 0.0


def retail_contract_mtm(contracts_df: pd.DataFrame, forward_price: float,
                        as_of: datetime.date) -> pd.DataFrame:
    rows = []
    for r in contracts_df.itertuples(index=False):
        d = r._asdict() if hasattr(r, "_asdict") else dict(r)
        vol = remaining_volume(d, as_of)
        ctype = d.get("contract_type")
        price = d.get("price_cny_mwh") or 0.0
        if ctype in ("fixed", "peak_offpeak"):
            mtm = (price - forward_price) * vol
        else:                                   # indexed / indexed_band: service uplift
            mtm = price * vol
        rows.append({**d, "remaining_mwh": vol, "forward_price": forward_price,
                     "mtm_cny": mtm})
    return pd.DataFrame(rows)


def book_mtm(conn, book_id: int, scenario: str = "spot_base",
             as_of: datetime.date | None = None) -> MtmResult:
    as_of = as_of or datetime.date.today()
    pos_df = pd.read_sql(text("""
        SELECT direction, volume_mwh, price_cny_mwh, province, start_date, end_date, channel
        FROM marketdata.rm_positions WHERE book_id = :b AND status = 'open'
    """), conn, params={"b": book_id})
    fwd = pd.read_sql(text("""
        SELECT DISTINCT ON (province) province, price_cny_kwh * 1000 AS price
        FROM marketdata.rm_forward_curves
        WHERE product = :sc AND delivery_date >= :d
        ORDER BY province, curve_date DESC, delivery_date
    """), conn, params={"sc": scenario, "d": as_of})
    forward_prices = dict(zip(fwd["province"], fwd["price"]))
    proc = procurement_mtm(pos_df.to_dict("records"), forward_prices) if not pos_df.empty else []
    proc_total = sum(p["unrealized_pnl_cny"] for p in proc)

    province = pos_df["province"].iloc[0] if not pos_df.empty else None
    contracts = pd.read_sql(text("""
        SELECT cc.contract_type, cc.price_cny_mwh, cc.monthly_forecast, cc.annual_forecast_mwh
        FROM marketdata.rm_customer_contracts cc
        JOIN marketdata.rm_customers c ON c.id = cc.customer_id
        WHERE c.province = :p AND cc.contract_status = 'active'
          AND cc.end_date >= :d
    """), conn, params={"p": province or "", "d": as_of})
    ctr_df = pd.DataFrame()
    retail_total = 0.0
    if not contracts.empty and province in forward_prices:
        ctr_df = retail_contract_mtm(contracts, forward_prices[province], as_of)
        retail_total = float(ctr_df["mtm_cny"].sum())
    return MtmResult(book_id=book_id, scenario=scenario, as_of=as_of,
                     procurement_mtm_cny=proc_total, retail_mtm_cny=retail_total,
                     total_mtm_cny=proc_total + retail_total,
                     positions_df=pd.DataFrame(proc), contracts_df=ctr_df)


def persist_mtm(conn, result: MtmResult) -> None:
    conn.execute(text("""
        INSERT INTO marketdata.rm_pnl_snapshots (book_id, snapshot_date, unrealized_mtm_cny)
        VALUES (:b, :d, :u)
        ON CONFLICT (book_id, snapshot_date) DO UPDATE SET
          unrealized_mtm_cny = EXCLUDED.unrealized_mtm_cny
    """), {"b": result.book_id, "d": result.as_of, "u": result.total_mtm_cny})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_mtm.py -v`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/mtm.py tests/services/retail_risk/test_mtm.py
git commit -m "Add per-book MtM engine for retail-risk"
```

---

### Task 15: `run_backfill.py` — CLI orchestrator

**Files:**
- Create: `services/retail_risk/run_backfill.py`
- Modify: `services/retail_risk/parsers/__init__.py` (registry)
- Test: `tests/services/retail_risk/test_run_backfill.py`

**Interfaces:**
- Consumes: all parsers (Tasks 3–11), loader (Task 2), engines (Tasks 12–14).
- Produces:
  - `trades_to_volumes(trades_df, load_df=None) -> pd.DataFrame` (VOLUMES_COLS; VWAP-merges same date/hour/channel rows incl. 绿电； attaches nominated/settled from load_df)
  - `run(root, provinces, kinds, dry_run, echo=print) -> dict` summary counts
  - CLI: `python services/retail_risk/run_backfill.py --root data/trading --province 冀南 浙江 --kinds trades invoices --dry-run`

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_run_backfill.py
import datetime
import pandas as pd
import pytest
from services.retail_risk import schemas
from services.retail_risk.run_backfill import trades_to_volumes


def test_trades_to_volumes_vwap_merges_green():
    trades = pd.DataFrame([
        [datetime.date(2026, 3, 1), 8, "annual", "bilateral", "buy", 10.0, 350.0, None, "年度双边", "f"],
        [datetime.date(2026, 3, 1), 8, "annual", "forward", "buy", 2.0, 400.0, "绿电", "绿电", "f"],
    ], columns=schemas.TRADES_COLS)
    out = trades_to_volumes(trades)
    assert len(out) == 1                       # green folded into annual, VWAP-merged
    row = out.iloc[0]
    assert row.volume_mwh == 12.0
    assert row.vwap_cny_mwh == pytest.approx((10 * 350 + 2 * 400) / 12)


def test_trades_to_volumes_attaches_load():
    trades = pd.DataFrame([
        [datetime.date(2026, 4, 1), 0, "annual", "bilateral", "buy", 5.0, 344.0, None, "yr_sb", "f"],
    ], columns=schemas.TRADES_COLS)
    load = pd.DataFrame([{"delivery_date": datetime.date(2026, 4, 1), "hour": 0,
                          "nominated_mwh": 42.2, "settled_mwh": 38.7}])
    out = trades_to_volumes(trades, load)
    assert out.iloc[0].nominated_mwh == 42.2 and out.iloc[0].settled_mwh == 38.7


def test_trades_to_volumes_propagates_estimated():
    trades = pd.DataFrame([
        [datetime.date(2026, 3, 1), 8, "monthly_auction", "forward", "buy",
         10.0, 400.0, None, "月度竞价 (est)", "f"],
    ], columns=schemas.TRADES_COLS)
    out = trades_to_volumes(trades)
    assert out.iloc[0].estimated == True  # noqa: E712
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/services/retail_risk/test_run_backfill.py -v`
Expected: FAIL — ModuleNotFoundError

- [ ] **Step 3: Write the implementation**

```python
# services/retail_risk/parsers/__init__.py
"""Province parser registry."""
from services.retail_risk.parsers import (
    trades_anhui, trades_jinan, trades_shandong, trades_zhejiang,
    invoice_anhui, invoice_jinan, invoice_shandong, invoice_zhejiang,
    mtm_workbook, benchmark_infohub,
)

TRADES_PARSERS = {
    "冀南": trades_jinan.parse_jinan_trades,
    "浙江": trades_zhejiang.parse_zhejiang_trades,
    "山东": trades_shandong.parse_shandong_trades,      # needs ratio_df arg
    "安徽": trades_anhui.parse_anhui_trades,
}

LOAD_PARSERS = {
    "浙江": trades_zhejiang.parse_zhejiang_load,
    "山东": trades_shandong.parse_shandong_load,
}

INVOICE_GLOBS = {   # province -> (glob relative to root, parser)
    "冀南": ("冀南/月结算单/*.pdf", invoice_jinan.parse_jinan_invoice),
    "浙江": ("浙江/月结算单/*.pdf", invoice_zhejiang.parse_zhejiang_invoice),
    "山东": ("山东/2026*/景融/7021-*.xlsx", invoice_shandong.parse_shandong_invoice),
    "安徽": ("安徽/结算单/*.pdf", invoice_anhui.parse_anhui_invoice),
}
```

```python
# services/retail_risk/run_backfill.py
"""Retail-risk backfill orchestrator.

Usage:
  python services/retail_risk/run_backfill.py --root data/trading \
      --province 冀南 浙江 山东 安徽 [--kinds trades invoices mtm benchmarks] [--dry-run]

--dry-run parses everything and prints per-file/per-frame counts, writes nothing.
Live run requires the Task-1 migration applied (user-confirmed).
"""
from __future__ import annotations

import argparse
import datetime
from pathlib import Path

import pandas as pd

from services.retail_risk import loader, schemas
from services.retail_risk.parsers import (
    INVOICE_GLOBS, LOAD_PARSERS, TRADES_PARSERS,
    benchmark_infohub, mtm_workbook, trades_shandong,
)


def trades_to_volumes(trades: pd.DataFrame, load: pd.DataFrame | None = None) -> pd.DataFrame:
    """Hour-level trades -> VOLUMES_COLS. VWAP-merges same (date, hour, channel)
    rows (绿电 folds into its tenor channel). Load attaches nominated/settled.
    `estimated` is True when any contributing row was spread from daily data
    (source_term marked ' (est)', spec D5)."""
    if trades.empty:
        return pd.DataFrame(columns=schemas.VOLUMES_COLS)
    df = trades.copy()
    df["pv"] = df["volume_mwh"] * df["price_cny_mwh"].fillna(0)
    df["est"] = df["source_term"].fillna("").str.contains(r"\(est\)")
    g = df.groupby(["delivery_date", "hour", "channel"], as_index=False)
    out = g.agg(volume_mwh=("volume_mwh", "sum"), pv=("pv", "sum"), estimated=("est", "max"))
    out["vwap_cny_mwh"] = (out["pv"] / out["volume_mwh"].replace(0, float("nan"))).round(4)
    out["nominated_mwh"] = None
    out["settled_mwh"] = None
    if load is not None and not load.empty:
        out = out.merge(load, on=["delivery_date", "hour"], how="left",
                        suffixes=("", "_ld"))
        for c in ("nominated_mwh", "settled_mwh"):
            out[c] = out[c + "_ld"].combine_first(out[c])
            out = out.drop(columns=[c + "_ld"])
    return out.drop(columns=["pv"])[schemas.VOLUMES_COLS]


def _shandong_ratios(root: Path) -> pd.DataFrame:
    for f in mtm_workbook.mtm_files(root):
        if mtm_workbook.province_for_filename(f.name) == "山东" \
                and mtm_workbook.scenario_for_filename(f.name) == "spot_base":
            return mtm_workbook.load_hourly_ratios(f)
    return pd.DataFrame(columns=["month", "hour", "ratio"])


def run(root, provinces, kinds, dry_run: bool, echo=print) -> dict:
    root = Path(root)
    summary: dict = {}
    engine = None if dry_run else loader.get_engine()

    for province in provinces:
        summary[province] = {}
        if "trades" in kinds and province in TRADES_PARSERS:
            if province == "山东":
                trades = trades_shandong.parse_shandong_trades(root, _shandong_ratios(root))
            else:
                trades = TRADES_PARSERS[province](root)
            load = LOAD_PARSERS[province](root) if province in LOAD_PARSERS else None
            volumes = trades_to_volumes(trades, load)
            summary[province]["trades_rows"] = len(trades)
            summary[province]["volumes_rows"] = len(volumes)
            echo(f"[{province}] trades={len(trades)} volumes={len(volumes)}")
            if not dry_run:
                with engine.begin() as conn:
                    bid = loader.get_or_create_book(conn, province)
                    ym = datetime.date.today().strftime("%Y%m")
                    n1 = loader.write_trades(conn, bid, trades,
                                             f"{province}_{ym}_trades", province)
                    n2 = loader.write_volumes(conn, bid, volumes,
                                              f"{province}_{ym}_volumes")
                    echo(f"[{province}] wrote positions={n1} volumes={n2} (book {bid})")

        if "invoices" in kinds and province in INVOICE_GLOBS:
            pattern, parser = INVOICE_GLOBS[province]
            files = sorted(root.glob(pattern))
            echo(f"[{province}] {len(files)} invoice files")
            n_new = 0
            for f in files:
                try:
                    doc = parser(f)
                except Exception as e:                       # noqa: BLE001
                    echo(f"  !! {f.name}: {e}")
                    continue
                if dry_run:
                    echo(f"  {f.name}: items={len(doc.items)} total={doc.total_amount_cny}")
                    continue
                with engine.begin() as conn:
                    bid = loader.get_or_create_book(conn, province)
                    sid = loader.write_invoice(conn, bid, doc, f.name,
                                               loader.file_sha256(str(f)))
                    n_new += 1 if sid else 0
            summary[province]["invoices_new"] = n_new

    if "mtm" in kinds:
        n_curves = n_contracts = 0
        for f in mtm_workbook.mtm_files(root):
            out = mtm_workbook.parse_mtm_workbook(f)
            echo(f"[mtm] {f.name}: curves={len(out['curves'])} contracts={len(out['contracts'])}")
            if not dry_run:
                with engine.begin() as conn:
                    n_curves += loader.write_curves(conn, out["curves"])
                    if not out["contracts"].empty:
                        _, nc = loader.write_contracts(conn, out["province"], out["contracts"])
                        n_contracts += nc
        summary["mtm"] = {"curves": n_curves, "contracts": n_contracts}

    if "benchmarks" in kinds:
        n = 0
        for f in benchmark_infohub.infohub_files(root):
            df = benchmark_infohub.parse_infohub_benchmarks(f)
            echo(f"[bench] {f.name}: rows={len(df)}")
            if not dry_run and not df.empty:
                with engine.begin() as conn:
                    n += loader.write_benchmarks(conn, df)
        summary["benchmarks"] = n

    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/trading")
    ap.add_argument("--province", nargs="+",
                    default=["冀南", "浙江", "山东", "安徽"])
    ap.add_argument("--kinds", nargs="+",
                    default=["trades", "invoices", "mtm", "benchmarks"])
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    run(args.root, args.province, args.kinds, args.dry_run)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/services/retail_risk/test_run_backfill.py -v`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add services/retail_risk/run_backfill.py services/retail_risk/parsers/__init__.py \
        tests/services/retail_risk/test_run_backfill.py
git commit -m "Add backfill orchestrator for retail-risk ingestion"
```

---

### Task 16: `tab_data_upload.py`

**Files:**
- Create: `apps/retail_risk/tab_data_upload.py`

**Interfaces:**
- Consumes: parsers registry, loader, schemas.
- Produces: `render_upload(engine)` — called from app.py (Task 19).

- [ ] **Step 1: Write the tab** (Streamlit tabs are UI glue; verified by the Task 19 smoke run, not unit tests — same pattern as existing tabs)

```python
# apps/retail_risk/tab_data_upload.py
"""Data Upload: parse a file with the retail parsers, preview, write to DB."""
from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from services.retail_risk import loader, schemas
from services.retail_risk.parsers import (
    INVOICE_GLOBS, LOAD_PARSERS, TRADES_PARSERS, benchmark_infohub, mtm_workbook,
)
from services.retail_risk.run_backfill import trades_to_volumes

_KINDS = ["trades", "invoice", "mtm_workbook", "benchmark"]


def _save_tmp(uploaded) -> Path:
    suffix = "." + uploaded.name.rsplit(".", 1)[-1]
    tmp = Path(tempfile.mkdtemp()) / uploaded.name
    tmp.write_bytes(uploaded.read())
    return tmp


def render_upload(engine):
    st.subheader("Data Upload")
    st.caption("Parse a single file with the retail parsers, preview the frames, then write.")

    col1, col2 = st.columns(2)
    with col1:
        province = st.selectbox("Province", schemas.LOAD_BOOK_PROVINCES, key="up_prov")
    with col2:
        kind = st.selectbox("Data type", _KINDS, key="up_kind")

    uploaded = st.file_uploader("File", type=["xlsx", "xls", "csv", "pdf"], key="up_file")
    if not uploaded:
        return

    if st.button("Parse", key="up_parse"):
        path = _save_tmp(uploaded)
        st.session_state["up_path"] = str(path)
        try:
            if kind == "trades":
                # single-file parse: build a minimal fake root for the province parser
                st.session_state["up_result"] = _parse_trades_single(province, path)
            elif kind == "invoice":
                doc = INVOICE_GLOBS[province][1](path)
                st.session_state["up_result"] = {"invoice": doc}
            elif kind == "mtm_workbook":
                st.session_state["up_result"] = mtm_workbook.parse_mtm_workbook(path)
            else:
                st.session_state["up_result"] = {
                    "benchmarks": benchmark_infohub.parse_infohub_benchmarks(path)}
        except Exception as e:  # noqa: BLE001
            st.error(f"Parse failed: {e}")
            return

    result = st.session_state.get("up_result")
    if not result:
        return

    if kind == "trades":
        trades, volumes = result["trades"], result["volumes"]
        st.write(f"trades rows: **{len(trades)}**, volumes rows: **{len(volumes)}**")
        st.dataframe(trades.head(50), use_container_width=True, hide_index=True)
        if st.button("Write trades to DB", key="up_write_trades"):
            with engine.begin() as conn:
                bid = loader.get_or_create_book(conn, province)
                ym = pd.Timestamp.today().strftime("%Y%m")
                n1 = loader.write_trades(conn, bid, trades, f"{province}_{ym}_upload", province)
                n2 = loader.write_volumes(conn, bid, volumes, f"{province}_{ym}_upload")
            st.success(f"Wrote {n1} positions, {n2} volume rows (book {bid}).")

    elif kind == "invoice":
        doc = result["invoice"]
        st.write(f"month: **{doc.settlement_month}**, items: **{len(doc.items)}**, "
                 f"printed total: **{doc.total_amount_cny}**")
        st.dataframe(pd.DataFrame(doc.items), use_container_width=True, hide_index=True)
        if st.button("Write invoice to DB", key="up_write_invoice"):
            with engine.begin() as conn:
                bid = loader.get_or_create_book(conn, province)
                sid = loader.write_invoice(conn, bid, doc, Path(st.session_state["up_path"]).name,
                                           loader.file_sha256(st.session_state["up_path"]))
            st.success(f"Invoice written (settlement id {sid})." if sid else "Already ingested (hash match).")

    elif kind == "mtm_workbook":
        st.write(f"province: **{result['province']}**, product: **{result['product']}**, "
                 f"curves: **{len(result['curves'])}**, contracts: **{len(result['contracts'])}**")
        st.dataframe(result["curves"].head(50), use_container_width=True, hide_index=True)
        if st.button("Write curves + contracts to DB", key="up_write_mtm"):
            with engine.begin() as conn:
                nc = loader.write_curves(conn, result["curves"])
                ncust = 0
                if not result["contracts"].empty:
                    _, ncust = loader.write_contracts(conn, result["province"], result["contracts"])
            st.success(f"Wrote {nc} curve rows, {ncust} contracts.")

    else:
        df = result["benchmarks"]
        st.write(f"benchmark rows: **{len(df)}**")
        st.dataframe(df, use_container_width=True, hide_index=True)
        if st.button("Write benchmarks to DB", key="up_write_bench"):
            with engine.begin() as conn:
                n = loader.write_benchmarks(conn, df)
            st.success(f"Wrote {n} benchmark rows.")


def _parse_trades_single(province: str, path: Path):
    """Parse one uploaded trades file by building a minimal fake root for the
    province parser (parsers glob under <root>/<province>/...)."""
    import shutil
    fake = Path(tempfile.mkdtemp())
    if province == "冀南":
        dest = fake / "冀南" / "中长期交易结果" / "2026年1月"
    elif province == "浙江":
        dest = fake / "浙江" / "交易记录"
    elif province == "山东":
        dest = fake / "山东" / "202601" / "景融"
    else:
        dest = fake / "安徽" / "交易记录"
    dest.mkdir(parents=True)
    shutil.copy(path, dest / path.name)
    if province == "山东":
        from services.retail_risk.run_backfill import _shandong_ratios
        trades = TRADES_PARSERS["山东"](fake, _shandong_ratios(Path("data/trading")))
    else:
        trades = TRADES_PARSERS[province](fake)
    load = LOAD_PARSERS[province](fake) if province in LOAD_PARSERS else None
    return {"trades": trades, "volumes": trades_to_volumes(trades, load)}
```

- [ ] **Step 2: Commit**

```bash
git add apps/retail_risk/tab_data_upload.py
git commit -m "Add data upload tab for retail-risk app"
```

---

### Task 17: `tab_reconciliation.py`

**Files:**
- Create: `apps/retail_risk/tab_reconciliation.py`

**Interfaces:**
- Consumes: `reconcile.reconcile_month`, `loader` (engine only via app.py).
- Produces: `render_reconciliation(engine)`.

- [ ] **Step 1: Write the tab**

```python
# apps/retail_risk/tab_reconciliation.py
"""Reconciliation tab: trades vs settlement invoices, per book x month (Goal 1)."""
from __future__ import annotations

import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import text

from services.retail_risk import reconcile as rc


@st.cache_data(ttl=300)
def _recon(_engine_url: str, book_id: int, month: str) -> dict:
    from sqlalchemy import create_engine
    eng = create_engine(_engine_url, pool_pre_ping=True)
    with eng.connect() as conn:
        r = rc.reconcile_month(conn, book_id, datetime.date.fromisoformat(month))
    return {"status": r.status, "tolerance": r.tolerance_cny,
            "lines": [vars(l) for l in r.lines]}


def render_reconciliation(engine):
    st.subheader("Trade ↔ Invoice Reconciliation")

    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books WHERE book_type = 'load' ORDER BY name"
        ), conn)
        months = pd.read_sql(text("""
            SELECT DISTINCT settlement_month FROM marketdata.rm_settlements
            ORDER BY settlement_month DESC
        """), conn)

    if books.empty:
        st.info("No load books yet. Run the backfill or upload data first.")
        return

    book_id = st.selectbox("Book", books["id"].tolist(),
                           format_func=lambda x: books[books["id"] == x]["name"].iloc[0],
                           key="recon_book")
    if months.empty:
        st.info("No invoices ingested for reconciliation yet.")
        return

    month_list = [m.isoformat() for m in months["settlement_month"]]
    status_rows = []
    for m in month_list:
        r = _recon(str(engine.url), book_id, m)
        status_rows.append({"month": m, "status": r["status"], "tolerance_cny": r["tolerance"]})
    status_df = pd.DataFrame(status_rows)

    def _color(s):
        return {"matched": "🟢", "explained": "🟡", "flagged": "🔴"}.get(s, "⚪")
    status_df["status"] = status_df["status"].map(lambda s: f"{_color(s)} {s}")
    st.dataframe(status_df, use_container_width=True, hide_index=True)

    sel = st.selectbox("Drill into month", month_list, key="recon_month")
    r = _recon(str(engine.url), book_id, sel)
    lines = pd.DataFrame(r["lines"])
    st.dataframe(lines, use_container_width=True, hide_index=True)
```

- [ ] **Step 2: Commit**

```bash
git add apps/retail_risk/tab_reconciliation.py
git commit -m "Add reconciliation tab for retail-risk app"
```

---

### Task 18: `tab_pnl.py` rebuild — Goal 2

**Files:**
- Modify: `apps/retail_risk/tab_pnl.py` (full rewrite of `render_pnl`; keep `_render_waterfall` helper signature)

**Interfaces:**
- Consumes: `pnl_bridge.bridge_month / channel_alpha / attribution_month / channel_fee`, `rm_pnl_snapshots`.
- Produces: `render_pnl(engine)` (same entry point app.py already calls).

- [ ] **Step 1: Write the tab**

```python
# apps/retail_risk/tab_pnl.py
"""Realised P&L (Goal 2): 批零价差 cards, bridge waterfall, channel alpha vs spot,
trader/sales attribution, YTD trend. 复盘-aligned."""
from __future__ import annotations

import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from services.retail_risk import pnl_bridge as pb


def render_pnl(engine):
    st.subheader("Realised P&L — 批零价差 & Source Breakdown")

    with engine.connect() as conn:
        books = pd.read_sql(text(
            "SELECT id, name FROM marketdata.rm_books WHERE book_type = 'load' ORDER BY name"
        ), conn)
    if books.empty:
        st.info("No load books yet.")
        return

    col1, col2 = st.columns(2)
    with col1:
        book_id = st.selectbox("Book", books["id"].tolist(),
                               format_func=lambda x: books[books["id"] == x]["name"].iloc[0],
                               key="pnl_book")
    with engine.connect() as conn:
        months = pd.read_sql(text("""
            SELECT DISTINCT settlement_month FROM marketdata.rm_settlements
            WHERE book_id = :b ORDER BY settlement_month DESC
        """), conn, params={"b": book_id})
    if months.empty:
        st.info("No settlement data for this book yet.")
        return
    with col2:
        month = st.selectbox("Month", [m for m in months["settlement_month"]], key="pnl_month")

    with engine.connect() as conn:
        bridge = pb.bridge_month(conn, book_id, month)
        alpha = pb.channel_alpha(conn, book_id, month)
        attrib = pb.attribution_month(conn, book_id, month)

    # --- 批零价差 cards
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("零售结算均价", _fmt(bridge.retail_avg_price, " ¥/MWh"))
    c2.metric("批发结算均价", _fmt(bridge.wholesale_avg_cost, " ¥/MWh"))
    c3.metric("批零价差", _fmt(bridge.spread, " ¥/MWh"))
    c4.metric("净毛利", f"¥{bridge.net:,.0f}")

    # --- bridge waterfall
    items = [("零售收入", bridge.retail_revenue)]
    items += [("中长期采购", -bridge.channel_costs.get("midlong", 0.0))]
    if bridge.channel_costs.get("green_premium"):
        items.append(("绿电溢价", -bridge.channel_costs["green_premium"]))
    items += [("现货结算", -bridge.spot_cost), ("偏差/考核", -bridge.deviation),
              ("附加/分摊", -bridge.other)]
    _render_waterfall(pd.DataFrame(items, columns=["category", "total"]),
                      title=f"P&L Bridge — {month}")

    # --- channel alpha
    st.subheader("Channel Alpha vs Spot (降本/增支)")
    if alpha.empty:
        st.info("No positions or spot data for this month.")
    else:
        st.dataframe(alpha.style.format({
            "volume_mwh": "{:,.1f}", "vwap": "{:,.1f}", "spot_vwap": "{:,.1f}",
            "alpha_cny_mwh": "{:+,.1f}", "alpha_cny": "{:+,.0f}"}),
            use_container_width=True, hide_index=True)

    # --- trader / sales attribution
    st.subheader("Trader / Sales Attribution")
    t = attrib["trader_by_channel"]
    if t.empty or t["mkt_avg"].isna().all():
        st.info("No market benchmark for this month (信息汇总 not ingested or stale).")
    else:
        st.dataframe(t.style.format({"volume_mwh": "{:,.1f}", "vwap": "{:,.1f}",
                                     "mkt_avg": "{:,.1f}", "trader_alpha_cny": "{:+,.0f}"}),
                     use_container_width=True, hide_index=True)
        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Trader alpha", _fmt(attrib["trader_alpha_total"], " ¥", signed=True))
        a2.metric("渠道费用", _fmt(attrib["channel_fee_cny"], " ¥"))
        a3.metric("Sales alpha", _fmt(attrib["sales_alpha_cny"], " ¥", signed=True))
        a4.metric("Identity residual", _fmt(attrib["identity_residual_cny"], " ¥", signed=True))
        st.caption(f"Blended wholesale benchmark: {attrib['blended_benchmark']:.1f} ¥/MWh. "
                   "Trader + Sales = 批零价差 net of 渠道费; residual = spot/deviation not in the pivot."
                   if attrib["blended_benchmark"] else "Blended benchmark N/A")

    # --- YTD trend
    st.subheader("月度盈亏 YTD")
    with engine.connect() as conn:
        snap = pd.read_sql(text("""
            SELECT snapshot_date, realized_cny, bilateral_pnl_cny, spot_pnl_cny,
                   deviation_pnl_cny, other_pnl_cny, unrealized_mtm_cny
            FROM marketdata.rm_pnl_snapshots
            WHERE book_id = :b ORDER BY snapshot_date
        """), conn, params={"b": book_id})
    if not snap.empty:
        st.line_chart(snap.set_index("snapshot_date")[["realized_cny", "unrealized_mtm_cny"]])
    else:
        st.info("No snapshots yet — run the engines (run_backfill or in-app compute).")


def _fmt(v, suffix: str, signed: bool = False) -> str:
    if v is None or pd.isna(v):
        return "N/A"
    return f"{'+' if signed and v >= 0 else ''}{v:,.1f}{suffix}"


def _render_waterfall(items_df: pd.DataFrame, title: str):
    categories = items_df["category"].tolist()
    values = items_df["total"].tolist()
    categories.append("Net Margin")
    values.append(sum(values))
    measures = ["relative"] * (len(categories) - 1) + ["total"]
    fig = go.Figure(go.Waterfall(
        orientation="v", measure=measures, x=categories, y=values,
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2ecc71"}},
        decreasing={"marker": {"color": "#e74c3c"}},
        totals={"marker": {"color": "#3498db"}},
    ))
    fig.update_layout(title=title, yaxis_title="CNY", showlegend=False, height=450)
    st.plotly_chart(fig, use_container_width=True)
```

- [ ] **Step 2: Commit**

```bash
git add apps/retail_risk/tab_pnl.py
git commit -m "Rebuild P&L tab with bridge, channel alpha and attribution"
```

---

### Task 19: `tab_positions.py` MtM scenario + book cards; `app.py` registration; local smoke test

**Files:**
- Modify: `apps/retail_risk/tab_positions.py` (replace `_render_mtm` body; add book cards at top of `render_positions`)
- Modify: `apps/retail_risk/app.py` (add Reconciliation + Data Upload tabs)

**Interfaces:**
- Consumes: `mtm.book_mtm`, `rm_pnl_snapshots`.
- Produces: unchanged entry points `render_positions(engine)`; app with 8 tabs.

- [ ] **Step 1: Replace `_render_mtm` in tab_positions.py**

```python
def _render_mtm(book_id: int, engine):
    """Per-book MtM: procurement (open positions x scenario curve) + retail contracts."""
    from services.retail_risk import mtm as mm

    scenario = st.selectbox("Scenario", ["spot_base", "spot_p10", "spot_m10"],
                            format_func={"spot_base": "Base", "spot_p10": "+10",
                                         "spot_m10": "−10"}.get,
                            key="mtm_scenario")
    with engine.connect() as conn:
        result = mm.book_mtm(conn, book_id, scenario=scenario)

    col1, col2, col3 = st.columns(3)
    col1.metric("Procurement MtM", f"¥{result.procurement_mtm_cny:,.0f}")
    col2.metric("Retail-contract MtM", f"¥{result.retail_mtm_cny:,.0f}")
    col3.metric("Total Unrealised", f"¥{result.total_mtm_cny:,.0f}")

    if not result.positions_df.empty:
        st.caption("Open positions (procurement)")
        cols = [c for c in ["channel", "direction", "volume_mwh", "price_cny_mwh",
                            "forward_price_cny_mwh", "unrealized_pnl_cny"]
                if c in result.positions_df.columns]
        st.dataframe(result.positions_df[cols], use_container_width=True, hide_index=True)
    if not result.contracts_df.empty:
        st.caption("Active retail contracts (remaining volume)")
        cols = [c for c in ["contract_type", "price_cny_mwh", "remaining_mwh",
                            "forward_price", "mtm_cny"] if c in result.contracts_df.columns]
        st.dataframe(result.contracts_df[cols], use_container_width=True, hide_index=True)
    if result.positions_df.empty and result.contracts_df.empty:
        st.info("No open positions or active contracts for MtM.")
```

Also add at the top of `render_positions` (after the book selector): realised-YTD + unrealised cards from `rm_pnl_snapshots`:

```python
    with engine.connect() as conn:
        snap = pd.read_sql(text("""
            SELECT SUM(realized_cny) AS ytd,
                   (SELECT unrealized_mtm_cny FROM marketdata.rm_pnl_snapshots s2
                    WHERE s2.book_id = :b ORDER BY snapshot_date DESC LIMIT 1) AS mtm
            FROM marketdata.rm_pnl_snapshots WHERE book_id = :b
        """), conn, params={"b": book_id}).iloc[0]
    c1, c2 = st.columns(2)
    c1.metric("Realised P&L YTD", f"¥{snap['ytd']:,.0f}" if pd.notna(snap["ytd"]) else "N/A")
    c2.metric("Latest Unrealised MtM", f"¥{snap['mtm']:,.0f}" if pd.notna(snap["mtm"]) else "N/A")
```

- [ ] **Step 2: Register tabs in app.py**

Change the tabs block to 8 tabs:

```python
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "CRM", "Settlement", "Realised P&L", "Positions & MtM",
    "Reconciliation", "Data Upload", "VaR & Greeks", "Agent"
])
# ... existing with-blocks, plus:
with tab5:
    from apps.retail_risk.tab_reconciliation import render_reconciliation
    render_reconciliation(engine)

with tab6:
    from apps.retail_risk.tab_data_upload import render_upload
    render_upload(engine)
```

(Renumber the VaR/Agent blocks to tab7/tab8.)

- [ ] **Step 3: Local smoke test**

Run: `streamlit run apps/retail_risk/app.py --server.port 8513` (env loaded from `config/.env`)
Verify: app starts without import errors; all 8 tabs render; Data Upload / Reconciliation / P&L / Positions show their empty-states ("No load books yet" etc.) against the empty DB.
Then stop the server.

- [ ] **Step 4: Commit**

```bash
git add apps/retail_risk/tab_positions.py apps/retail_risk/app.py
git commit -m "Add MtM scenarios, book cards and new tab registrations"
```

---

### Task 20: Migration + dry-run + live backfill (all gated by user confirmation)

**Files:**
- Modify: none (operational task)

- [ ] **Step 1: Apply the migration** — requires explicit "yes" in-session (DDL change on live RDS)

```bash
source ~/.venvs/bess-platform/bin/activate
python - <<'EOF'
import os
from dotenv import load_dotenv
load_dotenv("config/.env")
from sqlalchemy import create_engine, text
eng = create_engine(os.environ.get("PGURL") or os.environ.get("DB_DSN"))
sql = open("db/migrations/2026-09-30_rm_retail_categories_benchmarks.sql").read()
with eng.begin() as conn:
    for stmt in [s.strip() for s in sql.replace("BEGIN;", "").replace("COMMIT;", "").split(";") if s.strip()]:
        conn.execute(text(stmt))
print("migration applied")
EOF
```

Verify: `psql`-equivalent check that the 4 new categories are accepted and `rm_market_benchmarks` exists.

- [ ] **Step 2: Dry-run the full backfill**

```bash
python services/retail_risk/run_backfill.py --root data/trading --dry-run
```

Review the printed counts with the user: trades/volumes rows per province, invoice items + printed totals per file (spot-check 山东 202603 7021 total ≈ ¥75.77M; 冀南 202603 total = ¥175,251.68), curves rows per MTM workbook (~8,700 per file: 365 days × 24h), benchmark rows per 信息汇总.

- [ ] **Step 3: Live backfill** — requires explicit "yes" in-session (writes to live RDS)

```bash
python services/retail_risk/run_backfill.py --root data/trading
```

- [ ] **Step 4: Verification queries**

```sql
-- books
SELECT id, name FROM marketdata.rm_books WHERE book_type = 'load' ORDER BY id;
-- positions per book
SELECT book_id, COUNT(*), SUM(volume_mwh) FROM marketdata.rm_positions GROUP BY 1;
-- invoices per book/month with status
SELECT book_id, settlement_month, status, total_amount_cny FROM marketdata.rm_settlements
 WHERE book_id >= 21 ORDER BY 1, 2;
-- curves
SELECT province, product, COUNT(*) FROM marketdata.rm_forward_curves GROUP BY 1, 2;
-- benchmarks
SELECT province, channel, COUNT(*) FROM marketdata.rm_market_benchmarks GROUP BY 1, 2;
-- customers/contracts
SELECT province, COUNT(*) FROM marketdata.rm_customers GROUP BY 1;
SELECT COUNT(*) FROM marketdata.rm_customer_contracts;
```

- [ ] **Step 5: Run engines + recon review**

Open the Reconciliation tab (Task 17): for each load book, walk every invoice month and review the status matrix and drill-down lines with the user — this is the Review-Focus #4 verification gate for the 月度竞价 per-day interpretation （山东 3月 复盘： 批发结算电量 230,493.74 MWh vs 7021 合计 230,506.784; 冀南 3月 invoice 中长期 17,770 MWh vs trades-cleared volume).

Then persist snapshots:

```bash
python - <<'EOF'
import datetime
from services.retail_risk import loader, pnl_bridge, mtm as mm
eng = loader.get_engine()
with eng.begin() as conn:
    books = conn.execute(__import__("sqlalchemy").text(
        "SELECT id FROM marketdata.rm_books WHERE book_type='load'")).fetchall()
    months = conn.execute(__import__("sqlalchemy").text(
        "SELECT DISTINCT settlement_month FROM marketdata.rm_settlements")).fetchall()
    for (bid,) in books:
        for (m,) in months:
            b = pnl_bridge.bridge_month(conn, bid, m)
            a = pnl_bridge.channel_alpha(conn, bid, m)
            pnl_bridge.persist_bridge_snapshot(conn, bid, m, b,
                float(a["alpha_cny"].sum()) if not a.empty else None)
        r = mm.book_mtm(conn, bid)
        mm.persist_mtm(conn, r)
print("snapshots persisted")
EOF
```

- [ ] **Step 6: Final app smoke + commit**

Re-run the Streamlit smoke (Task 19 Step 3): all tabs now render with data; Reconciliation matrix shows statuses; P&L tab shows bridge/alpha/attribution; Positions MtM cards live.

```bash
git add -A services/retail_risk apps/retail_risk
git commit -m "Complete P1 retail-risk ingestion backfill and verification"
```
