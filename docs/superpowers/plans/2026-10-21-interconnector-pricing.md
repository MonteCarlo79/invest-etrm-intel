# 跨省绿电 Interconnector Pricing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a 跨省绿电 tab to bess-map: exact port of the 卡师傅27 4th-cycle landed-cost engine (S1–S4) plus an agreement-driven 2027 channel congestion screener with profile-matched spreads (S5).

**Architecture:** Pure-python package `libs/interconnector_pricing/` (params verbatim from the Excel, engine, agreements dataset + analytics — all DB-free and unit-testable), wired into a new bess-map tab whose only DB touches are three read-only helpers (RT hourly, interconnector trades/agreements).

**Tech Stack:** Python 3.13, Streamlit, pandas, plotly, psycopg2-style conn; pytest via `~/.venvs/bess-platform/bin/python`.

**Spec:** `docs/superpowers/specs/2026-10-21-interconnector-pricing-design.md`

## Global Constraints

- Engine math is FIXED by spec §5: per segment `损耗折价 G = D×F/(1−F)`, `段后 D' = D+E+G`; `落地 = D_last + 环境价`; composite `O = 1−Π(1−F_i)`. Golden values must reproduce within 1e-9.
- Missing/invalid input STOPS output with a reason string — never a silent zero (西藏 unpriced; 辛洹线/云霄 `computable: False`; blank inputs; `环境价 > 总价`).
- Profile buckets (spec §10): `solar = {10..14}`, `wind = {0..5, 19..23}` local; `mixed`/`hydro` → flat.
- 上海 has no RT series → proxy 江苏/浙江 with a visible flag on the panel.
- No DB writes, no schema changes, no other apps touched. Only reads: `marketdata.spot_prices_hourly`, `staging.interconnector_trades`, `staging.interconnector_mlt_snapshot`, `staging.interconnector_gov_agreements`.
- `git add` with explicit paths only (parallel sessions share this tree; the real index carries their staged work). If porcelain commit hangs (OneDrive), commit via temp-index plumbing: `GIT_INDEX_FILE=.git/tmp-index git read-tree HEAD && git add <files> && git write-tree && git commit-tree <tree> -p HEAD -m <msg> && git update-ref refs/heads/main <commit>`; remove only STALE `.git/index.lock` (check `ps` for a live git process first).
- Commit messages end with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Tests run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector_pricing/ -v` from repo root.
- i18n: bess-map has en/zh translation dicts; new keys prefixed `icp_` in BOTH dicts, existing `_t()` style.

## Review Focus

1. `环境价 == 总价` (电能量价 = 0) → valid calc, landed = Σfees + 环境价 (not a stop) — pinned in Task 2.
2. Zero-loss segment (e.g. 北京/上海 loss_rate = 0) → `G = 0`, no division issue — pinned in Task 2.
3. Trailing-12m RT series with missing months → averages over available months, no crash, count reported — pinned in Task 3.
4. Agreement with `path_id = None` → row marked 无路径 and excluded from landed scoring, never guessed — pinned in Task 3.
5. Custom path containing a duplicated segment → computed as given, no silent dedup — pinned in Task 2.

---

### Task 1: `params.py` — 4th-cycle tables verbatim

**Files:**
- Create: `libs/interconnector_pricing/__init__.py`
- Create: `libs/interconnector_pricing/params.py`
- Test: `tests/interconnector_pricing/test_params.py`

**Interfaces:**
- Consumes: nothing (source of truth is the Excel; spec §5/§6 carries the golden values).
- Produces: `CYCLE: str`; `PROVINCE_GRID: dict[str, dict]` (27 keys, each `{"tariff": float, "loss": float}`, 西藏 `{"priced": False}`); `REGIONAL_GRID: dict[str, dict]` (5); `SPECIAL_PROJECTS: dict[str, dict]` (40, each `{"tariff": float, "loss": float, "status": str, "pdf_page": int, "computable": bool}`); `PATHS: dict[str, list[str]]` (18 presets; segment labels exactly `区域：<name>` / `专项：<name>`); `SICHUAN_EXCEPTIONS: list[dict]`; `REGIONAL_CAPACITY_SHARE: list[dict]`; `PROFILE_BUCKETS: dict[str, list[int] | None]` (`{"solar": [10..14], "wind": [0..5,19..23], "mixed": None, "hydro": None}`); `RESOURCE_MIXES: tuple = ("solar", "wind", "mixed", "hydro")`.

- [ ] **Step 1: Write the failing test**

```python
# tests/interconnector_pricing/test_params.py
from libs.interconnector_pricing import params as P


def test_table_sizes():
    assert len(P.PROVINCE_GRID) == 27
    assert len(P.REGIONAL_GRID) == 5
    assert len(P.SPECIAL_PROJECTS) == 40
    assert len(P.PATHS) == 18


def test_unpriced_and_noncomputable_stops():
    assert P.PROVINCE_GRID["西藏"]["priced"] is False
    assert P.SPECIAL_PROJECTS["辛洹线"]["computable"] is False
    assert P.SPECIAL_PROJECTS["云霄"]["computable"] is False


def test_spot_values():
    assert P.PROVINCE_GRID["青海"] == {"tariff": 33.9, "loss": 0.0139}
    assert P.REGIONAL_GRID["西北"] == {"tariff": 12.1, "loss": 0.019}
    assert P.SPECIAL_PROJECTS["宝合直流"]["tariff"] == 53.23
    assert P.SPECIAL_PROJECTS["宝合直流"]["loss"] == 0.05


def test_paths_reference_known_segments():
    known = {f"区域：{k}" for k in P.REGIONAL_GRID} | {f"专项：{k}" for k in P.SPECIAL_PROJECTS}
    for name, segs in P.PATHS.items():
        assert segs and len(segs) <= 5, name
        for s in segs:
            assert s in known, (name, s)
    assert P.PATHS["西北—宝合—华东"] == ["区域：西北", "专项：宝合直流", "区域：华东"]


def test_profile_buckets():
    assert P.PROFILE_BUCKETS["solar"] == [10, 11, 12, 13, 14]
    assert sorted(P.PROFILE_BUCKETS["wind"]) == [0, 1, 2, 3, 4, 5, 19, 20, 21, 22, 23]
    assert P.PROFILE_BUCKETS["mixed"] is None and P.PROFILE_BUCKETS["hydro"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector_pricing/test_params.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `params.py`**

Port every table verbatim from the Excel (spec §6 + the workbook itself at `models/【卡师傅27】跨省绿电落地测算-第四周期.xlsx`, sheets 送出省价格 / 区域电网价格 / 专项工程价格 / 路径库 / 四川外送特例 / 区域容量分摊). `CYCLE = "第四周期 (发改价格〔2026〕1077号, 2026-07-22)"`. `__init__.py` re-exports nothing (import via `from libs.interconnector_pricing import params`).

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector_pricing/test_params.py -v`
Expected: 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/interconnector_pricing/__init__.py libs/interconnector_pricing/params.py tests/interconnector_pricing/test_params.py
git commit -m "Add 4th-cycle interconnector pricing parameter tables

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `engine.py` — landed-cost calculator

**Files:**
- Create: `libs/interconnector_pricing/engine.py`
- Test: `tests/interconnector_pricing/test_engine.py`

**Interfaces:**
- Consumes: `params` (Task 1).
- Produces:
  ```python
  @dataclass
  class SegmentDetail:
      label: str; pre_cost: float; tariff: float; loss_rate: float
      loss_cost: float; post_cost: float
  @dataclass
  class CalcResult:
      ok: bool; stop_reason: str | None
      landed: float | None; landed_kwh: float | None; increment: float | None
      tariff_sum: float | None; loss_sum: float | None; composite_loss: float | None
      segments: list[SegmentDetail]
  BASES: tuple = ("发电侧", "出省节点")
  def compute_landed(*, province: str, basis: str, path_segments: list[str],
                     green_total: float, env_price: float) -> CalcResult
  ```

- [ ] **Step 1: Write the failing test**

```python
# tests/interconnector_pricing/test_engine.py
import pytest
from libs.interconnector_pricing.engine import compute_landed

GOLDEN = [
    # province, basis, path, green_total, env, landed, tariff_sum, loss_sum, composite
    ("青海", "发电侧", ["区域：西北", "专项：宝合直流", "区域：华东"], 240, 5,
     377.782511764198, 108.43, 29.3525117641982, 0.0949728426040001),
    ("甘肃", "发电侧", ["区域：西北", "专项：宝合直流", "区域：华东"], 240, 5,
     370.357208258708, 102.53, 27.8272082587081, 0.0914852620360001),
    ("甘肃", "发电侧", ["区域：西北", "专项：宝合直流", "区域：华东"], 240, 30,
     367.839768028707, 102.53, 25.3097680287065, 0.0914852620360001),
    ("青海", "发电侧", ["区域：西北", "专项：宝合直流", "区域：华东"], 240, 30,
     375.159031302096, 108.43, 26.7290313020963, 0.0949728426040001),
    ("甘肃", "出省节点", ["区域：西北", "专项：宝合直流", "区域：华东"], 240, 5,
     337.236446709552, 74.53, 22.7064467095517, 0.08221564),
    ("山西", "发电侧", ["专项：雁淮直流", "区域：华东"], 353, 20,
     436.751853292325, 66.8, 16.9518532923255, 0.044298249976),
    ("安徽", "发电侧", ["区域：华东"], 376.45, 0,
     421.923395613323, 39.2, 6.2733956133225, 0.0152),
]


@pytest.mark.parametrize("prov,basis,path,gt,env,landed,tsum,lsum,comp", GOLDEN)
def test_golden_values(prov, basis, path, gt, env, landed, tsum, lsum, comp):
    r = compute_landed(province=prov, basis=basis, path_segments=path,
                       green_total=gt, env_price=env)
    assert r.ok, r.stop_reason
    assert r.landed == pytest.approx(landed, abs=1e-9)
    assert r.tariff_sum == pytest.approx(tsum, abs=1e-9)
    assert r.loss_sum == pytest.approx(lsum, abs=1e-9)
    assert r.composite_loss == pytest.approx(comp, abs=1e-9)
    assert r.increment == pytest.approx(landed - gt, abs=1e-9)
    assert r.landed_kwh == pytest.approx(landed / 1000, abs=1e-12)


def test_stops():
    assert not compute_landed(province="西藏", basis="发电侧",
                              path_segments=["区域：华东"], green_total=240, env_price=5).ok
    assert not compute_landed(province="青海", basis="发电侧",
                              path_segments=["专项：辛洹线"], green_total=240, env_price=5).ok
    assert not compute_landed(province="青海", basis="发电侧",
                              path_segments=["专项：云霄"], green_total=240, env_price=5).ok
    assert not compute_landed(province="青海", basis="发电侧",
                              path_segments=["区域：华东"], green_total=240, env_price=250).ok
    assert not compute_landed(province="青海", basis="发电侧",
                              path_segments=[], green_total=240, env_price=5).ok
    assert not compute_landed(province="青海", basis="发电侧",
                              path_segments=["专项：不存在直流"], green_total=240, env_price=5).ok


def test_env_equals_total_is_valid():
    r = compute_landed(province="青海", basis="发电侧", path_segments=["区域：华东"],
                       green_total=240, env_price=240)
    # 电能量价=0: seg1 省网青海 D=0 → post=33.9; seg2 区域华东 D=33.9 → G=33.9×0.0152/(1−0.0152)
    expected = 33.9 + 33.9 * 0.0152 / (1 - 0.0152) + 9.2 + 240
    assert r.ok and r.landed == pytest.approx(expected, abs=1e-9)


def test_zero_loss_segment():
    r = compute_landed(province="北京", basis="发电侧", path_segments=["区域：华北"],
                       green_total=240, env_price=0)
    assert r.ok
    assert r.segments[0].loss_cost == 0.0


def test_duplicate_segment_computed_as_given():
    r = compute_landed(province="安徽", basis="发电侧",
                       path_segments=["区域：华东", "区域：华东"], green_total=376.45, env_price=0)
    assert r.ok and len(r.segments) == 3  # 省网 + two 区域：华东
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector_pricing/test_engine.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `compute_landed` in `libs/interconnector_pricing/engine.py`**

Spec §5 math. Order of validation: inputs present → `env_price ≤ green_total` and `env_price ≥ 0` → province known + priced → basis in `BASES` → path non-empty and every segment resolvable (`区域：`/`专项：` prefix → table lookup) and `computable`. 发电侧 prepends `省网：<province>` using `PROVINCE_GRID[province]` (`loss` may be 0). Each stop returns `CalcResult(ok=False, stop_reason=<Chinese reason string matching the Excel's messages>)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector_pricing/ -v`
Expected: all PASS (params 5 + engine 11).

- [ ] **Step 5: Commit**

```bash
git add libs/interconnector_pricing/engine.py tests/interconnector_pricing/test_engine.py
git commit -m "Add landed-cost engine with golden-value tests

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `agreements.py` — 2027 seed + analytics

**Files:**
- Create: `libs/interconnector_pricing/agreements.py`
- Test: `tests/interconnector_pricing/test_agreements.py`

**Interfaces:**
- Consumes: `engine.compute_landed`, `params.PROFILE_BUCKETS`, `params.RESOURCE_MIXES` (Tasks 1–2).
- Produces:
  ```python
  AGREEMENTS_2027: list[dict]   # keys exactly: sender, receiver, path_id, channel_label,
                                # volume_100gwh, green, resource_mix, source_type, source_ref, confidence
  def reference_landed(agreement: dict, green_total: float = 240.0,
                       env_price: float = 5.0) -> CalcResult
      # engine on agreement's path (province=sender, basis=发电侧, PATHS[path_id]);
      # path_id None or unknown → CalcResult(ok=False, stop_reason="无路径")
  def profile_hours(resource_mix: str) -> set[int] | None              # from PROFILE_BUCKETS
  def monthly_avg_rt(rows: list[tuple], hours: set[int] | None = None) -> list[tuple[str, float]]
      # rows: (datetime-like, price) → [("YYYY-MM", avg)] filtered to hours (None = all), input unsorted OK
  def congestion_scores(agreements: list[dict], spreads: dict[str, float]) -> list[dict]
      # spreads keyed by id(agreement) is unusable — key by index; see test. Rows: {**agreement,
      # "spread": float|None, "score": float|None, "no_path": bool} sorted score desc (None last)
  def undershoot_flags(actual_ttm_100gwh: dict[str, float], agreements: list[dict],
                       threshold: float = 0.8) -> dict[int, bool]
      # actual keyed by channel_label; flag True when actual < threshold × volume
  def seasonality_shares(monthly_volumes: list[tuple[str, float]]) -> list[float]
      # [("YYYY-MM", vol)] → 12 shares summing 1 by calendar month; <24 months of data → flat 1/12
  ```

- [ ] **Step 1: Write the failing test**

```python
# tests/interconnector_pricing/test_agreements.py
import datetime as dt
import pytest
from libs.interconnector_pricing import agreements as A


def test_seed_integrity():
    assert len(A.AGREEMENTS_2027) >= 9
    for a in A.AGREEMENTS_2027:
        assert a["volume_100gwh"] > 0
        assert a["resource_mix"] in ("solar", "wind", "mixed", "hydro")
        assert a["confidence"] in ("公告确认", "行业信息", "推算")
        assert a["source_type"] in ("公告", "行业信息")
    nx = [a for a in A.AGREEMENTS_2027 if a["sender"] == "宁夏" and a["receiver"] == "福建"][0]
    assert nx["volume_100gwh"] == 20 and nx["confidence"] == "公告确认"
    qh = [a for a in A.AGREEMENTS_2027 if a["sender"] == "青海" and a["receiver"] == "上海"][0]
    assert qh["volume_100gwh"] == 40 and qh["resource_mix"] == "solar"


def test_reference_landed_uses_engine():
    a = {"sender": "甘肃", "receiver": "上海", "path_id": "西北—宝合—华东",
         "channel_label": "宝合直流", "volume_100gwh": 20, "resource_mix": "solar"}
    r = A.reference_landed(a, green_total=240.0, env_price=5.0)
    assert r.ok and r.landed == pytest.approx(370.357208258708, abs=1e-9)


def test_monthly_avg_rt_profile_filter():
    rows = [(dt.datetime(2026, 3, 5, 12), 400.0), (dt.datetime(2026, 3, 5, 22), 600.0),
            (dt.datetime(2026, 4, 9, 11), 500.0)]
    assert A.monthly_avg_rt(rows) == [("2026-03", 500.0), ("2026-04", 500.0)]
    assert A.monthly_avg_rt(rows, hours={10, 11, 12}) == [("2026-03", 400.0), ("2026-04", 500.0)]
    assert A.monthly_avg_rt(rows, hours={22}) == [("2026-03", 600.0)]


def test_monthly_avg_rt_missing_months_no_crash():
    rows = [(dt.datetime(2026, 1, 5, 12), 300.0), (dt.datetime(2026, 5, 5, 12), 700.0)]
    out = A.monthly_avg_rt(rows)
    assert out == [("2026-01", 300.0), ("2026-05", 700.0)]


def test_congestion_scores_order_and_no_path():
    ags = [
        {"sender": "甲", "path_id": "西北—宝合—华东", "channel_label": "c1", "volume_100gwh": 10},
        {"sender": "乙", "path_id": None, "channel_label": "c2", "volume_100gwh": 99},
        {"sender": "丙", "path_id": "西北—灵绍—华东", "channel_label": "c3", "volume_100gwh": 5},
    ]
    spreads = {0: 50.0, 2: 100.0}   # index-keyed; 1 has no spread (no path)
    out = A.congestion_scores(ags, spreads)
    assert out[0]["channel_label"] == "c3" and out[0]["score"] == 500.0
    assert out[1]["channel_label"] == "c1" and out[1]["score"] == 500.0
    assert out[2]["no_path"] is True and out[2]["score"] is None


def test_undershoot_flags():
    ags = [{"channel_label": "c1", "volume_100gwh": 100},
           {"channel_label": "c2", "volume_100gwh": 100}]
    flags = A.undershoot_flags({"c1": 70.0, "c2": 90.0}, ags)
    assert flags == {0: True, 1: False}


def test_seasonality_shares():
    hist = [(f"2025-{m:02d}", 10.0 if m == 6 else 5.0) for m in range(1, 13)] + \
           [(f"2026-{m:02d}", 10.0 if m == 6 else 5.0) for m in range(1, 13)]
    shares = A.seasonality_shares(hist)
    assert len(shares) == 12 and sum(shares) == pytest.approx(1.0)
    assert shares[5] == pytest.approx(20.0 / 130.0)
    assert A.seasonality_shares([("2026-03", 7.0)]) == [1 / 12] * 12
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector_pricing/test_agreements.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `agreements.py`**

Seed `AGREEMENTS_2027` exactly per spec §6: the 9 path agreements, plus the 甘肃 127亿 context row as `{"sender": "甘肃", "receiver": "多省", "path_id": None, "channel_label": "甘肃2026绿电外送合计", "volume_100gwh": 127.0, "green": True, "resource_mix": "solar", "source_type": "公告", "source_ref": "<甘肃经济网 link>", "confidence": "公告确认"}` — display-only; the `no_path` scoring logic excludes it from S5b automatically. `reference_landed` maps `province=agreement["sender"], basis="发电侧", path_segments=params.PATHS[agreement["path_id"]]`; `path_id` None or unknown → `CalcResult(ok=False, stop_reason="无路径")`. Analytics helpers are pure pandas-free functions.

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector_pricing/ -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/interconnector_pricing/agreements.py tests/interconnector_pricing/test_agreements.py
git commit -m "Add 2027 agreement seed and congestion analytics

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 4: bess-map tab 跨省绿电 (S1–S5)

**Files:**
- Modify: `apps/bess-map/app.py` — tab list + new tab body + 3 DB-read helpers + i18n dicts (en ~top, zh ~middle; anchor on existing `icp_`-absent `fr_*` keys style)
- Test: `tests/interconnector_pricing/test_app_helpers.py`

**Interfaces:**
- Consumes: `engine.compute_landed`, `params` (all tables), `agreements` (all helpers); bess-map's `_eng()`/`_conn()` pattern for DB.
- Produces (module-level helpers, unit-tested with fake conns):
  ```python
  def _icp_load_rt_hourly(conn, province: str, months: int = 12) -> list[tuple]
      # SELECT datetime, rt_price FROM marketdata.spot_prices_hourly
      # WHERE province=%s AND datetime >= now() - make_interval(months => %s) ORDER BY datetime
      # → [(datetime, float)]
  def _icp_load_trade_ttm(conn) -> dict[str, float]
      # SELECT channel_1, SUM(vol_post_mwh) FROM staging.interconnector_trades
      # WHERE month_start >= now() - interval '12 months' GROUP BY channel_1
      # → {channel_1: 亿kWh}; 1 亿kWh = 1e5 MWh, so SUM(vol_post_mwh)/1e5
  def _icp_load_gov_agreements(conn) -> list[tuple]
      # SELECT send_prov, recv_prov, annual_gwh, period, channel_hint, source
      # FROM staging.interconnector_gov_agreements ORDER BY period DESC
  ```
- Tab sections S1–S5 per spec §4. Session keys: `icp_scenarios` (list of dicts with all S1 inputs + CalcResult + resource_mix), `icp_custom_segments` (5-slot list).

- [ ] **Step 1: Write the failing helper test**

```python
# tests/interconnector_pricing/test_app_helpers.py
class _Cur:
    def __init__(self, rows): self._rows = rows
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None): self.sql, self.params = sql, params
    def fetchall(self): return self._rows

class _Conn:
    def __init__(self, rows): self._cur = _Cur(rows)
    def cursor(self): return self._cur


def test_load_rt_hourly_rows_and_sql():
    from libs.interconnector_pricing.db_reads import load_rt_hourly
    rows = [("2026-01-01 10:00", 401.2), ("2026-01-01 11:00", 402.3)]
    conn = _Conn(rows)
    out = load_rt_hourly(conn, "江苏", months=12)
    assert out == rows
    assert "marketdata.spot_prices_hourly" in conn.cursor().sql


def test_load_trade_ttm_converts_mwh_to_100gwh():
    from libs.interconnector_pricing.db_reads import load_trade_ttm
    conn = _Conn([("宝合直流", 2_000_000.0)])   # 2e6 MWh = 20 亿kWh
    assert load_trade_ttm(conn) == {"宝合直流": 20.0}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector_pricing/test_app_helpers.py -v`
Expected: FAIL — `libs/interconnector_pricing/db_reads.py` not found.

- [ ] **Step 3: Implement `libs/interconnector_pricing/db_reads.py` + the tab**

`db_reads.py`: the 3 conn-injected SQL readers above (exact SQL per Interfaces).

Tab (new entry at the end of bess-map's tab list, label from `_t("icp_tab")` = "跨省绿电" / "Interconnector Green"):

- **S1 测算工作台**: selectbox 送出省 (`params.PROVINCE_GRID` keys), radio 口径 (`engine.BASES`), selectbox 路径 (`params.PATHS` keys + "自定义"), two number_inputs (绿电总价 default 240, 环境价 default 5), selectbox resource_mix (`params.RESOURCE_MIXES`, default solar). Custom expander: 5 ordered selectboxes of segments (`区域：`+`专项：` union, first required). On 计算: `compute_landed` → if not ok, `st.error(r.stop_reason)`; else 3 metrics (落地元/MWh, 折合元/kWh, 输送增加), segment dataframe, Σtariff/Σloss/composite row, caveat caption (结果边界 from spec §4 S1). Save button appends to `st.session_state["icp_scenarios"]`.
- **S2 方案对比**: dataframe of saved scenarios ranked by landed asc; per-row delete button.
- **S3 现货价差**: selectbox receiver from live `SELECT DISTINCT province FROM marketdata.spot_prices_hourly` (fallback list = 29 LingFeng markets on error); 上海 absent → caption flag with 江苏/浙江 proxy note. Load RT via `load_rt_hourly`, chart (plotly) monthly avg RT line (`agreements.monthly_avg_rt` flat) + each scenario's landed line; per-scenario metric: profile-matched spread + % positive months (use `profile_hours(scenario["resource_mix"])`).
- **S4 参数库**: expanders with dataframes of all params tables + CYCLE label + caveats bullets (京津唐, 报备线损清算, 未列示≠取消, 结果边界).
- **S5 通道拥挤度**: S5a two tables (`AGREEMENTS_2027` + `load_gov_agreements` render, labeled 历史协议提取). S5b: two number_inputs (参考绿电总价 240 / 环境价 5) → per agreement `reference_landed` → spread = receiver TTM monthly-avg RT (profile-matched) − landed → `congestion_scores` with S2-override (saved scenarios matching sender+path_id replace the reference landed) → score dataframe sorted. S5c: matrix rows per agreement with volume, seasonality note (flat vs historical via `load_trade_ttm` + `seasonality_shares`), undershoot flags via `undershoot_flags`, confidence labels.

i18n keys for every user-visible string (prefix `icp_`), en + zh.

- [ ] **Step 4: Verify**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/interconnector_pricing/ -v` (all pass) and `~/.venvs/bess-platform/bin/python -c "import ast; ast.parse(open('apps/bess-map/app.py').read())"` (silent).

- [ ] **Step 5: Commit**

```bash
git add libs/interconnector_pricing/db_reads.py apps/bess-map/app.py tests/interconnector_pricing/test_app_helpers.py
git commit -m "Add 跨省绿电 tab to bess-map (calculator, spread, congestion screener)

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Local live verification

**Files:** none (verification only).

- [ ] **Step 1: Golden check through the real package**

Run from repo root:
```bash
~/.venvs/bess-platform/bin/python -c "
from libs.interconnector_pricing.engine import compute_landed
r = compute_landed(province='青海', basis='发电侧',
                   path_segments=['区域：西北','专项：宝合直流','区域：华东'],
                   green_total=240, env_price=5)
assert r.ok and abs(r.landed - 377.782511764198) < 1e-9, r
print('golden OK:', r.landed)"
```
Expected: `golden OK: 377.782511764198`

- [ ] **Step 2: bess-map renders locally with live DB**

Load `config/.env`, `streamlit run apps/bess-map/app.py --server.port 8503`. In 跨省绿电： S1 compute 青海→华东 (expect 377.78); save scenario; S3 select 江苏 (chart renders monthly RT + landed line; spread metric shows solar bucket); S4 tables render; S5a/S5b/S5c render (scores sorted, 甘浙 line flagged as H2-2027 commissioned, undershoot flags where applicable, 历史协议提取 table populated or graceful empty).

- [ ] **Step 3: Commit any fixes surfaced**

```bash
git add <only files actually fixed>
git commit -m "Fix issues found in interconnector tab local run

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** S1→T2+T4, S2→T4, S3→T3(helpers)+T4, S4→T1+T4, S5a/b/c→T3+T4; engine math §5→T2 golden; validation stops→T2; profile matching→T3(profile_hours/monthly_avg_rt)+T4; undershoot→T3; seasonality→T3; gov-agreements secondary layer→T4; 上海 proxy→T4; CYCLE label→T1+T4; caveats→T4. Deployment is a separate confirmed step (out of plan).
- **Type consistency:** `CalcResult` fields consumed by name in T4 per T2's Interfaces; `congestion_scores` index-keyed spreads documented in its docstring (id() trap called out in the test); TTM unit conversion (MWh→亿kWh = ÷1e5) fixed in T4 Step 3.
- **Review Focus:** all five lines pinned — RF1/RF2/RF5 in Task 2 tests, RF3 in Task 3 (`test_monthly_avg_rt_missing_months_no_crash`), RF4 in Task 3 (`test_congestion_scores_order_and_no_path`).
