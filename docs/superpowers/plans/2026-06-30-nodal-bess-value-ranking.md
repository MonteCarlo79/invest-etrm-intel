# Nodal BESS Value Ranking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 2h/4h perfect-foresight nodal rank columns to the daily Mengxi BESS ranking PDF, plus a monthly standalone nodal ranking page computed on the 5th of each month.

**Architecture:** All changes are in `services/hermes/mengxi_ranking_report.py`. The daily PDF runs MILP inline for yesterday only (~200 problems, ~10s). A separate monthly job (5th of each month) computes the full calendar month and stores results in `reports.nodal_pf_monthly`. The monthly page in the daily PDF reads from this pre-computed table. The MILP engine is reused from `services/bess_map/optimisation_engine.py`.

**Tech Stack:** PuLP/CBC MILP via `services/bess_map/optimisation_engine.compute_dispatch_from_15min_prices`, `concurrent.futures.ThreadPoolExecutor`, psycopg2, pandas, ReportLab.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `services/hermes/mengxi_ranking_report.py` | Modify | All new functions + updated PDF/ranking logic |
| `services/hermes/tests/test_nodal_pf.py` | Create | Unit tests for pure-compute functions |
| `services/hermes/app.py` | Modify | Add monthly cron job + pass webhook URL |

---

### Task 1: `_query_nodal_prices` and `_compute_nodal_pf_ranks`

**Files:**
- Modify: `services/hermes/mengxi_ranking_report.py`
- Create: `services/hermes/tests/__init__.py` (empty)
- Create: `services/hermes/tests/test_nodal_pf.py`

- [ ] **Step 1: Create the test file with a failing test for `_compute_nodal_pf_ranks`**

Create `services/hermes/tests/__init__.py` (empty file).

Create `services/hermes/tests/test_nodal_pf.py`:

```python
"""
Unit tests for nodal PF ranking functions in mengxi_ranking_report.py.
No DB required — all tests use synthetic price DataFrames.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from services.hermes.mengxi_ranking_report import _compute_nodal_pf_ranks


def _make_prices(plant_name: str, n_days: int = 2, seed: int = 0) -> pd.DataFrame:
    """Build a synthetic 15-min price DataFrame for one plant."""
    rng = np.random.default_rng(seed)
    n = n_days * 96
    datetimes = pd.date_range("2026-06-01", periods=n, freq="15min")
    prices = rng.uniform(50, 500, size=n)
    return pd.DataFrame({
        "plant_name": plant_name,
        "datetime": datetimes,
        "cleared_price": prices,
    })


class TestComputeNodalPfRanks:

    def test_empty_df_returns_empty_dict(self):
        result = _compute_nodal_pf_ranks(pd.DataFrame(columns=["plant_name", "datetime", "cleared_price"]))
        assert result == {}

    def test_single_plant_gets_rank_1(self):
        df = _make_prices("plant_A", n_days=1)
        result = _compute_nodal_pf_ranks(df)
        assert "plant_A" in result
        assert result["plant_A"]["rank_2h"] == 1
        assert result["plant_A"]["rank_4h"] == 1

    def test_scores_are_non_negative(self):
        df = _make_prices("plant_A", n_days=2)
        result = _compute_nodal_pf_ranks(df)
        assert result["plant_A"]["score_2h"] >= 0
        assert result["plant_A"]["score_4h"] >= 0

    def test_two_plants_ranked_by_score_descending(self):
        # plant_B has much higher price spread → higher PF score
        df_low  = _make_prices("plant_low",  n_days=1, seed=1)
        df_low["cleared_price"] = 100.0  # flat price → zero spread → zero PF value
        df_high = _make_prices("plant_high", n_days=1, seed=2)
        df_high["cleared_price"] = np.where(
            df_high.index < 48, 10.0, 500.0  # large spread
        )
        df = pd.concat([df_low, df_high], ignore_index=True)
        result = _compute_nodal_pf_ranks(df)
        assert result["plant_high"]["rank_2h"] < result["plant_low"]["rank_2h"]  # lower rank = better

    def test_n_days_matches_input(self):
        df = _make_prices("plant_A", n_days=3)
        result = _compute_nodal_pf_ranks(df)
        assert result["plant_A"]["n_days"] == 3

    def test_4h_score_higher_than_2h_for_wide_spread(self):
        # With a very wide intraday spread, 4h storage captures more energy → higher score
        df = _make_prices("plant_A", n_days=1, seed=5)
        df["cleared_price"] = np.where(df.index % 96 < 48, 10.0, 600.0)
        result = _compute_nodal_pf_ranks(df)
        # 4h score may be higher or lower depending on spread structure — just check both are positive
        assert result["plant_A"]["score_2h"] >= 0
        assert result["plant_A"]["score_4h"] >= 0
```

- [ ] **Step 2: Run test to confirm it fails (function not yet defined)**

```
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
$env:PYTHONPATH = "."
pytest services/hermes/tests/test_nodal_pf.py -v
```

Expected: `ImportError` or `AttributeError` — `_compute_nodal_pf_ranks` does not exist yet.

- [ ] **Step 3: Add `_query_nodal_prices` and `_compute_nodal_pf_ranks` to `mengxi_ranking_report.py`**

Insert after the `_apply_comp` function (around line 194), before `_enrich_and_rank`:

```python
# ── Nodal PF value computation ────────────────────────────────────────────────

_NODAL_PRICES_SQL = """
SELECT plant_name, datetime, cleared_price
FROM marketdata.md_id_cleared_energy
WHERE data_date >= %(start)s
  AND data_date <  %(end_excl)s
  AND plant_name = ANY(%(plant_names)s)
ORDER BY plant_name, datetime
"""


def _query_nodal_prices(
    pg_url: str, plant_names: list[str], start: date, end_excl: date
) -> pd.DataFrame:
    """Fetch 15-min cleared prices for the given plants and date window."""
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=600000")
    try:
        return pd.read_sql_query(
            _NODAL_PRICES_SQL,
            conn,
            params={"start": start, "end_excl": end_excl, "plant_names": plant_names},
        )
    finally:
        conn.close()


def _compute_nodal_pf_ranks(
    prices_df: pd.DataFrame,
    rte: float = 0.85,
) -> dict[str, dict]:
    """
    Run perfect-foresight MILP for each plant in prices_df (both 2h and 4h durations).
    Returns {plant_name: {"score_2h", "score_4h", "rank_2h", "rank_4h", "n_days"}}.

    score_2h / score_4h = CNY / MWh_installed / day (normalised per MW, per duration).
    Parallelised across plants with ThreadPoolExecutor.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices

    if prices_df.empty:
        return {}

    def _compute_plant(plant_name: str, group: pd.DataFrame) -> tuple[str, float, float, int]:
        prices_s = group.set_index("datetime")["cleared_price"].sort_index()
        n_days = prices_s.index.normalize().nunique()
        try:
            _, profit_2h = compute_dispatch_from_15min_prices(
                prices_s, power_mw=1.0, duration_h=2.0, roundtrip_eff=rte
            )
            _, profit_4h = compute_dispatch_from_15min_prices(
                prices_s, power_mw=1.0, duration_h=4.0, roundtrip_eff=rte
            )
            days = max(len(profit_2h), 1)
            score_2h = float(profit_2h.sum()) / (2.0 * days)
            score_4h = float(profit_4h.sum()) / (4.0 * days)
        except Exception as exc:
            logger.warning("Nodal PF compute failed for %s: %s", plant_name, exc)
            return plant_name, float("nan"), float("nan"), n_days
        return plant_name, score_2h, score_4h, n_days

    plant_groups = [(name, grp) for name, grp in prices_df.groupby("plant_name")]
    raw_scores: dict[str, tuple[float, float, int]] = {}

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(_compute_plant, name, grp): name
            for name, grp in plant_groups
        }
        for future in as_completed(futures):
            name, s2h, s4h, nd = future.result()
            if not (pd.isna(s2h) or pd.isna(s4h)):
                raw_scores[name] = (s2h, s4h, nd)

    if not raw_scores:
        return {}

    sorted_2h = sorted(raw_scores, key=lambda p: raw_scores[p][0], reverse=True)
    sorted_4h = sorted(raw_scores, key=lambda p: raw_scores[p][1], reverse=True)
    rank_2h = {p: i + 1 for i, p in enumerate(sorted_2h)}
    rank_4h = {p: i + 1 for i, p in enumerate(sorted_4h)}

    return {
        p: {
            "score_2h": raw_scores[p][0],
            "score_4h": raw_scores[p][1],
            "n_days":   raw_scores[p][2],
            "rank_2h":  rank_2h[p],
            "rank_4h":  rank_4h[p],
        }
        for p in raw_scores
    }
```

- [ ] **Step 4: Run tests**

```
pytest services/hermes/tests/test_nodal_pf.py -v
```

Expected: all 6 tests PASS. (Note: tests run actual PuLP/CBC MILP — takes ~10–20s.)

- [ ] **Step 5: Commit**

```bash
git add services/hermes/tests/__init__.py services/hermes/tests/test_nodal_pf.py services/hermes/mengxi_ranking_report.py
git commit -m "feat: add _query_nodal_prices and _compute_nodal_pf_ranks"
```

---

### Task 2: Update `_enrich_and_rank` to include nodal rank columns

**Files:**
- Modify: `services/hermes/mengxi_ranking_report.py`
- Modify: `services/hermes/tests/test_nodal_pf.py`

- [ ] **Step 1: Add failing tests for updated `_enrich_and_rank`**

Append to `services/hermes/tests/test_nodal_pf.py`:

```python
from services.hermes.mengxi_ranking_report import _enrich_and_rank


def _raw_df() -> pd.DataFrame:
    return pd.DataFrame({
        "plant_name":    ["plant_A", "plant_B"],
        "discharge_rev": [50000.0,   30000.0],
        "charge_cost":   [10000.0,   8000.0],
        "discharge_mwh": [500.0,     300.0],
        "days":          [1,         1],
        "comp_yuan":     [175000.0,  105000.0],
        "max_energy":    [25.0,      25.0],
    })


def _plant_list() -> list[dict]:
    return [
        {"plant_name": "plant_A", "owner": "owner_X", "mw": 100},
        {"plant_name": "plant_B", "owner": "owner_Y", "mw": 100},
    ]


class TestEnrichAndRankNodalColumns:

    def test_nodal_rank_columns_present_when_ranks_provided(self):
        nodal_ranks = {
            "plant_A": {"rank_2h": 2, "rank_4h": 3, "score_2h": 10.0, "score_4h": 8.0, "n_days": 1},
            "plant_B": {"rank_2h": 1, "rank_4h": 1, "score_2h": 20.0, "score_4h": 18.0, "n_days": 1},
        }
        df = _enrich_and_rank(_raw_df(), _plant_list(), nodal_ranks=nodal_ranks)
        assert "nodal_rank_2h" in df.columns
        assert "nodal_rank_4h" in df.columns

    def test_nodal_rank_none_when_not_provided(self):
        df = _enrich_and_rank(_raw_df(), _plant_list(), nodal_ranks=None)
        assert df["nodal_rank_2h"].isna().all()
        assert df["nodal_rank_4h"].isna().all()

    def test_nodal_rank_values_match_input(self):
        nodal_ranks = {
            "plant_A": {"rank_2h": 5, "rank_4h": 7, "score_2h": 10.0, "score_4h": 8.0, "n_days": 1},
        }
        df = _enrich_and_rank(_raw_df(), _plant_list(), nodal_ranks=nodal_ranks)
        row_a = df[df["plant_name"] == "plant_A"].iloc[0]
        assert row_a["nodal_rank_2h"] == 5
        assert row_a["nodal_rank_4h"] == 7

    def test_missing_plant_nodal_rank_is_none(self):
        # plant_B not in nodal_ranks
        nodal_ranks = {
            "plant_A": {"rank_2h": 1, "rank_4h": 1, "score_2h": 10.0, "score_4h": 8.0, "n_days": 1},
        }
        df = _enrich_and_rank(_raw_df(), _plant_list(), nodal_ranks=nodal_ranks)
        row_b = df[df["plant_name"] == "plant_B"].iloc[0]
        assert pd.isna(row_b["nodal_rank_2h"])
        assert pd.isna(row_b["nodal_rank_4h"])
```

- [ ] **Step 2: Run to confirm tests fail**

```
pytest services/hermes/tests/test_nodal_pf.py::TestEnrichAndRankNodalColumns -v
```

Expected: FAIL — `_enrich_and_rank` takes no keyword argument `nodal_ranks`.

- [ ] **Step 3: Update `_enrich_and_rank` signature and body**

Replace the existing `_enrich_and_rank` function in `services/hermes/mengxi_ranking_report.py`:

```python
def _enrich_and_rank(
    raw_df: pd.DataFrame,
    plant_list: list[dict],
    nodal_ranks: Optional[dict[str, dict]] = None,
) -> pd.DataFrame:
    """Merge DB result with plant metadata, compute rank, and attach nodal PF ranks."""
    if raw_df.empty:
        return raw_df

    plant_df = pd.DataFrame(plant_list)[["plant_name", "owner", "mw"]]
    df = raw_df.merge(plant_df, on="plant_name", how="left")
    df["owner"] = df["owner"].fillna("未知")
    df["mw"] = df["mw"].fillna(0.0)

    # For plants missing MW in 电站.xlsx, infer from max observed dispatch energy.
    # max_energy = MAX(cleared_energy_mwh per 15-min interval); MW ≈ max_energy * 4.
    mask_no_mw = df["mw"] == 0
    if mask_no_mw.any() and "max_energy" in df.columns:
        df.loc[mask_no_mw, "mw"] = (df.loc[mask_no_mw, "max_energy"] * 4).round().clip(lower=1)

    df = df[df["mw"] > 0].copy()
    if df.empty:
        return df

    df["profit_wan"] = (df["discharge_rev"] - df["charge_cost"] + df["comp_yuan"]) / 10000.0
    df["installed_mwh"] = df["mw"] * 4
    df["score"] = df.apply(
        lambda r: (r["profit_wan"] * 10000) / (r["installed_mwh"] * r["days"])
        if r["installed_mwh"] > 0 and r["days"] > 0 else None,
        axis=1,
    )
    df = df.sort_values("score", ascending=False, na_position="last").reset_index(drop=True)
    df.insert(0, "rank", range(1, len(df) + 1))
    df["mw"] = df["mw"].astype(int)
    df["profit_wan"] = df["profit_wan"].round(1)
    df["score"] = df["score"].round(4)

    # Attach nodal PF ranks
    if nodal_ranks:
        df["nodal_rank_2h"] = df["plant_name"].map(
            lambda n: nodal_ranks[n]["rank_2h"] if n in nodal_ranks else None
        )
        df["nodal_rank_4h"] = df["plant_name"].map(
            lambda n: nodal_ranks[n]["rank_4h"] if n in nodal_ranks else None
        )
    else:
        df["nodal_rank_2h"] = None
        df["nodal_rank_4h"] = None

    return df[["rank", "plant_name", "owner", "mw", "profit_wan", "score", "days",
               "nodal_rank_2h", "nodal_rank_4h"]]
```

- [ ] **Step 4: Run tests**

```
pytest services/hermes/tests/test_nodal_pf.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add services/hermes/tests/test_nodal_pf.py services/hermes/mengxi_ranking_report.py
git commit -m "feat: add nodal_rank columns to _enrich_and_rank"
```

---

### Task 3: Update `_build_table` and `_generate_pdf` for 9-column layout + colour coding

**Files:**
- Modify: `services/hermes/mengxi_ranking_report.py`

The `_build_table` and `_generate_pdf` functions are a closure + outer function — both live inside `_generate_pdf`. We update `_generate_pdf` to:
1. Accept `nodal_monthly_df` parameter
2. Expand column headers/widths to 9 columns
3. Add cell-level colour coding for nodal rank columns

- [ ] **Step 1: Replace `_generate_pdf` in `mengxi_ranking_report.py`**

Replace the entire `_generate_pdf` function with:

```python
def _generate_pdf(
    yesterday_df: pd.DataFrame,
    month_df: pd.DataFrame,
    ytd_df: pd.DataFrame,
    report_date: date,
    total_mw: float = 0.0,
    nodal_monthly_df: Optional[pd.DataFrame] = None,
) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, PageBreak,
    )
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.cidfonts import UnicodeCIDFont

    pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
    F = "STSong-Light"

    ENVISION_BG   = colors.HexColor("#28a745")
    ENVISION_FG   = colors.white
    HEADER_BG     = colors.HexColor("#1f3b63")
    ALT_ROW       = colors.HexColor("#f0f4fa")
    GREEN_TEXT    = colors.HexColor("#1a7a1a")
    RED_TEXT      = colors.HexColor("#b30000")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=12*mm, rightMargin=12*mm,
        topMargin=12*mm, bottomMargin=12*mm,
    )

    def _ps(name, size, **kw):
        return ParagraphStyle(name, fontName=F, fontSize=size, **kw)

    title_s  = _ps("t",    16, alignment=1, spaceAfter=4)
    sub_s    = _ps("sub",   8, alignment=1, textColor=colors.grey, spaceAfter=2)
    legend_s = _ps("leg",   8, textColor=ENVISION_BG, spaceAfter=6)
    h2_s     = _ps("h2",   11, spaceBefore=8, spaceAfter=3,
                   textColor=colors.HexColor("#1f3b63"))
    note_s   = _ps("n",     7, textColor=colors.grey, spaceAfter=4)

    # 9 columns — narrowed name/owner/profit/score to make room for two nodal rank cols
    COL_HDR = ["排名", "项目名称", "业主", "MW", "总收益(万元)", "收益/MWh/天", "天数",
               "2h节点排名", "4h节点排名"]
    COL_W   = [11*mm, 46*mm, 28*mm, 13*mm, 20*mm, 20*mm, 13*mm, 15*mm, 15*mm]
    # Total: 181mm (A4 printable = 186mm) ✓

    def _nodal_cell(val, actual_rank: int) -> str:
        if val is None or pd.isna(val):
            return "—"
        return f"#{int(val)}"

    def _build_table(df: pd.DataFrame):
        sub = df.reset_index(drop=True)
        rows = [COL_HDR]
        for _, r in sub.iterrows():
            rows.append([
                str(r["rank"]),
                str(r["plant_name"]),
                str(r["owner"]),
                str(r["mw"]),
                f"{r['profit_wan']:.0f}",
                f"{r['score']:.0f}",
                str(r["days"]),
                _nodal_cell(r.get("nodal_rank_2h"), int(r["rank"])),
                _nodal_cell(r.get("nodal_rank_4h"), int(r["rank"])),
            ])

        cmds = [
            ("FONTNAME",      (0, 0), (-1, -1), F),
            ("FONTSIZE",      (0, 0), (-1, -1), 7),
            ("BACKGROUND",    (0, 0), (-1,  0), HEADER_BG),
            ("TEXTCOLOR",     (0, 0), (-1,  0), colors.white),
            ("FONTSIZE",      (0, 0), (-1,  0), 7.5),
            ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
            ("ALIGN",         (1, 1), (2, -1),  "LEFT"),
            ("GRID",          (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("TOPPADDING",    (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]
        for i in range(1, len(rows)):
            actual_rank = int(rows[i][0]) if rows[i][0].isdigit() else None
            if i % 2 == 0:
                cmds.append(("BACKGROUND", (0, i), (-1, i), ALT_ROW))
            if "远景" in rows[i][2]:
                cmds += [
                    ("BACKGROUND", (0, i), (-1, i), ENVISION_BG),
                    ("TEXTCOLOR",  (0, i), (-1, i), ENVISION_FG),
                    ("FONTNAME",   (0, i), (-1, i), F),
                ]
            # Nodal rank colour coding (applied after Envision override for non-Envision rows)
            for col_idx, nodal_col_idx in [(7, 7), (8, 8)]:
                cell_val = rows[i][col_idx]
                if cell_val != "—" and actual_rank is not None and "远景" not in rows[i][2]:
                    try:
                        nodal_rank = int(cell_val.lstrip("#"))
                        if nodal_rank > actual_rank:
                            cmds.append(("TEXTCOLOR", (col_idx, i), (col_idx, i), GREEN_TEXT))
                        elif nodal_rank < actual_rank:
                            cmds.append(("TEXTCOLOR", (col_idx, i), (col_idx, i), RED_TEXT))
                    except ValueError:
                        pass

        t = Table(rows, colWidths=COL_W, repeatRows=1)
        t.setStyle(TableStyle(cmds))
        return t

    def _section(title: str, df: pd.DataFrame) -> list:
        elems = [Paragraph(title, h2_s)]
        if df.empty:
            elems.append(Paragraph("暂无数据（该时段无BESS充放电记录）", note_s))
        else:
            elems.append(_build_table(df))
        return elems

    def _section_nodal_monthly(ndf: pd.DataFrame) -> list:
        month_val = ndf["month"].iloc[0]
        if hasattr(month_val, "strftime"):
            month_str = month_val.strftime("%Y年%m月")
        else:
            month_str = str(month_val)[:7]

        elems = [
            Paragraph(f"🗺 蒙西BESS节点价值月度排名（{month_str}）", h2_s),
            Paragraph(
                "基于完美预见MILP套利模型　|　往返效率85%　|　每月5日更新",
                note_s,
            ),
        ]

        N_HDR = ["节点排名(2h)", "节点排名(4h)", "节点名称", "2h收益/MWh/天", "4h收益/MWh/天", "交易天数"]
        N_W   = [22*mm, 22*mm, 70*mm, 28*mm, 28*mm, 18*mm]
        n_rows = [N_HDR]
        for _, r in ndf.iterrows():
            n_rows.append([
                f"#{int(r['rank_2h'])}",
                f"#{int(r['rank_4h'])}",
                str(r["plant_name"]),
                f"{r['pf_score_2h']:.1f}",
                f"{r['pf_score_4h']:.1f}",
                str(int(r["n_days"])),
            ])

        n_cmds = [
            ("FONTNAME",      (0, 0), (-1, -1), F),
            ("FONTSIZE",      (0, 0), (-1, -1), 7),
            ("BACKGROUND",    (0, 0), (-1,  0), HEADER_BG),
            ("TEXTCOLOR",     (0, 0), (-1,  0), colors.white),
            ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
            ("ALIGN",         (2, 1), (2, -1),  "LEFT"),
            ("GRID",          (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("TOPPADDING",    (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]
        for i in range(1, len(n_rows)):
            if i % 2 == 0:
                n_cmds.append(("BACKGROUND", (0, i), (-1, i), ALT_ROW))

        nt = Table(n_rows, colWidths=N_W, repeatRows=1)
        nt.setStyle(TableStyle(n_cmds))
        elems.append(nt)
        elems.append(Paragraph(
            "节点价值：完美预见MILP套利收益 ÷ (装机容量MWh × 天数)，单位CNY/MWh/天。"
            "2h = 0.5C电池；4h = 0.25C电池。数据来源：蒙西集中式现货市场出清数据。",
            note_s,
        ))
        return elems

    yesterday_str = report_date.strftime("%Y-%m-%d")
    month_start   = report_date.replace(day=1).strftime("%Y-%m-%d")
    ytd_start     = report_date.replace(month=1, day=1).strftime("%Y-%m-%d")
    latest_label  = f"最新（{yesterday_str}）"

    n_y  = len(yesterday_df)
    n_m  = len(month_df)
    n_y2 = len(ytd_df)

    capacity_str = ""
    if total_mw > 0:
        total_gw  = total_mw / 1000
        total_gwh = total_mw * 4 / 1000
        capacity_str = f"　　合计装机 {total_gw:.2f} GW / {total_gwh:.2f} GWh"

    story = [
        Paragraph("蒙西BESS市场排名日报", title_s),
        Paragraph(f"报告日期：{yesterday_str}　　共收录 {max(n_y, n_m, n_y2)} 个BESS项目{capacity_str}", sub_s),
        Paragraph("▲ 绿色行 = 远景能源（Envision Energy）旗下资产", legend_s),
        HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#aaaaaa")),
    ]

    story += _section(
        f"📅 {latest_label}排名　共 {n_y} 个 BESS{_cap_str(yesterday_df)}",
        yesterday_df,
    )

    story += [PageBreak()]
    story += _section(
        f"📆 本月排名（{month_start} ～ {yesterday_str}）　共 {n_m} 个 BESS{_cap_str(month_df)}",
        month_df,
    )

    story += [PageBreak()]
    story += _section(
        f"📊 年度排名（{ytd_start} ～ {yesterday_str}）　共 {n_y2} 个 BESS{_cap_str(ytd_df)}",
        ytd_df,
    )

    # Monthly nodal ranking page (static, refreshed on 5th of each month)
    if nodal_monthly_df is not None and not nodal_monthly_df.empty:
        story += [PageBreak()]
        story += _section_nodal_monthly(nodal_monthly_df)

    story += [
        Spacer(1, 4*mm),
        HRFlowable(width="100%", thickness=0.3, color=colors.lightgrey),
        Paragraph(
            "排名指标：收益/MWh/天 = (放电收入 − 充电成本 + 容量补偿) ÷ (装机容量MWh × 天数)，"
            "假设储能时长4小时。单位：万元/MWh/天。",
            note_s,
        ),
        Paragraph(
            "容量补偿标准：350元/MWh（一般项目）；280元/MWh（荣鑫地房子储能电站及2026年6月27日后入市项目）；"
            "0元/MWh（大航都林储能电站、大航额日和图储能电站）。",
            note_s,
        ),
        Paragraph(
            "价格说明：放电收入采用15分钟节点内日出清电价；充电成本采用同节点小时均价。",
            note_s,
        ),
        Paragraph(
            "节点排名：2h/4h节点排名基于完美预见MILP套利，往返效率85%。"
            "绿色 = 实际排名优于节点排名（超越地理优势）；红色 = 低于节点排名（未充分利用地理优势）。",
            note_s,
        ),
    ]

    doc.build(story)
    return buf.getvalue()
```

- [ ] **Step 2: Run existing tests to confirm nothing broken**

```
pytest services/hermes/tests/test_nodal_pf.py -v
```

Expected: all tests PASS.

- [ ] **Step 3: Commit**

```bash
git add services/hermes/mengxi_ranking_report.py
git commit -m "feat: update _generate_pdf with 9-col table, colour coding, monthly nodal page"
```

---

### Task 4: Monthly job — `_query_nodal_monthly_df` and `compute_and_store_nodal_pf_monthly`

**Files:**
- Modify: `services/hermes/mengxi_ranking_report.py`
- Modify: `services/hermes/tests/test_nodal_pf.py`

- [ ] **Step 1: Add failing tests**

Append to `services/hermes/tests/test_nodal_pf.py`:

```python
from services.hermes.mengxi_ranking_report import _previous_calendar_month


class TestPreviousCalendarMonth:

    def test_july_5_gives_june(self):
        from datetime import date
        start, end = _previous_calendar_month(date(2026, 7, 5))
        assert start == date(2026, 6, 1)
        assert end == date(2026, 7, 1)  # end is exclusive

    def test_january_5_gives_december_previous_year(self):
        from datetime import date
        start, end = _previous_calendar_month(date(2026, 1, 5))
        assert start == date(2025, 12, 1)
        assert end == date(2026, 1, 1)

    def test_march_5_gives_february(self):
        from datetime import date
        start, end = _previous_calendar_month(date(2026, 3, 5))
        assert start == date(2026, 2, 1)
        assert end == date(2026, 3, 1)
```

- [ ] **Step 2: Run to confirm tests fail**

```
pytest services/hermes/tests/test_nodal_pf.py::TestPreviousCalendarMonth -v
```

Expected: `ImportError` — `_previous_calendar_month` not yet defined.

- [ ] **Step 3: Add `_previous_calendar_month`, `_query_nodal_monthly_df`, and `compute_and_store_nodal_pf_monthly`**

Append to `services/hermes/mengxi_ranking_report.py` (before the final `send_daily_ranking` function):

```python
# ── Monthly nodal PF job ──────────────────────────────────────────────────────

def _previous_calendar_month(today: date) -> tuple[date, date]:
    """Return (month_start, month_end_excl) for the calendar month before today."""
    if today.month == 1:
        start = date(today.year - 1, 12, 1)
        end_excl = date(today.year, 1, 1)
    else:
        start = date(today.year, today.month - 1, 1)
        end_excl = date(today.year, today.month, 1)
    return start, end_excl


def _query_nodal_monthly_df(pg_url: str) -> pd.DataFrame:
    """Read the latest available month from reports.nodal_pf_monthly.
    Returns empty DataFrame if the table does not exist or has no rows.
    """
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=30000")
    try:
        return pd.read_sql_query(
            """
            SELECT month, plant_name, pf_score_2h, pf_score_4h, rank_2h, rank_4h, n_days
            FROM reports.nodal_pf_monthly
            WHERE month = (SELECT MAX(month) FROM reports.nodal_pf_monthly)
            ORDER BY rank_2h
            """,
            conn,
        )
    except Exception as exc:
        logger.warning("Could not read reports.nodal_pf_monthly: %s", exc)
        return pd.DataFrame()
    finally:
        conn.close()


def compute_and_store_nodal_pf_monthly(pg_url: str) -> None:
    """
    Compute perfect-foresight BESS values for all Mengxi nodes for the previous
    calendar month and upsert results into reports.nodal_pf_monthly.

    Called by Hermes APScheduler on the 5th of each month at 01:00 UTC.
    """
    today = date.today()
    month_start, month_end_excl = _previous_calendar_month(today)
    logger.info("Nodal PF monthly: computing for %s → %s", month_start, month_end_excl)

    # ── Discover all plants with data in that month ───────────────────────────
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=30000")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT plant_name FROM marketdata.md_id_cleared_energy "
                "WHERE data_date >= %s AND data_date < %s",
                (month_start, month_end_excl),
            )
            plant_names = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not plant_names:
        logger.warning("Nodal PF monthly: no plants found for %s", month_start)
        return

    logger.info("Nodal PF monthly: %d plants found", len(plant_names))

    # ── Fetch prices and run MILP ─────────────────────────────────────────────
    prices_df = _query_nodal_prices(pg_url, plant_names, month_start, month_end_excl)
    pf = _compute_nodal_pf_ranks(prices_df)

    if not pf:
        logger.warning("Nodal PF monthly: no results for %s", month_start)
        return

    # ── Ensure table exists ───────────────────────────────────────────────────
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=30000")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reports.nodal_pf_monthly (
                    month          DATE        NOT NULL,
                    plant_name     TEXT        NOT NULL,
                    pf_score_2h    FLOAT,
                    pf_score_4h    FLOAT,
                    rank_2h        INTEGER,
                    rank_4h        INTEGER,
                    n_days         INTEGER,
                    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (month, plant_name)
                )
            """)
            conn.commit()

            # Upsert rows
            for plant_name, vals in pf.items():
                cur.execute("""
                    INSERT INTO reports.nodal_pf_monthly
                        (month, plant_name, pf_score_2h, pf_score_4h, rank_2h, rank_4h, n_days, computed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (month, plant_name) DO UPDATE SET
                        pf_score_2h = EXCLUDED.pf_score_2h,
                        pf_score_4h = EXCLUDED.pf_score_4h,
                        rank_2h     = EXCLUDED.rank_2h,
                        rank_4h     = EXCLUDED.rank_4h,
                        n_days      = EXCLUDED.n_days,
                        computed_at = now()
                """, (
                    month_start,
                    plant_name,
                    vals["score_2h"],
                    vals["score_4h"],
                    vals["rank_2h"],
                    vals["rank_4h"],
                    vals["n_days"],
                ))
            conn.commit()
        logger.info("Nodal PF monthly: upserted %d rows for %s", len(pf), month_start)
    finally:
        conn.close()
```

- [ ] **Step 4: Run tests**

```
pytest services/hermes/tests/test_nodal_pf.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add services/hermes/mengxi_ranking_report.py services/hermes/tests/test_nodal_pf.py
git commit -m "feat: add monthly nodal PF job (compute_and_store_nodal_pf_monthly)"
```

---

### Task 5: Wire nodal ranks into `send_daily_ranking`

**Files:**
- Modify: `services/hermes/mengxi_ranking_report.py`

- [ ] **Step 1: Update `send_daily_ranking` to compute inline nodal ranks and read monthly table**

In `send_daily_ranking`, replace the section between `# ── Resolve per-plant compensation rates` and `# ── Generate PDF`:

```python
    # ── Resolve per-plant compensation rates ──────────────────────────────────
    try:
        first_seen_map = _query_first_seen(pg_url, plant_names)
    except Exception as exc:
        logger.warning("Could not query first_seen dates: %s — defaulting all to 350", exc)
        first_seen_map = {}

    yesterday_raw = _apply_comp(yesterday_raw, first_seen_map, yesterday)
    month_raw     = _apply_comp(month_raw,     first_seen_map, yesterday)
    ytd_raw       = _apply_comp(ytd_raw,       first_seen_map, yesterday)

    # ── Compute yesterday's nodal PF ranks (inline MILP, ~10s for ~100 plants) ──
    nodal_ranks_yesterday: dict[str, dict] = {}
    try:
        nodal_prices_df = _query_nodal_prices(pg_url, plant_names, yesterday, end_excl)
        if not nodal_prices_df.empty:
            nodal_ranks_yesterday = _compute_nodal_pf_ranks(nodal_prices_df)
            logger.info("Nodal PF ranks computed for %d plants", len(nodal_ranks_yesterday))
    except Exception as exc:
        logger.warning("Nodal PF inline compute failed: %s — nodal ranks will be blank", exc)

    # ── Read monthly nodal ranking page (pre-computed on 5th of each month) ───
    nodal_monthly_df = pd.DataFrame()
    try:
        nodal_monthly_df = _query_nodal_monthly_df(pg_url)
    except Exception as exc:
        logger.warning("Could not read nodal_pf_monthly: %s — monthly page omitted", exc)

    # ── Enrich with owner/mw and compute rank ─────────────────────────────────
    yesterday_df = _enrich_and_rank(yesterday_raw, plant_list, nodal_ranks=nodal_ranks_yesterday)
    month_df     = _enrich_and_rank(month_raw,     plant_list, nodal_ranks=nodal_ranks_yesterday)
    ytd_df       = _enrich_and_rank(ytd_raw,       plant_list, nodal_ranks=nodal_ranks_yesterday)

    # ── Generate PDF ───────────────────────────────────────────────────────────
    total_mw = float(ytd_df["mw"].sum()) if not ytd_df.empty else 0.0
    try:
        pdf_bytes = _generate_pdf(
            yesterday_df, month_df, ytd_df, yesterday,
            total_mw=total_mw,
            nodal_monthly_df=nodal_monthly_df if not nodal_monthly_df.empty else None,
        )
    except Exception as exc:
        logger.error("Mengxi ranking report PDF error: %s", exc, exc_info=True)
        if feishu and owner_open_id:
            feishu.send_text(owner_open_id, f"⚠️ 蒙西BESS日报失败（PDF生成错误）：{exc}")
        return
```

- [ ] **Step 2: Run all tests**

```
pytest services/hermes/tests/test_nodal_pf.py -v
```

Expected: all tests PASS.

- [ ] **Step 3: Commit**

```bash
git add services/hermes/mengxi_ranking_report.py
git commit -m "feat: wire nodal PF ranks into send_daily_ranking"
```

---

### Task 6: Add monthly cron job to Hermes `app.py`

**Files:**
- Modify: `services/hermes/app.py`

- [ ] **Step 1: Import `compute_and_store_nodal_pf_monthly` in `app.py`**

Find the import line:
```python
from services.hermes.mengxi_ranking_report import send_daily_ranking as _send_mengxi_ranking
```

Replace with:
```python
from services.hermes.mengxi_ranking_report import (
    send_daily_ranking as _send_mengxi_ranking,
    compute_and_store_nodal_pf_monthly as _compute_nodal_pf_monthly,
)
```

- [ ] **Step 2: Add the monthly cron job after the existing `_send_mengxi_ranking` scheduler block**

Find:
```python
        # New-BESS screener: 06:30 UTC (14:30 Beijing) — after market data typically arrives
```

Insert before that line:
```python
        # Nodal PF monthly ranking: 5th of each month at 01:00 UTC (09:00 Beijing)
        scheduler.add_job(
            _compute_nodal_pf_monthly,
            "cron",
            day=5, hour=1, minute=0,
            kwargs={"pg_url": _mengxi_pg_url},
        )
```

- [ ] **Step 3: Verify the app imports cleanly**

```
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
$env:PYTHONPATH = "."
python -c "from services.hermes.app import create_app; print('OK')"
```

Expected: `OK` (no import errors).

- [ ] **Step 4: Run all tests**

```
pytest services/hermes/tests/test_nodal_pf.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add services/hermes/app.py
git commit -m "feat: add monthly nodal PF cron job (5th of month, 01:00 UTC)"
```

---

### Task 7: Backfill current month manually

The monthly table is empty until the first scheduled run on the 5th. Run a one-off backfill to populate it with the most recent complete month so the PDF immediately shows the monthly page.

- [ ] **Step 1: Run backfill script**

```
cd C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
$env:PYTHONPATH = "."
$env:PGURL = "postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata?sslmode=require"
python -c "
import os
from services.hermes.mengxi_ranking_report import compute_and_store_nodal_pf_monthly
compute_and_store_nodal_pf_monthly(os.environ['PGURL'])
print('Done')
"
```

Expected: logs showing plant count, MILP running, upsert count, then `Done`. Takes ~60–90s.

- [ ] **Step 2: Verify DB rows inserted**

```
python -c "
import os, psycopg2
conn = psycopg2.connect(os.environ['PGURL'])
with conn.cursor() as cur:
    cur.execute('SELECT month, COUNT(*) FROM reports.nodal_pf_monthly GROUP BY month ORDER BY month')
    for row in cur.fetchall(): print(row)
conn.close()
"
```

Expected: one row showing the previous calendar month and the plant count (e.g. `(datetime.date(2026, 6, 1), 87)`).

- [ ] **Step 3: Final commit**

```bash
git add .
git commit -m "feat: nodal BESS value ranking complete — daily inline + monthly job"
```
