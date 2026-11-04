# apps/spot-market/fundamentals_tab.py
"""Market Fundamentals tab — single-province monthly-evolution sections (S1–S4).

Data: marketdata.province_fundamentals_monthly (long table, confirmed-only)
via services.province_fundamentals.store.read_series. Layout per
docs/superpowers/specs/2026-11-02-market-fundamentals-tab-design.md:

  S1 分技术装机   — annual cards (prev year) + monthly stacked area (cur year)
  S2 分电源发电量 — same structure
  S3 分类型利用小时 — prev-year + cur-year monthly lines; 推算值 (derived) are
                    dashed traces with a （推算） suffix, NEVER merged into the
                    same line as source-calibre hours (累计平均 vs 单月口径)
  S4 用电需求     — monthly bar + YoY line (dual axis); annual sector split bar

Benchmark strip: province vs 全国 annual growth; grey 数据缺失 when absent
(national rows only exist once a parser/writer for them lands).

口径 rule: per chart prefer <metric>_full (全口径) when present, else the 省调
row; a caption states which 口径 is shown. Exception — annual utilisation
hours prefer hours_<stem> (交接表利用小时数 sheet, carries the yoy rows) over
hours_<stem>_full. yoy rows are labelled by their own unit: '小时' = hour
delta (jiaojie), '%' = percent — never assumed.

All UI strings arrive via `t` (the app's bilingual _t); render() is called
from app.py's `with tab_fundamentals:` block, above the legacy
ranking/tightness sections. Module lives beside app.py (interconnector_tab
precedent) so tests can import it without executing the 8000-line app.
"""
from __future__ import annotations

import os
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

from services.market_fundamentals.loader import (
    FUEL_COLORS as _FUEL_COLORS,
    PROVINCE_EN as _PROVINCE_EN,
)

NATIONAL = "全国"

STEMS = ["thermal", "hydro", "nuclear", "wind", "solar", "storage"]
STEM_EN = {"thermal": "Thermal", "hydro": "Hydro", "nuclear": "Nuclear",
           "wind": "Wind", "solar": "Solar", "storage": "Storage"}
STEM_COLORS = {s: _FUEL_COLORS.get(e, "#aaaaaa") for s, e in STEM_EN.items()}

CAP_BASE = {s: f"cap_{s}" for s in STEMS}
GEN_BASE = {s: f"gen_{s}" for s in STEMS}
HOURS_BASE = {s: f"hours_{s}" for s in STEMS}

SECTOR_METRICS = [("demand_p1", "fund_demand_p1"), ("demand_p2", "fund_demand_p2"),
                  ("demand_p3", "fund_demand_p3"), ("demand_residential", "fund_demand_res")]

_PROV_LABEL_EN = {**_PROVINCE_EN, NATIONAL: "National"}
_GREY = "#9E9E9E"
_ACCENT = "#4C9BE8"


# ── loaders (cached; DB failure degrades to empty → grey markers, no crash) ──

def _pg_url() -> str:
    return (os.environ.get("PGURL") or os.environ.get("DATABASE_URL")
            or os.environ.get("DB_URL")
            or "postgresql://postgres:root@127.0.0.1:5433/marketdata")


@st.cache_data(ttl=300, show_spinner=False)
def load_provinces(pg_url: str) -> list[str]:
    """Provinces with any confirmed row in province_fundamentals_monthly."""
    try:
        with psycopg2.connect(pg_url) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT province FROM marketdata.province_fundamentals_monthly"
                " WHERE status = 'confirmed' ORDER BY province"
            )
            return [r[0] for r in cur.fetchall()]
    except Exception:
        return []


@st.cache_data(ttl=300, show_spinner=False)
def load_series(province: str, section: str, pg_url: str, months: int = 24) -> list[dict]:
    """Cached wrapper over store.read_series (confirmed-only, DISTINCT-ON latest)."""
    from services.province_fundamentals import store
    try:
        return store.read_series(province, section, pg_url=pg_url, months=months)
    except Exception:
        return []


@st.cache_data(ttl=300, show_spinner=False)
def load_section_all(section: str, pg_url: str, year: int | None = None,
                     month: date | None = None) -> list[dict]:
    """Cached wrapper over store.read_section_all_provinces (one period, all provinces)."""
    from services.province_fundamentals import store
    try:
        return store.read_section_all_provinces(section, pg_url=pg_url,
                                                year=year, month=month)
    except Exception:
        return []


# ── pure data-shaping helpers ─────────────────────────────────────────────────

def pick_scope(present: set[str], base_map: dict[str, str],
               prefer_full: bool = True) -> tuple[dict[str, str], str]:
    """Per stem choose <base>_full (全口径) or <base> (省调/源口径).

    Returns (chosen {stem: metric}, scope) with scope ∈ full|base|mixed|none.
    prefer_full=False (annual utilisation hours) prefers the plain metric.
    """
    chosen: dict[str, str] = {}
    kinds: set[str] = set()
    for stem, base in base_map.items():
        full, plain = f"{base}_full", base
        for cand in ((full, plain) if prefer_full else (plain, full)):
            if cand in present:
                chosen[stem] = cand
                kinds.add("full" if cand == full else "base")
                break
    scope = "none" if not kinds else ("mixed" if len(kinds) > 1 else next(iter(kinds)))
    return chosen, scope


def annual_values(rows: list[dict], year: int) -> dict[str, dict]:
    """period='year' rows for `year`, keyed by metric."""
    return {r["metric"]: r for r in rows
            if r["period"] == "year" and r["month"].year == year}


def monthly_series(rows: list[dict], year: int, metric: str) -> dict[int, float | None]:
    """period='month' values for one metric in `year`, keyed by month number.
    None values survive (reported-but-empty cell ≠ absent row)."""
    return {r["month"].month: r["value"] for r in rows
            if r["period"] == "month" and r["month"].year == year
            and r["metric"] == metric}


def hours_map(rows: list[dict], metric: str) -> dict[tuple[int, int], float | None]:
    """period='month' values keyed by (year, month) — for the two-year S3 chart."""
    return {(r["month"].year, r["month"].month): r["value"] for r in rows
            if r["period"] == "month" and r["metric"] == metric}


# ── S5 省际对比 helpers ──────────────────────────────────────────────────────

def common_month(rows: list[dict]) -> date | None:
    """统一公共月: 各省最新月的最小值 (各省有月度数据的集合内可比口径)."""
    per_prov: dict[str, date] = {}
    for r in rows:
        if r["period"] != "month":
            continue
        cur = per_prov.get(r["province"])
        if cur is None or r["month"] > cur:
            per_prov[r["province"]] = r["month"]
    return min(per_prov.values()) if per_prov else None


def pivot_rows(rows: list[dict], province_order: list[str] | None = None):
    """pivot comparison rows → (provinces, metrics, {prov: {metric: value}})."""
    metrics = sorted({r["metric"] for r in rows})
    grid: dict[str, dict[str, float | None]] = {}
    for r in rows:
        grid.setdefault(r["province"], {})[r["metric"]] = r["value"]
    provs = province_order or sorted(grid)
    return provs, metrics, grid


def annual_growth(rows: list[dict], metrics: list[str], year: int) -> float | None:
    """YoY % for the first available of `metrics` at annual `year`.

    A <metric>_yoy row counts only when its unit is '%' (jiaojie hours yoy is
    an HOUR delta — unit checked, never misread). Otherwise computed from this
    year's and last year's annual rows. None when unavailable."""
    ann, prev = annual_values(rows, year), annual_values(rows, year - 1)
    for m in metrics:
        yoy = ann.get(f"{m}_yoy")
        if yoy and yoy["value"] is not None and yoy["unit"] == "%":
            return yoy["value"]
        cur, old = ann.get(m), prev.get(m)
        if cur and old and cur["value"] is not None and old["value"]:
            return (cur["value"] / old["value"] - 1.0) * 100.0
    return None


# ── small render helpers ──────────────────────────────────────────────────────

def _scope_caption(t, scope: str, base_scope_key: str) -> None:
    key = {"full": "fund_scope_full", "base": base_scope_key,
           "mixed": "fund_scope_mixed"}.get(scope)
    if key:
        st.caption(t("fund_scope_caption", scope=t(key)))


def _missing_markers(fig: go.Figure, x_labels: list[str], t) -> None:
    if not x_labels:
        return
    fig.add_trace(go.Scatter(
        x=x_labels, y=[0] * len(x_labels), mode="markers",
        marker=dict(symbol="x", size=11, color=_GREY),
        name=t("fund_missing"),
        hovertemplate="%{x} — " + t("fund_missing") + "<extra></extra>"))


def _missing_caption(t, labels: list[str]) -> None:
    if labels:
        st.caption(t("fund_missing_months", months=", ".join(labels)))


def _annual_cards(t, rows: list[dict], year: int, base_map: dict[str, str], *,
                  unit: str, decimals: int, prefer_full: bool,
                  base_scope_key: str, with_yoy: bool = False) -> None:
    """One st.metric card per stem; '—' + grey 数据缺失 caption when absent."""
    ann = annual_values(rows, year)
    chosen, scope = pick_scope(set(ann), base_map, prefer_full=prefer_full)
    st.markdown(f"**{t('fund_annual_cards', year=year)}**")
    cols = st.columns(len(base_map))
    for col, stem in zip(cols, base_map):
        metric = chosen.get(stem)
        row = ann.get(metric) if metric else None
        if row and row["value"] is not None:
            delta = None
            if with_yoy:
                yoy = ann.get(f"{metric}_yoy") or ann.get(f"{base_map[stem]}_yoy")
                if yoy and yoy["value"] is not None:
                    delta = (f"{yoy['value']:+.1f}%" if yoy["unit"] == "%"
                             else f"{yoy['value']:+,.0f} {yoy['unit']}")
            col.metric(t(f"fund_tech_{stem}"), f"{row['value']:,.{decimals}f}",
                       delta=delta)
        else:
            col.metric(t(f"fund_tech_{stem}"), "—")
            col.caption(t("fund_missing"))
    _scope_caption(t, scope, base_scope_key)
    if with_yoy:
        yoy_units = {(ann.get(f"{m}_yoy") or {}).get("unit") for m in chosen.values()}
        if "小时" in yoy_units:
            st.caption(t("fund_yoy_hours_note"))


def _benchmark(t, prov: str, prov_label: str, section: str, metrics: list[str],
               year: int, label: str, pg_url: str) -> None:
    """Slim province-vs-national annual growth strip; grey when data absent."""
    if prov == NATIONAL:
        return
    g_prov = annual_growth(load_series(prov, section, pg_url), metrics, year)
    g_nat = annual_growth(load_series(NATIONAL, section, pg_url), metrics, year)
    title = t("fund_benchmark_title", label=label)
    if g_prov is None and g_nat is None:
        st.caption(f"{title}: {t('fund_missing')}")
        return
    vals = [g_prov, g_nat]
    fig = go.Figure(go.Bar(
        x=[prov_label, t("fund_national")],
        y=[g if g is not None else 0 for g in vals],
        marker_color=[c if g is not None else "#cccccc"
                      for c, g in zip((_ACCENT, _GREY), vals)],
        text=[f"{g:+.1f}%" if g is not None else t("fund_missing") for g in vals],
        textposition="outside",
        hovertemplate="%{x}: %{text}<extra></extra>"))
    fig.update_layout(title=dict(text=title, font=dict(size=13)),
                      height=170, margin=dict(t=34, b=10), yaxis_title="%",
                      showlegend=False)
    st.plotly_chart(fig, use_container_width=True)


# ── sections ──────────────────────────────────────────────────────────────────

def _stacked_area_section(t, rows: list[dict], base_map: dict[str, str], *,
                          year: int, today: date, unit: str,
                          base_scope_key: str) -> None:
    """Monthly stacked-area chart for S1/S2 (current calendar year)."""
    st.markdown(f"**{t('fund_monthly_chart', year=year)}**")
    chosen, scope = pick_scope(
        {r["metric"] for r in rows if r["period"] == "month"},
        base_map, prefer_full=True)
    if not chosen:
        st.info(t("fund_missing"))
        return
    upto = today.month if year == today.year else 12
    months = list(range(1, 13))
    labels = [f"{year}-{m:02d}" for m in months]
    series = {stem: monthly_series(rows, year, metric)
              for stem, metric in chosen.items()}
    present = {m for s in series.values() for m, v in s.items() if v is not None}
    missing = [labels[m - 1] for m in range(1, upto + 1) if m not in present]
    fig = go.Figure()
    for stem, s in series.items():
        fig.add_trace(go.Scatter(
            x=labels, y=[s.get(m) for m in months],
            name=t(f"fund_tech_{stem}"), mode="lines", stackgroup="one",
            line=dict(color=STEM_COLORS[stem], width=0.5),
            hovertemplate="%{x}<br>%{y:,.1f} " + unit + "<extra></extra>"))
    _missing_markers(fig, missing, t)
    fig.update_layout(height=360, margin=dict(t=20, b=40),
                      yaxis_title=unit, legend_title="")
    st.plotly_chart(fig, use_container_width=True)
    _missing_caption(t, missing)
    _scope_caption(t, scope, base_scope_key)


def _capacity_section(t, prov: str, prov_label: str, annual_year: int,
                      cur_year: int, today: date, pg_url: str) -> None:
    st.divider()
    st.subheader(t("fund_s1_title"))
    rows = load_series(prov, "capacity", pg_url)
    _annual_cards(t, rows, annual_year, CAP_BASE, unit=t("fund_capacity_unit"),
                  decimals=0, prefer_full=True, base_scope_key="fund_scope_prov")
    _stacked_area_section(t, rows, CAP_BASE, year=cur_year, today=today,
                          unit=t("fund_capacity_unit"),
                          base_scope_key="fund_scope_prov")
    _benchmark(t, prov, prov_label, "capacity", ["cap_total_full", "cap_total"],
               annual_year, t("fund_bench_cap", year=annual_year), pg_url)


def _generation_section(t, prov: str, prov_label: str, annual_year: int,
                        cur_year: int, today: date, pg_url: str) -> None:
    st.divider()
    st.subheader(t("fund_s2_title"))
    rows = load_series(prov, "generation", pg_url)
    _annual_cards(t, rows, annual_year, GEN_BASE, unit=t("fund_generation_unit"),
                  decimals=1, prefer_full=True, base_scope_key="fund_scope_prov")
    _stacked_area_section(t, rows, GEN_BASE, year=cur_year, today=today,
                          unit=t("fund_generation_unit"),
                          base_scope_key="fund_scope_prov")
    _benchmark(t, prov, prov_label, "generation", ["gen_total_full", "gen_total"],
               annual_year, t("fund_bench_gen", year=annual_year), pg_url)


def _hours_section(t, prov: str, annual_year: int, cur_year: int,
                   today: date, pg_url: str) -> None:
    st.divider()
    st.subheader(t("fund_s3_title"))
    rows = load_series(prov, "util_hours", pg_url)
    # Annual cards prefer hours_<stem> (交接表利用小时数 sheet — carries the yoy
    # rows) over hours_<stem>_full; yoy labelled by its own unit (小时 delta vs %).
    _annual_cards(t, rows, annual_year, HOURS_BASE, unit=t("fund_hours_unit"),
                  decimals=0, prefer_full=False, base_scope_key="fund_scope_src",
                  with_yoy=True)
    st.caption(t("fund_s3_annual_note"))
    st.markdown(f"**{t('fund_monthly_chart', year=f'{annual_year}–{cur_year}')}**")
    upto_cur = today.month if cur_year == today.year else 12
    months = [(annual_year, m) for m in range(1, 13)] + \
             [(cur_year, m) for m in range(1, 13)]
    labels = [f"{y}-{m:02d}" for y, m in months]
    src_maps = {s: hours_map(rows, f"hours_{s}") for s in STEMS}
    der_maps = {s: hours_map(rows, f"hours_{s}_derived") for s in STEMS}
    fig = go.Figure()
    any_trace = False
    for stem in STEMS:
        src, der = src_maps[stem], der_maps[stem]
        # Source (累计平均口径) and derived (单月推算) are separate traces —
        # never merged; derived is dashed with a （推算） suffix.
        if any(v is not None for v in src.values()):
            fig.add_trace(go.Scatter(
                x=labels, y=[src.get(ym) for ym in months],
                name=t(f"fund_tech_{stem}"), mode="lines",
                line=dict(color=STEM_COLORS[stem]),
                hovertemplate="%{x}<br>%{y:,.0f}<extra></extra>"))
            any_trace = True
        if any(v is not None for v in der.values()):
            fig.add_trace(go.Scatter(
                x=labels, y=[der.get(ym) for ym in months],
                name=t(f"fund_tech_{stem}") + t("fund_derived"), mode="lines",
                line=dict(color=STEM_COLORS[stem], dash="dash"),
                hovertemplate="%{x}<br>%{y:,.0f}<extra></extra>"))
            any_trace = True
    if not any_trace:
        st.info(t("fund_missing"))
        return
    present = {ym for m in list(src_maps.values()) + list(der_maps.values())
               for ym, v in m.items() if v is not None}
    expected = [(annual_year, m) for m in range(1, 13)] + \
               [(cur_year, m) for m in range(1, upto_cur + 1)]
    missing = [f"{y}-{m:02d}" for y, m in expected if (y, m) not in present]
    _missing_markers(fig, missing, t)
    fig.update_layout(height=360, margin=dict(t=20, b=40),
                      yaxis_title=t("fund_hours_unit"), legend_title="")
    st.plotly_chart(fig, use_container_width=True)
    _missing_caption(t, missing)


def _demand_section(t, prov: str, prov_label: str, annual_year: int,
                    cur_year: int, today: date, pg_url: str) -> None:
    st.divider()
    st.subheader(t("fund_s4_title"))
    rows = load_series(prov, "demand", pg_url)
    ann = annual_values(rows, annual_year)

    st.markdown(f"**{t('fund_s4_monthly_title', year=cur_year)}**")
    total = monthly_series(rows, cur_year, "demand_total")
    yoy = monthly_series(rows, cur_year, "demand_yoy")
    present = {m for m, v in total.items() if v is not None}
    if not present:
        st.info(t("fund_missing"))
    else:
        upto = today.month if cur_year == today.year else 12
        months = list(range(1, 13))
        labels = [f"{cur_year}-{m:02d}" for m in months]
        missing = [labels[m - 1] for m in range(1, upto + 1) if m not in present]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=labels, y=[total.get(m) for m in months], name=t("fund_tech_total"),
            marker_color=_ACCENT,
            hovertemplate="%{x}<br>%{y:,.1f}<extra></extra>"))
        if any(v is not None for v in yoy.values()):
            fig.add_trace(go.Scatter(
                x=labels, y=[yoy.get(m) for m in months], name=t("fund_yoy"),
                mode="lines+markers", line=dict(color="#F5C842"), yaxis="y2",
                hovertemplate="%{x}<br>%{y:+.1f}%<extra></extra>"))
        _missing_markers(fig, missing, t)
        fig.update_layout(height=340, margin=dict(t=20, b=40),
                          yaxis_title=t("fund_generation_unit"),
                          yaxis2=dict(overlaying="y", side="right", title="%",
                                      showgrid=False),
                          legend_title="")
        st.plotly_chart(fig, use_container_width=True)
        _missing_caption(t, missing)

    st.markdown(f"**{t('fund_s4_split_title', year=annual_year)}**")
    split = [(t(lbl_key), ann[metric]["value"]) for metric, lbl_key in SECTOR_METRICS
             if metric in ann and ann[metric]["value"] is not None]
    if not split:
        st.info(t("fund_missing"))
    else:
        fig = go.Figure(go.Bar(
            x=[v for _, v in split], y=[lbl for lbl, _ in split],
            orientation="h", marker_color=_ACCENT,
            hovertemplate="%{y}: %{x:,.1f}<extra></extra>"))
        fig.update_layout(height=220, margin=dict(t=10, b=30),
                          xaxis_title=t("fund_generation_unit"))
        st.plotly_chart(fig, use_container_width=True)
    _benchmark(t, prov, prov_label, "demand", ["demand_total"],
               annual_year, t("fund_bench_demand", year=annual_year), pg_url)


# ── S5 省际对比 ──────────────────────────────────────────────────────────────

_CMP_METRICS = {
    "capacity": [("cap_total_full", "cap_total"), ("cap_thermal_full", "cap_thermal"),
                 ("cap_hydro_full", "cap_hydro"), ("cap_nuclear_full", "cap_nuclear"),
                 ("cap_wind_full", "cap_wind"), ("cap_solar_full", "cap_solar"),
                 ("cap_storage_full", "cap_storage")],
    "generation": [("gen_total_full", "gen_total"), ("gen_thermal_full", "gen_thermal"),
                   ("gen_hydro_full", "gen_hydro"), ("gen_nuclear_full", "gen_nuclear"),
                   ("gen_wind_full", "gen_wind"), ("gen_solar_full", "gen_solar")],
    "util_hours": [("hours_total",), ("hours_thermal",), ("hours_hydro",),
                   ("hours_nuclear",), ("hours_wind",), ("hours_solar",)],
    "demand": [("demand_total",), ("demand_yoy",)],
    "interprov": [("import_gwh",), ("export_gwh",)],
}
_CMP_STEM_LABEL = {
    "cap_total": "fund_metric_total", "cap_thermal": "fund_metric_thermal",
    "cap_hydro": "fund_metric_hydro", "cap_nuclear": "fund_metric_nuclear",
    "cap_wind": "fund_metric_wind", "cap_solar": "fund_metric_solar",
    "cap_storage": "fund_metric_storage",
    "gen_total": "fund_metric_total", "gen_thermal": "fund_metric_thermal",
    "gen_hydro": "fund_metric_hydro", "gen_nuclear": "fund_metric_nuclear",
    "gen_wind": "fund_metric_wind", "gen_solar": "fund_metric_solar",
    "hours_total": "fund_metric_total", "hours_thermal": "fund_metric_thermal",
    "hours_hydro": "fund_metric_hydro", "hours_nuclear": "fund_metric_nuclear",
    "hours_wind": "fund_metric_wind", "hours_solar": "fund_metric_solar",
    "demand_total": "fund_metric_demand", "demand_yoy": "fund_metric_demand_yoy",
    "import_gwh": "fund_metric_import", "export_gwh": "fund_metric_export",
}
_CMP_STEM_UNIT = {"capacity": "fund_capacity_unit", "generation": "fund_generation_unit",
                  "util_hours": "fund_hours_unit", "demand": "fund_generation_unit",
                  "interprov": "fund_generation_unit"}


def _comparison_section(t, prov: str, annual_year: int, pg_url: str) -> None:
    """S5: province × metric comparison table at one period (年度 or 公共月)."""
    st.divider()
    st.subheader(t("fund_cmp_title"))
    c1, c2, c3 = st.columns([2, 1.4, 1.4])
    with c1:
        section = st.selectbox(
            t("fund_cmp_section"),
            list(_CMP_METRICS),
            format_func=lambda s: t(f"fund_cmp_sec_{s}"),
            key="fund_cmp_section")
    with c2:
        period_mode = st.radio(t("fund_cmp_period"),
                               ["year", "month"], horizontal=True,
                               format_func=lambda m: t(f"fund_cmp_period_{m}"),
                               key="fund_cmp_period")

    if period_mode == "year":
        rows = load_section_all(section, pg_url, year=annual_year)
        period_label = str(annual_year)
        month = None
    else:
        all_monthly = load_section_all_monthly_preflight(section, pg_url)
        month = common_month(all_monthly)
        if month is None:
            st.info(t("fund_cmp_no_monthly"))
            return
        period_label = month.strftime("%Y-%m")
        rows = [r for r in all_monthly if r["month"] == month]
        st.caption(t("fund_cmp_common_note", month=period_label))

    if not rows:
        st.info(t("fund_missing"))
        return

    provs, metrics, grid = pivot_rows(rows)
    unit = t(_CMP_STEM_UNIT[section])
    header = [t("fund_cmp_province")] + [
        f"{t(_CMP_STEM_LABEL.get(next(iter(mkey)), ''))} ({unit})"
        if section != "demand" or next(iter(mkey)) != "demand_yoy"
        else f"{t(_CMP_STEM_LABEL['demand_yoy'])} (%)"
        for mkey in metrics_of(_CMP_METRICS[section], {r["metric"] for r in rows})]

    body = []
    for p in provs:
        mark = "▶ " if p == prov else ""
        cells = [mark + p]
        for mkey in metrics_of(_CMP_METRICS[section], {r["metric"] for r in rows}):
            v = pick_first(grid.get(p, {}), mkey)
            cells.append(f"{v:,.1f}" if v is not None else "—")
        body.append(cells)
    st.dataframe(pd.DataFrame(body, columns=header), use_container_width=True,
                 hide_index=True)
    st.caption(t("fund_cmp_scope_note"))


def metrics_of(pairs: list[tuple[str, ...]], present: set[str]) -> list[tuple[str, ...]]:
    """Ordered metric candidates whose any variant is present (full or plain)."""
    return [pair for pair in pairs if any(m in present for m in pair)]


def pick_first(grid_row: dict[str, float | None], pair: tuple[str, ...]):
    for m in pair:
        if m in grid_row:
            return grid_row[m]
    return None


@st.cache_data(ttl=300, show_spinner=False)
def load_section_all_monthly_preflight(section: str, pg_url: str) -> list[dict]:
    """All confirmed monthly rows for a section (drives the 公共月 computation)."""
    import psycopg2 as _p
    try:
        with _p.connect(pg_url) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT province, month, period, section, metric, value, unit,"
                " source, created_at FROM marketdata.province_fundamentals_monthly"
                " WHERE section = %s AND period = 'month' AND status = 'confirmed'",
                (section,))
            rows = cur.fetchall()
        return [
            {"province": p, "month": m, "period": per, "section": sec,
             "metric": met, "value": float(v) if v is not None else None,
             "unit": u, "source": src, "created_at": ts}
            for p, m, per, sec, met, v, u, src, ts in rows
        ]
    except Exception:
        return []

# ── S6 省间受入/外送 ─────────────────────────────────────────────────────────

def _interprov_section(t, prov: str, prov_label: str, cur_year: int,
                       today: date, pg_url: str) -> None:
    """Monthly 受入/外送电量 bars + net line for the selected province."""
    rows = load_series(prov, "interprov", pg_url, months=24)
    monthly = [r for r in rows if r["period"] == "month"]
    if not monthly:
        return
    st.divider()
    st.subheader(t("fund_s6_title", prov=prov_label))
    imp = hours_map(monthly, "import_gwh")
    exp = hours_map(monthly, "export_gwh")
    months = sorted(set(imp) | set(exp))
    months = [m for m in months if m <= (today.year, today.month)]
    if not months:
        return
    labels = [f"{y}-{m:02d}" for y, m in months]
    fig = go.Figure()
    fig.add_bar(x=labels, y=[imp.get(m) for m in months],
                name=t("fund_s6_import"), marker_color="#4C9BE8")
    fig.add_bar(x=labels, y=[exp.get(m) for m in months],
                name=t("fund_s6_export"), marker_color="#E8A44C")
    net = [(exp.get(m) or 0) - (imp.get(m) or 0)
           if (exp.get(m) is not None or imp.get(m) is not None) else None
           for m in months]
    fig.add_trace(go.Scatter(x=labels, y=net, name=t("fund_s6_net"),
                             mode="lines+markers", line=dict(color="#F5C842", dash="dot")))
    fig.update_layout(height=300, margin=dict(t=20, b=40), barmode="group",
                      yaxis_title=t("fund_generation_unit"), legend_title="")
    st.plotly_chart(fig, use_container_width=True)
    st.caption(t("fund_s6_note"))


def render(t, is_zh: bool, pg_url: str | None = None) -> None:
    """Province selector + S1–S6. `t` is the app's bilingual _t callable."""
    pg_url = pg_url or _pg_url()
    today = date.today()
    cur_year, annual_year = today.year, today.year - 1

    provinces = load_provinces(pg_url)
    candidates = list(provinces)
    if NATIONAL not in candidates:
        candidates.append(NATIONAL)
    label_of = (lambda p: p) if is_zh else (lambda p: _PROV_LABEL_EN.get(p, p))
    default_idx = candidates.index("安徽") if "安徽" in candidates else 0
    prov = st.selectbox(t("fund_province"), candidates, index=default_idx,
                        format_func=label_of, key="fund2_province")
    st.caption(t("fund_timeline_note", ay=annual_year, cy=cur_year))
    if not provinces:
        st.info(t("fund_no_rows"))
    if not prov:
        return
    prov_label = label_of(prov)
    _capacity_section(t, prov, prov_label, annual_year, cur_year, today, pg_url)
    _generation_section(t, prov, prov_label, annual_year, cur_year, today, pg_url)
    _hours_section(t, prov, annual_year, cur_year, today, pg_url)
    _demand_section(t, prov, prov_label, annual_year, cur_year, today, pg_url)
    _comparison_section(t, prov, annual_year, pg_url)
    _interprov_section(t, prov, prov_label, cur_year, today, pg_url)
