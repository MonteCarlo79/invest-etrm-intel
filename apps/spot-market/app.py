"""
China Spot Market Price Cockpit
Visualises daily DA / RT clearing prices from spot_daily.

Run:
    py -m streamlit run apps/spot-market/app.py
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import psycopg2
import streamlit as st
from dotenv import load_dotenv

# ── path / env setup ─────────────────────────────────────────────────────────
_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

for _env in [_REPO / "config" / ".env", _REPO / ".env"]:
    if _env.exists():
        load_dotenv(_env)
_spot_env = _REPO / "apps" / "spot-agent" / ".env"
if _spot_env.exists():
    load_dotenv(_spot_env)  # sets DB_URL=postgresql://...5433/marketdata

# ── page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Spot Market Cockpit",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── DB connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def _get_conn():
    url = (
        os.environ.get("PGURL")          # RDS — set by config/.env
        or os.environ.get("DATABASE_URL")
        or os.environ.get("DB_URL")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    return psycopg2.connect(url, keepalives=1, keepalives_idle=60,
                            keepalives_interval=10, keepalives_count=5)

def _conn():
    conn = _get_conn()
    try:
        conn.cursor().execute("SELECT 1")
    except Exception:
        _get_conn.clear()
        conn = _get_conn()
    return conn

# ── data quality filter ───────────────────────────────────────────────────────
# Rows from early-January PDFs have da_avg extracted from the wrong column
# (picks da_min position instead of da_avg), producing avg < min.
# Filter these out so they don't distort charts and distributions.
def _apply_quality_filter(df: pd.DataFrame) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    for m in ("da", "rt"):
        avg, mx, mn = f"{m}_avg", f"{m}_max", f"{m}_min"
        # Drop rows where avg is outside [min, max] by more than a rounding tolerance
        bad_lo = df[avg].notna() & df[mn].notna() & (df[avg] < df[mn] - 0.001)
        bad_hi = df[avg].notna() & df[mx].notna() & (df[avg] > df[mx] + 0.001)
        # Also drop physically implausible values (outside ±2 ¥/kWh)
        bad_range = df[avg].notna() & ((df[avg] < -0.5) | (df[avg] > 2.0))
        mask &= ~(bad_lo | bad_hi | bad_range)
    return df[mask].copy()

# ── data loaders ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner=False)
def load_all(start: date, end: date, quality_filter: bool) -> pd.DataFrame:
    q = """
        SELECT report_date::date AS report_date,
               province_en, province_cn,
               da_avg, da_max, da_min,
               rt_avg, rt_max, rt_min
        FROM spot_daily
        WHERE report_date BETWEEN %s AND %s
          AND (da_avg IS NOT NULL OR rt_avg IS NOT NULL)
        ORDER BY report_date, province_en
    """
    df = pd.read_sql(q, _conn(), params=(start, end), parse_dates=["report_date"])
    for c in ["da_avg","da_max","da_min","rt_avg","rt_max","rt_min"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if quality_filter:
        df = _apply_quality_filter(df)
    return df


@st.cache_data(ttl=120, show_spinner=False)
def load_provinces() -> list[str]:
    cur = _conn().cursor()
    cur.execute(
        "SELECT DISTINCT province_en FROM spot_daily "
        "WHERE report_date >= '2026-01-01' ORDER BY 1"
    )
    return [r[0] for r in cur.fetchall()]


@st.cache_data(ttl=60, show_spinner=False)
def load_kpis(quality_filter: bool) -> dict:
    cur = _conn().cursor()
    cur.execute("""
        SELECT
            MAX(report_date)                              AS latest_date,
            COUNT(DISTINCT report_date)                   AS total_dates,
            COUNT(DISTINCT province_en)                   AS total_provinces,
            SUM(CASE WHEN da_avg IS NOT NULL AND rt_avg IS NOT NULL THEN 1 ELSE 0 END) AS complete_rows,
            COUNT(*)                                      AS total_rows
        FROM spot_daily
        WHERE report_date >= '2026-01-01'
    """)
    r = cur.fetchone()
    return {
        "latest_date":     r[0],
        "total_dates":     r[1],
        "total_provinces": r[2],
        "complete_rows":   r[3],
        "total_rows":      r[4],
    }


# ── colour helpers ────────────────────────────────────────────────────────────
_PALETTE = px.colors.qualitative.Plotly + px.colors.qualitative.Dark24

def _prov_colour(provinces: list[str]) -> dict[str, str]:
    return {p: _PALETTE[i % len(_PALETTE)] for i, p in enumerate(sorted(provinces))}


# ── chart builders ────────────────────────────────────────────────────────────
def chart_timeseries(df: pd.DataFrame, provinces: list[str],
                     metric: str, show_band: bool) -> go.Figure:
    fig = go.Figure()
    colours = _prov_colour(provinces)
    avg_col, max_col, min_col = f"{metric}_avg", f"{metric}_max", f"{metric}_min"

    for prov in sorted(provinces):
        sub = df[df["province_en"] == prov].sort_values("report_date")
        if sub.empty or sub[avg_col].isna().all():
            continue
        col = colours[prov]

        if show_band and sub[max_col].notna().any():
            x_band = pd.concat([sub["report_date"], sub["report_date"].iloc[::-1]])
            y_band = pd.concat([sub[max_col], sub[min_col].iloc[::-1]])
            fig.add_trace(go.Scatter(
                x=x_band, y=y_band,
                fill="toself", fillcolor=col, opacity=0.10,
                line=dict(width=0), showlegend=False, hoverinfo="skip",
            ))

        fig.add_trace(go.Scatter(
            x=sub["report_date"], y=sub[avg_col],
            name=prov, mode="lines+markers",
            line=dict(color=col, width=1.8), marker=dict(size=4),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.4f} ¥/kWh<extra>" + prov + "</extra>",
        ))

    label = "Day-Ahead (DA)" if metric == "da" else "Real-Time (RT)"
    fig.update_layout(
        height=430,
        title=dict(text=f"{label} Clearing Price  (¥/kWh)", font=dict(size=14)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11)),
        xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
    )
    return fig


def chart_da_rt_overlay(df: pd.DataFrame, province: str) -> go.Figure:
    sub = df[df["province_en"] == province].sort_values("report_date")
    fig = go.Figure()

    for metric, label, colour in [("da", "DA avg", "#1f77b4"), ("rt", "RT avg", "#ff7f0e")]:
        avg_col, max_col, min_col = f"{metric}_avg", f"{metric}_max", f"{metric}_min"
        if sub[avg_col].isna().all():
            continue
        if sub[max_col].notna().any():
            fig.add_trace(go.Scatter(
                x=pd.concat([sub["report_date"], sub["report_date"].iloc[::-1]]),
                y=pd.concat([sub[max_col], sub[min_col].iloc[::-1]]),
                fill="toself", fillcolor=colour, opacity=0.12,
                line=dict(width=0), showlegend=False, hoverinfo="skip",
            ))
        fig.add_trace(go.Scatter(
            x=sub["report_date"], y=sub[avg_col],
            name=label, mode="lines+markers",
            line=dict(color=colour, width=2), marker=dict(size=4),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.4f} ¥/kWh<extra>" + label + "</extra>",
        ))

    fig.update_layout(
        height=390,
        title=dict(text=f"{province} — DA vs RT  (¥/kWh)", font=dict(size=13)),
        margin=dict(l=10, r=10, t=45, b=60),
        legend=dict(orientation="h", yanchor="top", y=-0.15,
                    xanchor="center", x=0.5, font=dict(size=11)),
        xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
    )
    return fig


def chart_spread(df: pd.DataFrame, provinces: list[str]) -> go.Figure:
    fig = go.Figure()
    colours = _prov_colour(provinces)

    for prov in sorted(provinces):
        sub = df[df["province_en"] == prov].dropna(subset=["da_avg", "rt_avg"]).copy()
        if sub.empty:
            continue
        sub["spread"] = sub["da_avg"] - sub["rt_avg"]
        fig.add_trace(go.Bar(
            x=sub["report_date"], y=sub["spread"],
            name=prov, marker_color=colours[prov], opacity=0.8,
            hovertemplate="%{x|%Y-%m-%d}<br>Spread: %{y:.4f} ¥/kWh<extra>" + prov + "</extra>",
        ))

    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    fig.update_layout(
        height=360, barmode="group",
        title=dict(text="DA − RT Spread  (¥/kWh)  |  +ve = DA premium, −ve = RT spike",
                   font=dict(size=13)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.22,
                    xanchor="center", x=0.5, font=dict(size=11)),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
    )
    return fig


def chart_heatmap(df: pd.DataFrame, metric: str) -> go.Figure:
    avg_col = f"{metric}_avg"
    pivot = (
        df[["report_date", "province_en", avg_col]]
        .dropna(subset=[avg_col])
        .pivot_table(index="province_en", columns="report_date", values=avg_col)
    )
    if pivot.empty:
        return go.Figure()

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.strftime("%m-%d"),
        y=pivot.index.tolist(),
        colorscale="RdYlGn_r",
        colorbar=dict(title="¥/kWh", thickness=12),
        hoverongaps=False,
        hovertemplate="Date: %{x}<br>Province: %{y}<br>Price: %{z:.4f} ¥/kWh<extra></extra>",
    ))
    label = "Day-Ahead" if metric == "da" else "Real-Time"
    fig.update_layout(
        height=max(350, len(pivot) * 24),
        title=dict(text=f"{label} Average Clearing Price — Province × Date Heatmap",
                   font=dict(size=13)),
        margin=dict(l=120, r=20, t=45, b=60),
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        yaxis=dict(tickfont=dict(size=11)),
    )
    return fig


def chart_distributions(df: pd.DataFrame, provinces: list[str],
                         metric: str, nbins: int, show_kde: bool) -> go.Figure:
    """
    Overlapping histogram + optional KDE for DA or RT avg prices.
    One trace per province, semi-transparent fill so all are visible.
    """
    avg_col = f"{metric}_avg"
    colours = _prov_colour(provinces)
    fig = go.Figure()

    for prov in sorted(provinces):
        vals = df[df["province_en"] == prov][avg_col].dropna().values
        if len(vals) < 2:
            continue
        col = colours[prov]

        fig.add_trace(go.Histogram(
            x=vals,
            name=prov,
            nbinsx=nbins,
            marker_color=col,
            opacity=0.45,
            histnorm="probability density",
            hovertemplate="Price: %{x:.4f} ¥/kWh<br>Density: %{y:.3f}<extra>" + prov + "</extra>",
        ))

        if show_kde and len(vals) >= 5:
            # Gaussian KDE via numpy
            std = vals.std()
            if std > 0:
                bw = 1.06 * std * len(vals) ** (-0.2)  # Silverman's rule
                x_grid = np.linspace(vals.min() - 2 * bw, vals.max() + 2 * bw, 300)
                kde = np.zeros_like(x_grid)
                for v in vals:
                    kde += np.exp(-0.5 * ((x_grid - v) / bw) ** 2)
                kde /= len(vals) * bw * np.sqrt(2 * np.pi)
                fig.add_trace(go.Scatter(
                    x=x_grid, y=kde,
                    name=f"{prov} KDE",
                    mode="lines",
                    line=dict(color=col, width=2, dash="solid"),
                    showlegend=False,
                    hovertemplate="%{x:.4f} ¥/kWh<br>KDE: %{y:.3f}<extra>" + prov + "</extra>",
                ))

    label = "Day-Ahead (DA)" if metric == "da" else "Real-Time (RT)"
    fig.update_layout(
        height=430,
        barmode="overlay",
        title=dict(text=f"{label} Price Distribution  (¥/kWh)", font=dict(size=14)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11)),
        xaxis=dict(title="Price (¥/kWh)", showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(title="Probability density", showgrid=True, gridcolor="#f0f0f0"),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    return fig


def chart_violin(df: pd.DataFrame, provinces: list[str], metric: str) -> go.Figure:
    """Violin + box plot per province, DA and RT side-by-side."""
    avg_col = f"{metric}_avg"
    colours = _prov_colour(provinces)
    fig = go.Figure()

    for prov in sorted(provinces):
        vals = df[df["province_en"] == prov][avg_col].dropna().values
        if len(vals) < 3:
            continue
        fig.add_trace(go.Violin(
            y=vals, name=prov,
            box_visible=True,
            meanline_visible=True,
            fillcolor=colours[prov],
            opacity=0.65,
            line_color=colours[prov],
            hoverinfo="y+name",
        ))

    label = "Day-Ahead (DA)" if metric == "da" else "Real-Time (RT)"
    fig.update_layout(
        height=430,
        title=dict(text=f"{label} — Violin / Box Plot  (¥/kWh)", font=dict(size=14)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11)),
        yaxis=dict(title="Price (¥/kWh)", showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
        violinmode="group",
    )
    return fig


def _dist_stats(df: pd.DataFrame, provinces: list[str], metric: str) -> pd.DataFrame:
    avg_col = f"{metric}_avg"
    rows = []
    for prov in sorted(provinces):
        vals = df[df["province_en"] == prov][avg_col].dropna()
        if vals.empty:
            continue
        rows.append({
            "Province": prov,
            "N": len(vals),
            "Mean":   f"{vals.mean():.4f}",
            "Median": f"{vals.median():.4f}",
            "Std":    f"{vals.std():.4f}",
            "P10":    f"{vals.quantile(0.10):.4f}",
            "P25":    f"{vals.quantile(0.25):.4f}",
            "P75":    f"{vals.quantile(0.75):.4f}",
            "P90":    f"{vals.quantile(0.90):.4f}",
            "Min":    f"{vals.min():.4f}",
            "Max":    f"{vals.max():.4f}",
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# LAYOUT
# ─────────────────────────────────────────────────────────────────────────────

st.title("⚡ China Spot Market Price Cockpit")

# ── KPI strip ────────────────────────────────────────────────────────────────
with st.spinner("Loading…"):
    try:
        provinces_all = load_provinces()
    except Exception as e:
        st.error(f"DB connection failed: {e}")
        st.stop()

# ── Sidebar controls ──────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    _today = date.today()
    date_range = st.date_input(
        "Date range",
        value=(date(2026, 1, 1), _today),
        min_value=date(2026, 1, 1),
        max_value=_today,
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        d_start, d_end = date_range
    else:
        d_start, d_end = date(2026, 1, 1), _today

    prov_options = sorted(provinces_all)
    default_provs = [p for p in ["Shandong", "Shanxi", "Mengxi", "Guangdong", "Sichuan"]
                     if p in prov_options] or prov_options[:5]
    selected_provs = st.multiselect(
        "Provinces (multi-select)",
        prov_options,
        default=default_provs,
        help="Select one or more provinces to compare",
    )

    show_band = st.checkbox("Show min/max band", value=True)
    quality_filter = st.checkbox(
        "Filter bad data",
        value=True,
        help="Exclude rows where avg is outside [min, max] bounds — caused by early-Jan PDF format differences",
    )

    st.divider()
    st.caption("Data: spot_daily · units: ¥/kWh")

if not selected_provs:
    st.info("Select at least one province in the sidebar.")
    st.stop()

# ── Load data ─────────────────────────────────────────────────────────────────
kpis = load_kpis(quality_filter)
df = load_all(d_start, d_end, quality_filter)
df_sel = df[df["province_en"].isin(selected_provs)]

# ── KPI strip ─────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Latest Date",   str(kpis["latest_date"]) if kpis["latest_date"] else "—")
k2.metric("Dates in DB",   kpis["total_dates"])
k3.metric("Provinces",     kpis["total_provinces"])
k4.metric("Complete Rows", kpis["complete_rows"],
          delta=f"/ {kpis['total_rows']} total", delta_color="off")
k5.metric("Coverage",
          f"{100*kpis['complete_rows']/kpis['total_rows']:.0f}%" if kpis["total_rows"] else "—")

if quality_filter:
    n_bad = load_kpis(False)["total_rows"] - kpis["total_rows"]
    if n_bad > 0:
        st.caption(f"ℹ️ {n_bad} rows with invalid avg/min/max values hidden (toggle 'Filter bad data' in sidebar to include)")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────
tab_overview, tab_spread, tab_heatmap, tab_province, tab_dist, tab_mgmt = st.tabs(
    ["Overview", "DA−RT Spread", "Heatmap", "Province Deep-Dive", "Distributions", "Data Management"]
)

# ── Tab 1: Overview ───────────────────────────────────────────────────────────
with tab_overview:
    col_da, col_rt = st.columns(2)
    with col_da:
        st.plotly_chart(chart_timeseries(df_sel, selected_provs, "da", show_band),
                        use_container_width=True)
    with col_rt:
        st.plotly_chart(chart_timeseries(df_sel, selected_provs, "rt", show_band),
                        use_container_width=True)

    st.subheader("Latest prices")
    latest = (
        df[df["province_en"].isin(selected_provs)]
        .sort_values("report_date", ascending=False)
        .groupby("province_en")
        .first()
        .reset_index()
        [["province_en", "province_cn", "report_date",
          "da_avg", "da_max", "da_min",
          "rt_avg", "rt_max", "rt_min"]]
        .rename(columns={"province_en": "Province", "province_cn": "省份",
                         "report_date": "Date"})
        .sort_values("Province")
    )
    latest["Date"] = pd.to_datetime(latest["Date"]).dt.date  # remove timestamp
    for c in ["da_avg","da_max","da_min","rt_avg","rt_max","rt_min"]:
        latest[c] = latest[c].map(lambda v: f"{v:.4f}" if pd.notna(v) else "—")
    st.dataframe(latest, use_container_width=True, hide_index=True)

# ── Tab 2: Spread ─────────────────────────────────────────────────────────────
with tab_spread:
    st.plotly_chart(chart_spread(df_sel, selected_provs), use_container_width=True)

    st.subheader("Spread statistics (¥/kWh)")
    spread_rows = []
    for prov in sorted(selected_provs):
        sub = df_sel[df_sel["province_en"] == prov].dropna(subset=["da_avg", "rt_avg"])
        if sub.empty:
            continue
        s = sub["da_avg"] - sub["rt_avg"]
        spread_rows.append({
            "Province":    prov,
            "Mean":        f"{s.mean():.4f}",
            "Std":         f"{s.std():.4f}",
            "Min":         f"{s.min():.4f}",
            "Max":         f"{s.max():.4f}",
            "DA > RT (%)": f"{(s > 0).mean()*100:.0f}%",
            "Days":        len(s),
        })
    if spread_rows:
        st.dataframe(pd.DataFrame(spread_rows), use_container_width=True, hide_index=True)

# ── Tab 3: Heatmap ────────────────────────────────────────────────────────────
with tab_heatmap:
    hm_metric = st.radio("Metric", ["DA", "RT"], horizontal=True)
    fig_hm = chart_heatmap(df[df["province_en"].isin(selected_provs)], hm_metric.lower())
    if fig_hm.data:
        st.plotly_chart(fig_hm, use_container_width=True)
    else:
        st.info("No data for selected range / provinces.")

# ── Tab 4: Province Deep-Dive ────────────────────────────────────────────────
with tab_province:
    dive_prov = st.selectbox("Select province", sorted(selected_provs))
    if dive_prov:
        st.plotly_chart(chart_da_rt_overlay(df_sel, dive_prov), use_container_width=True)

        sub = df_sel[df_sel["province_en"] == dive_prov].sort_values("report_date").copy()
        sub["report_date"] = pd.to_datetime(sub["report_date"]).dt.date
        st.subheader(f"{dive_prov} — raw data")
        st.dataframe(
            sub[["report_date","da_avg","da_max","da_min","rt_avg","rt_max","rt_min"]]
            .rename(columns={"report_date": "Date"})
            .style.format(
                {c: "{:.4f}" for c in ["da_avg","da_max","da_min","rt_avg","rt_max","rt_min"]},
                na_rep="—",
            ),
            use_container_width=True, hide_index=True,
        )

# ── Tab 5: Distributions ─────────────────────────────────────────────────────
with tab_dist:
    dc1, dc2, dc3 = st.columns([2, 1, 1])
    with dc1:
        dist_metric = st.radio("Market", ["DA", "RT", "Both"], horizontal=True, key="dist_metric")
    with dc2:
        nbins = st.slider("Histogram bins", 10, 80, 30, key="dist_bins")
    with dc3:
        show_kde = st.checkbox("Overlay KDE curve", value=True, key="dist_kde")

    metrics_to_show = (
        ["da", "rt"] if dist_metric == "Both"
        else [dist_metric.lower()]
    )

    for m in metrics_to_show:
        st.plotly_chart(
            chart_distributions(df_sel, selected_provs, m, nbins, show_kde),
            use_container_width=True,
        )
        st.plotly_chart(
            chart_violin(df_sel, selected_provs, m),
            use_container_width=True,
        )
        st.subheader(f"{'DA' if m == 'da' else 'RT'} — Descriptive statistics (¥/kWh)")
        stats_df = _dist_stats(df_sel, selected_provs, m)
        if not stats_df.empty:
            st.dataframe(stats_df, use_container_width=True, hide_index=True)
        if dist_metric == "Both" and m == "da":
            st.divider()

# ── Tab 6: Data Management ────────────────────────────────────────────────────
with tab_mgmt:
    import re as _re
    from pathlib import Path as _Path

    PROVINCES_MAP: dict[str, str] = {
        "山东": "Shandong", "山西": "Shanxi", "蒙西": "Mengxi", "内蒙古": "Mengxi",
        "甘肃": "Gansu", "广东": "Guangdong", "四川": "Sichuan", "云南": "Yunnan",
        "贵州": "Guizhou", "广西": "Guangxi", "湖南": "Hunan", "湖北": "Hubei",
        "安徽": "Anhui", "浙江": "Zhejiang", "江苏": "Jiangsu", "福建": "Fujian",
        "河南": "Henan", "陕西": "Shaanxi", "宁夏": "Ningxia", "新疆": "Xinjiang",
        "辽宁": "Liaoning", "吉林": "Jilin", "黑龙江": "Heilongjiang", "蒙东": "Mengdong",
        "河北": "Hebei", "冀北": "Hebei-North", "冀南": "Hebei-South", "青海": "Qinghai",
        "江西": "Jiangxi", "海南": "Hainan", "重庆": "Chongqing", "上海": "Shanghai",
        "北京": "Beijing", "天津": "Tianjin",
    }

    def _parse_pdf_date_range(stem: str, year: int = 2026):
        """
        Parse date range from PDF stem.  Handles three filename styles:
          • M.D  /  M.D-M.D     e.g. "2.14", "2.7-2.9", "1.31-2.2"  (2025/2026)
          • MMDD / MMDD-MMDD    e.g. "0731", "0816-0818", "1001-1009" (mid-2025)
        Full-width vs half-width parens are handled by the caller regex.
        Returns (start_date, end_date) or None.
        """
        stem = stem.strip().rstrip("）)） ")

        # Pattern 1: MMDD no-dot  (4 digits, no separator between MM and DD)
        m = _re.fullmatch(r"(\d{2})(\d{2})(?:-(\d{2})(\d{2}))?", stem)
        if m:
            try:
                start = date(year, int(m.group(1)), int(m.group(2)))
                end   = date(year, int(m.group(3) or m.group(1)),
                             int(m.group(4) or m.group(2)))
                return start, end
            except ValueError:
                pass

        # Pattern 2: M.D  /  M.D-D (same month)  /  M.D-M.D
        # e.g. "2.14", "10.19-21", "2.7-2.9", "1.31-2.2", "12.30-1.2"
        m = _re.search(r"(\d{1,2})\.(\d{1,2})(?:-(?:(\d{1,2})\.)?(\d{1,2}))?", stem)
        if m:
            try:
                m1, d1 = int(m.group(1)), int(m.group(2))
                start  = date(year, m1, d1)
                if m.group(4):  # end day present
                    m2 = int(m.group(3)) if m.group(3) else m1  # same month if omitted
                    d2 = int(m.group(4))
                    # Handle Dec→Jan year-boundary in filename (e.g. 12.30-1.2)
                    end_year = year + 1 if m2 < m1 else year
                    end = date(end_year, m2, d2)
                else:
                    end = start
                return start, end
            except ValueError:
                pass

        return None

    @st.cache_data(ttl=60, show_spinner=False)
    def _scan_pdf_inventory(year: int = 2026):
        """Scan PDF folder and return list of (filename, start_date, end_date, path)."""
        data_dir = _REPO / "data" / "spot reports" / str(year)
        pdfs = []
        if not data_dir.exists():
            return pdfs
        for p in sorted(data_dir.glob("*.pdf")):
            stem = p.stem
            # Match both full-width （…） and half-width (…) parentheses
            m = _re.search(r"[（(]([^)）]+)[）)]", stem)
            if not m:
                continue
            date_range = _parse_pdf_date_range(m.group(1).strip(), year)
            if date_range:
                pdfs.append((p.name, date_range[0], date_range[1], p))
        return pdfs

    @st.cache_data(ttl=30, show_spinner=False)
    def _db_coverage(year: int = 2026):
        """Return set of report_dates that have at least one row in DB."""
        cur = _conn().cursor()
        cur.execute(
            "SELECT DISTINCT report_date FROM spot_daily "
            "WHERE report_date BETWEEN %s AND %s",
            (date(year, 1, 1), date(year, 12, 31)),
        )
        return {r[0] for r in cur.fetchall()}

    @st.cache_data(ttl=30, show_spinner=False)
    def _db_coverage_detail(year: int = 2026):
        """Return dict date → (da_count, rt_count) for the year."""
        cur = _conn().cursor()
        cur.execute(
            """SELECT report_date::date, COUNT(da_avg), COUNT(rt_avg)
               FROM spot_daily
               WHERE report_date BETWEEN %s AND %s
               GROUP BY 1""",
            (date(year, 1, 1), date(year, 12, 31)),
        )
        return {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    # ── Layout ────────────────────────────────────────────────────────────────
    st.subheader("Data Management")

    col_yr, _, _ = st.columns([1, 2, 1])
    with col_yr:
        sel_year = st.selectbox("Report year", [2026, 2025, 2024], key="mgmt_year")

    c_left, c_right = st.columns([2, 1])

    with c_left:
        mgmt_mode = st.radio(
            "Mode",
            ["Fill gaps (ingest missing dates only)",
             "Backfill date range (ingest all PDFs covering the range)"],
            horizontal=False,
            key="mgmt_mode",
        )

    with c_right:
        _yr_end = date(sel_year, 12, 31) if sel_year < date.today().year else date.today() - timedelta(days=1)
        bf_start = st.date_input("Start date", date(sel_year, 1, 1), key=f"bf_start_{sel_year}")
        bf_end   = st.date_input("End date",   _yr_end,              key=f"bf_end_{sel_year}")

    st.divider()

    # ── PDF inventory + gap analysis ──────────────────────────────────────────
    inventory = _scan_pdf_inventory(sel_year)
    coverage = _db_coverage_detail(sel_year)
    existing_dates = set(coverage.keys())

    # Filter PDFs that overlap with [bf_start, bf_end]
    relevant_pdfs = [
        (fname, s, e, path)
        for fname, s, e, path in inventory
        if s <= bf_end and e >= bf_start
    ]

    # Build summary table
    inv_rows = []
    for fname, s, e, path in relevant_pdfs:
        dates_in_range = [
            s + timedelta(days=i)
            for i in range((e - s).days + 1)
            if bf_start <= s + timedelta(days=i) <= bf_end
        ]
        missing = [d for d in dates_in_range if d not in existing_dates]
        partial = [
            d for d in dates_in_range
            if d in existing_dates and (coverage[d][0] == 0 or coverage[d][1] == 0)
        ]
        inv_rows.append({
            "PDF": fname,
            "Covers": f"{s} → {e}",
            "Dates in range": len(dates_in_range),
            "Missing from DB": len(missing),
            "Partial (DA or RT=0)": len(partial),
            "Status": "Missing" if missing else ("Partial" if partial else "OK"),
        })

    if inv_rows:
        inv_df = pd.DataFrame(inv_rows)
        st.dataframe(
            inv_df.style.apply(
                lambda col: [
                    "background-color: #ffe0e0" if v == "Missing"
                    else "background-color: #fff3cd" if v == "Partial"
                    else "background-color: #d4edda"
                    for v in col
                ],
                subset=["Status"],
            ),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No PDFs found in the selected date range.")

    if relevant_pdfs:
        needs_work = [
            (fname, s, e, path)
            for fname, s, e, path in relevant_pdfs
            if any(
                s + timedelta(days=i) not in existing_dates
                for i in range((e - s).days + 1)
                if bf_start <= s + timedelta(days=i) <= bf_end
            )
        ]
        partial_pdfs = [
            (fname, s, e, path)
            for fname, s, e, path in relevant_pdfs
            if any(
                (s + timedelta(days=i)) in existing_dates
                and (coverage[s + timedelta(days=i)][0] == 0
                     or coverage[s + timedelta(days=i)][1] == 0)
                for i in range((e - s).days + 1)
                if bf_start <= s + timedelta(days=i) <= bf_end
            )
        ]

        if mgmt_mode.startswith("Fill gaps"):
            pdfs_to_run = needs_work
            label = f"Backfill {len(pdfs_to_run)} PDF(s) with missing dates"
        else:
            pdfs_to_run = relevant_pdfs
            label = f"Re-ingest all {len(pdfs_to_run)} PDF(s) in range"

        col_btn, col_info = st.columns([1, 3])
        with col_btn:
            run_backfill = st.button(label, type="primary", disabled=len(pdfs_to_run) == 0)
        with col_info:
            if mgmt_mode.startswith("Fill gaps") and not needs_work and partial_pdfs:
                st.warning(
                    f"{len(partial_pdfs)} PDF(s) have partial data (DA or RT missing). "
                    "Switch to 'Backfill date range' mode to re-ingest them."
                )
            elif not pdfs_to_run:
                st.success("All dates in range are present in DB.")

        if run_backfill:
            from services.spot_ingest.pdf_parser import parse_pdf as _parse_pdf
            from services.spot_ingest.db_upsert import upsert_rows as _upsert_rows

            provinces_cn = list(PROVINCES_MAP.keys())
            total = len(pdfs_to_run)
            progress = st.progress(0, text="Starting…")
            log_area = st.empty()
            results = []

            for i, (fname, s, e, path) in enumerate(pdfs_to_run):
                progress.progress((i) / total, text=f"Parsing {fname}…")
                try:
                    parsed = _parse_pdf(path, int(path.parent.name) if path.parent.name.isdigit() else 2026, provinces_cn)
                    rows = []
                    for rdate, provs in parsed.items():
                        for pcn, vals in provs.items():
                            rows.append({
                                "report_date": rdate,
                                "province_cn": pcn,
                                "province_en": PROVINCES_MAP.get(pcn, pcn),
                                **vals,
                            })
                    n = _upsert_rows(rows)
                    results.append({"PDF": fname, "Dates": str(sorted(parsed.keys())), "Rows upserted": n, "Error": ""})
                except Exception as exc:
                    results.append({"PDF": fname, "Dates": "", "Rows upserted": 0, "Error": str(exc)[:120]})

            progress.progress(1.0, text="Done.")
            st.success(f"Backfill complete — processed {total} PDF(s).")
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

            # Clear cached data so charts refresh
            load_all.clear()
            load_kpis.clear()
            _db_coverage.clear()
            _db_coverage_detail.clear()
            st.rerun()
