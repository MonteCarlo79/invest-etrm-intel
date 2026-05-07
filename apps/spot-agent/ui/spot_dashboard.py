"""
China Spot Power Market — auto-loading dashboard.

Displays:
  1. DA / RT prices for the three most recent report dates, all provinces.
  2. Market highlights (LLM summaries) for the most recent date.

Data source: spot_daily table, populated by spot_ingest.py.
No manual input required — page loads and refreshes automatically every 5 min.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st

try:
    from dotenv import load_dotenv
    for _p in [Path(__file__).resolve().parent.parent,
               Path(__file__).resolve().parent.parent.parent]:
        _env = _p / ".env"
        if _env.exists():
            load_dotenv(_env)
            break
except Exception:
    pass

# ── DB connection ──────────────────────────────────────────────────────────────

def _get_db_url() -> str:
    url = (
        os.getenv("DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("DB_DSN")
        or os.getenv("MARKETDATA_DB_URL")
    )
    if url and url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    if url:
        return url
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "5433")
    name = os.getenv("DB_NAME", "marketdata")
    user = os.getenv("DB_USER", "postgres")
    pw   = os.getenv("DB_PASSWORD", "root")
    return f"postgresql://{user}:{pw}@{host}:{port}/{name}"


@st.cache_data(ttl=300, show_spinner=False)
def _load_last_n_dates(n: int = 3) -> list[dt.date]:
    sql = """
        SELECT DISTINCT report_date
        FROM spot_daily
        ORDER BY report_date DESC
        LIMIT %s
    """
    with psycopg2.connect(_get_db_url()) as conn, conn.cursor() as cur:
        cur.execute(sql, (n,))
        return [r[0] for r in cur.fetchall()]


@st.cache_data(ttl=300, show_spinner=False)
def _load_prices(dates: tuple[dt.date, ...]) -> pd.DataFrame:
    if not dates:
        return pd.DataFrame()
    placeholders = ",".join(["%s"] * len(dates))
    sql = f"""
        SELECT report_date, province_en,
               da_avg, da_max, da_min,
               rt_avg, rt_max, rt_min
        FROM spot_daily
        WHERE report_date IN ({placeholders})
        ORDER BY report_date DESC, province_en
    """
    with psycopg2.connect(_get_db_url()) as conn, conn.cursor() as cur:
        cur.execute(sql, list(dates))
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=[
        "report_date", "province_en",
        "da_avg", "da_max", "da_min",
        "rt_avg", "rt_max", "rt_min",
    ])


@st.cache_data(ttl=300, show_spinner=False)
def _load_highlights(date: dt.date) -> pd.DataFrame:
    sql = """
        SELECT province_en, highlights
        FROM spot_daily
        WHERE report_date = %s
          AND highlights IS NOT NULL
          AND highlights <> ''
        ORDER BY province_en
    """
    with psycopg2.connect(_get_db_url()) as conn, conn.cursor() as cur:
        cur.execute(sql, (date,))
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["province_en", "highlights"])


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _fmt(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{float(v):,.1f}"


def _price_table(df_date: pd.DataFrame) -> pd.DataFrame:
    """Return a display-ready DataFrame for one date."""
    out = df_date[["province_en",
                   "da_avg", "da_max", "da_min",
                   "rt_avg", "rt_max", "rt_min"]].copy()
    for col in ["da_avg", "da_max", "da_min", "rt_avg", "rt_max", "rt_min"]:
        out[col] = out[col].apply(
            lambda v: float(v) if v is not None else None
        )
    out = out.rename(columns={
        "province_en": "Province",
        "da_avg": "DA Avg", "da_max": "DA Max", "da_min": "DA Min",
        "rt_avg": "RT Avg", "rt_max": "RT Max", "rt_min": "RT Min",
    })
    return out.reset_index(drop=True)


# ── Page ───────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="China Spot Market", layout="wide")

st.title("China Spot Power Market")
st.caption("DA/RT daily prices — last 3 report dates · auto-refreshes every 5 min")

# ── Load data ──────────────────────────────────────────────────────────────────

try:
    recent_dates = _load_last_n_dates(3)
except Exception as e:
    st.error(f"Cannot connect to database: {e}")
    st.stop()

if not recent_dates:
    st.warning("No data in spot_daily. Run spot_ingest.py to populate the database.")
    st.stop()

try:
    df_all = _load_prices(tuple(recent_dates))
except Exception as e:
    st.error(f"Failed to load price data: {e}")
    st.stop()

most_recent = recent_dates[0]

# ── Section 1: Prices ─────────────────────────────────────────────────────────

st.subheader(f"Daily Prices — Last {len(recent_dates)} Report Dates (CNY / MWh)")

tab_labels = [d.strftime("%Y-%m-%d") for d in recent_dates]
tabs = st.tabs(tab_labels)

for tab, date in zip(tabs, recent_dates):
    with tab:
        df_date = df_all[df_all["report_date"] == date]
        if df_date.empty:
            st.info("No data for this date.")
            continue

        tbl = _price_table(df_date)

        # Metric row: national averages across all provinces for this date
        def _avg(col):
            s = tbl[col].dropna()
            return f"{s.mean():,.1f}" if not s.empty else "—"

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Provinces reporting", len(tbl))
        c2.metric("DA Avg (all prov.)", _avg("DA Avg"))
        c3.metric("RT Avg (all prov.)", _avg("RT Avg"))
        c4.metric("RT Max", _avg("RT Max"))

        st.dataframe(
            tbl.style.format(
                {c: "{:.1f}" for c in ["DA Avg", "DA Max", "DA Min",
                                        "RT Avg", "RT Max", "RT Min"]},
                na_rep="—",
            ).background_gradient(
                subset=["DA Avg", "RT Avg"], cmap="RdYlGn_r", axis=0
            ),
            use_container_width=True,
            hide_index=True,
        )

        # RT avg bar chart
        chart_df = tbl[["Province", "RT Avg"]].dropna().set_index("Province")
        if not chart_df.empty:
            st.caption("RT Average Price by Province")
            st.bar_chart(chart_df, height=250)

# ── Section 2: Highlights ─────────────────────────────────────────────────────

st.divider()
st.subheader(f"Market Highlights — {most_recent.strftime('%Y-%m-%d')} (Most Recent)")

try:
    df_hi = _load_highlights(most_recent)
except Exception as e:
    st.warning(f"Could not load highlights: {e}")
    df_hi = pd.DataFrame()

if df_hi.empty:
    st.info(
        "No highlights available for this date. "
        "Re-run ingestion with LLM enabled (remove `--no-llm`) "
        "and use `--force` to re-process already-ingested files:\n\n"
        "```\npython agent/spot_ingest.py --header agent/spot_header_bess.yaml --force\n```"
    )
else:
    cols = st.columns(3)
    for i, row in df_hi.iterrows():
        with cols[i % 3]:
            with st.container(border=True):
                st.markdown(f"**{row['province_en']}**")
                st.write(row["highlights"])

# ── Footer ────────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    f"Data: `spot_daily` table · "
    f"Updated: {dt.datetime.now().strftime('%Y-%m-%d %H:%M')} · "
    f"Hourly data not available (PDFs contain chart images, not tables)"
)
