"""
Mengxi 15-min Market Data Dashboard
Displays time-series from 21 public.hist_mengxi_*_15min tables.

Run:
    PGURL=postgresql://... streamlit run app.py
    or source config/.env && streamlit run app.py
"""
from __future__ import annotations

import os
from datetime import date, timedelta, datetime

import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Mengxi Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
@st.cache_resource
def get_conn():
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        st.error("PGURL environment variable is not set.")
        st.stop()
    return psycopg2.connect(
        url,
        keepalives=1,
        keepalives_idle=60,
        keepalives_interval=10,
        keepalives_count=5,
    )


# ---------------------------------------------------------------------------
# Table catalogue — grouped by theme, paired forecast/actual where applicable
# ---------------------------------------------------------------------------
GROUPS: dict[str, list[dict]] = {
    "Clearing Prices (CNY/MWh)": [
        {"table": "hist_mengxi_provincerealtimeclearprice_15min",    "label": "Province RT Clear",       "style": "solid",  "color": "#1f77b4"},
        {"table": "hist_mengxi_provincerealtimepriceforecast_15min", "label": "Province RT Forecast",    "style": "dash",   "color": "#1f77b4"},
        {"table": "hist_mengxi_hubaodongrealtimeclearprice_15min",   "label": "HuBaoDong RT Clear",      "style": "solid",  "color": "#ff7f0e"},
        {"table": "hist_mengxi_hubaodongrealtimepriceforecast_15min","label": "HuBaoDong RT Forecast",   "style": "dash",   "color": "#ff7f0e"},
        {"table": "hist_mengxi_hubaoxirealtimeclearprice_15min",     "label": "HuBaoXi RT Clear",        "style": "solid",  "color": "#2ca02c"},
        {"table": "hist_mengxi_hubaoxirealtimepriceforecast_15min",  "label": "HuBaoXi RT Forecast",     "style": "dash",   "color": "#2ca02c"},
    ],
    "New Energy Generation (MW)": [
        {"table": "hist_mengxi_newenergyreal_15min",      "label": "New Energy Real",         "style": "solid", "color": "#1f77b4"},
        {"table": "hist_mengxi_newenergyforecast_15min",  "label": "New Energy Forecast",     "style": "dash",  "color": "#1f77b4"},
        {"table": "hist_mengxi_solarpowerreal_15min",     "label": "Solar Real",              "style": "solid", "color": "#ff7f0e"},
        {"table": "hist_mengxi_solarpowerforecast_15min", "label": "Solar Forecast",          "style": "dash",  "color": "#ff7f0e"},
        {"table": "hist_mengxi_windpowerreal_15min",      "label": "Wind Real",               "style": "solid", "color": "#2ca02c"},
        {"table": "hist_mengxi_windpowerforecast_15min",  "label": "Wind Forecast",           "style": "dash",  "color": "#2ca02c"},
        {"table": "hist_mengxi_inhouse_windforecast_15min","label": "In-House Wind Forecast", "style": "dot",   "color": "#9467bd"},
    ],
    "Power Balance & Market (MW)": [
        {"table": "hist_mengxi_loadregulationreal_15min",      "label": "Load Regulation Real",    "style": "solid", "color": "#1f77b4"},
        {"table": "hist_mengxi_loadregulationforecast_15min",  "label": "Load Regulation Forecast","style": "dash",  "color": "#1f77b4"},
        {"table": "hist_mengxi_notmarketpowerreal_15min",      "label": "Non-Market Power Real",   "style": "solid", "color": "#d62728"},
        {"table": "hist_mengxi_notmarketpowerforecast_15min",  "label": "Non-Market Power Forecast","style":"dash",  "color": "#d62728"},
    ],
    "Capacity Plans (MW)": [
        {"table": "hist_mengxi_biddingspacereal_15min",       "label": "Bidding Space Real",      "style": "solid", "color": "#1f77b4"},
        {"table": "hist_mengxi_biddingspaceforecast_15min",   "label": "Bidding Space Forecast",  "style": "dash",  "color": "#1f77b4"},
        {"table": "hist_mengxi_eastwardplanreal_15min",       "label": "Eastward Plan Real",      "style": "solid", "color": "#ff7f0e"},
        {"table": "hist_mengxi_eastwardplanforecast_15min",   "label": "Eastward Plan Forecast",  "style": "dash",  "color": "#ff7f0e"},
    ],
}

DASH_MAP = {"solid": None, "dash": "dash", "dot": "dot"}

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def load_series(table: str, start: date, end: date, freq: str) -> pd.DataFrame:
    conn = get_conn()
    try:
        conn.cursor().execute("SELECT 1")
    except Exception:
        get_conn.clear()
        conn = get_conn()

    if freq == "15min":
        q = """
            SELECT time, price
            FROM public.{t}
            WHERE time >= %s AND time < %s
            ORDER BY time
        """.format(t=table)
        params = (start, end + timedelta(days=1))
    else:
        pg_trunc = "hour" if freq == "hourly" else "day"
        q = """
            SELECT date_trunc(%s, time) AS time, AVG(price) AS price
            FROM public.{t}
            WHERE time >= %s AND time < %s
            GROUP BY 1
            ORDER BY 1
        """.format(t=table)
        params = (pg_trunc, start, end + timedelta(days=1))

    try:
        df = pd.read_sql(q, conn, params=params, parse_dates=["time"])
        return df
    except Exception:
        return pd.DataFrame(columns=["time", "price"])


def make_chart(group_name: str, series_defs: list[dict],
               start: date, end: date, freq: str,
               height: int, selected: list[str]) -> go.Figure:
    fig = go.Figure()

    for s in series_defs:
        if s["label"] not in selected:
            continue
        df = load_series(s["table"], start, end, freq)
        if df.empty:
            continue
        fig.add_trace(go.Scatter(
            x=df["time"],
            y=df["price"],
            name=s["label"],
            mode="lines",
            line=dict(
                color=s["color"],
                dash=DASH_MAP[s["style"]],
                width=1.5 if s["style"] == "solid" else 1,
            ),
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>%{y:,.2f}<extra>" + s["label"] + "</extra>",
        ))

    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
        xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="x unified",
    )
    return fig


# ---------------------------------------------------------------------------
# Tab layout
# ---------------------------------------------------------------------------
tab_market, tab_cockpit = st.tabs(["Market Data", "Options Cockpit"])

# ---------------------------------------------------------------------------
# Sidebar controls (market data tab)
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("⚡ Mengxi Dashboard")
    st.markdown("---")

    st.subheader("Date Range")
    preset = st.selectbox(
        "Preset",
        ["Last 7 days", "Last 30 days", "Last 90 days", "Last 180 days", "Custom"],
        index=1,
    )
    today = date.today()
    if preset == "Last 7 days":
        default_start, default_end = today - timedelta(days=7), today
    elif preset == "Last 30 days":
        default_start, default_end = today - timedelta(days=30), today
    elif preset == "Last 90 days":
        default_start, default_end = today - timedelta(days=90), today
    elif preset == "Last 180 days":
        default_start, default_end = today - timedelta(days=180), today
    else:
        default_start, default_end = today - timedelta(days=30), today

    if preset == "Custom":
        start_date = st.date_input("Start date", value=default_start)
        end_date   = st.date_input("End date",   value=default_end)
    else:
        start_date, end_date = default_start, default_end
        st.caption(f"{start_date} → {end_date}")

    st.markdown("---")
    st.subheader("Display")
    freq = st.radio("Granularity", ["15min", "hourly", "daily"], index=0, horizontal=True)
    chart_height = st.slider("Chart height", 250, 700, 380, step=50)

    st.markdown("---")
    st.subheader("Series visibility")
    series_toggles: dict[str, list[str]] = {}
    for group_name, series_defs in GROUPS.items():
        with st.expander(group_name, expanded=False):
            selected = []
            for s in series_defs:
                checked = st.checkbox(s["label"], value=True, key=f"chk_{s['table']}")
                if checked:
                    selected.append(s["label"])
            series_toggles[group_name] = selected

    st.markdown("---")
    st.caption(f"Data source: `public.hist_mengxi_*_15min`  \nRefresh: every 5 min")

# ---------------------------------------------------------------------------
# Tab 1: Market Data
# ---------------------------------------------------------------------------
with tab_market:
    st.title("⚡ Mengxi 15-min Market Data Dashboard")
    st.caption(
        f"Period: **{start_date}** → **{end_date}** | Granularity: **{freq}** | "
        f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )

    # Data availability check
    _probe_table = "hist_mengxi_provincerealtimeclearprice_15min"
    _probe_df = load_series(_probe_table, start_date, end_date, "15min")
    if _probe_df.empty:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT MAX(time) FROM public.{_probe_table}")
            latest = cur.fetchone()[0]
            cur.close()
        except Exception:
            latest = None
        if latest:
            st.warning(
                f"No data for {start_date} → {end_date}. "
                f"Latest row in DB: **{latest.strftime('%Y-%m-%d %H:%M')}**. "
                f"Try adjusting the date range in the sidebar."
            )
        else:
            st.error(f"Table `{_probe_table}` appears to be empty or unreachable.")

    for group_name, series_defs in GROUPS.items():
        selected = series_toggles.get(group_name, [s["label"] for s in series_defs])
        if not selected:
            continue

        with st.expander(f"**{group_name}**", expanded=True):
            with st.spinner(f"Loading {group_name}..."):
                fig = make_chart(
                    group_name, series_defs,
                    start_date, end_date, freq,
                    chart_height, selected,
                )
            st.plotly_chart(fig, width="stretch", config={"displayModeBar": True}, key=f"chart_{group_name}")

            # Quick data freshness note
            freshest = None
            for s in series_defs:
                if s["label"] not in selected:
                    continue
                df = load_series(s["table"], start_date, end_date, freq)
                if not df.empty:
                    mx = df["time"].max()
                    if freshest is None or mx > freshest:
                        freshest = mx
            if freshest is not None:
                lag = (datetime.now() - pd.Timestamp(freshest)).days
                badge = "🟢" if lag <= 1 else ("🟡" if lag <= 7 else "🔴")
                st.caption(f"{badge} Latest data point: **{freshest.strftime('%Y-%m-%d %H:%M')}** ({lag}d ago)")

    st.markdown("---")
    with st.expander("🗄️ Raw data export", expanded=False):
        all_labels = [(s["table"], s["label"]) for defs in GROUPS.values() for s in defs]
        chosen_label = st.selectbox("Series", [lbl for _, lbl in all_labels])
        chosen_table = next(t for t, lbl in all_labels if lbl == chosen_label)
        df_raw = load_series(chosen_table, start_date, end_date, "15min")
        st.dataframe(df_raw, width="stretch", height=300)
        if not df_raw.empty:
            csv = df_raw.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇ Download CSV",
                data=csv,
                file_name=f"{chosen_table}_{start_date}_{end_date}.csv",
                mime="text/csv",
            )

# ---------------------------------------------------------------------------
# Tab 2: Options Cockpit
# ---------------------------------------------------------------------------
with tab_cockpit:
    import sys
    import os as _os
    # Ensure repo root is importable when running from apps/mengxi-dashboard/
    _repo_root = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", ".."))
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)

    from libs.decision_models.adapters.app.cockpit_page import render_cockpit_page
    render_cockpit_page()
