"""
Pillar 3 — Mengxi BESS Trading Operations Dashboard

Purpose: Break down BESS revenue into attribution components across the full
dispatch chain, from perfect-foresight upper bound to actual cleared dispatch.

P&L Waterfall:
  PF Unrestricted (true upper bound)
  → [grid restriction loss]
  PF Grid-Feasible
  → [forecast error loss]
  Forecast Optimal (LP on forecast prices)
  → [nomination gap loss]
  Nomination P&L (申报曲线 × nodal price)
  → [market clearing loss]
  Trading Cleared (md_id_cleared_energy × cleared price)
  → [execution loss]
  Actual Cleared (实际充放曲线 × nodal price)

Tabs:
  1. Market Data          — provincial RT prices, wind/solar, load, capacity
  2. Dispatch & P&L       — hero: 5-step P&L waterfall + dispatch chart
  3. Daily Ops            — 4-asset daily strategy comparison + LP benchmark
  4. Strategy Comparison  — multi-day strategy analysis + YTD reporting
  5. Options Cockpit      — spread call strip valuation + realization overlay

Run locally:
  set -a && source config/.env && set +a
  streamlit run apps/mengxi-dashboard/app.py --server.port 8511
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta, datetime

import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

# ---------------------------------------------------------------------------
# Ensure repo root is importable
# ---------------------------------------------------------------------------
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Mengxi BESS Trading Ops",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# DB connection (psycopg2 — for Market Data tab legacy queries)
# ---------------------------------------------------------------------------
@st.cache_resource
def _get_pg_conn():
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
# Market Data — table catalogue (provincial fundamentals)
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
        {"table": "hist_mengxi_newenergyreal_15min",       "label": "New Energy Real",         "style": "solid", "color": "#1f77b4"},
        {"table": "hist_mengxi_newenergyforecast_15min",   "label": "New Energy Forecast",     "style": "dash",  "color": "#1f77b4"},
        {"table": "hist_mengxi_solarpowerreal_15min",      "label": "Solar Real",              "style": "solid", "color": "#ff7f0e"},
        {"table": "hist_mengxi_solarpowerforecast_15min",  "label": "Solar Forecast",          "style": "dash",  "color": "#ff7f0e"},
        {"table": "hist_mengxi_windpowerreal_15min",       "label": "Wind Real",               "style": "solid", "color": "#2ca02c"},
        {"table": "hist_mengxi_windpowerforecast_15min",   "label": "Wind Forecast",           "style": "dash",  "color": "#2ca02c"},
        {"table": "hist_mengxi_inhouse_windforecast_15min","label": "In-House Wind Forecast",  "style": "dot",   "color": "#9467bd"},
    ],
    "Power Balance & Market (MW)": [
        {"table": "hist_mengxi_loadregulationreal_15min",      "label": "Load Regulation Real",     "style": "solid", "color": "#1f77b4"},
        {"table": "hist_mengxi_loadregulationforecast_15min",  "label": "Load Regulation Forecast", "style": "dash",  "color": "#1f77b4"},
        {"table": "hist_mengxi_notmarketpowerreal_15min",      "label": "Non-Market Power Real",    "style": "solid", "color": "#d62728"},
        {"table": "hist_mengxi_notmarketpowerforecast_15min",  "label": "Non-Market Power Forecast","style": "dash",  "color": "#d62728"},
    ],
    "Capacity Plans (MW)": [
        {"table": "hist_mengxi_biddingspacereal_15min",     "label": "Bidding Space Real",     "style": "solid", "color": "#1f77b4"},
        {"table": "hist_mengxi_biddingspaceforecast_15min", "label": "Bidding Space Forecast", "style": "dash",  "color": "#1f77b4"},
        {"table": "hist_mengxi_eastwardplanreal_15min",     "label": "Eastward Plan Real",     "style": "solid", "color": "#ff7f0e"},
        {"table": "hist_mengxi_eastwardplanforecast_15min", "label": "Eastward Plan Forecast", "style": "dash",  "color": "#ff7f0e"},
    ],
}

DASH_MAP = {"solid": None, "dash": "dash", "dot": "dot"}


@st.cache_data(ttl=300, show_spinner=False)
def _load_market_series(table: str, start: date, end: date, freq: str) -> pd.DataFrame:
    try:
        conn = _get_pg_conn()
    except Exception:
        return pd.DataFrame(columns=["time", "price"])
    try:
        conn.cursor().execute("SELECT 1")
    except Exception:
        _get_pg_conn.clear()
        try:
            conn = _get_pg_conn()
        except Exception:
            return pd.DataFrame(columns=["time", "price"])

    if freq == "15min":
        q = "SELECT time, price FROM public.{t} WHERE time >= %s AND time < %s ORDER BY time".format(t=table)
        params = (start, end + timedelta(days=1))
    else:
        pg_trunc = "hour" if freq == "hourly" else "day"
        q = ("SELECT date_trunc(%s, time) AS time, AVG(price) AS price "
             "FROM public.{t} WHERE time >= %s AND time < %s GROUP BY 1 ORDER BY 1").format(t=table)
        params = (pg_trunc, start, end + timedelta(days=1))

    try:
        return pd.read_sql(q, conn, params=params, parse_dates=["time"])
    except Exception:
        return pd.DataFrame(columns=["time", "price"])


def _make_market_chart(
    group_name: str,
    series_defs: list[dict],
    start: date,
    end: date,
    freq: str,
    height: int,
    selected: list[str],
) -> go.Figure:
    fig = go.Figure()
    for s in series_defs:
        if s["label"] not in selected:
            continue
        df = _load_market_series(s["table"], start, end, freq)
        if df.empty:
            continue
        fig.add_trace(go.Scatter(
            x=df["time"],
            y=df["price"],
            name=s["label"],
            mode="lines",
            line=dict(color=s["color"], dash=DASH_MAP[s["style"]],
                      width=1.5 if s["style"] == "solid" else 1),
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
# Asset constants (4 IM assets)
# ---------------------------------------------------------------------------
_IM_ASSET_CODES = ["suyou", "hangjinqi", "siziwangqi", "gushanliang"]
_IM_ASSET_DISPLAY = {
    "suyou":       "SuYou (景蓝乌尔图)",
    "hangjinqi":   "HangJinQi (悦杭独贵)",
    "siziwangqi":  "SiZiWangQi (景通四益堂储)",
    "gushanliang": "GuShanLiang (裕昭沙子坝)",
}

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("⚡ Mengxi BESS Trading Ops")
    st.caption("Pillar 3 — Asset Operations & Portfolio Optimisation")
    st.markdown("---")

    # Global asset + date controls (used by Dispatch & P&L tab)
    st.subheader("Asset & Date")
    selected_asset = st.selectbox(
        "Asset",
        _IM_ASSET_CODES,
        format_func=lambda x: _IM_ASSET_DISPLAY.get(x, x),
        key="sidebar_asset",
    )
    today = date.today()
    selected_date = st.date_input(
        "Trading date",
        value=today - timedelta(days=1),
        key="sidebar_date",
    )

    st.markdown("---")

    # Market Data controls (shown for reference)
    st.subheader("Market Data Range")
    preset = st.selectbox(
        "Preset",
        ["Last 7 days", "Last 30 days", "Last 90 days", "Last 180 days", "Custom"],
        index=1,
        key="mkt_preset",
    )
    if preset == "Last 7 days":
        mkt_start, mkt_end = today - timedelta(days=7), today
    elif preset == "Last 30 days":
        mkt_start, mkt_end = today - timedelta(days=30), today
    elif preset == "Last 90 days":
        mkt_start, mkt_end = today - timedelta(days=90), today
    elif preset == "Last 180 days":
        mkt_start, mkt_end = today - timedelta(days=180), today
    else:
        mkt_start, mkt_end = today - timedelta(days=30), today

    if preset == "Custom":
        mkt_start = st.date_input("Start date", value=mkt_start, key="mkt_start")
        mkt_end   = st.date_input("End date",   value=mkt_end,   key="mkt_end")
    else:
        st.caption(f"{mkt_start} → {mkt_end}")

    mkt_freq = st.radio("Granularity", ["15min", "hourly", "daily"], index=0,
                        horizontal=True, key="mkt_freq")
    mkt_chart_height = st.slider("Chart height", 250, 700, 380, step=50, key="mkt_height")

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
    st.caption(f"Data: `public.hist_mengxi_*_15min`  |  Refresh: 5 min")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_market, tab_dispatch_pnl, tab_daily_ops, tab_strategy, tab_cockpit = st.tabs([
    "Market Data",
    "Dispatch & P&L Waterfall",
    "Daily Ops",
    "Strategy Comparison",
    "Options Cockpit",
])

# ---------------------------------------------------------------------------
# Tab 1: Market Data
# ---------------------------------------------------------------------------
with tab_market:
    st.title("Mengxi Provincial Market Data")
    st.caption(
        f"Period: **{mkt_start}** → **{mkt_end}** | Granularity: **{mkt_freq}** | "
        f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )

    _probe_table = "hist_mengxi_provincerealtimeclearprice_15min"
    _db_error: str | None = None
    try:
        _get_pg_conn()
    except Exception as _e:
        _db_error = str(_e)

    if _db_error:
        st.error(
            "**Database unreachable.** Check network / VPN, then refresh.\n\n"
            f"```\n{_db_error}\n```"
        )
    else:
        _probe_df = _load_market_series(_probe_table, mkt_start, mkt_end, "15min")
        if _probe_df.empty:
            try:
                _cur = _get_pg_conn().cursor()
                _cur.execute(f"SELECT MAX(time) FROM public.{_probe_table}")
                _latest = _cur.fetchone()[0]
                _cur.close()
            except Exception:
                _latest = None
            if _latest:
                st.warning(
                    f"No data for {mkt_start} → {mkt_end}. "
                    f"Latest in DB: **{_latest.strftime('%Y-%m-%d %H:%M')}**. "
                    "Adjust the date range in the sidebar."
                )

    for group_name, series_defs in GROUPS.items():
        selected = series_toggles.get(group_name, [s["label"] for s in series_defs])
        if not selected:
            continue
        with st.expander(f"**{group_name}**", expanded=True):
            with st.spinner(f"Loading {group_name}…"):
                fig = _make_market_chart(
                    group_name, series_defs, mkt_start, mkt_end,
                    mkt_freq, mkt_chart_height, selected,
                )
            st.plotly_chart(fig, use_container_width=True,
                            config={"displayModeBar": True},
                            key=f"chart_{group_name}")
            freshest = None
            for s in series_defs:
                if s["label"] not in selected:
                    continue
                df = _load_market_series(s["table"], mkt_start, mkt_end, mkt_freq)
                if not df.empty:
                    mx = df["time"].max()
                    if freshest is None or mx > freshest:
                        freshest = mx
            if freshest is not None:
                lag = (datetime.now() - pd.Timestamp(freshest)).days
                badge = "🟢" if lag <= 1 else ("🟡" if lag <= 7 else "🔴")
                st.caption(f"{badge} Latest: **{freshest.strftime('%Y-%m-%d %H:%M')}** ({lag}d ago)")

    st.markdown("---")
    with st.expander("🗄️ Raw data export", expanded=False):
        all_labels = [(s["table"], s["label"]) for defs in GROUPS.values() for s in defs]
        chosen_label = st.selectbox("Series", [lbl for _, lbl in all_labels])
        chosen_table = next(t for t, lbl in all_labels if lbl == chosen_label)
        df_raw = _load_market_series(chosen_table, mkt_start, mkt_end, "15min")
        st.dataframe(df_raw, use_container_width=True, height=300)
        if not df_raw.empty:
            c1, c2 = st.columns(2)
            c1.download_button(
                "⬇ Download CSV",
                data=df_raw.to_csv(index=False).encode("utf-8"),
                file_name=f"{chosen_table}_{mkt_start}_{mkt_end}.csv",
                mime="text/csv",
            )
        try:
            import io as _io
            sheets = {}
            for group_name, series_defs in GROUPS.items():
                sel_in_group = series_toggles.get(group_name, [])
                frames = []
                for s in series_defs:
                    if s["label"] not in sel_in_group:
                        continue
                    sdf = _load_market_series(s["table"], mkt_start, mkt_end, "15min")
                    if not sdf.empty:
                        frames.append(sdf.rename(columns={"price": s["label"]}))
                if frames:
                    merged = frames[0]
                    for gf in frames[1:]:
                        merged = merged.merge(gf, on="time", how="outer")
                    sheets[group_name[:31]] = merged.sort_values("time")
            if sheets:
                buf = _io.BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as _writer:
                    for sname, sdf in sheets.items():
                        sdf.to_excel(_writer, sheet_name=sname, index=False)
                c2.download_button(
                    "⬇ Download All Visible (Excel)",
                    data=buf.getvalue(),
                    file_name=f"market_data_{mkt_start}_{mkt_end}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
        except Exception as _exc:
            st.caption(f"Excel export error: {_exc}")

# ---------------------------------------------------------------------------
# Tab 2: Dispatch & P&L Waterfall — HERO TAB
# ---------------------------------------------------------------------------
with tab_dispatch_pnl:
    from libs.decision_models.adapters.app.dispatch_pnl_page import render_dispatch_pnl_page
    render_dispatch_pnl_page(selected_asset, selected_date)

# ---------------------------------------------------------------------------
# Tab 3: Daily Ops
# ---------------------------------------------------------------------------
with tab_daily_ops:
    from libs.decision_models.adapters.app.daily_ops_page import render_daily_ops_page
    render_daily_ops_page()

# ---------------------------------------------------------------------------
# Tab 4: Strategy Comparison
# ---------------------------------------------------------------------------
with tab_strategy:
    from libs.decision_models.adapters.app.strategy_comparison_page import render_strategy_comparison_page
    render_strategy_comparison_page()

# ---------------------------------------------------------------------------
# Tab 5: Options Cockpit
# ---------------------------------------------------------------------------
with tab_cockpit:
    from libs.decision_models.adapters.app.cockpit_page import render_cockpit_page
    render_cockpit_page()
