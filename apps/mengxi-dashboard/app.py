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


@st.cache_resource
def _get_sqlalchemy_engine():
    from sqlalchemy import create_engine
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        st.error("PGURL environment variable is not set.")
        st.stop()
    return create_engine(url, pool_pre_ping=True)


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
# Data Management — monitored tables + query helpers
# ---------------------------------------------------------------------------

# (display_name, fully_qualified_table, date_column, group)
_MONITORED_TABLES = [
    ("md_id_cleared_energy",         "marketdata.md_id_cleared_energy",         "data_date",  "Ingestion"),
    ("md_rt_nodal_price",            "marketdata.md_rt_nodal_price",            "data_date",  "Ingestion"),
    ("md_da_cleared_energy",         "marketdata.md_da_cleared_energy",         "data_date",  "Ingestion"),
    ("md_rt_total_cleared_energy",   "marketdata.md_rt_total_cleared_energy",   "data_date",  "Ingestion"),
    ("md_id_fuel_summary",           "marketdata.md_id_fuel_summary",           "data_date",  "Ingestion"),
    ("md_da_fuel_summary",           "marketdata.md_da_fuel_summary",           "data_date",  "Ingestion"),
    ("md_avg_bid_price",             "marketdata.md_avg_bid_price",             "data_date",  "Ingestion"),
    ("md_settlement_ref_price",      "marketdata.md_settlement_ref_price",      "data_date",  "Ingestion"),
    ("ops_bess_dispatch_15min",      "marketdata.ops_bess_dispatch_15min",      "data_date",  "Ops"),
    ("nodal_rt_price_15min",         "canon.nodal_rt_price_15min",              "time::date", "Canon"),
    ("bess_asset_daily_attribution", "reports.bess_asset_daily_attribution",    "trade_date", "Reports"),
]


def _stale_badge(days_stale):
    if days_stale is None:
        return "🔴 No data"
    if days_stale <= 2:
        return f"🟢 {days_stale}d"
    if days_stale <= 7:
        return f"🟡 {days_stale}d"
    return f"🔴 {days_stale}d"


@st.cache_data(ttl=60, show_spinner=False)
def _load_table_freshness() -> pd.DataFrame:
    today_d = date.today()
    try:
        conn = _get_pg_conn()
    except Exception:
        return pd.DataFrame()
    rows = []
    for name, fqn, date_col, group in _MONITORED_TABLES:
        try:
            df = pd.read_sql(f"SELECT MAX({date_col}) AS latest_date FROM {fqn}", conn)
            latest = df["latest_date"].iloc[0]
            if latest is not None:
                latest = pd.Timestamp(latest).date()
                days_stale = (today_d - latest).days
            else:
                latest, days_stale = None, None
        except Exception:
            latest, days_stale = None, None
        rows.append({
            "Group": group,
            "Table": name,
            "Latest Date": str(latest) if latest else "—",
            "Staleness": _stale_badge(days_stale),
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=60, show_spinner=False)
def _load_quality_status(days: int = 60) -> pd.DataFrame:
    try:
        conn = _get_pg_conn()
        df = pd.read_sql(
            """
            SELECT
                data_date,
                CASE WHEN is_complete THEN '🟢 Complete' ELSE '🔴 Incomplete' END AS status,
                ROUND(interval_coverage * 100, 1)  AS "coverage_%",
                actual_intervals                    AS intervals,
                ROUND(file_size_mb, 1)              AS "size_mb",
                TO_CHAR(check_time, 'MM-DD HH24:MI') AS checked,
                LEFT(notes, 120)                    AS notes
            FROM marketdata.data_quality_status
            WHERE province = 'mengxi'
              AND data_date >= CURRENT_DATE - %s
            ORDER BY data_date DESC
            """,
            conn,
            params=(days,),
        )
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _load_gb_daily_index(start_date: str, end_date: str) -> pd.DataFrame:
    try:
        from sqlalchemy import text as _sql_text
        return pd.read_sql(
            _sql_text("""
                SELECT settlement_date, duration, market,
                       revenue_permw, revenue_permwh
                FROM intl_market.gb_bess_daily_index
                WHERE settlement_date >= :start AND settlement_date <= :end
                ORDER BY settlement_date, market
            """),
            _get_sqlalchemy_engine(),
            params={"start": start_date, "end": end_date},
            parse_dates=["settlement_date"],
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _load_gb_monthly_index(month_from: str, month_to: str) -> pd.DataFrame:
    try:
        from sqlalchemy import text as _sql_text
        return pd.read_sql(
            _sql_text("""
                SELECT month, duration, market,
                       revenue_permw, revenue_permwh
                FROM intl_market.gb_bess_monthly_index
                WHERE month >= :mfrom AND month <= :mto
                ORDER BY month, market
            """),
            _get_sqlalchemy_engine(),
            params={"mfrom": month_from, "mto": month_to},
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _load_gb_leaderboard(start_date: str, end_date: str, market: str | None = None) -> pd.DataFrame:
    try:
        from sqlalchemy import text as _sql_text
        where = "settlement_date >= :start AND settlement_date <= :end"
        params: dict = {"start": start_date, "end": end_date}
        if market:
            where += " AND market = :market"
            params["market"] = market
        return pd.read_sql(
            _sql_text(f"""
                SELECT asset, market,
                       SUM(revenue)    AS total_revenue,
                       AVG(revspermw)  AS avg_revspermw,
                       COUNT(DISTINCT settlement_date) AS trading_days
                FROM intl_market.gb_bess_leaderboard
                WHERE {where}
                GROUP BY asset, market
                ORDER BY avg_revspermw DESC
                LIMIT 50
            """),
            _get_sqlalchemy_engine(),
            params=params,
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def _load_load_log(n: int = 50) -> pd.DataFrame:
    _badge = {"success": "🟢 success", "partial_success": "🟡 partial", "failed": "🔴 failed", "skipped": "⚪ skipped"}
    try:
        conn = _get_pg_conn()
        df = pd.read_sql(
            f"""
            SELECT
                file_date,
                status,
                TO_CHAR(loaded_at, 'MM-DD HH24:MI') AS loaded_at,
                file_name,
                LEFT(message, 200)                   AS message
            FROM marketdata.md_load_log
            ORDER BY loaded_at DESC
            LIMIT {n}
            """,
            conn,
        )
        if not df.empty:
            df["status"] = df["status"].map(lambda s: _badge.get(s, s))
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _load_missing_dates(table_fqn: str, date_col: str, start_date: str) -> tuple:
    """Return (total_weekdays, missing_count, missing_dates_list) for weekdays since start_date."""
    try:
        conn = _get_pg_conn()
        result = pd.read_sql(
            f"""
            WITH weekdays AS (
                SELECT d::date AS dt
                FROM generate_series(%s::date, CURRENT_DATE - 1, interval '1 day') d
                WHERE extract(isodow from d) < 6
            )
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE t.d IS NULL) AS missing
            FROM weekdays w
            LEFT JOIN (SELECT DISTINCT {date_col} AS d FROM {table_fqn}) t ON t.d = w.dt
            """,
            conn,
            params=(start_date,),
        )
        total = int(result["total"].iloc[0])
        missing_count = int(result["missing"].iloc[0])

        if missing_count == 0:
            return total, 0, []

        missing_df = pd.read_sql(
            f"""
            WITH weekdays AS (
                SELECT d::date AS dt
                FROM generate_series(%s::date, CURRENT_DATE - 1, interval '1 day') d
                WHERE extract(isodow from d) < 6
            )
            SELECT w.dt AS missing_date
            FROM weekdays w
            LEFT JOIN (SELECT DISTINCT {date_col} AS d FROM {table_fqn}) t ON t.d = w.dt
            WHERE t.d IS NULL
            ORDER BY w.dt DESC
            """,
            conn,
            params=(start_date,),
        )
        return total, missing_count, [str(d) for d in missing_df["missing_date"]]
    except Exception:
        return 0, 0, []


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
tab_market, tab_dispatch_pnl, tab_daily_ops, tab_strategy, tab_cockpit, tab_data_mgmt, tab_trader = st.tabs([
    "Market Data",
    "Dispatch & P&L Waterfall",
    "Daily Ops",
    "Strategy Comparison",
    "Options Cockpit",
    "Data Management",
    "Trader",
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

# ---------------------------------------------------------------------------
# Tab 6: Data Management
# ---------------------------------------------------------------------------
with tab_data_mgmt:
    st.title("Data Management")
    st.caption("Freshness and quality of all tables feeding this dashboard. Auto-refreshes every 60 s.")

    if st.button("Refresh now", key="dm_refresh"):
        _load_table_freshness.clear()
        _load_quality_status.clear()
        _load_load_log.clear()
        st.rerun()

    # ── Section 1: Table freshness ──────────────────────────────────────────
    st.subheader("Table Freshness")
    st.caption("🟢 ≤2 days  🟡 3–7 days  🔴 >7 days or no data")

    df_fresh = _load_table_freshness()
    if df_fresh.empty:
        st.warning("Could not load freshness data — check DB connection.")
    else:
        for group in ["Ingestion", "Ops", "Canon", "Reports"]:
            sub = df_fresh[df_fresh["Group"] == group].drop(columns=["Group"]).reset_index(drop=True)
            if sub.empty:
                continue
            st.markdown(f"**{group}**")
            st.dataframe(
                sub,
                use_container_width=True,
                hide_index=True,
                height=min(38 * len(sub) + 38, 400),
            )

    st.markdown("---")

    # ── Section 2: Missing dates coverage ───────────────────────────────────
    st.subheader("Ingestion Coverage — Missing Dates")
    st.caption("Counts weekdays (Mon–Fri) since the start date with no row in the selected table.")

    cov_c1, cov_c2, _ = st.columns([3, 2, 5])
    cov_table_label = cov_c1.selectbox(
        "Table",
        ["md_id_cleared_energy", "md_rt_nodal_price", "md_da_cleared_energy",
         "md_rt_total_cleared_energy", "md_id_fuel_summary", "md_da_fuel_summary",
         "md_avg_bid_price", "md_settlement_ref_price"],
        key="dm_cov_table",
    )
    cov_start = cov_c2.date_input("Since", value=date(2026, 1, 1), key="dm_cov_start")

    cov_fqn = f"marketdata.{cov_table_label}"
    total_days, missing_count, missing_dates = _load_missing_dates(cov_fqn, "data_date", str(cov_start))

    if total_days == 0:
        st.warning("Could not query table — it may not exist yet.")
    else:
        present = total_days - missing_count
        pct = present / total_days * 100
        badge = "🟢" if missing_count == 0 else ("🟡" if missing_count <= 5 else "🔴")
        st.metric(
            label=f"{badge} Coverage since {cov_start}",
            value=f"{present} / {total_days} weekdays",
            delta=f"{missing_count} missing" if missing_count else "complete",
            delta_color="inverse" if missing_count else "normal",
        )
        if missing_dates:
            with st.expander(f"Missing dates ({missing_count})", expanded=missing_count <= 20):
                # Show as a compact grid
                chunks = [missing_dates[i:i+7] for i in range(0, len(missing_dates), 7)]
                for chunk in chunks:
                    st.text("  ".join(chunk))

    st.markdown("---")

    # ── Section 3: Data quality status (pipeline-tracked) ───────────────────
    st.subheader("Pipeline Quality Log — Last 60 Days")
    st.caption("Populated only when the new ingestion pipeline version runs. Source: `marketdata.data_quality_status`")

    col_days, _ = st.columns([2, 8])
    quality_days = col_days.number_input("Days to show", min_value=7, max_value=365, value=60, step=7, key="dm_days")

    df_quality = _load_quality_status(int(quality_days))
    if df_quality.empty:
        st.info("No quality records yet — will populate after the next ingestion pipeline run.")
    else:
        st.dataframe(
            df_quality,
            use_container_width=True,
            hide_index=True,
            height=min(38 * len(df_quality) + 38, 600),
            column_config={
                "data_date":   st.column_config.DateColumn("Date",       width="small"),
                "status":      st.column_config.TextColumn("Status",     width="medium"),
                "coverage_%":  st.column_config.NumberColumn("Coverage %", format="%.1f", width="small"),
                "intervals":   st.column_config.NumberColumn("Intervals", width="small"),
                "size_mb":     st.column_config.NumberColumn("Size MB",   format="%.1f", width="small"),
                "checked":     st.column_config.TextColumn("Checked",    width="small"),
                "notes":       st.column_config.TextColumn("Notes",      width="large"),
            },
        )
        n_incomplete = (df_quality["status"].str.startswith("🔴")).sum()
        if n_incomplete:
            st.warning(f"{n_incomplete} incomplete day(s) in the last {quality_days} days.")

    st.markdown("---")

    # ── Section 4: Load log ──────────────────────────────────────────────────
    st.subheader("Load Log — Last 50 Entries")
    st.caption("Source: `marketdata.md_load_log`")

    df_log = _load_load_log()
    if df_log.empty:
        st.info("No load log records found.")
    else:
        st.dataframe(
            df_log,
            use_container_width=True,
            hide_index=True,
            height=min(38 * len(df_log) + 38, 600),
            column_config={
                "file_date":  st.column_config.DateColumn("File Date",  width="small"),
                "status":     st.column_config.TextColumn("Status",     width="medium"),
                "loaded_at":  st.column_config.TextColumn("Loaded At",  width="small"),
                "file_name":  st.column_config.TextColumn("File",       width="medium"),
                "message":    st.column_config.TextColumn("Message",    width="large"),
            },
        )

    st.markdown("---")

    # ── Section 5: Manual file upload & ingest ──────────────────────────────
    st.subheader("Manual File Upload & Ingest")
    st.caption(
        "Upload `YYYY-MM-DD.xlsx` files downloaded manually from the portal. "
        "Each file is parsed and inserted directly into the database."
    )

    uploaded_files = st.file_uploader(
        "Excel files (YYYY-MM-DD.xlsx)",
        type=["xlsx"],
        accept_multiple_files=True,
        key="dm_upload",
    )
    force_reload = st.checkbox(
        "Force reload — delete and replace existing rows for these dates",
        value=True,
        key="dm_force_reload",
        help="Recommended for remediation: ensures the manually downloaded file fully replaces any previously partial data.",
    )

    if uploaded_files:
        st.write(f"{len(uploaded_files)} file(s) selected:")
        for uf in uploaded_files:
            st.text(f"  • {uf.name}  ({uf.size / 1024 / 1024:.1f} MB)")

        if st.button("Ingest files", type="primary", key="dm_ingest_btn"):
            from services.mengxi_ingestion.loader import load_excel_file, ensure_schema_and_log

            engine = _get_sqlalchemy_engine()
            ensure_schema_and_log(engine, "marketdata")

            results = []
            progress_bar = st.progress(0, text="Starting…")

            for i, uf in enumerate(uploaded_files):
                progress_bar.progress((i) / len(uploaded_files), text=f"Loading {uf.name}…")
                file_bytes = uf.read()
                result = load_excel_file(
                    file_bytes=file_bytes,
                    filename=uf.name,
                    engine=engine,
                    schema="marketdata",
                    province="mengxi",
                    force_reload=force_reload,
                )
                results.append(result)

            progress_bar.progress(1.0, text="Done.")

            st.markdown("**Results:**")
            any_success = False
            for r in results:
                if r["status"] == "success":
                    icon, colour = "✅", "success"
                    any_success = True
                elif r["status"] == "partial_success":
                    icon, colour = "⚠️", "warning"
                    any_success = True
                elif r["status"] == "skipped":
                    icon, colour = "⏭️", "info"
                else:
                    icon, colour = "❌", "error"

                label = (
                    f"{icon} **{r['filename']}**"
                    + (f" — {r['file_date']}" if r["file_date"] else "")
                    + f" — `{r['status']}`"
                )
                with st.expander(label, expanded=(r["status"] != "success")):
                    if r["sheets_ok"]:
                        st.success(f"Loaded {len(r['sheets_ok'])} sheet(s): " + ", ".join(r["sheets_ok"]))
                    if r["sheets_failed"]:
                        for err in r["sheets_failed"]:
                            st.error(err)
                    if r["message"] and not r["sheets_ok"]:
                        st.error(r["message"])
                    elif r["message"] and r["sheets_failed"]:
                        st.warning(r["message"])

            if any_success:
                st.info("Upload complete. Click **Refresh now** above to update coverage stats.")

# ---------------------------------------------------------------------------
# Tab 7: Trader
# ---------------------------------------------------------------------------
with tab_trader:
    import anthropic as _ant
    import json as _json

    _TRADER_APP = "mengxi_trader"
    _TRADER_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
    if not _TRADER_API_KEY:
        st.error("ANTHROPIC_API_KEY not set — Trader agent unavailable.")
        st.stop()

    _trader_client = _ant.Anthropic(api_key=_TRADER_API_KEY)

    # ── memory helpers ────────────────────────────────────────────────────────
    @st.cache_resource
    def _ensure_trader_memory_table():
        try:
            from sqlalchemy import text as _sql_text
            with _get_sqlalchemy_engine().begin() as _conn:
                _conn.execute(_sql_text("""
                    CREATE TABLE IF NOT EXISTS marketdata.agent_memory (
                        id       SERIAL PRIMARY KEY,
                        app      TEXT NOT NULL,
                        category TEXT NOT NULL,
                        subject  TEXT NOT NULL,
                        content  TEXT NOT NULL,
                        source   TEXT NOT NULL DEFAULT 'manual',
                        active   BOOLEAN NOT NULL DEFAULT TRUE,
                        saved    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """))
                _conn.execute(_sql_text(
                    "CREATE INDEX IF NOT EXISTS ix_agent_memory_app "
                    "ON marketdata.agent_memory (app)"
                ))
        except Exception:
            pass
        return True

    @st.cache_data(ttl=60)
    def _load_trader_memories() -> pd.DataFrame:
        try:
            from sqlalchemy import text as _sql_text
            return pd.read_sql(
                _sql_text("""
                    SELECT id, category, subject, content, source, saved
                    FROM marketdata.agent_memory
                    WHERE app = :app AND active = TRUE
                    ORDER BY saved DESC
                    LIMIT 100
                """),
                _get_sqlalchemy_engine(),
                params={"app": _TRADER_APP},
            )
        except Exception:
            return pd.DataFrame(columns=["id", "category", "subject", "content", "source", "saved"])

    def _save_trader_memory(category: str, subject: str, content: str, source: str = "auto") -> None:
        from sqlalchemy import text as _sql_text
        with _get_sqlalchemy_engine().begin() as _conn:
            _conn.execute(
                _sql_text("INSERT INTO marketdata.agent_memory "
                          "(app, category, subject, content, source) "
                          "VALUES (:app, :cat, :sub, :con, :src)"),
                {"app": _TRADER_APP, "cat": category, "sub": subject,
                 "con": content, "src": source},
            )
        _load_trader_memories.clear()

    # ── system prompt ─────────────────────────────────────────────────────────
    _TRADER_BASE_SYSTEM = (
        "You are the Trader — a BESS operations analyst specialising in Inner Mongolia "
        "(Mengxi) dispatch performance, P&L attribution, and market trading analysis. "
        "Your scope: 4 operating BESS assets — SuYou (景蓝乌尔图), HangJinQi (悦杭独贵), "
        "SiZiWangQi (景通四益堂储), GuShanLiang (裕昭沙子坝). "
        "You help the operations and trading team understand daily P&L drivers, "
        "execution gaps, dispatch quality, and RT price dynamics.\n\n"
        "Rules:\n"
        "1. Use get_asset_pnl first before making any financial claims.\n"
        "2. Use get_dispatch_data to analyse specific dispatch days.\n"
        "3. Use get_rt_prices to contextualise market conditions.\n"
        "4. Use search_knowledge_base when asked about market rules, trading policies, "
        "settlement procedures, ancillary service rules, or grid codes.\n"
        "5. Attribute losses clearly: PF Unrestricted → PF Grid-Feasible → "
        "Forecast Optimal → Strategy → Nominated → Cleared Actual.\n"
        "6. Respond concisely with actionable insights for the trading team.\n"
        "7. Asset codes: suyou / hangjinqi / siziwangqi / gushanliang."
    )

    def _build_trader_system() -> str:
        mem_df = _load_trader_memories()
        if mem_df.empty:
            mem_block = ""
        else:
            lines = [f"[{r.category}] {r.subject}: {r.content}"
                     for r in mem_df.itertuples()]
            mem_block = "\n\n## Memory from prior sessions:\n" + "\n".join(lines)
        return _TRADER_BASE_SYSTEM + mem_block

    # ── tools ─────────────────────────────────────────────────────────────────
    _TRADER_TOOLS = [
        {
            "name": "get_asset_pnl",
            "description": (
                "Get daily P&L attribution for one or all BESS assets over a date range. "
                "Returns: trade_date, asset_code, pf_unrestricted_pnl, pf_grid_feasible_pnl, "
                "tt_forecast_optimal_pnl, tt_strategy_pnl, nominated_pnl, cleared_actual_pnl (CNY). "
                "Use to analyse revenue performance and loss waterfall."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "asset_code": {
                        "type": "string",
                        "description": "suyou / hangjinqi / siziwangqi / gushanliang. Omit for all assets.",
                    },
                    "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "end_date":   {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "get_dispatch_data",
            "description": (
                "Get 15-min dispatch data for a BESS asset on a specific date. "
                "Returns: interval_start, charge_mw, discharge_mw, soc_mwh, "
                "cleared_price_yuan_mwh. Use to check dispatch quality and SoC profile."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "asset_code": {"type": "string",
                                  "description": "suyou / hangjinqi / siziwangqi / gushanliang"},
                    "date":       {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["asset_code", "date"],
            },
        },
        {
            "name": "get_rt_prices",
            "description": (
                "Get hourly average Mengxi province RT clearing prices (CNY/MWh) "
                "for a date range. Use to contextualise market conditions."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "end_date":   {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "search_knowledge_base",
            "description": (
                "Search the company knowledge base for policies, trading rules, "
                "market regulations, and settlement rules. Use when the user asks "
                "about market rules, BESS trading policies, ancillary service rules, "
                "grid codes, or settlement procedures."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search terms in Chinese or English.",
                    },
                    "category": {
                        "type": "string",
                        "description": (
                            "Optional filter: market_rules | policy_doc | "
                            "annual_report | technical_spec | research_report"
                        ),
                    },
                },
                "required": ["query"],
            },
        },
    ]

    def _dispatch_trader_tool(name: str, inp: dict) -> str:
        from sqlalchemy import text as _sql_text
        engine = _get_sqlalchemy_engine()

        if name == "get_asset_pnl":
            where_clauses = ["trade_date >= :start", "trade_date <= :end"]
            params: dict = {"start": inp["start_date"], "end": inp["end_date"]}
            if inp.get("asset_code"):
                where_clauses.append("asset_code = :asset")
                params["asset"] = inp["asset_code"]
            where_sql = " AND ".join(where_clauses)
            try:
                df = pd.read_sql(
                    _sql_text(f"""
                        SELECT trade_date, asset_code,
                               pf_unrestricted_pnl, pf_grid_feasible_pnl,
                               tt_forecast_optimal_pnl, tt_strategy_pnl,
                               nominated_pnl, cleared_actual_pnl
                        FROM reports.bess_asset_daily_attribution
                        WHERE {where_sql}
                        ORDER BY trade_date, asset_code
                        LIMIT 200
                    """),
                    engine, params=params,
                )
                return df.to_json(orient="records", default_handler=str)
            except Exception as _e:
                return f"Error querying P&L: {_e}"

        elif name == "get_dispatch_data":
            try:
                df = pd.read_sql(
                    _sql_text("""
                        SELECT interval_start, asset_code,
                               charge_mw, discharge_mw, soc_mwh,
                               cleared_price_yuan_mwh
                        FROM marketdata.ops_bess_dispatch_15min
                        WHERE asset_code = :asset AND data_date = :dt
                        ORDER BY interval_start
                        LIMIT 100
                    """),
                    engine, params={"asset": inp["asset_code"], "dt": inp["date"]},
                )
                return df.to_json(orient="records", default_handler=str)
            except Exception as _e:
                return f"Error querying dispatch data: {_e}"

        elif name == "get_rt_prices":
            try:
                df = _load_market_series(
                    "hist_mengxi_provincerealtimeclearprice_15min",
                    date.fromisoformat(inp["start_date"]),
                    date.fromisoformat(inp["end_date"]),
                    "hourly",
                )
                return df.to_json(orient="records", default_handler=str)
            except Exception as _e:
                return f"Error querying RT prices: {_e}"

        elif name == "search_knowledge_base":
            try:
                from services.knowledge_pool.knowledge_docs import search_reference_docs
                results = search_reference_docs(
                    query=inp["query"],
                    category=inp.get("category"),
                    app="trader",
                    limit=5,
                )
                if not results:
                    return "No matching documents found in the knowledge base."
                out = []
                for r in results:
                    out.append(
                        f"[{r['category']}] {r['file_name']} (p.{r['page_no']})\n"
                        f"{r['chunk_text']}"
                    )
                return "\n\n---\n\n".join(out)
            except Exception as _e:
                return f"Error searching knowledge base: {_e}"

        return "Unknown tool"

    # ── auto-extract memories ─────────────────────────────────────────────────
    def _extract_trader_memories(user_msg: str, agent_reply: str) -> list[dict]:
        try:
            resp = _trader_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=600,
                system=(
                    "Extract key facts, operational observations, and decisions from "
                    "BESS trading conversations worth remembering long-term. "
                    "Output ONLY a JSON array (no markdown). Each item: "
                    "{\"category\": one of [pnl_insight, asset_note, market_view, "
                    "execution_gap, strategy_decision], "
                    "\"subject\": short title (≤60 chars), "
                    "\"content\": the key fact (≤200 chars)}. "
                    "Return [] if nothing worth persisting."
                ),
                messages=[{"role": "user", "content":
                    f"User: {user_msg}\n\nTrader: {agent_reply[:1500]}\n\n"
                    "What facts or observations are worth persisting across sessions?"}],
            )
            raw = resp.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            return _json.loads(raw)
        except Exception:
            return []

    # ── agent runner ──────────────────────────────────────────────────────────
    def _run_trader_agent(messages: list[dict]) -> str:
        system = _build_trader_system()
        current_msgs = list(messages)
        for _ in range(10):
            resp = _trader_client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=4096,
                system=system,
                tools=_TRADER_TOOLS,
                messages=current_msgs,
            )
            if resp.stop_reason == "end_turn":
                return "\n".join(b.text for b in resp.content if hasattr(b, "text"))
            tool_calls = [b for b in resp.content if b.type == "tool_use"]
            if not tool_calls:
                return "\n".join(b.text for b in resp.content if hasattr(b, "text"))
            current_msgs.append({"role": "assistant", "content": resp.content})
            tool_results = []
            for tc in tool_calls:
                result = _dispatch_trader_tool(tc.name, tc.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": str(result)[:8000],
                })
            current_msgs.append({"role": "user", "content": tool_results})
        return "Agent loop reached max iterations."

    # ── UI ────────────────────────────────────────────────────────────────────
    _ensure_trader_memory_table()

    if "trader_msgs" not in st.session_state:
        st.session_state["trader_msgs"] = []

    hcol1, hcol2 = st.columns([6, 1])
    with hcol1:
        st.subheader("Mengxi BESS Trader")
        st.caption(
            "Analyse P&L attribution, dispatch quality, and RT market conditions "
            "across the 4 Inner Mongolia BESS assets."
        )
    with hcol2:
        if st.button("Clear chat", key="trader_clear_btn"):
            st.session_state["trader_msgs"] = []
            st.rerun()

    for _tmsg in st.session_state["trader_msgs"]:
        with st.chat_message(_tmsg["role"]):
            st.markdown(_tmsg["content"])

    if not st.session_state["trader_msgs"]:
        with st.chat_message("assistant"):
            st.markdown(
                "Hello! I'm the Trader. I can analyse P&L attribution, dispatch quality, "
                "and RT market conditions for SuYou, HangJinQi, SiZiWangQi, and GuShanLiang. "
                "What would you like to investigate?"
            )

    if _trader_input := st.chat_input(
        "Ask about asset P&L, dispatch, or market conditions…", key="trader_chat_input"
    ):
        st.session_state["trader_msgs"].append({"role": "user", "content": _trader_input})
        with st.chat_message("user"):
            st.markdown(_trader_input)

        with st.chat_message("assistant"):
            with st.spinner("Analysing…"):
                _trader_reply = _run_trader_agent(st.session_state["trader_msgs"])
            st.markdown(_trader_reply)

        st.session_state["trader_msgs"].append({"role": "assistant", "content": _trader_reply})

        # auto-save memories
        try:
            _tmems = _extract_trader_memories(_trader_input, _trader_reply)
            for _tm in _tmems:
                _save_trader_memory(_tm["category"], _tm["subject"], _tm["content"], source="auto")
            if _tmems:
                st.toast(f"Saved {len(_tmems)} memory item(s).")
        except Exception:
            pass
        st.rerun()

    # ── memory management ─────────────────────────────────────────────────────
    with st.expander("Memory Management", expanded=False):
        st.caption("Persistent facts and observations auto-saved from Trader conversations.")
        _tmem_df = _load_trader_memories()
        if _tmem_df.empty:
            st.info("No memories saved yet.")
        else:
            for _trow in _tmem_df.itertuples():
                _tc1, _tc2 = st.columns([10, 1])
                with _tc1:
                    st.markdown(
                        f"**[{_trow.category}]** {_trow.subject}: {_trow.content}"
                    )
                    st.caption(f"Saved: {_trow.saved}  |  Source: {_trow.source}")
                with _tc2:
                    if st.button("🗑", key=f"del_trader_mem_{_trow.id}"):
                        from sqlalchemy import text as _tdel_text
                        with _get_sqlalchemy_engine().begin() as _tdel_conn:
                            _tdel_conn.execute(
                                _tdel_text(
                                    "UPDATE marketdata.agent_memory "
                                    "SET active=FALSE WHERE id=:id AND app=:app"
                                ),
                                {"id": _trow.id, "app": _TRADER_APP},
                            )
                        _load_trader_memories.clear()
                        st.rerun()

    # ── GB BESS Benchmark ─────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("GB BESS Benchmark")
    st.caption(
        "Industry-average revenue index and asset leaderboard from Modo Energy. "
        "Use as context when benchmarking IM asset performance against GB peers."
    )

    _gb_c1, _gb_c2, _gb_c3 = st.columns([2, 2, 2])
    _gb_preset = _gb_c1.selectbox(
        "Period",
        ["Last 7 days", "Last 30 days", "Last 90 days"],
        index=1, key="gb_bench_preset",
    )
    _gb_today = date.today()
    if _gb_preset == "Last 7 days":
        _gb_start, _gb_end = _gb_today - timedelta(days=7), _gb_today
    elif _gb_preset == "Last 30 days":
        _gb_start, _gb_end = _gb_today - timedelta(days=30), _gb_today
    else:
        _gb_start, _gb_end = _gb_today - timedelta(days=90), _gb_today

    _gb_market_filter = _gb_c2.selectbox(
        "Market (leaderboard)",
        ["All", "bm", "cm", "frequency_response", "dml", "dcl", "drl", "dmh", "dch", "drh"],
        index=0, key="gb_market_filter",
    )
    _gb_dur_filter = _gb_c3.selectbox(
        "Duration (index)",
        ["All", "1h", "2h", "4h"],
        index=0, key="gb_duration_filter",
    )

    _GB_COLOURS = {
        "bm": "#1f77b4", "cm": "#ff7f0e", "frequency_response": "#2ca02c",
        "dml": "#d62728", "dcl": "#9467bd", "drl": "#8c564b",
        "dmh": "#e377c2", "dch": "#7f7f7f", "drh": "#bcbd22",
    }

    # Daily index stacked bar
    _gb_idx = _load_gb_daily_index(str(_gb_start), str(_gb_end))
    if not _gb_idx.empty:
        if _gb_dur_filter != "All":
            _gb_idx = _gb_idx[_gb_idx["duration"] == _gb_dur_filter]
        _gb_pivot = _gb_idx.pivot_table(
            index="settlement_date", columns="market",
            values="revenue_permw", aggfunc="mean",
        ).reset_index()
        _gb_idx_fig = go.Figure()
        for _m in [c for c in _gb_pivot.columns if c != "settlement_date"]:
            _gb_idx_fig.add_trace(go.Bar(
                x=_gb_pivot["settlement_date"],
                y=_gb_pivot[_m],
                name=_m.upper(),
                marker_color=_GB_COLOURS.get(_m, "#aec7e8"),
            ))
        _gb_idx_fig.update_layout(
            barmode="stack", height=320,
            margin=dict(l=10, r=10, t=30, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.01),
            xaxis_title=None, yaxis_title="£/MW/day",
            plot_bgcolor="white", paper_bgcolor="white",
        )
        st.markdown("**Daily Revenue Index (£/MW/day) by Market Stream**")
        st.plotly_chart(_gb_idx_fig, use_container_width=True, key="gb_daily_idx_chart")
    else:
        st.info("No GB daily index data. Run backfill from GB Market > Data Management.")

    # Monthly index line chart
    _gb_mo_from = (_gb_today - timedelta(days=365)).replace(day=1).strftime("%Y-%m")
    _gb_mo_to = _gb_today.strftime("%Y-%m")
    _gb_mo = _load_gb_monthly_index(_gb_mo_from, _gb_mo_to)
    if not _gb_mo.empty:
        if _gb_dur_filter != "All":
            _gb_mo = _gb_mo[_gb_mo["duration"] == _gb_dur_filter]
        _gb_mo_pivot = _gb_mo.pivot_table(
            index="month", columns="market",
            values="revenue_permw", aggfunc="mean",
        ).reset_index()
        _gb_mo_fig = go.Figure()
        for _m in [c for c in _gb_mo_pivot.columns if c != "month"]:
            _gb_mo_fig.add_trace(go.Scatter(
                x=_gb_mo_pivot["month"],
                y=_gb_mo_pivot[_m],
                name=_m.upper(),
                mode="lines+markers",
                line=dict(color=_GB_COLOURS.get(_m, "#aec7e8"), width=2),
            ))
        _gb_mo_fig.update_layout(
            height=280,
            margin=dict(l=10, r=10, t=30, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.01),
            xaxis_title=None, yaxis_title="£/MW/month",
            plot_bgcolor="white", paper_bgcolor="white",
        )
        st.markdown("**Monthly Revenue Index (£/MW/month) — Last 12 Months**")
        st.plotly_chart(_gb_mo_fig, use_container_width=True, key="gb_monthly_idx_chart")

    # Leaderboard table
    st.markdown("**Asset Leaderboard — Top 20 by Avg Revenue/MW**")
    _lb_market = None if _gb_market_filter == "All" else _gb_market_filter
    _gb_lb = _load_gb_leaderboard(str(_gb_start), str(_gb_end), market=_lb_market)
    if not _gb_lb.empty:
        _gb_lb = _gb_lb.head(20).reset_index(drop=True)
        _gb_lb.index += 1
        _gb_lb_display = _gb_lb.copy()
        _gb_lb_display.columns = ["Asset", "Market", "Total Revenue (£)", "Avg £/MW/day", "Trading Days"]
        _gb_lb_display["Total Revenue (£)"] = _gb_lb_display["Total Revenue (£)"].map(
            lambda x: f"£{x:,.0f}" if pd.notna(x) else "—"
        )
        _gb_lb_display["Avg £/MW/day"] = _gb_lb_display["Avg £/MW/day"].map(
            lambda x: f"£{x:,.2f}" if pd.notna(x) else "—"
        )
        st.dataframe(_gb_lb_display, use_container_width=True,
                     height=min(38 * len(_gb_lb_display) + 38, 500))
    else:
        st.info("No leaderboard data for this period.")
