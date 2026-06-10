"""
Mengxi Province Trading Management

Province-level BESS & wind trading intelligence for Inner Mongolia (Mengxi).

Tabs:
  1. Market Fundamentals  — provincial RT prices, wind/solar, load, capacity
  2. BESS Market Ranking  — all-BESS arbitrage ranking (async ECS pipeline)
  3. Our BESS Portfolio   — P&L waterfall · daily ops · strategy comparison
  4. Options Pricing      — spread call strip valuation + realization overlay
  5. Wind Farm Ranking    — all-wind generation & revenue ranking
  6. Wind Farm Trading    — placeholder for future wind trading management
  7. Data Management      — table freshness, coverage, manual upload
  8. Trader               — Claude agent for P&L attribution + market analysis

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
# Ensure repo root is importable + load .env for local dev
# ---------------------------------------------------------------------------
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_repo_root, "config", ".env"), override=False)
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Mengxi Province Trading Management",
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
    st.title("⚡ Mengxi Province Trading")
    st.caption("Inner Mongolia BESS & Wind — Market Intelligence")
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
tab_market, tab_bess_rank, tab_portfolio, tab_cockpit, tab_wind_rank, tab_wind_trading, tab_data_mgmt, tab_nodal_maps, tab_pypsa, tab_trader = st.tabs([
    "Market Fundamentals",
    "BESS Market Ranking",
    "Our BESS Portfolio",
    "Options Pricing",
    "Wind Farm Ranking",
    "Wind Farm Trading",
    "Data Management",
    "Nodal Maps",
    "PyPSA Mengxi",
    "Trader",
])

# ---------------------------------------------------------------------------
# Tab 1: Market Data
# ---------------------------------------------------------------------------
with tab_market:
    st.title("Mengxi Province — Market Fundamentals")
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
# Tab 2: BESS Market Ranking
# ---------------------------------------------------------------------------
with tab_bess_rank:
    from bess_market_tab import render as _render_bess_rank
    _render_bess_rank(_get_sqlalchemy_engine())

# ---------------------------------------------------------------------------
# Tab 3: Our BESS Portfolio — P&L Waterfall · Daily Ops · Strategy Comparison
# ---------------------------------------------------------------------------
with tab_portfolio:
    sub_pnl, sub_daily, sub_strategy = st.tabs([
        "P&L Waterfall",
        "Daily Ops",
        "Strategy Comparison",
    ])

    with sub_pnl:
        from libs.decision_models.adapters.app.dispatch_pnl_page import render_dispatch_pnl_page
        render_dispatch_pnl_page(selected_asset, selected_date)

    with sub_daily:
        from libs.decision_models.adapters.app.daily_ops_page import render_daily_ops_page
        render_daily_ops_page()

    with sub_strategy:
        from libs.decision_models.adapters.app.strategy_comparison_page import render_strategy_comparison_page
        render_strategy_comparison_page()

# ---------------------------------------------------------------------------
# Tab 4: Options Pricing
# ---------------------------------------------------------------------------
with tab_cockpit:
    from libs.decision_models.adapters.app.cockpit_page import render_cockpit_page
    render_cockpit_page()

# ---------------------------------------------------------------------------
# Tab 5: Wind Farm Ranking
# ---------------------------------------------------------------------------
with tab_wind_rank:
    from wind_farm_tab import render as _render_wind_rank
    _render_wind_rank(_get_sqlalchemy_engine())

# ---------------------------------------------------------------------------
# Tab 6: Wind Farm Trading (placeholder)
# ---------------------------------------------------------------------------
with tab_wind_trading:
    st.subheader("Wind Farm Trading Management")
    st.info(
        "Coming soon — active wind farm trading management, dispatch optimisation, "
        "and curtailment analysis for Mengxi wind assets."
    )

# ---------------------------------------------------------------------------
# Tab 7: Data Management
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

    st.markdown("---")

    # ── Section 6: Shanxi Nodal Prices (Fengxing API) ───────────────────────
    st.subheader("Shanxi Nodal Prices — Fengxing API")
    st.caption(
        "Download 15-min avg nodal prices for all Shanxi nodes from the Fengxing API "
        "and store in `marketdata.md_shanxi_nodal_price_96`. "
        "API key is read from the `FENGXING_API_KEY` environment variable."
    )

    _fx_api_key = os.environ.get("FENGXING_API_KEY", "")
    if not _fx_api_key:
        st.warning(
            "⚠️ `FENGXING_API_KEY` environment variable is not set. "
            "Contact the ops team for the API key and add it to `config/.env`."
        )

    _fx_c1, _fx_c2, _fx_c3 = st.columns([2, 2, 2])
    _fx_start = _fx_c1.date_input(
        "Start date", value=date.today() - timedelta(days=7), key="fx_start"
    )
    _fx_end = _fx_c2.date_input(
        "End date", value=date.today() - timedelta(days=1), key="fx_end"
    )

    # Coverage check — how many distinct dates already in DB for this range
    @st.cache_data(ttl=60, show_spinner=False)
    def _fx_coverage(start: date, end: date) -> tuple[int, int]:
        """Return (dates_in_db, total_days) for the given range."""
        try:
            conn = _get_pg_conn()
            df = pd.read_sql(
                """
                SELECT COUNT(DISTINCT metric_time::date) AS n
                FROM marketdata.md_shanxi_nodal_price_96
                WHERE metric_time::date >= %s AND metric_time::date <= %s
                """,
                conn,
                params=(start, end),
            )
            n_in_db = int(df["n"].iloc[0])
        except Exception:
            n_in_db = 0
        total = max((end - start).days + 1, 1)
        return n_in_db, total

    if _fx_api_key:
        _fx_n_db, _fx_total = _fx_coverage(_fx_start, _fx_end)
        _fx_badge = "🟢" if _fx_n_db >= _fx_total else ("🟡" if _fx_n_db > 0 else "🔴")
        _fx_c3.metric(
            f"{_fx_badge} DB coverage",
            f"{_fx_n_db} / {_fx_total} days",
        )

    _fx_btn_col, _fx_probe_col = st.columns([2, 1])

    if _fx_probe_col.button("🔌 Test connection", key="fx_probe_btn", disabled=not _fx_api_key):
        from services.fengxing.nodal_price import probe as _fx_probe
        with st.spinner("Testing API connectivity…"):
            _probe_result = _fx_probe(_fx_api_key)
        if _probe_result == "ok":
            st.success("✅ API reachable.")
        else:
            st.error(f"❌ {_probe_result}")

    if _fx_btn_col.button(
        "⬇ Download nodal prices",
        key="fx_download_btn",
        type="primary",
        disabled=not _fx_api_key or _fx_start > _fx_end,
    ):
        from services.fengxing.nodal_price import download_and_upsert as _fx_dl

        _fx_n_days = (_fx_end - _fx_start).days + 1
        _fx_progress = st.progress(0, text=f"0 / {_fx_n_days} days…")
        _fx_log = st.empty()
        _fx_done_count = [0]
        _fx_log_lines: list[str] = []

        def _fx_day_cb(day, status, n_rows, msg):
            _fx_done_count[0] += 1
            icon = "✅" if status == "ok" else "❌"
            _fx_log_lines.append(f"{icon} {day}  {msg}")
            pct = _fx_done_count[0] / _fx_n_days
            _fx_progress.progress(pct, text=f"{_fx_done_count[0]} / {_fx_n_days} days…")
            _fx_log.code("\n".join(_fx_log_lines[-20:]))   # show last 20 lines

        try:
            engine = _get_sqlalchemy_engine()
            _fx_results = _fx_dl(
                start_date=_fx_start,
                end_date=_fx_end,
                api_key=_fx_api_key,
                engine=engine,
                day_cb=_fx_day_cb,
            )
            _fx_progress.progress(1.0, text="Done.")
            _ok = [r for r in _fx_results if r["status"] == "ok"]
            _err = [r for r in _fx_results if r["status"] == "error"]
            _total_rows = sum(r["rows"] for r in _ok)
            if _err:
                st.warning(
                    f"✅ {len(_ok)} day(s) saved ({_total_rows:,} rows)  |  "
                    f"❌ {len(_err)} day(s) failed — see log above."
                )
            else:
                st.success(f"✅ {len(_ok)} day(s) saved, {_total_rows:,} rows total.")
            _fx_coverage.clear()
        except Exception as _fx_exc:
            _fx_progress.empty()
            st.error(f"Download failed: {_fx_exc}")

    # Quick data preview
    with st.expander("Preview stored data", expanded=False):
        try:
            _fx_prev = pd.read_sql(
                """
                SELECT metric_time, node_name, market_name, time_order_96, avg_node_price
                FROM marketdata.md_shanxi_nodal_price_96
                ORDER BY metric_time DESC, node_name
                LIMIT 200
                """,
                _get_sqlalchemy_engine(),
            )
            if _fx_prev.empty:
                st.info("No data yet.")
            else:
                st.dataframe(
                    _fx_prev,
                    use_container_width=True,
                    hide_index=True,
                    height=300,
                )
        except Exception as _fx_prev_exc:
            st.info(f"Table not yet created or empty. ({_fx_prev_exc})")

    st.markdown("---")

    # ── Section 7: Multi-province nodal price local CSV download ────────────
    st.subheader("Nodal Prices — Local CSV Download (Multi-Province)")
    st.caption(
        "Download avg_node_price for any province from the Fengxing API and save locally. "
        "One CSV file per province per month — no database writes. "
        "Output: `data/nodal/<province>_<YYYY-MM>.csv`"
    )

    _NODAL_PROVINCES = [
        "山西", "陕西", "湖南", "浙江", "云南",
        "贵州", "广东", "广西", "海南", "甘肃",
        "山东", "河北南网", "黑龙江", "辽宁", "蒙西", "湖北", "安徽", "江西",
    ]

    _nd_c1, _nd_c2 = st.columns(2)
    _nd_start = _nd_c1.date_input(
        "Start date",
        value=date(date.today().year, 1, 1),
        key="nd_start",
        help="The download will be split into one CSV per province per month.",
    )
    _nd_end = _nd_c2.date_input(
        "End date",
        value=date.today() - timedelta(days=1),
        key="nd_end",
    )
    _nd_provinces = st.multiselect(
        "Provinces",
        _NODAL_PROVINCES,
        default=["山西"],
        key="nd_provinces",
    )

    _nd_out_dir = os.path.join(_repo_root, "data", "nodal")
    st.caption(f"Output directory: `{_nd_out_dir}`")

    if st.button(
        "Download CSV files",
        key="nd_download_btn",
        type="primary",
        disabled=not _fx_api_key or not _nd_provinces or _nd_start > _nd_end,
    ):
        import csv
        import calendar as _calendar
        from services.fengxing.nodal_price import (
            _fetch_day as _nd_fetch_day,
            _COLUMNS as _nd_cols,
        )

        os.makedirs(_nd_out_dir, exist_ok=True)

        # Build list of (year, month) periods spanning the date range
        _nd_months: list[tuple[int, int]] = []
        _y, _m = _nd_start.year, _nd_start.month
        while (_y, _m) <= (_nd_end.year, _nd_end.month):
            _nd_months.append((_y, _m))
            _m += 1
            if _m > 12:
                _m = 1
                _y += 1

        _nd_total_tasks = len(_nd_provinces) * len(_nd_months)
        _nd_progress = st.progress(0, text=f"0 / {_nd_total_tasks} tasks…")
        _nd_log = st.empty()
        _nd_log_lines: list[str] = []
        _nd_done_count = [0]

        def _nd_append_log(msg: str) -> None:
            _nd_log_lines.append(msg)
            _nd_log.code("\n".join(_nd_log_lines[-25:]))

        _nd_fieldnames = _nd_cols + ["avg_node_price"]
        _nd_errors: list[str] = []

        for _nd_province in _nd_provinces:
            _nd_filter = [f'[market_name] = "{_nd_province}"']
            for _nd_yr, _nd_mo in _nd_months:
                # Clamp to user-specified date range
                _mo_first = date(_nd_yr, _nd_mo, 1)
                _mo_last = date(_nd_yr, _nd_mo, _calendar.monthrange(_nd_yr, _nd_mo)[1])
                _day_start = max(_mo_first, _nd_start)
                _day_end = min(_mo_last, _nd_end)

                _fname = f"{_nd_province}_{_nd_yr:04d}-{_nd_mo:02d}.csv"
                _fpath = os.path.join(_nd_out_dir, _fname)

                _nd_append_log(f"[{_nd_province}] {_nd_yr}-{_nd_mo:02d}  downloading…")
                _month_rows: list[dict] = []
                _month_failed: list[str] = []
                _d = _day_start
                while _d <= _day_end:
                    try:
                        _day_rows = _nd_fetch_day(_d, _fx_api_key, filters=_nd_filter)
                        _month_rows.extend(_day_rows)
                    except Exception as _nd_exc:
                        _month_failed.append(str(_d))
                        _nd_append_log(f"  FAIL {_d}: {_nd_exc}")
                    _d += timedelta(days=1)

                with open(_fpath, "w", newline="", encoding="utf-8-sig") as _nd_fh:
                    _nd_writer = csv.DictWriter(_nd_fh, fieldnames=_nd_fieldnames, extrasaction="ignore")
                    _nd_writer.writeheader()
                    _nd_writer.writerows(_month_rows)

                _nd_done_count[0] += 1
                _nd_progress.progress(
                    _nd_done_count[0] / _nd_total_tasks,
                    text=f"{_nd_done_count[0]} / {_nd_total_tasks} tasks…",
                )

                if _month_failed:
                    _nd_errors.append(f"{_nd_province} {_nd_yr}-{_nd_mo:02d}: {len(_month_failed)} day(s) failed")
                    _nd_append_log(
                        f"  Saved {len(_month_rows):,} rows → {_fname}  ({len(_month_failed)} failure(s))"
                    )
                else:
                    _nd_append_log(f"  Saved {len(_month_rows):,} rows → {_fname}")

        _nd_progress.progress(1.0, text="Done.")
        if _nd_errors:
            st.warning("Completed with errors:\n" + "\n".join(_nd_errors))
        else:
            st.success(f"All {_nd_total_tasks} file(s) saved to `{_nd_out_dir}`")

    # ── Section 8: Ingest local CSV files → RDS ──────────────────────────────
    st.markdown("---")
    st.subheader("8 · Ingest local nodal CSV files → RDS")
    st.caption("Scan `data/nodal/` for downloaded CSV files and upsert into `marketdata.md_shanxi_nodal_price_96`.")

    def _scan_nodal_csvs(nodal_root: str) -> list[dict]:
        """Return list of {province, filename, month, path, size_kb} for all *_YYYY-MM.csv files."""
        import re as _re
        entries = []
        if not os.path.isdir(nodal_root):
            return entries
        _pat = _re.compile(r"^(.+)_(\d{4}-\d{2})\.csv$")
        for _prov_dir in sorted(os.listdir(nodal_root)):
            _prov_path = os.path.join(nodal_root, _prov_dir)
            if not os.path.isdir(_prov_path):
                continue
            for _fname in sorted(os.listdir(_prov_path)):
                _m = _pat.match(_fname)
                if not _m:
                    continue
                _fpath = os.path.join(_prov_path, _fname)
                _size_kb = os.path.getsize(_fpath) / 1024
                entries.append({
                    "province":  _prov_dir,
                    "filename":  _fname,
                    "month":     _m.group(2),
                    "path":      _fpath,
                    "size_kb":   round(_size_kb, 1),
                })
        return entries

    _ingest_nodal_root = os.path.join(_repo_root, "data", "nodal")
    _csv_entries = _scan_nodal_csvs(_ingest_nodal_root)

    if not _csv_entries:
        st.info(f"No `*_YYYY-MM.csv` files found under `{_ingest_nodal_root}`. Use Section 7 above to download files first.")
    else:
        import pandas as _pd_ingest

        _csv_df = _pd_ingest.DataFrame(_csv_entries)[["province", "month", "filename", "size_kb"]]
        _csv_df.columns = ["Province", "Month", "File", "Size (KB)"]

        _all_labels = [f"{r['province']} / {r['month']}" for r in _csv_entries]
        _selected_labels = st.multiselect(
            "Select files to ingest",
            options=_all_labels,
            default=_all_labels,
            key="nodal_ingest_sel",
        )
        st.dataframe(_csv_df, use_container_width=True, hide_index=True)

        _ingest_selected = [e for e, lbl in zip(_csv_entries, _all_labels) if lbl in _selected_labels]

        _can_ingest = bool(_ingest_selected)

        if st.button(
            f"Ingest {len(_ingest_selected)} file(s) to RDS",
            key="nodal_ingest_btn",
            type="primary",
            disabled=not _can_ingest,
        ):
            from services.fengxing.nodal_price import init_table as _nodal_init_table, upsert as _nodal_upsert

            _ingest_engine = _get_sqlalchemy_engine()
            _nodal_init_table(_ingest_engine)

            _ingest_progress = st.progress(0.0, text="Starting…")
            _ingest_log_lines: list[str] = []
            _ingest_errors: list[str] = []
            _ingest_log_area = st.empty()

            def _ingest_append_log(msg: str) -> None:
                _ingest_log_lines.append(msg)
                _ingest_log_area.code("\n".join(_ingest_log_lines[-30:]))

            for _i, _entry in enumerate(_ingest_selected):
                _label = f"{_entry['province']} / {_entry['month']}"
                _ingest_append_log(f"Reading {_entry['filename']} …")
                try:
                    _csv_data = _pd_ingest.read_csv(
                        _entry["path"],
                        encoding="utf-8-sig",
                        dtype={"time_order_96": "Int64"},
                    )
                    _rows_to_upsert = _csv_data.to_dict("records")
                    _n_upserted = _nodal_upsert(_rows_to_upsert, _ingest_engine)
                    _ingest_append_log(f"  ✓ {_label}: {_n_upserted:,} rows upserted")
                except Exception as _exc:
                    _ingest_errors.append(_label)
                    _ingest_append_log(f"  ✗ {_label}: {_exc}")

                _ingest_progress.progress(
                    (_i + 1) / len(_ingest_selected),
                    text=f"{_i + 1} / {len(_ingest_selected)} files…",
                )

            _ingest_progress.progress(1.0, text="Done.")
            if _ingest_errors:
                st.warning(f"Completed with {len(_ingest_errors)} error(s): " + ", ".join(_ingest_errors))
            else:
                st.success(f"All {len(_ingest_selected)} file(s) ingested successfully.")

# ---------------------------------------------------------------------------
# Tab 9: Nodal Maps — per-province PF spread ranking
# ---------------------------------------------------------------------------
with tab_nodal_maps:
    import pandas as _nm_pd
    from datetime import date as _nm_date

    st.header("Nodal Investment Maps — Perfect Foresight Spread Ranking")
    st.caption(
        "Queries `marketdata.md_shanxi_nodal_price_96` and runs the BESS PF optimisation "
        "on each node to estimate annual revenue. Use this to identify the highest-value "
        "nodes for new BESS investment in each province."
    )

    _nm_c1, _nm_c2, _nm_c3 = st.columns(3)
    _nm_province = _nm_c1.selectbox(
        "Province",
        ["山西", "广东", "广西", "海南", "甘肃", "贵州", "陕西", "湖南", "浙江", "云南",
         "山东", "河北南网", "黑龙江", "辽宁", "蒙西", "湖北", "安徽", "江西"],
        key="nm_province",
    )
    _nm_start = _nm_c2.date_input(
        "Start date",
        value=_nm_date(_nm_date.today().year, 1, 1),
        key="nm_start",
    )
    _nm_end = _nm_c3.date_input(
        "End date",
        value=_nm_date.today(),
        key="nm_end",
    )

    _nm_bc1, _nm_bc2, _nm_bc3 = st.columns(3)
    _nm_power_mw   = _nm_bc1.number_input("BESS Power (MW)",  min_value=0.1, value=50.0, step=10.0, key="nm_power")
    _nm_duration_h = _nm_bc2.number_input("Duration (h)",      min_value=0.5, value=2.0,  step=0.5,  key="nm_dur")
    _nm_eff_pct    = _nm_bc3.number_input("Round-trip eff. (%)", min_value=50.0, max_value=100.0, value=85.0, step=1.0, key="nm_eff")
    _nm_top_n      = st.slider("Top-N nodes to highlight", 5, 50, 20, key="nm_topn")

    _nm_date_ok = _nm_start <= _nm_end
    if not _nm_date_ok:
        st.warning("Start date must be ≤ end date.")

    if st.button("Run PF Optimisation", key="nm_run_btn", type="primary", disabled=not _nm_date_ok):
        import plotly.express as _nm_px
        import plotly.graph_objects as _nm_go
        from sqlalchemy import text as _nm_sql_text
        from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices as _nm_pf

        _nm_engine = _get_sqlalchemy_engine()

        with st.spinner(f"Fetching nodal prices for {_nm_province} from {_nm_start} to {_nm_end}…"):
            _nm_query = _nm_sql_text("""
                SELECT node_name, metric_time, avg_node_price
                FROM marketdata.md_shanxi_nodal_price_96
                WHERE market_name = :prov
                  AND metric_time >= :start_dt
                  AND metric_time <  :end_dt
                ORDER BY node_name, metric_time
            """)
            with _nm_engine.connect() as _nm_conn:
                _nm_df = _nm_pd.read_sql(
                    _nm_query,
                    _nm_conn,
                    params={
                        "prov":     _nm_province,
                        "start_dt": _nm_start.strftime("%Y-%m-%d"),
                        "end_dt":   (_nm_end + _nm_pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                    },
                    parse_dates=["metric_time"],
                )

        if _nm_df.empty:
            st.warning(f"No data found for {_nm_province} in the selected date range. Ingest CSV files via Data Management first.")
        else:
            _nm_df["metric_time"] = _nm_pd.to_datetime(_nm_df["metric_time"], utc=True).dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
            _nm_df["avg_node_price"] = _nm_pd.to_numeric(_nm_df["avg_node_price"], errors="coerce")
            _nm_nodes = sorted(_nm_df["node_name"].unique())
            st.info(f"Loaded {len(_nm_df):,} rows across {len(_nm_nodes)} nodes. Running PF optimisation…")

            _nm_results = []
            _nm_monthly_profits: dict[str, dict] = {}  # node → {month_str → profit}
            _nm_prog = st.progress(0.0, text="Optimising nodes…")

            for _nm_i, _nm_node in enumerate(_nm_nodes):
                _nm_series = (
                    _nm_df[_nm_df["node_name"] == _nm_node]
                    .set_index("metric_time")["avg_node_price"]
                    .sort_index()
                    .dropna()
                    .astype(float)
                )
                if len(_nm_series) < 96:
                    _nm_prog.progress((_nm_i + 1) / len(_nm_nodes))
                    continue
                try:
                    _, _nm_profit_s = _nm_pf(
                        _nm_series,
                        power_mw=_nm_power_mw,
                        duration_h=_nm_duration_h,
                        roundtrip_eff=_nm_eff_pct / 100.0,
                    )
                    _nm_total = float(_nm_profit_s.sum())
                    _nm_per_mw = _nm_total / _nm_power_mw
                    _nm_results.append({
                        "node_name":          _nm_node,
                        "total_profit_cny":   _nm_total,
                        "annual_rev_per_mw":  _nm_per_mw,
                    })
                    # Monthly breakdown for heatmap
                    _nm_monthly = _nm_profit_s.resample("ME").sum() / _nm_power_mw
                    _nm_monthly_profits[_nm_node] = {
                        str(ts.to_period("M")): float(v) for ts, v in _nm_monthly.items()
                    }
                except Exception:
                    pass
                _nm_prog.progress((_nm_i + 1) / len(_nm_nodes), text=f"{_nm_i+1}/{len(_nm_nodes)} nodes…")

            _nm_prog.progress(1.0, text="Done.")

            if not _nm_results:
                st.warning("No optimisation results — check that the data has valid prices.")
            else:
                _nm_res_df = _nm_pd.DataFrame(_nm_results).sort_values("annual_rev_per_mw", ascending=False).reset_index(drop=True)
                _nm_res_df["rank"] = range(1, len(_nm_res_df) + 1)

                # ── Ranked bar chart ──────────────────────────────────────────
                st.subheader("Ranked nodes by PF annual revenue / MW")
                _nm_top_df = _nm_res_df.head(_nm_top_n)
                _nm_bar = _nm_px.bar(
                    _nm_top_df,
                    x="node_name",
                    y="annual_rev_per_mw",
                    labels={"node_name": "Node", "annual_rev_per_mw": "Annual Rev / MW (CNY)"},
                    title=f"Top {_nm_top_n} nodes — {_nm_province} PF spread ({_nm_start} → {_nm_end})",
                    color="annual_rev_per_mw",
                    color_continuous_scale="Blues",
                )
                _nm_bar.update_layout(xaxis_tickangle=-45, showlegend=False)
                st.plotly_chart(_nm_bar, use_container_width=True)

                # ── Heatmap: node × month ─────────────────────────────────────
                st.subheader("Monthly PF revenue / MW heatmap")
                if _nm_monthly_profits:
                    _nm_hm_df = _nm_pd.DataFrame(_nm_monthly_profits).T.fillna(0.0)
                    _nm_hm_df = _nm_hm_df.reindex(
                        _nm_res_df["node_name"].tolist()
                    ).head(_nm_top_n)
                    _nm_hm_df.columns = [str(c) for c in _nm_hm_df.columns]
                    _nm_hm = _nm_px.imshow(
                        _nm_hm_df.values,
                        x=list(_nm_hm_df.columns),
                        y=list(_nm_hm_df.index),
                        labels={"x": "Month", "y": "Node", "color": "Rev/MW (CNY)"},
                        title=f"Monthly PF revenue / MW — top {_nm_top_n} nodes",
                        color_continuous_scale="RdYlGn",
                        aspect="auto",
                    )
                    _nm_hm.update_layout(height=max(400, _nm_top_n * 20))
                    st.plotly_chart(_nm_hm, use_container_width=True)

                # ── Top-N investment table ────────────────────────────────────
                st.subheader(f"Top {_nm_top_n} node investment summary")
                _nm_disp_df = _nm_res_df.head(_nm_top_n)[["rank", "node_name", "annual_rev_per_mw", "total_profit_cny"]].copy()
                _nm_disp_df.columns = ["Rank", "Node", "Annual Rev / MW (CNY)", f"Total Profit {_nm_power_mw:.0f}MW (CNY)"]
                _nm_disp_df["Annual Rev / MW (CNY)"] = _nm_disp_df["Annual Rev / MW (CNY)"].map("{:,.0f}".format)
                _nm_disp_df[f"Total Profit {_nm_power_mw:.0f}MW (CNY)"] = _nm_disp_df[f"Total Profit {_nm_power_mw:.0f}MW (CNY)"].map("{:,.0f}".format)
                st.dataframe(_nm_disp_df, use_container_width=True, hide_index=True)

                st.session_state["nm_last_results"] = _nm_res_df

# ---------------------------------------------------------------------------
# Tab 10: PyPSA Mengxi — nodal supply/demand modelling
# ---------------------------------------------------------------------------
with tab_pypsa:
    import pandas as _pypsa_pd

    st.header("PyPSA Mengxi — Nodal Supply & Demand Dynamics")
    st.caption(
        "Build a PyPSA network from the Mengxi RT nodal price table and cleared energy data. "
        "Requires `pypsa>=0.28` to be installed."
    )

    # ── Dependency check ─────────────────────────────────────────────────────
    try:
        import pypsa as _pypsa  # type: ignore
        _pypsa_available = True
    except ImportError:
        _pypsa_available = False

    if not _pypsa_available:
        st.error(
            "**PyPSA is not installed.** To enable this tab, run:\n\n"
            "```\npip install pypsa>=0.28\n```\n\n"
            "Then add `pypsa>=0.28` to `apps/mengxi-dashboard/requirements.txt` "
            "and rebuild the Docker image."
        )
    else:
        from sqlalchemy import text as _pypsa_sql

        st.success(f"PyPSA {_pypsa.__version__} detected.")

        _pp_c1, _pp_c2 = st.columns(2)
        _pp_date = _pp_c1.date_input("Date", value=_pypsa_pd.Timestamp.today().date() - _pypsa_pd.Timedelta(days=1), key="pp_date")
        _pp_node_filter = _pp_c2.text_input("Node filter (leave blank for all)", value="", key="pp_node")

        if st.button("Build Network", key="pp_build_btn", type="primary"):
            _pp_engine = _get_sqlalchemy_engine()
            _pp_date_str = _pp_date.strftime("%Y-%m-%d")

            with st.spinner("Loading nodal prices…"):
                _pp_price_q = _pypsa_sql("""
                    SELECT datetime, node_name, node_price, energy_price, congestion_price
                    FROM marketdata.md_rt_nodal_price
                    WHERE data_date = :d
                    ORDER BY node_name, datetime
                """)
                with _pp_engine.connect() as _pp_conn:
                    _pp_price_df = _pypsa_pd.read_sql(
                        _pp_price_q, _pp_conn,
                        params={"d": _pp_date_str},
                        parse_dates=["datetime"],
                    )

            if _pp_price_df.empty:
                st.warning(f"No nodal price data found for {_pp_date_str}.")
            else:
                if _pp_node_filter.strip():
                    _pp_price_df = _pp_price_df[_pp_price_df["node_name"].str.contains(_pp_node_filter.strip(), na=False)]

                with st.spinner("Loading cleared energy & BESS dispatch…"):
                    # md_id_cleared_energy columns: data_date, datetime, plant_name,
                    # dispatch_unit_name, energy_mwh, cleared_energy_mwh, cleared_price, price
                    # (no node_name or unit_type — copper-plate model for generators)
                    _pp_energy_q = _pypsa_sql("""
                        SELECT datetime, plant_name, dispatch_unit_name,
                               cleared_energy_mwh, cleared_price
                        FROM marketdata.md_id_cleared_energy
                        WHERE data_date = :d
                        ORDER BY dispatch_unit_name, datetime
                    """)
                    with _pp_engine.connect() as _pp_conn2:
                        try:
                            _pp_energy_df = _pypsa_pd.read_sql(
                                _pp_energy_q, _pp_conn2,
                                params={"d": _pp_date_str},
                                parse_dates=["datetime"],
                            )
                        except Exception:
                            _pp_energy_df = _pypsa_pd.DataFrame()

                    # Derive BESS units: dispatch units that have negative cleared_energy
                    # on this day (charging periods) — bidirectional behaviour = storage
                    if not _pp_energy_df.empty:
                        _pp_bidi = (
                            _pp_energy_df.groupby("dispatch_unit_name")["cleared_energy_mwh"]
                            .min()
                            .loc[lambda s: s < 0]
                            .index.tolist()
                        )
                        _pp_bess_df = (
                            _pp_energy_df[_pp_energy_df["dispatch_unit_name"].isin(_pp_bidi)]
                            .rename(columns={"dispatch_unit_name": "asset_name"})
                            .copy()
                        )
                    else:
                        _pp_bess_df = _pypsa_pd.DataFrame()

                # ── Build PyPSA network ───────────────────────────────────────
                with st.spinner("Building PyPSA network…"):
                    _pp_nodes = sorted(_pp_price_df["node_name"].unique())
                    _pp_snapshots = sorted(_pp_price_df["datetime"].unique())

                    _pp_net = _pypsa.Network()
                    _pp_net.set_snapshots(_pp_snapshots)

                    # Buses — one per node (from RT nodal price)
                    for _pp_bus in _pp_nodes:
                        _pp_net.add("Bus", _pp_bus)

                    # Generators — one per dispatch_unit_name, copper-plate (first bus)
                    # Positive cleared_energy_mwh = generation/discharge
                    if not _pp_energy_df.empty and _pp_nodes:
                        _pp_unit_max = (
                            _pp_energy_df.groupby("dispatch_unit_name")["cleared_energy_mwh"]
                            .max()
                        )
                        for _pp_unit, _pp_max_e in _pp_unit_max.items():
                            _pp_p_nom = max(float(_pp_max_e) * 4, 1.0)  # MWh → MW
                            _pp_net.add(
                                "Generator", str(_pp_unit),
                                bus=_pp_nodes[0],
                                p_max_pu=1.0,
                                p_nom=_pp_p_nom,
                                marginal_cost=0.0,
                            )

                    # Storage units — BESS (bidirectional units), copper-plate (first bus)
                    if not _pp_bess_df.empty and _pp_nodes:
                        for _pp_asset, _pp_bgrp in _pp_bess_df.groupby("asset_name"):
                            _pp_max_d = float(_pp_bgrp["cleared_energy_mwh"].clip(lower=0).max()) * 4
                            _pp_net.add(
                                "StorageUnit", str(_pp_asset),
                                bus=_pp_nodes[0],
                                p_nom=max(_pp_max_d, 1.0),
                                max_hours=2.0,
                                efficiency_store=0.92,
                                efficiency_dispatch=0.92,
                            )

                # ── Panel 1: Network summary ──────────────────────────────────
                st.subheader("Panel 1 — Network Summary")
                _pp_sum_cols = st.columns(4)
                _pp_sum_cols[0].metric("Buses", len(_pp_net.buses))
                _pp_sum_cols[1].metric("Generators", len(_pp_net.generators))
                _pp_sum_cols[2].metric("Storage Units", len(_pp_net.storage_units))
                _pp_sum_cols[3].metric("Snapshots", len(_pp_net.snapshots))

                import plotly.express as _pp_px
                import plotly.graph_objects as _pp_go

                # ── Panel 2: Nodal price choropleth (bar chart by node) ───────
                st.subheader("Panel 2 — Nodal Price by Node (avg)")
                _pp_avg_price = (
                    _pp_price_df.groupby("node_name")["node_price"]
                    .mean()
                    .reset_index()
                    .rename(columns={"node_price": "avg_price_cny_mwh"})
                    .sort_values("avg_price_cny_mwh", ascending=False)
                )
                _pp_fig2 = _pp_px.bar(
                    _pp_avg_price, x="node_name", y="avg_price_cny_mwh",
                    labels={"node_name": "Node", "avg_price_cny_mwh": "Avg Price (CNY/MWh)"},
                    title=f"Mengxi avg nodal price — {_pp_date_str}",
                    color="avg_price_cny_mwh", color_continuous_scale="RdYlGn",
                )
                _pp_fig2.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(_pp_fig2, use_container_width=True)

                # ── Panel 3: Energy vs congestion price split by node ─────────
                st.subheader("Panel 3 — Energy vs Congestion Component by Node")
                if "energy_price" in _pp_price_df.columns and "congestion_price" in _pp_price_df.columns:
                    _pp_comp = (
                        _pp_price_df.groupby("node_name")[["energy_price", "congestion_price"]]
                        .mean()
                        .reset_index()
                        .sort_values("congestion_price", ascending=False)
                    )
                    _pp_fig3 = _pp_go.Figure()
                    _pp_fig3.add_bar(x=_pp_comp["node_name"], y=_pp_comp["energy_price"], name="Energy component")
                    _pp_fig3.add_bar(x=_pp_comp["node_name"], y=_pp_comp["congestion_price"], name="Congestion component")
                    _pp_fig3.update_layout(
                        barmode="stack", title=f"Energy vs congestion price — {_pp_date_str}",
                        xaxis_tickangle=-45,
                        yaxis_title="CNY/MWh",
                    )
                    st.plotly_chart(_pp_fig3, use_container_width=True)
                else:
                    st.info("energy_price / congestion_price columns not available in this dataset.")

                # ── Panel 4: Nodal price time series for all nodes ────────────
                st.subheader("Panel 4 — Nodal Price Time Series")
                _pp_pivot_price = _pp_price_df.pivot_table(index="datetime", columns="node_name", values="node_price", aggfunc="mean")
                _pp_fig4 = _pp_px.line(
                    _pp_pivot_price,
                    labels={"datetime": "Time", "value": "Price (CNY/MWh)", "node_name": "Node"},
                    title=f"15-min nodal prices — {_pp_date_str}",
                )
                _pp_fig4.update_layout(height=400, showlegend=len(_pp_pivot_price.columns) <= 20)
                st.plotly_chart(_pp_fig4, use_container_width=True)

                # ── Panel 5: BESS cleared energy (intraday) ───────────────────
                st.subheader("Panel 5 — BESS Intraday Cleared Energy")
                if not _pp_bess_df.empty:
                    st.caption(
                        f"{len(_pp_bess_df['asset_name'].unique())} bidirectional unit(s) detected "
                        f"(had negative cleared_energy on {_pp_date_str}). "
                        "Positive = discharge (injection), negative = charge (absorption)."
                    )
                    _pp_bess_pivot = _pp_bess_df.pivot_table(
                        index="datetime", columns="asset_name",
                        values="cleared_energy_mwh", aggfunc="sum",
                    )
                    _pp_fig5 = _pp_px.bar(
                        _pp_bess_pivot,
                        labels={"datetime": "Time", "value": "Cleared Energy (MWh)", "asset_name": "Asset"},
                        title=f"BESS intraday cleared energy — {_pp_date_str}",
                        barmode="group",
                        color_discrete_sequence=_pp_px.colors.qualitative.Set2,
                    )
                    _pp_fig5.update_layout(height=350)
                    st.plotly_chart(_pp_fig5, use_container_width=True)

                    # Show asset list
                    _pp_bess_summary = (
                        _pp_bess_df.groupby("asset_name")["cleared_energy_mwh"]
                        .agg(max_discharge=("max"), max_charge=("min"), net_mwh=("sum"))
                        .reset_index()
                    )
                    _pp_bess_summary.columns = ["Asset", "Max Discharge (MWh)", "Max Charge (MWh)", "Net (MWh)"]
                    st.dataframe(_pp_bess_summary, use_container_width=True, hide_index=True)
                else:
                    st.info(
                        f"No bidirectional (BESS) dispatch units found for {_pp_date_str} "
                        "in `marketdata.md_id_cleared_energy`. "
                        "Ensure the Excel file for this date has been ingested via Data Management."
                    )

# ---------------------------------------------------------------------------
# Tab 8: Trader
# ---------------------------------------------------------------------------
with tab_trader:
    import anthropic as _ant
    import json as _json

    _TRADER_APP = "mengxi_trader"
    _TRADER_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
    if not _TRADER_API_KEY:
        st.error("ANTHROPIC_API_KEY not set — Trader agent unavailable.")
        _trader_client = None
    else:
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
                "Returns: interval_start, nominated_dispatch_mw (申报曲线, MW), "
                "actual_dispatch_mw (实际充放曲线, MW), nodal_price_excel (CNY/MWh). "
                "Positive = discharge, negative = charge. "
                "Use to analyse execution gaps, nomination vs actual, and intraday dispatch patterns."
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
                               nominated_dispatch_mw, actual_dispatch_mw,
                               nodal_price_excel
                        FROM marketdata.ops_bess_dispatch_15min
                        WHERE asset_code = :asset AND data_date = :dt
                        ORDER BY interval_start
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
        "Ask about asset P&L, dispatch, or market conditions…",
        disabled=not _trader_client,
        key="trader_chat_input",
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

