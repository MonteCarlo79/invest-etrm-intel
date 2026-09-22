"""Nodal Trading tab — L1/L2 forecasts, L3 asset agents, trader comparison.

Rendered inside apps/mengxi-dashboard/app.py as the "Nodal Trading" tab
(after "Nodal Analysis"). Data sources:
  - marketdata.nodal_strategy_daily      (L3 converged per-asset strategies: 96-pt MW curves + assumptions)
  - marketdata.nodal_fc_grid_daily       (L1 grid price level; LingFeng RT fallback via services.nodal_forecast.db.get_grid_forecast)
  - marketdata.md_mengxi_nodal_price_96  (Fengxing actual RT nodal prices, 96 intervals/day)
  - marketdata.nodal_node_registry       (reviewed node map: substation, N-1 caps, zones)
  - marketdata.nodal_asset_registry      (living BESS asset registry — editor in S4)
  - marketdata.strategy_experiments      (promote loop, scope='nodal_agent')
  - reports.bess_asset_daily_attribution (trader realized P&L ladder)

Timezone note (mirrors services/mengxi_nodal/data.py): md_mengxi_nodal_price_96
.metric_time is timestamptz (CST) — single-day display queries use explicit
+08 bounds. S3 P&L bucketing mirrors services/nodal_agents/writer.py
(metric_time::date in session TZ) so tab numbers match the promote loop.
"""
from __future__ import annotations

import json
from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text as _text

from services.nodal_forecast import db as _fc_db
from services.nodal_forecast import registry_extract as _reg_extract

_GRID_PROVINCE = "蒙西"  # matches services.nodal_agents.writer._GRID_PROVINCE
_EXPERIMENT_SCOPE = "nodal_agent"
_SHAPE_LOOKBACK_DAYS = 30  # matches writer.SHAPE_LOOKBACK_DAYS

_STATUS_BADGE = {
    "champion": "🟢 champion",
    "candidate": "🟡 candidate",
    "retired-candidate": "🔴 retired-candidate",
}

# Editable columns in the S4 registry grid (plant_name is the key, updated_at
# is server-managed). Upsert never deletes — retirement is active=FALSE.
_DIFF_COLS = ["node", "substation", "capacity_mw", "duration_h", "rte_pct",
              "zone", "settle_node", "active"]


# ------------------------------------------------------------------- helpers

def _realized_pnl(curve, prices) -> float | None:
    """Realized CNY/day = sum_t curve_t x price_t x 0.25 (curve MW net,
    15-min intervals). Same arithmetic as
    services.nodal_agents.writer.register_strategies. None on bad input."""
    c = np.asarray(curve, dtype=float)
    p = np.asarray(prices, dtype=float)
    if c.shape != (96,) or p.shape != (96,):
        return None
    if not (np.isfinite(c).all() and np.isfinite(p).all()):
        return None
    return float((c * p).sum() * 0.25)


def _parse_json(raw):
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        if pd.isna(raw):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _parse_curve(raw) -> list | None:
    v = _parse_json(raw)
    if isinstance(v, list) and len(v) == 96:
        try:
            return [float(x) for x in v]
        except (TypeError, ValueError):
            return None
    return None


def _badge(status) -> str:
    if status is None:
        return "—"
    try:
        if pd.isna(status):
            return "—"
    except (TypeError, ValueError):
        pass
    return _STATUS_BADGE.get(str(status), "—")


def _price_node(reg_row: dict, assumptions: dict):
    """Mirrors services.nodal_agents.writer._price_node."""
    return (reg_row.get("fengxing_node_name") or assumptions.get("settle_node")
            or assumptions.get("node"))


def _numeric(df: pd.DataFrame, cols) -> pd.DataFrame:
    """NUMERIC columns arrive as Decimal objects (Arrow-unsafe) — coerce."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _cst_day_bounds(day: date) -> tuple[str, str]:
    return f"{day} 00:00:00+08", f"{day + timedelta(days=1)} 00:00:00+08"


def _clean(v):
    """DBAPI-safe scalar: numpy/pandas sentinels -> python None."""
    if v is None:
        return None
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v) if np.isfinite(v) else None
    if isinstance(v, float) and np.isnan(v):
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def _cell_eq(a, b) -> bool:
    if a is None and b is None:
        return True
    try:
        if pd.isna(a) and pd.isna(b):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(a, (bool, np.bool_)) or isinstance(b, (bool, np.bool_)):
        return bool(a) == bool(b)
    if (isinstance(a, (int, float, np.integer, np.floating))
            and isinstance(b, (int, float, np.integer, np.floating))):
        return float(a) == float(b)
    return a == b


def _diff_registry_rows(orig: pd.DataFrame, edited: pd.DataFrame) -> list[dict]:
    """Rows of `edited` that differ from `orig` on any editable column."""
    if orig.empty or edited.empty:
        return []
    orig_idx = {r["plant_name"]: r for r in orig.to_dict("records")}
    out = []
    for er in edited.to_dict("records"):
        name = er.get("plant_name")
        orow = orig_idx.get(name)
        if orow is None:
            continue  # grid is fixed-row; no inserts through the editor
        if any(not _cell_eq(orow.get(c), er.get(c)) for c in _DIFF_COLS):
            out.append({c: _clean(er.get(c)) for c in ["plant_name"] + _DIFF_COLS})
    return out


# ------------------------------------------------------------------- loaders

@st.cache_data(ttl=300, show_spinner="Loading strategies…")
def load_strategies(_engine, start: date, end: date) -> pd.DataFrame:
    """Latest converged strategy per (plant, target_date) in [start, end]."""
    q = _text("""
        SELECT DISTINCT ON (plant_name, target_date)
               plant_name, target_date, curve_json, assumptions_json,
               iterations, convergence_delta_mwh, model_version
        FROM marketdata.nodal_strategy_daily
        WHERE target_date BETWEEN :s AND :e
        ORDER BY plant_name, target_date, created_at DESC, model_version DESC
    """)
    df = pd.read_sql(q, _engine, params={"s": start, "e": end})
    return _numeric(df, ["iterations", "convergence_delta_mwh"])


@st.cache_data(ttl=300)
def load_grid_forecast(_engine, target_date: date) -> dict:
    """L1 grid price level for target_date; LingFeng RT fallback when the L1
    table has no row (same fallback as the L3 writer, via raw DBAPI conn)."""
    q = _text("""
        SELECT price_hat, model_version, fc_date
        FROM marketdata.nodal_fc_grid_daily
        WHERE province = :p AND target_date = :d AND price_hat IS NOT NULL
        ORDER BY fc_date DESC
    """)
    df = pd.read_sql(q, _engine, params={"p": _GRID_PROVINCE, "d": target_date})
    if not df.empty:
        row = df.iloc[0]
        return {"price_hat": float(row["price_hat"]),
                "model_version": str(row["model_version"]),
                "source": "L1 nodal_fc_grid_daily"}
    conn = _engine.raw_connection()
    try:
        fb = _fc_db.get_grid_forecast(conn, target_date)
    finally:
        conn.close()
    if fb.empty or fb["price_hat"].dropna().empty:
        return {"price_hat": None, "model_version": None, "source": "no data"}
    return {"price_hat": float(fb["price_hat"].dropna().astype(float).mean()),
            "model_version": str(fb["model_version"].iloc[0]),
            "source": "LingFeng RT fallback"}


@st.cache_data(ttl=300)
def load_node_actual_curve(_engine, node: str, day: date) -> list | None:
    """Actual RT curve (96 floats/None) for one node on one CST day."""
    s, e = _cst_day_bounds(day)
    q = _text("""
        SELECT time_order_96, avg_node_price
        FROM marketdata.md_mengxi_nodal_price_96
        WHERE node_name = :n AND metric_time >= :s AND metric_time < :e
    """)
    df = pd.read_sql(q, _engine, params={"n": node, "s": s, "e": e})
    if df.empty:
        return None
    v = np.full(97, np.nan)
    for slot, px in zip(df["time_order_96"], df["avg_node_price"]):
        if pd.notna(px) and 1 <= int(slot) <= 96:
            v[int(slot)] = float(px)
    return [None if np.isnan(x) else float(x) for x in v[1:]]


@st.cache_data(ttl=300)
def load_node_shape(_engine, node: str, day: date) -> list | None:
    """30-day mean RT curve for the node, normalized to mean 1 (same recipe
    as services.nodal_agents.writer._load_shapes)."""
    s = f"{day - timedelta(days=_SHAPE_LOOKBACK_DAYS)} 00:00:00+08"
    e = f"{day} 00:00:00+08"
    q = _text("""
        SELECT time_order_96, AVG(avg_node_price) AS p
        FROM marketdata.md_mengxi_nodal_price_96
        WHERE node_name = :n AND metric_time >= :s AND metric_time < :e
        GROUP BY time_order_96
    """)
    df = pd.read_sql(q, _engine, params={"n": node, "s": s, "e": e})
    if df.empty:
        return None
    v = np.full(97, np.nan)
    for slot, px in zip(df["time_order_96"], df["p"]):
        if pd.notna(px) and 1 <= int(slot) <= 96:
            v[int(slot)] = float(px)
    vec = v[1:]
    mean = np.nanmean(vec)
    if not np.isfinite(mean) or abs(mean) < 1e-9:
        return None
    shape = np.nan_to_num(vec / mean, nan=1.0)
    return [float(x) for x in shape]


@st.cache_data(ttl=300)
def load_node_registry(_engine) -> pd.DataFrame:
    q = _text("""
        SELECT name, voltage_kv, substation, transformers, rated_mva,
               n1_firm_mva, fengxing_node_name, zone, connected_plants, source
        FROM marketdata.nodal_node_registry
        ORDER BY name
    """)
    df = pd.read_sql(q, _engine)
    return _numeric(df, ["voltage_kv", "transformers", "rated_mva", "n1_firm_mva"])


@st.cache_data(ttl=300)
def load_asset_registry(_engine) -> pd.DataFrame:
    q = _text("""
        SELECT plant_name, node, substation, capacity_mw, duration_h,
               rte_pct, zone, settle_node, active, updated_at
        FROM marketdata.nodal_asset_registry
        ORDER BY plant_name
    """)
    df = pd.read_sql(q, _engine)
    df = _numeric(df, ["capacity_mw", "duration_h", "rte_pct"])
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
    if "active" in df.columns:
        df["active"] = df["active"].astype("boolean")
    return df


@st.cache_data(ttl=300)
def load_promote_status(_engine) -> pd.DataFrame:
    """Latest strategy_experiments row per plant (scope='nodal_agent')."""
    q = _text("""
        SELECT DISTINCT ON (province)
               province AS plant_name, model, status, mean_capture_rate,
               delta_vs_champion, window_end
        FROM marketdata.strategy_experiments
        WHERE scope = :scope
        ORDER BY province, window_end DESC
    """)
    df = pd.read_sql(q, _engine, params={"scope": _EXPERIMENT_SCOPE})
    return _numeric(df, ["mean_capture_rate", "delta_vs_champion"])


@st.cache_data(ttl=300)
def load_trader_attribution(_engine, start: date, end: date) -> pd.DataFrame:
    q = _text("""
        SELECT trade_date, asset_code, cleared_actual_pnl,
               grid_restriction_loss, forecast_error_loss, strategy_error_loss
        FROM reports.bess_asset_daily_attribution
        WHERE trade_date BETWEEN :s AND :e
    """)
    df = pd.read_sql(q, _engine, params={"s": start, "e": end})
    return _numeric(df, ["cleared_actual_pnl", "grid_restriction_loss",
                         "forecast_error_loss", "strategy_error_loss"])


@st.cache_data(ttl=300)
def load_price_node_map(_engine) -> pd.DataFrame:
    """plant -> pricing node (fengxing_node_name via node, else settle_node,
    else node) — same resolution as writer.register_strategies."""
    q = _text("""
        SELECT a.plant_name, a.node, a.settle_node, n.fengxing_node_name
        FROM marketdata.nodal_asset_registry a
        LEFT JOIN marketdata.nodal_node_registry n ON n.name = a.node
        WHERE a.active IS TRUE
    """)
    return pd.read_sql(q, _engine)


@st.cache_data(ttl=300)
def load_actual_price_vectors(_engine, nodes: tuple, start: date, end: date) -> dict:
    """{(node, 'YYYY-MM-DD'): [96 floats]} for days with all 96 slots finite
    (partial days misalign revenue — skipped, same as the writer). Day
    bucketing mirrors writer.register_strategies (metric_time::date)."""
    if not nodes:
        return {}
    s = f"{start} 00:00:00+08"
    e = f"{end + timedelta(days=1)} 00:00:00+08"
    q = _text("""
        SELECT node_name, metric_time::date AS d, time_order_96, avg_node_price
        FROM marketdata.md_mengxi_nodal_price_96
        WHERE node_name = ANY(:nodes) AND metric_time >= :s AND metric_time < :e
    """)
    df = pd.read_sql(q, _engine, params={"nodes": list(nodes), "s": s, "e": e})
    out: dict = {}
    if df.empty:
        return out
    df = df[pd.notna(df["avg_node_price"])]
    for (node, d), g in df.groupby(["node_name", "d"]):
        v = np.full(97, np.nan)
        for slot, px in zip(g["time_order_96"], g["avg_node_price"]):
            if 1 <= int(slot) <= 96:
                v[int(slot)] = float(px)
        vec = v[1:]
        if np.isfinite(vec).all():
            out[(str(node), str(d)[:10])] = [float(x) for x in vec]
    return out


_REG_UPSERT = """INSERT INTO marketdata.nodal_asset_registry
    (plant_name, node, substation, capacity_mw, duration_h, rte_pct,
     zone, settle_node, active, updated_at)
    VALUES (%(plant_name)s, %(node)s, %(substation)s, %(capacity_mw)s,
            %(duration_h)s, %(rte_pct)s, %(zone)s, %(settle_node)s,
            %(active)s, NOW())
    ON CONFLICT (plant_name) DO UPDATE SET
        node = EXCLUDED.node,
        substation = EXCLUDED.substation,
        capacity_mw = EXCLUDED.capacity_mw,
        duration_h = EXCLUDED.duration_h,
        rte_pct = EXCLUDED.rte_pct,
        zone = EXCLUDED.zone,
        settle_node = EXCLUDED.settle_node,
        active = EXCLUDED.active,
        updated_at = NOW()"""


def save_asset_registry_rows(_engine, rows: list[dict]) -> int:
    """Upsert edited registry rows (ON CONFLICT plant_name). Never deletes —
    retirement is active=FALSE edited through the grid like any other change."""
    if not rows:
        return 0
    conn = _engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.executemany(_REG_UPSERT, rows)
        conn.commit()
    finally:
        conn.close()
    load_asset_registry.clear()
    return len(rows)


def refresh_registry_from_md(_engine, lookback_days: int = 90) -> dict:
    """Re-run the md_id_cleared_energy extraction and upsert into
    nodal_asset_registry (living-registry requirement 2026-09-22;
    services.nodal_forecast.registry_extract does the upsert + soft-retire)."""
    conn = _engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT plant_name, data_date, datetime, cleared_energy_mwh
               FROM marketdata.md_id_cleared_energy
               WHERE data_date >= CURRENT_DATE - %s""",
            (lookback_days,),
        )
        cols = ["plant_name", "data_date", "datetime", "cleared_energy_mwh"]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        plants = _reg_extract.extract_bess_plants(rows)
        result = _reg_extract.update_asset_registry(conn, plants,
                                                    source="md_extract_ui")
    finally:
        conn.close()
    load_asset_registry.clear()
    return result


# -------------------------------------------------------------------- render

def render(get_engine) -> None:
    st.title("Nodal Trading — Forecasts, Asset Agents & Trader Comparison")
    st.caption(
        "Sources: marketdata.nodal_strategy_daily · nodal_fc_grid_daily · "
        "md_mengxi_nodal_price_96 · nodal_node_registry / nodal_asset_registry · "
        "strategy_experiments (scope='nodal_agent') · reports.bess_asset_daily_attribution"
    )

    engine = get_engine()

    # ── S1: Forecast & stack ────────────────────────────────────────────────
    st.header("1 · Forecast & Stack")
    node_df = load_node_registry(engine)
    _s1c1, _s1c2 = st.columns([2, 1])
    with _s1c1:
        if node_df.empty:
            st.info("nodal_node_registry is empty — populate the node map first (S4 below).")
            _s1_node = None
        else:
            _s1_node = st.selectbox("Node", node_df["name"].tolist(), key="nt_s1_node")
    with _s1c2:
        _s1_day = st.date_input("Date", value=date.today() - timedelta(days=1),
                                key="nt_s1_day")

    if _s1_node is not None:
        _fc = load_grid_forecast(engine, _s1_day)
        _m1, _m2, _m3 = st.columns(3)
        _m1.metric("L1 grid price forecast",
                   f"{_fc['price_hat']:.1f} CNY/MWh" if _fc.get("price_hat") is not None else "—")
        _m2.metric("forecast source", _fc.get("source") or "—")
        _m3.metric("model version", _fc.get("model_version") or "—")

        _actual = load_node_actual_curve(engine, _s1_node, _s1_day)
        _implied = None
        for _r in load_strategies(engine, _s1_day, _s1_day).itertuples():
            _asm = _parse_json(_r.assumptions_json) or {}
            if _asm.get("node") == _s1_node and _asm.get("grid_price_hat") is not None:
                _shape = load_node_shape(engine, _s1_node, _s1_day)
                if _shape is not None:
                    _implied = [float(_asm["grid_price_hat"]) * s for s in _shape]
                break

        if _actual is None and _implied is None and _fc.get("price_hat") is None:
            st.info(f"No actual RT data, strategy, or grid forecast for {_s1_node} on {_s1_day}.")
        else:
            _fig = go.Figure()
            _xs = list(range(1, 97))
            if _actual is not None:
                _fig.add_trace(go.Scatter(
                    x=_xs, y=_actual, name="actual RT node price", mode="lines",
                    line=dict(color="#d62728", width=1.6)))
            if _implied is not None:
                _fig.add_trace(go.Scatter(
                    x=_xs, y=_implied, name="strategy forecast (L1 level × 30d shape)",
                    mode="lines", line=dict(color="#1f77b4", width=1.4, dash="dash")))
            if _fc.get("price_hat") is not None:
                _fig.add_hline(y=_fc["price_hat"],
                               line=dict(color="#2ca02c", width=1.2, dash="dot"),
                               annotation_text="L1 grid level",
                               annotation_position="top left")
            _fig.update_layout(height=380, margin=dict(l=40, r=20, t=30, b=40),
                               xaxis_title="15-min interval", yaxis_title="CNY/MWh",
                               legend=dict(orientation="h", y=1.12))
            st.plotly_chart(_fig, use_container_width=True)
            if _implied is not None:
                st.caption(
                    "Strategy forecast = grid_price_hat from the strategy's assumptions × the node's "
                    "30-day mean RT curve normalized to mean 1 (same shape source as the L3 writer). "
                    "The per-iteration BESS-sensitivity adjustment is not persisted and is excluded."
                )

    # ── S2: Asset agents ────────────────────────────────────────────────────
    st.header("2 · Asset Agents")
    _s2_day = st.date_input("Strategy date", value=date.today() - timedelta(days=1),
                            key="nt_s2_day")
    _strat = load_strategies(engine, _s2_day, _s2_day)
    _promo = load_promote_status(engine)
    if _strat.empty:
        st.info(
            f"No converged strategies in nodal_strategy_daily for {_s2_day}. "
            "The daily L3 writer (services.nodal_agents.writer.run_day) populates this table."
        )
    else:
        _promo_map = {}
        if not _promo.empty:
            _promo_map = _promo.set_index("plant_name")["status"].to_dict()
        _rows = []
        for _r in _strat.itertuples():
            _asm = _parse_json(_r.assumptions_json) or {}
            _ep = _asm.get("expected_profit_yuan")
            _rows.append({
                "plant": str(_r.plant_name),
                "model_version": str(_r.model_version),
                "iterations": int(_r.iterations) if pd.notna(_r.iterations) else 0,
                "convergence_delta_mwh": (float(_r.convergence_delta_mwh)
                                          if pd.notna(_r.convergence_delta_mwh) else np.nan),
                "expected_profit_yuan": float(_ep) if _ep is not None else np.nan,
                "solver_status": str(_asm.get("solver_status") or "—"),
                "promote": _badge(_promo_map.get(_r.plant_name)),
            })
        _summary = pd.DataFrame(_rows)
        st.dataframe(_summary, use_container_width=True, hide_index=True)

        _s2_plant = st.selectbox("Asset detail", _summary["plant"].tolist(),
                                 key="nt_s2_plant")
        _sel = next(r for r in _strat.itertuples() if str(r.plant_name) == _s2_plant)
        _asm = _parse_json(_sel.assumptions_json) or {}

        def _f1(key, fmt="{:.1f}"):
            v = _asm.get(key)
            return fmt.format(float(v)) if v is not None else "—"

        _c = st.columns(3)
        _c[0].metric("capacity", f"{_f1('capacity_mw')} MW")
        _c[1].metric("duration", f"{_f1('duration_h')} h")
        _c[2].metric("round-trip eff", f"{_f1('rte_pct')} %")
        _c = st.columns(3)
        _c[0].metric("substation cap", f"{_f1('cap_mw')} MW")
        _c[1].metric("effective power", f"{_f1('power_mw_eff')} MW")
        _c[2].metric("expected profit",
                     f"{float(_asm['expected_profit_yuan']):,.0f} CNY"
                     if _asm.get("expected_profit_yuan") is not None else "—")
        _c = st.columns(3)
        _c[0].metric("no-discharge intervals", int(_asm.get("zone_no_discharge_intervals") or 0))
        _c[1].metric("no-charge intervals", int(_asm.get("zone_no_charge_intervals") or 0))
        _c[2].metric("headroom min", f"{_f1('substation_headroom_min_mw')} MW")
        st.markdown(
            f"**Promote status:** {_badge(_promo_map.get(_s2_plant))}  ·  "
            f"**solver:** {_asm.get('solver_status') or '—'}  ·  "
            f"**node:** `{_asm.get('node') or '—'}`  ·  "
            f"**substation:** {_asm.get('substation') or '—'}  ·  "
            f"**zone:** {_asm.get('zone') or '—'}  ·  "
            f"**settle node:** `{_asm.get('settle_node') or '—'}`"
        )
        _curve = _parse_curve(_sel.curve_json)
        if _curve is None:
            st.warning("curve_json is missing or unparseable for this row.")
        else:
            _fig2 = go.Figure(go.Scatter(
                x=list(range(1, 97)), y=_curve, mode="lines",
                name="nominated strategy (MW net)", line=dict(color="#1f77b4", width=1.8)))
            _fig2.add_hline(y=0, line=dict(color="#888", width=0.8))
            _fig2.update_layout(height=320, margin=dict(l=40, r=20, t=30, b=40),
                                xaxis_title="15-min interval",
                                yaxis_title="MW ( + discharge / − charge )")
            st.plotly_chart(_fig2, use_container_width=True)

    # ── S3: Trader vs recursive-optimal ─────────────────────────────────────
    st.header("3 · Trader vs Recursive-Optimal")
    st.caption(
        "Trader realized ladder (reports.bess_asset_daily_attribution) side-by-side with "
        "recursive-optimal realized (strategy curve × actual RT price × 0.25 — same arithmetic as "
        "services.nodal_agents.writer.register_strategies), summed per asset per month."
    )
    _today = date.today()
    _s3_default_start = (_today.replace(day=1) - timedelta(days=1)).replace(day=1)
    _s3c1, _s3c2 = st.columns(2)
    with _s3c1:
        _s3_start = st.date_input("From", value=_s3_default_start, key="nt_s3_start")
    with _s3c2:
        _s3_end = st.date_input("To", value=_today - timedelta(days=1), key="nt_s3_end")

    if _s3_start > _s3_end:
        st.warning("Start date must be ≤ end date.")
    else:
        _s3_strat = load_strategies(engine, _s3_start, _s3_end)
        _attr = load_trader_attribution(engine, _s3_start, _s3_end)
        if _s3_strat.empty or _attr.empty:
            st.info(
                "S3 needs BOTH converged strategies (marketdata.nodal_strategy_daily) AND trader "
                "attribution rows (reports.bess_asset_daily_attribution) in the selected window. "
                "Until the daily L3 writer and the attribution job have both run over overlapping "
                "days, there is nothing to compare."
            )
        else:
            _plants = sorted(str(p) for p in _s3_strat["plant_name"].unique())
            _codes = sorted(str(c) for c in _attr["asset_code"].unique())
            _exact = sorted(set(_plants) & set(_codes))
            _lower: dict = {}
            for _p in _plants:
                _lower.setdefault(_p.lower(), _p)
            _ci = {}  # attr asset_code -> strategy plant_name (case-insensitive)
            for _a in _codes:
                if _a not in _exact and _a.lower() in _lower:
                    _ci[_a] = _lower[_a.lower()]
            _overlap = sorted(set(_exact) | set(_ci.values()))
            if not _overlap:
                st.info(
                    f"No overlap between strategy plants ({len(_plants)}) and attribution assets "
                    f"({len(_codes)}) — check plant_name ↔ asset_code naming."
                )
            else:
                _code2plant = {a: a for a in _exact}
                _code2plant.update(_ci)
                _pairs = [f"{a} ↔ {p}" for a, p in sorted(_ci.items())]
                st.caption(
                    f"Overlap: {len(_overlap)} asset(s) (exact match)"
                    + (f"; case-insensitive mapping: {', '.join(_pairs)}" if _pairs else "")
                )
                _pmap_df = load_price_node_map(engine)
                _pmap = ({r["plant_name"]: r for r in _pmap_df.to_dict("records")}
                         if not _pmap_df.empty else {})

                _needed_rows = []
                for _r in _s3_strat.itertuples():
                    _plant = str(_r.plant_name)
                    if _plant not in _overlap:
                        continue
                    _asm = _parse_json(_r.assumptions_json) or {}
                    _node = _price_node(_pmap.get(_plant) or {}, _asm)
                    if _node is not None:
                        _needed_rows.append((_plant, str(_node), _r))
                _prices = load_actual_price_vectors(
                    engine, tuple(sorted({n for _, n, _ in _needed_rows})),
                    _s3_start, _s3_end)

                _rec: dict = {}  # (plant, month) -> [pnl_sum, days]
                for _plant, _node, _r in _needed_rows:
                    _curve = _parse_curve(_r.curve_json)
                    if _curve is None:
                        continue
                    _dstr = pd.Timestamp(_r.target_date).strftime("%Y-%m-%d")
                    _px = _prices.get((_node, _dstr))
                    if _px is None:
                        continue
                    _pnl = _realized_pnl(_curve, _px)
                    if _pnl is None:
                        continue
                    _k = (_plant, _dstr[:7])
                    _s, _n = _rec.get(_k, (0.0, 0))
                    _rec[_k] = (_s + _pnl, _n + 1)

                _trader: dict = {}  # (plant, month) -> [cleared, grid, fcst, strat]
                for _r in _attr.itertuples():
                    _plant = _code2plant.get(str(_r.asset_code))
                    if _plant is None:
                        continue
                    _k = (_plant, pd.Timestamp(_r.trade_date).strftime("%Y-%m"))
                    _a = _trader.get(_k, [0.0, 0.0, 0.0, 0.0])
                    _a[0] += float(_r.cleared_actual_pnl) if pd.notna(_r.cleared_actual_pnl) else 0.0
                    _a[1] += float(_r.grid_restriction_loss) if pd.notna(_r.grid_restriction_loss) else 0.0
                    _a[2] += float(_r.forecast_error_loss) if pd.notna(_r.forecast_error_loss) else 0.0
                    _a[3] += float(_r.strategy_error_loss) if pd.notna(_r.strategy_error_loss) else 0.0
                    _trader[_k] = _a

                _keys = sorted(set(_trader) | set(_rec))
                if not _keys:
                    st.info("Overlap found but no comparable days (strategies lack full-day actual RT prices).")
                else:
                    _tbl = []
                    for _plant, _month in _keys:
                        _ca, _grl, _fel, _sel = _trader.get((_plant, _month),
                                                            [0.0, 0.0, 0.0, 0.0])
                        _rp, _nd = _rec.get((_plant, _month), (0.0, 0))
                        _tbl.append({
                            "asset": _plant, "month": _month,
                            "cleared_actual_pnl": _ca,
                            "grid_restriction_loss": _grl,
                            "forecast_error_loss": _fel,
                            "strategy_error_loss": _sel,
                            "recursive_optimal_pnl": _rp,
                            "recursive_days": int(_nd),
                            "delta (recursive − actual)": _rp - _ca,
                        })
                    st.dataframe(pd.DataFrame(_tbl), use_container_width=True,
                                 hide_index=True)

    # ── S4: Registry & data ─────────────────────────────────────────────────
    st.header("4 · Registry & Data")

    st.subheader("Node map (nodal_node_registry)")
    if node_df.empty:
        st.info("nodal_node_registry is empty — load the reviewed node map first.")
    else:
        st.dataframe(node_df, use_container_width=True, hide_index=True)

    st.subheader("Asset registry (nodal_asset_registry)")
    _reg = load_asset_registry(engine)
    if _reg.empty:
        st.info("nodal_asset_registry is empty — use the refresh button below to seed it from md_* data.")
    else:
        _edited = st.data_editor(
            _reg, key="nt_reg_editor", hide_index=True, use_container_width=True,
            disabled=["plant_name", "updated_at"],
        )
        if st.button("Save registry changes", key="nt_reg_save"):
            _changed = _diff_registry_rows(_reg, _edited)
            if not _changed:
                st.info("No registry changes to save.")
            else:
                _n = save_asset_registry_rows(engine, _changed)
                st.success(
                    f"Upserted {_n} registry row(s): "
                    + ", ".join(str(r["plant_name"]) for r in _changed)
                )
        st.caption(
            "Edits upsert ON CONFLICT (plant_name) — rows are never deleted; retire an asset "
            "by unticking **active**. plant_name and updated_at are locked."
        )

    if st.button("Refresh from md_* extraction", key="nt_reg_refresh"):
        _res = refresh_registry_from_md(engine)
        st.success(
            f"Registry refreshed: upserted {_res.get('upserted', 0)}, "
            f"retired {_res.get('retired', 0)} (source={_res.get('source', '?')})."
        )

    st.subheader("Backtest metrics")
    st.info(
        "The nodal backtest runs offline: services/nodal_forecast/backtest.py "
        "`nodal_mae(model_fn, days)` scores the L2 nodal model vs actuals per cluster and "
        "always reports the grid-only baseline alongside (gate: nodal MAE ≤ baseline). "
        "No persisted backtest-results table exists yet — this section will surface the "
        "metrics once one lands."
    )
