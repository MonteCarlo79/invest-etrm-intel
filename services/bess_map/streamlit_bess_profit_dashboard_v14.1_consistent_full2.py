# streamlit_bess_profit_dashboard_v14.1_consistent_full.py
# ==========================================================
# v14.1 CONSISTENT FULL VERSION

import os
import argparse
from datetime import timedelta
import numpy as np
import pandas as pd
import streamlit as st
import altair as alt
from sqlalchemy import create_engine, text

import sys
from pathlib import Path

st.set_page_config(page_title="BESS Profit Dashboard", layout="wide")

ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT))

from auth.rbac import get_user, get_groups, get_role, get_email, require_role

require_role(["Admin", "Trader", "Quant", "Analyst"])

def resolve_role() -> str | None:
    email = (get_email() or "").strip().lower()
    groups = [g.strip().lower() for g in (get_groups() or [])]

    group_role_map = {
        "admin": "Admin",
        "trader": "Trader",
        "quant": "Quant",
        "analyst": "Analyst",
    }

    for g in groups:
        if g in group_role_map:
            return group_role_map[g]

    raw_map = os.getenv("EMAIL_ROLE_MAP", "")
    mapping = {}

    for item in raw_map.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        k, v = item.split("=", 1)
        mapping[k.strip().lower()] = v.strip()

    if email in mapping:
        return mapping[email]

    if email == "chen_dpeng@hotmail.com":
        return "Admin"

    return "Analyst"


allowed_roles = ["Admin", "Trader", "Quant", "Analyst"]

user = get_user()
if not user:
    st.warning("Please log in via SSO.")
    st.stop()

role = get_role() or resolve_role()

if not role:
    st.error(f"Access denied. No valid role found. Email: {get_email()}")
    st.stop()

if role not in allowed_roles:
    st.error(f"Access denied. Your role: {role}. Allowed roles: {allowed_roles}")
    st.stop()

user_email = user.get("email", "unknown")
st.caption(f"User: {user_email} | Role: {role}")

st.write("### SIGNATURE: bess-map v11 full2 2026-02-18")


DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "!BESSmap2026")
DB_NAME = os.getenv("POSTGRES_DB", "marketdata")
DB_HOST = os.getenv("DB_HOST", "bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com")
DB_PORT = os.getenv("DB_PORT", "5432")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require",
    pool_pre_ping=True
)



CACHE_BUST_FILE = "/data/cache_bust.txt"

@st.cache_data
def get_cache_bust():
    if os.path.exists(CACHE_BUST_FILE):
        return os.path.getmtime(CACHE_BUST_FILE)
    return 0

cache_bust = get_cache_bust()

if st.button("🔄 Refresh data (clear cache)"):
    st.cache_data.clear()
    st.rerun()


# -----------------------------
# DB helpers
# -----------------------------
def get_provinces(_engine, schema: str):
    df = pd.read_sql(
        f"SELECT DISTINCT province FROM {schema}.spot_prices_hourly ORDER BY 1",
        _engine,
    )
    return df["province"].tolist()


def get_min_max_ts(_engine, schema: str, price_type: str, duration_h: float, power_mw: float):
    table = "spot_dispatch_hourly_theoretical" if price_type == "rt" else "spot_dispatch_hourly_rt_forecast"

    df = pd.read_sql(
        text(f'''
            SELECT MIN(datetime) min_ts, MAX(datetime) max_ts
            FROM {schema}.{table}
            WHERE ABS(duration_h-:d)<1e-9
              AND ABS(power_mw-:p)<1e-9
        '''),
        _engine,
        params={"d": duration_h, "p": power_mw},
    )

    return pd.to_datetime(df.loc[0, "min_ts"]), pd.to_datetime(df.loc[0, "max_ts"])


# -----------------------------
# Data loading (hourly + capture)
# -----------------------------
@st.cache_data(ttl=60)
def load_hourly(_engine, schema: str, price_type: str, duration_h: float, power_mw: float, provinces, start_dt, end_dt, cache_bust):
    price_col = "rt_price" if price_type == "rt" else "da_price"
    table = "spot_dispatch_hourly_theoretical" if price_type == "rt" else "spot_dispatch_hourly_rt_forecast"

    sql = f'''
        SELECT d.datetime AS ts,
               d.province,
               d.charge_mw,
               d.discharge_mw,
               d.dispatch_grid_mw,
               p.{price_col} AS price
        FROM {schema}.{table} d
        JOIN {schema}.spot_prices_hourly p
          ON p.province=d.province AND p.datetime=d.datetime
        WHERE ABS(d.duration_h-:d)<1e-9
          AND ABS(d.power_mw-:p)<1e-9
          AND d.datetime BETWEEN :s AND :e
    '''
    params = {
        "d": duration_h,
        "p": power_mw,
        "s": start_dt,
        "e": end_dt,
    }

    if provinces:
        sql += " AND d.province = ANY(:provs)"
        params["provs"] = provinces

    df = pd.read_sql(text(sql), _engine, params=params, parse_dates=["ts"])
    df["dispatch_batt_mw"] = df["charge_mw"] - df["discharge_mw"]
    return df


@st.cache_data(ttl=60)
def load_capture_period(
    _engine, schema: str, model: str, duration_h: float, power_mw: float, eff: float,
    start_date, end_date, provinces, cache_bust,
):
    sql = f'''
        SELECT
            province,
            SUM(realized_profit_per_mwh_day) AS realised_s,
            SUM(theoretical_profit_per_mwh_day) AS theo_s
        FROM {schema}.bess_capture_daily
        WHERE model=:m
          AND ABS(duration_h-:d)<1e-9
          AND ABS(power_mw-:p)<1e-9
          AND ABS(roundtrip_eff-:e)<1e-9
          AND date BETWEEN :s AND :ed
    '''
    params = {
        "m": model,
        "d": duration_h,
        "p": power_mw,
        "e": eff,
        "s": start_date,
        "ed": end_date,
    }
    if provinces:
        sql += " AND province = ANY(:provs)"
        params["provs"] = provinces
    sql += " GROUP BY province"

    return pd.read_sql(text(sql), _engine, params=params)


def compute_period_kpi(hourly: pd.DataFrame, capture_df: pd.DataFrame, start_date, end_date, power_mw: float, duration_h: float):
    days = (end_date - start_date).days + 1
    rows = {}

    for prov, g in hourly.groupby("province"):
        g = g.dropna()

        charged = g.loc[g.dispatch_batt_mw > 0, "dispatch_batt_mw"].sum()
        discharged = -g.loc[g.dispatch_batt_mw < 0, "dispatch_batt_mw"].sum()

        charge_cost = (-g.loc[g.dispatch_grid_mw < 0, "dispatch_grid_mw"] * g.loc[g.dispatch_grid_mw < 0, "price"]).sum()
        discharge_rev = (g.loc[g.dispatch_grid_mw > 0, "dispatch_grid_mw"] * g.loc[g.dispatch_grid_mw > 0, "price"]).sum()

        theo_profit = discharge_rev - charge_cost

        cap = capture_df[capture_df.province == prov]
        realised_spread = np.nan
        capture_rate = np.nan
        if not cap.empty:
            realised_spread = cap.iloc[0].realised_s / charged if charged > 0 else np.nan
            capture_rate = cap.iloc[0].realised_s / cap.iloc[0].theo_s if cap.iloc[0].theo_s > 0 else np.nan

        rows[prov] = {
            "Total charged MWh (batt)": int(round(charged)),
            "Total discharged MWh (batt)": int(round(discharged)),
            "Total charging cost (grid)": int(round(charge_cost)),
            "Total discharging revenue (grid)": int(round(discharge_rev)),
            "Profitability (theoretical)": int(round(theo_profit)),
            "Unit charging cost": round(charge_cost / charged, 2) if charged > 0 else np.nan,
            "Unit discharging price": round(discharge_rev / discharged, 2) if discharged > 0 else np.nan,
            "Theoretical price spread": round(theo_profit / charged, 2) if charged > 0 else np.nan,
            "Realised price spread": round(realised_spread, 2),
            "Capture rate (%)": round(capture_rate * 100, 2) if pd.notna(capture_rate) else np.nan,
            "Avg daily cycles": round(charged / (days * power_mw * duration_h), 3) if days > 0 and power_mw > 0 and duration_h > 0 else np.nan,
        }

    return pd.DataFrame(rows)

@st.cache_data(ttl=60)
def load_daily_theoretical(_engine, schema: str, price_type: str, duration_h: float, power_mw: float, provinces, start_dt, end_dt, cache_bust):
    table = "spot_dispatch_hourly_theoretical" if price_type == "rt" else "spot_dispatch_hourly_rt_forecast"

    sql = f'''
        SELECT
            date_trunc('day', d.datetime)::date AS date,
            d.province AS province,
            SUM(d.charge_mw) AS charged_mwh,
            SUM(d.discharge_mw) AS discharged_mwh,
            SUM(CASE WHEN d.dispatch_grid_mw < 0 THEN -d.dispatch_grid_mw * p.rt_price ELSE 0 END) AS charge_cost,
            SUM(CASE WHEN d.dispatch_grid_mw > 0 THEN  d.dispatch_grid_mw * p.rt_price ELSE 0 END) AS discharge_rev
        FROM {schema}.{table} d
        JOIN {schema}.spot_prices_hourly p
          ON p.province=d.province AND p.datetime=d.datetime
        WHERE ABS(d.duration_h-:d)<1e-9
          AND ABS(d.power_mw-:p)<1e-9
          AND d.datetime BETWEEN :s AND :e
    '''

    params = {"d": duration_h, "p": power_mw, "s": start_dt, "e": end_dt}

    if provinces:
        sql += " AND d.province = ANY(:provs)"
        params["provs"] = provinces

    sql += " GROUP BY 1,2 ORDER BY 1,2"

    df = pd.read_sql(text(sql), _engine, params=params, parse_dates=["date"])
    df["theo_profit"] = df["discharge_rev"] - df["charge_cost"]
    return df


@st.cache_data(ttl=60)
def load_capture_monthly(_engine, schema: str, model: str, duration_h: float, power_mw: float, eff: float, start_date, end_date, provinces, cache_bust):
    sql = f'''
        SELECT
            date_trunc('month', date)::date AS month,
            province,
            SUM(realized_profit_per_mwh_day) AS realised_s,
            SUM(theoretical_profit_per_mwh_day) AS theo_s
        FROM {schema}.bess_capture_daily
        WHERE model=:m
          AND ABS(duration_h-:d)<1e-9
          AND ABS(power_mw-:p)<1e-9
          AND ABS(roundtrip_eff-:e)<1e-9
          AND date BETWEEN :s AND :ed
    '''
    params = {"m": model, "d": duration_h, "p": power_mw, "e": eff, "s": start_date, "ed": end_date}
    if provinces:
        sql += " AND province = ANY(:provs)"
        params["provs"] = provinces
    sql += " GROUP BY 1,2 ORDER BY 1,2"

    return pd.read_sql(text(sql), _engine, params=params, parse_dates=["month"])


def build_monthly_metrics(daily_theo: pd.DataFrame, cap_monthly: pd.DataFrame, power_mw: float, duration_h: float, start_date, end_date):
    d = daily_theo.copy()
    d["month"] = d["date"].dt.to_period("M").dt.to_timestamp().dt.date

    monthly_theo = (
        d.groupby(["month", "province"], as_index=False)
         .agg(
            charged_mwh=("charged_mwh", "sum"),
            discharged_mwh=("discharged_mwh", "sum"),
            charge_cost=("charge_cost", "sum"),
            discharge_rev=("discharge_rev", "sum"),
            theo_profit=("theo_profit", "sum"),
         )
    )

    rng = pd.date_range(pd.Timestamp(start_date), pd.Timestamp(end_date), freq="D")
    days_per_month = pd.Series(1, index=rng).groupby(rng.to_period("M")).sum()
    days_per_month.index = days_per_month.index.to_timestamp().date
    monthly_theo["days_in_range_month"] = monthly_theo["month"].map(days_per_month.to_dict()).astype(float)

    cap_m = cap_monthly.copy()
    cap_m["month"] = cap_m["month"].dt.date

    m = monthly_theo.merge(cap_m, on=["month", "province"], how="left")

    m["theoretical_spread"] = np.where(m["charged_mwh"] > 0, m["theo_profit"] / m["charged_mwh"], np.nan)
    m["realised_spread"] = np.where(m["charged_mwh"] > 0, m["realised_s"] / m["charged_mwh"], np.nan)
    m["capture_rate"] = np.where(m["theo_s"] > 0, m["realised_s"] / m["theo_s"], np.nan)
    m["cycles_avg_daily"] = np.where(
        (m["days_in_range_month"] > 0) & (power_mw > 0) & (duration_h > 0),
        m["charged_mwh"] / (m["days_in_range_month"] * power_mw * duration_h),
        np.nan,
    )

    m["theoretical_spread"] = m["theoretical_spread"].round(2)
    m["realised_spread"] = m["realised_spread"].round(2)
    m["capture_rate_pct"] = (m["capture_rate"] * 100).round(2)
    m["cycles_avg_daily"] = m["cycles_avg_daily"].round(3)

    for c in ["charged_mwh", "discharged_mwh", "charge_cost", "discharge_rev", "theo_profit"]:
        m[c] = m[c].round(0).astype("Int64")

    return m


def build_rolling_spread_method_b(daily_theo: pd.DataFrame, window_days: int):
    df = daily_theo.copy()
    df = df.sort_values(["province", "date"])

    out_frames = []
    for prov, g in df.groupby("province"):
        g = g.sort_values("date").set_index("date")
        roll_profit = g["theo_profit"].rolling(window_days, min_periods=max(3, window_days // 4)).sum()
        roll_charged = g["charged_mwh"].rolling(window_days, min_periods=max(3, window_days // 4)).sum()
        roll_spread = roll_profit / roll_charged
        tmp = pd.DataFrame({
            "date": roll_spread.index,
            "province": prov,
            "rolling_theoretical_spread": roll_spread.values,
        })
        out_frames.append(tmp)

    out = pd.concat(out_frames, ignore_index=True) if out_frames else pd.DataFrame(columns=["date", "province", "rolling_theoretical_spread"])
    out["rolling_theoretical_spread"] = out["rolling_theoretical_spread"].astype(float).round(2)
    return out


def altair_line(df: pd.DataFrame, x: str, y: str, color: str, title: str, y_domain=None):
    enc_y = alt.Y(f"{y}:Q", title=title)
    if y_domain is not None:
        enc_y = alt.Y(f"{y}:Q", title=title, scale=alt.Scale(domain=y_domain))
    return (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X(f"{x}:T"),
            y=enc_y,
            color=alt.Color(f"{color}:N"),
            tooltip=[x, color, y],
        )
        .properties(height=360)
    )


def altair_bar(df: pd.DataFrame, x: str, y: str, title: str, y_domain=None):
    enc_y = alt.Y(f"{y}:Q", title=title)
    if y_domain is not None:
        enc_y = alt.Y(f"{y}:Q", title=title, scale=alt.Scale(domain=y_domain))
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{x}:N", sort="-y"),
            y=enc_y,
            tooltip=[x, y],
        )
        .properties(height=360)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True)
    parser.add_argument("--schema", default="marketdata")
    args, _ = parser.parse_known_args()

    pgurl = os.getenv("PGURL")
    if not pgurl:
        st.error("PGURL not found in .env (expected key: PGURL)")
        return

    schema = args.schema

    st.title("BESS Profit Dashboard — v14.1 (Consistent Full)")

    price_type = st.selectbox("Price type (drives dispatch + spot price columns)", ["rt", "da"])
    duration_h = st.selectbox("Duration (h)", [2.0, 4.0], index=1)
    power_mw = st.number_input("Power (MW)", value=1.0)
    eff = st.number_input("Roundtrip efficiency", value=0.85)
    model = st.text_input("Capture model (used for capture table)", value="ols_da_time_v1")

    mn, mx = get_min_max_ts(engine, schema, price_type, duration_h, power_mw)
    start = st.date_input("Start date", value=(mx.date() - timedelta(days=90)))
    end = st.date_input("End date", value=mx.date())

    provinces = get_provinces(engine, schema)
    sel = st.multiselect("Provinces", provinces, default=provinces)

    hourly = load_hourly(engine, schema, price_type, duration_h, power_mw, sel, start, end, cache_bust)
    capture_period = load_capture_period(engine, schema, model, duration_h, power_mw, eff, start, end, sel, cache_bust)

    st.header("A. Period KPI (IC-safe) — preserved logic")
    kpi = compute_period_kpi(hourly, capture_period, start, end, power_mw, duration_h)
    st.dataframe(kpi, use_container_width=True)

    rank_df = (
        kpi.T
        .assign(score=lambda d: d["Theoretical price spread"] * d["Avg daily cycles"])
        .reset_index()
        .rename(columns={"index": "province"})
        .sort_values("score", ascending=False)
    )

    st.subheader("Theoretical spread × cycles (ranking)")
    st.bar_chart(rank_df.set_index("province")["score"], use_container_width=True)

    st.header("B. Monthly & Rolling analytics (totals-based, consistent with Period KPI)")

    window_days = (end - start).days + 1
    st.caption(f"Rolling Method B window = {window_days} days (your selected date range length). Rolling is computed as rolling sums of theoretical profit / charged MWh.")

    if window_days < 3:
        st.warning("Selected date range is very short; rolling results may be unstable. Consider >= 7 days.")

    daily_all = load_daily_theoretical(engine, schema, price_type, duration_h, power_mw, sel, mn.date(), mx.date(), cache_bust)
    daily_in_range = daily_all[(daily_all["date"].dt.date >= start) & (daily_all["date"].dt.date <= end)].copy()

    cap_monthly = load_capture_monthly(engine, schema, model, duration_h, power_mw, eff, start, end, sel, cache_bust)
    monthly_metrics = build_monthly_metrics(daily_in_range, cap_monthly, power_mw, duration_h, start, end)

    st.subheader("B1. Monthly metrics table")
    month_options = sorted(monthly_metrics["month"].unique())
    if not month_options:
        st.info("No monthly data available for the selected date range.")
    else:
        show_month = st.selectbox("Select a month to display", options=month_options, index=len(month_options) - 1)
        msel = monthly_metrics[monthly_metrics["month"] == show_month].copy()

        metrics_order = [
            ("charged_mwh", "Total charged MWh (batt)"),
            ("discharged_mwh", "Total discharged MWh (batt)"),
            ("charge_cost", "Total charging cost (grid)"),
            ("discharge_rev", "Total discharging revenue (grid)"),
            ("theo_profit", "Profitability (theoretical)"),
            ("theoretical_spread", "Theoretical price spread"),
            ("realised_spread", "Realised price spread"),
            ("capture_rate_pct", "Capture rate (%)"),
            ("cycles_avg_daily", "Avg daily cycles"),
        ]
        monthly_pivot = pd.DataFrame({label: msel.set_index("province")[col] for col, label in metrics_order}).T
        st.dataframe(monthly_pivot, use_container_width=True)

        st.subheader("B2. Monthly charts")
        mplot = monthly_metrics.copy()
        mplot["month_ts"] = pd.to_datetime(mplot["month"])

        c1, c2 = st.columns(2)
        with c1:
            st.caption("Monthly theoretical price spread")
            ch = altair_line(
                mplot.rename(columns={"theoretical_spread": "value"}),
                x="month_ts", y="value", color="province", title="Theoretical price spread",
            )
            st.altair_chart(ch, use_container_width=True)

    st.subheader("B3. Rolling theoretical price spread — Method B (window = selected date range)")
    rolling = build_rolling_spread_method_b(daily_all, window_days=window_days)
    rolling = rolling.dropna(subset=["rolling_theoretical_spread"])
    rolling = rolling[rolling["province"].isin(sel)]

    if rolling.empty:
        st.info("Rolling spread is empty (insufficient data for the selected window).")
    else:
        rplot = rolling.copy()
        rplot["date_ts"] = pd.to_datetime(rplot["date"])
        ch = altair_line(
            rplot.rename(columns={"rolling_theoretical_spread": "value"}),
            x="date_ts", y="value", color="province",
            title=f"Rolling theoretical price spread (window={window_days}d)",
        )
        st.altair_chart(ch, use_container_width=True)

        st.caption("Latest rolling values (most recent date per province)")
        latest = (
            rplot.sort_values(["province", "date_ts"])
                .groupby("province", as_index=False)
                .tail(1)[["province", "date_ts", "rolling_theoretical_spread"]]
                .rename(columns={"date_ts": "date"})
                .sort_values("rolling_theoretical_spread", ascending=False)
        )

        st.dataframe(latest, use_container_width=True)

if __name__ == "__main__":
    main()