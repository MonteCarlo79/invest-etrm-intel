"""Headless (no-Streamlit) BESS Map China Province analyst agent.

Extracts the agent from apps/bess-map/app.py without any Streamlit dependency.

Usage:
    from services.bess_map.headless_agent import run_bess_map_query
    answer = run_bess_map_query("Which provinces have best 4h BESS economics?", api_key, pg_url)
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text as sql_text

logger = logging.getLogger(__name__)

_MODEL = "ols_rt_time_v1"


def _make_engine(pg_url: str):
    url = pg_url or os.environ.get("PGURL") or os.environ.get("DATABASE_URL", "")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg2://"):
        url = "postgresql+psycopg2://" + url[len("postgresql://"):]
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 10})


def _load_province_ranking(engine, start: str, end: str, model: str = _MODEL) -> pd.DataFrame:
    sql = sql_text("""
        SELECT t.province, t.duration_h, t.annual_theo, t.days,
               r.annual_real, r.capture_pct
        FROM (
            SELECT province, duration_h,
                   ROUND((AVG(theoretical_profit_per_mwh_day) * 365)::numeric, 0) AS annual_theo,
                   COUNT(DISTINCT date) AS days
            FROM marketdata.bess_capture_daily
            WHERE date BETWEEN :start AND :end
            GROUP BY province, duration_h
        ) t
        LEFT JOIN (
            SELECT province, duration_h,
                   ROUND((AVG(realized_profit_per_mwh_day) * 365)::numeric, 0) AS annual_real,
                   ROUND((AVG(NULLIF(capture_rate, 'NaN'::double precision)) * 100)::numeric, 1) AS capture_pct
            FROM marketdata.bess_capture_daily
            WHERE date BETWEEN :start AND :end AND model = :model
            GROUP BY province, duration_h
        ) r USING (province, duration_h)
        ORDER BY annual_theo DESC NULLS LAST
    """)
    try:
        return pd.read_sql(sql, engine, params={"start": start, "end": end, "model": model})
    except Exception:
        return pd.DataFrame()


def _load_dispatch_day(engine, province: str, duration_h: float, day: str) -> pd.DataFrame:
    sql = sql_text("""
        SELECT d.datetime, d.charge_mw, d.discharge_mw, d.soc_mwh,
               p.rt_price, p.da_price
        FROM marketdata.spot_dispatch_hourly_theoretical d
        JOIN marketdata.spot_prices_hourly p
          ON p.province = d.province AND p.datetime = d.datetime
        WHERE d.province = :p AND ABS(d.duration_h - :d) < 0.01
          AND d.datetime::date = :day
        ORDER BY d.datetime
    """)
    try:
        return pd.read_sql(sql, engine, params={"p": province, "d": duration_h, "day": day}, parse_dates=["datetime"])
    except Exception:
        return pd.DataFrame()


def _load_avg_economics(engine, province: str, duration_h: float, model: str = _MODEL) -> dict:
    sql = sql_text("""
        SELECT t.theo_per_mwh_day, r.real_per_mwh_day, r.capture_rate
        FROM (
            SELECT AVG(theoretical_profit_per_mwh_day) AS theo_per_mwh_day
            FROM marketdata.bess_capture_daily
            WHERE province = :p AND ABS(duration_h - :d) < 0.01
        ) t
        CROSS JOIN (
            SELECT AVG(realized_profit_per_mwh_day) AS real_per_mwh_day,
                   AVG(NULLIF(capture_rate, 'NaN'::double precision)) AS capture_rate
            FROM marketdata.bess_capture_daily
            WHERE province = :p AND ABS(duration_h - :d) < 0.01
              AND model = :model
        ) r
    """)
    try:
        row = pd.read_sql(sql, engine, params={"p": province, "d": duration_h, "model": model}).iloc[0]
        return row.to_dict()
    except Exception:
        return {"theo_per_mwh_day": 0, "real_per_mwh_day": 0, "capture_rate": 0}


def _compute_irr(cashflows: list) -> Optional[float]:
    if not cashflows or cashflows[0] >= 0:
        return None
    r = 0.1
    for _ in range(300):
        npv = sum(cf / (1 + r) ** t for t, cf in enumerate(cashflows))
        dnpv = sum(-t * cf / (1 + r) ** (t + 1) for t, cf in enumerate(cashflows))
        if abs(dnpv) < 1e-12:
            break
        r -= npv / dnpv
        if r <= -1:
            return None
    return r if -1 < r < 10 else None


def _compute_npv(cashflows: list, rate: float = 0.08) -> float:
    return sum(cf / (1 + rate) ** t for t, cf in enumerate(cashflows))


def _build_cashflows(
    theo_per_mwh_day: float, capture_rate: float, duration_h: float,
    capex_per_kwh: float, rte: float, om_per_mw_yr: float,
    subsidy_per_mwh: float, degradation: float, equity_pct: float,
    loan_rate: float, loan_tenure: int, project_life: int, power_mw: float = 1.0,
) -> list:
    e_cap = power_mw * duration_h
    capex = capex_per_kwh * e_cap * 1000
    equity_capex = capex * equity_pct
    debt = capex * (1 - equity_pct)
    ann_debt = (
        debt * loan_rate / (1 - (1 + loan_rate) ** (-loan_tenure))
        if debt > 0 and loan_rate > 0 else (debt / loan_tenure if loan_tenure > 0 else 0)
    )
    om_annual = om_per_mw_yr * power_mw
    daily_discharge = e_cap * rte
    base_rev_daily = theo_per_mwh_day * capture_rate * e_cap + subsidy_per_mwh * daily_discharge

    cfs = [-equity_capex]
    for yr in range(1, project_life + 1):
        rev = base_rev_daily * 365 * (1 - degradation) ** (yr - 1)
        ds = ann_debt if yr <= loan_tenure else 0.0
        cfs.append(rev - om_annual - ds)
    return cfs


_TOOLS = [
    {
        "name": "get_bess_economics",
        "description": "Province-level BESS economics: annual theoretical and realised revenue per MWh of installed capacity, capture rate. Screen provinces first.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                "end_date":   {"type": "string", "description": "YYYY-MM-DD"},
                "duration_h": {"type": "number", "description": "2 or 4 — omit for both"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "get_dispatch_detail",
        "description": "Hourly LP-theoretical dispatch (charge MW, discharge MW, SoC MWh, RT price) for a province on a specific date.",
        "input_schema": {
            "type": "object",
            "properties": {
                "province":   {"type": "string"},
                "duration_h": {"type": "number", "description": "2 or 4"},
                "date":       {"type": "string", "description": "YYYY-MM-DD"},
            },
            "required": ["province", "duration_h", "date"],
        },
    },
    {
        "name": "get_irr_estimate",
        "description": "Calculate BESS equity IRR, simple payback, and NPV for a province. Revenue pulled from DB.",
        "input_schema": {
            "type": "object",
            "properties": {
                "province":           {"type": "string"},
                "duration_h":         {"type": "number"},
                "capex_yuan_per_kwh": {"type": "number", "description": "¥/kWh e.g. 600"},
                "rte_pct":            {"type": "number", "description": "Round-trip efficiency %, default 85"},
                "om_per_mw_yr":       {"type": "number", "description": "O&M ¥/MW/year, default 24000"},
                "subsidy_per_mwh":    {"type": "number", "description": "Discharge subsidy ¥/MWh, default 0"},
                "degradation_pct":    {"type": "number", "description": "Annual capacity fade %, default 2"},
                "equity_pct":         {"type": "number", "description": "Equity %, default 30"},
                "loan_rate_pct":      {"type": "number", "description": "Loan rate %, default 5.5"},
                "loan_tenure":        {"type": "integer", "description": "Loan years, default 10"},
                "project_life":       {"type": "integer", "description": "Project life years, default 15"},
                "use_realised":       {"type": "boolean", "description": "Use realised vs theoretical revenue"},
            },
            "required": ["province", "duration_h", "capex_yuan_per_kwh"],
        },
    },
]


def run_bess_map_query(question: str, api_key: str, pg_url: str) -> str:
    """Run the China BESS Province analyst agent and return its text answer."""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    engine = _make_engine(pg_url)

    system = (
        "You are a specialist BESS investment analyst for PJH ETRM, "
        "focused on China's provincial electricity spot markets.\n\n"
        "GROUNDING RULE: Only use data returned by tools. "
        "Never cite external reports or general knowledge you were not given.\n\n"
        "DOMAIN DEFINITIONS:\n"
        "- Revenue unit: ¥/MWh of INSTALLED CAPACITY per day\n"
        "- Capture rate = realized OLS-forecast revenue ÷ theoretical LP perfect-foresight revenue\n"
        "- Simple payback = capex (¥/kWh × 1000 ¥/MWh) ÷ annual_revenue_per_MWh_cap\n"
        "- O&M baseline: 24,000 ¥/MW/year\n"
        "- Capex range: 400–600 ¥/kWh for LFP\n\n"
        "ANALYTICAL FRAMEWORK:\n"
        "1. Province screening → call get_bess_economics\n"
        "2. Dispatch quality → call get_dispatch_detail\n"
        "3. Financial case → call get_irr_estimate (IRR < 8% = marginal, < 0% = rejected)\n"
    )

    def dispatch(name: str, inp: dict) -> str:
        try:
            from datetime import date
            if name == "get_bess_economics":
                df = _load_province_ranking(
                    engine,
                    inp.get("start_date", "2025-01-01"),
                    inp.get("end_date", str(date.today())),
                )
                if inp.get("duration_h"):
                    df = df[abs(df["duration_h"] - float(inp["duration_h"])) < 0.01]
                return df.to_json(orient="records", default_handler=str) if not df.empty else "No data."

            elif name == "get_dispatch_detail":
                df = _load_dispatch_day(engine, inp["province"], float(inp.get("duration_h", 4.0)), inp["date"])
                return df.head(24).to_json(orient="records", default_handler=str) if not df.empty else "No dispatch data."

            elif name == "get_irr_estimate":
                econ = _load_avg_economics(engine, inp["province"], float(inp.get("duration_h", 4.0)))
                td = float(econ.get("theo_per_mwh_day") or 0)
                rd = float(econ.get("real_per_mwh_day") or 0)
                rev_day = rd if inp.get("use_realised") else td
                cfs = _build_cashflows(
                    theo_per_mwh_day=rev_day,
                    capture_rate=1.0,
                    duration_h=float(inp.get("duration_h", 4.0)),
                    capex_per_kwh=float(inp.get("capex_yuan_per_kwh", 600)),
                    rte=float(inp.get("rte_pct", 85)) / 100,
                    om_per_mw_yr=float(inp.get("om_per_mw_yr", 24000)),
                    subsidy_per_mwh=float(inp.get("subsidy_per_mwh", 0)),
                    degradation=float(inp.get("degradation_pct", 2)) / 100,
                    equity_pct=float(inp.get("equity_pct", 30)) / 100,
                    loan_rate=float(inp.get("loan_rate_pct", 5.5)) / 100,
                    loan_tenure=int(inp.get("loan_tenure", 10)),
                    project_life=int(inp.get("project_life", 15)),
                )
                irr = _compute_irr(cfs)
                npv = _compute_npv(cfs, 0.08)
                cum, payback = 0.0, None
                for yr, cf in enumerate(cfs[1:], 1):
                    cum += cf
                    if cum >= 0 and payback is None:
                        payback = yr
                return str({
                    "province": inp["province"], "duration_h": inp.get("duration_h"),
                    "revenue_basis": "realised" if inp.get("use_realised") else "theoretical",
                    "rev_per_mwh_cap_day": round(rev_day, 2),
                    "irr_pct": round(irr * 100, 2) if irr is not None else None,
                    "simple_payback_yr": payback,
                    "npv_yuan": round(npv, 0),
                })
        except Exception as e:
            return f"Error: {e}"
        return "Unknown tool"

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6", max_tokens=4096,
            system=system, tools=_TOOLS, messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            engine.dispose()
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result_str = dispatch(block.name, block.input)
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_str})
        if not tool_results:
            engine.dispose()
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
