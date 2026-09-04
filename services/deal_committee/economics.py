# services/deal_committee/economics.py
"""Economics section — runs the libs/deal_models engine in-process (not an agent)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from libs.deal_models.contracts import (
    DispatchRequest, MCRequest, MCResult, PriceSimRequest, ProjectFinancials,
)
from libs.deal_models.dispatch_valuation import dispatch_annual
from libs.deal_models.monte_carlo import run_monte_carlo
from libs.deal_models.price_simulator import simulate_prices
from services.deal_committee.brief import DealBrief

# solar/solar_bess reuse the wind dispatch models (July spec: same model, different profile)
_DISPATCH_TYPE = {"bess": "bess", "wind": "wind", "solar": "wind",
                  "wind_bess": "wind_bess", "solar_bess": "wind_bess"}
_FIXED_OM_YUAN = 3e6  # matches cashflow_tab default


@dataclass
class EconomicsResult:
    mc: MCResult
    monthly_price: list[tuple[str, float]]  # (YYYY-MM, avg yuan/MWh), 12 entries
    n_price_hours: int
    n_simulations: int
    model: str


def _default_fetch(province: str, start: str, end: str) -> list[float]:
    from services.deal_engine.price_data import fetch_price_history
    return fetch_price_history(province, start, end)


def _default_monthly(engine, province: str) -> list[tuple[str, float]]:
    from sqlalchemy import text
    from services.common.db_utils import get_engine
    engine = engine or get_engine()
    sql = text("""
        SELECT TO_CHAR(DATE_TRUNC('month', datetime), 'YYYY-MM') AS month,
               AVG(CASE WHEN da_price IS NOT NULL AND da_price != 0
                        THEN da_price ELSE rt_price END) AS avg_price
        FROM marketdata.spot_prices_hourly
        WHERE province = :p
          AND datetime >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY 1 ORDER BY 1
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"p": province}).fetchall()
    vals = [float(r[1]) for r in rows if r[1] is not None]
    scale = 1000.0 if vals and sorted(vals)[len(vals) // 2] < 5.0 else 1.0  # kWh → MWh
    return [(r[0], float(r[1]) * scale) for r in rows if r[1] is not None]


def run_economics(brief: DealBrief, n_simulations: int = 1000,
                  fetch_fn=None, monthly_fn=None) -> EconomicsResult:
    if not brief.capex_total_yuan:
        raise ValueError("经济性测算需要总投资额(capex_total_yuan)——请在交易要素表中填写")
    if not brief.province:
        raise ValueError("经济性测算需要省份(province)")

    fetch_fn = fetch_fn or _default_fetch
    monthly_fn = monthly_fn or _default_monthly

    today = date.today()
    end = today.replace(day=1)
    start = (end - timedelta(days=370)).replace(day=1)
    prices = fetch_fn(brief.province, start.isoformat(), end.isoformat())

    at = brief.asset_type
    dispatch_req = DispatchRequest(
        asset_type=_DISPATCH_TYPE[at],
        capacity_mwh=brief.capacity_mwh if "bess" in at else 0.0,
        power_mw=brief.capacity_mw if "bess" in at else 0.0,
        roundtrip_eff=brief.efficiency,
        cycles_per_day=brief.cycles_per_day,
        installed_mw=brief.installed_mw if at != "bess" else 0.0,
    )
    price_req = PriceSimRequest(
        province=brief.province, n_simulations=n_simulations, n_years=1,
        model="ou", price_history_yuan_mwh=prices,
    )
    paths = simulate_prices(price_req, seed=42)
    base_rev = dispatch_annual(paths, dispatch_req).p50
    fin = ProjectFinancials(
        capex_total_yuan=brief.capex_total_yuan,
        commissioning_year=brief.commissioning_year,
        project_life_years=brief.tenor_years,
        debt_ratio=brief.debt_ratio, loan_term_years=brief.loan_term_years,
        interest_rate=brief.loan_rate,
        annual_revenue_yuan=[base_rev] * brief.tenor_years,
        annual_om_yuan=_FIXED_OM_YUAN,
    )
    mc = run_monte_carlo(MCRequest(price_sim=price_req, dispatch=dispatch_req,
                                   financials=fin, n_simulations=n_simulations))
    return EconomicsResult(
        mc=mc, monthly_price=monthly_fn(None, brief.province),
        n_price_hours=len(prices), n_simulations=n_simulations, model="ou",
    )


def economics_section_markdown(res: EconomicsResult, brief: DealBrief) -> str:
    mc = res.mc
    return f"""**测算口径**：{brief.province} · {res.model.upper()} 模型 · {res.n_simulations} 条路径 · 历史价格 {res.n_price_hours} 小时 · 固定运维 ¥{_FIXED_OM_YUAN/1e6:.1f}M/年

| 指标 | P10 | P50 | P90 |
|---|---|---|---|
| 年收入 (¥M) | {mc.revenue_p10/1e6:.1f} | {mc.revenue_p50/1e6:.1f} | {mc.revenue_p90/1e6:.1f} |
| 股权 IRR | {mc.equity_irr_p10:.1%} | {mc.equity_irr_p50:.1%} | {mc.equity_irr_p90:.1%} |
| NPV (¥M) | {mc.npv_p10/1e6:.1f} | {mc.npv_p50/1e6:.1f} | {mc.npv_p90/1e6:.1f} |

- 收入 VaR(5%)：¥{mc.revenue_var_5pct/1e6:.1f}M · CVaR：¥{mc.revenue_cvar_5pct/1e6:.1f}M
- 股权 IRR 低于基准（8%）概率：{mc.irr_prob_below_hurdle:.0%}
"""
