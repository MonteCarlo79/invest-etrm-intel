import pandas as pd
from shared.agents.strategy_agent import load_top_provinces


def build_candidate_portfolio(limit: int = 8) -> pd.DataFrame:
    df = load_top_provinces(limit=limit).copy()
    if df.empty:
        return df

    df["irr_total"] = df["irr_total"].fillna(0)
    df["payback_years_total"] = df["payback_years_total"].fillna(999)

    df["risk_adjusted_score"] = (
        df["irr_total"] * 100
        - df["payback_years_total"] * 0.8
    )

    total_score = df["risk_adjusted_score"].clip(lower=0).sum()
    if total_score <= 0:
        df["suggested_weight"] = 1 / len(df)
    else:
        df["suggested_weight"] = df["risk_adjusted_score"].clip(lower=0) / total_score

    return df.sort_values("risk_adjusted_score", ascending=False)


def stress_test_portfolio(df: pd.DataFrame, spread_down_pct: float = 0.15) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out["stressed_irr_total"] = out["irr_total"] * (1 - spread_down_pct)
    out["irr_drop"] = out["irr_total"] - out["stressed_irr_total"]
    return out.sort_values("stressed_irr_total", ascending=False)


def build_risk_flags(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    def risk_label(row):
        if row["payback_years_total"] > 8:
            return "High payback risk"
        if row["irr_total"] < 0.08:
            return "Low return"
        return "Normal"

    out["risk_flag"] = out.apply(risk_label, axis=1)
    return out[["province", "irr_total", "payback_years_total", "suggested_weight", "risk_flag"]]


def simple_portfolio_memo(user_prompt: str, df: pd.DataFrame, stressed: pd.DataFrame) -> str:
    if df.empty:
        return "Portfolio & Risk Agent v2: no portfolio data available."

    top = df.iloc[0]
    stress_line = ""
    if not stressed.empty:
        s = stressed.iloc[0]
        stress_line = (
            f"After stress, top province remains {s['province']} with stressed IRR "
            f"{s['stressed_irr_total']:.2%}."
        )

    return f"""
Portfolio & Risk Agent v2

User request:
{user_prompt}

Recommendation:
Prioritise {top['province']} with suggested portfolio weight {top['suggested_weight']:.1%}.

Primary rationale:
- IRR: {top['irr_total']:.2%}
- Payback: {top['payback_years_total']:.2f} years
- Risk-adjusted score: {top['risk_adjusted_score']:.2f}

Stress view:
{stress_line}
""".strip()