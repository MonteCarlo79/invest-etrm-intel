"""Serialize CommitteeResult to/from JSONB-safe records for the DAF history store.

Numpy path arrays are deliberately dropped — only scalar KPIs persist
(the distributions live in the PDF charts; re-derivation needs a fresh run).
"""
from __future__ import annotations

from libs.deal_models.contracts import MCResult
from services.deal_committee.economics import EconomicsResult
from services.deal_committee.orchestrator import CommitteeResult
from services.deal_committee.sections import SectionResult


def economics_to_dict(econ: EconomicsResult | None) -> dict | None:
    if econ is None:
        return None
    mc = econ.mc
    return {
        "revenue_p10": mc.revenue_p10, "revenue_p50": mc.revenue_p50, "revenue_p90": mc.revenue_p90,
        "revenue_var_5pct": mc.revenue_var_5pct, "revenue_cvar_5pct": mc.revenue_cvar_5pct,
        "equity_irr_p10": mc.equity_irr_p10, "equity_irr_p50": mc.equity_irr_p50,
        "equity_irr_p90": mc.equity_irr_p90,
        "irr_prob_below_hurdle": mc.irr_prob_below_hurdle,
        "npv_p10": mc.npv_p10, "npv_p50": mc.npv_p50, "npv_p90": mc.npv_p90,
        "monthly_price": econ.monthly_price,
        "n_price_hours": econ.n_price_hours,
        "n_simulations": econ.n_simulations,
        "model": econ.model,
    }


def dict_to_economics(d: dict | None) -> EconomicsResult | None:
    """Rebuild an EconomicsResult from a stored dict (paths empty — scalars only)."""
    if d is None:
        return None
    mc = MCResult(
        revenue_p10=d["revenue_p10"], revenue_p50=d["revenue_p50"], revenue_p90=d["revenue_p90"],
        revenue_var_5pct=d["revenue_var_5pct"], revenue_cvar_5pct=d["revenue_cvar_5pct"],
        equity_irr_p10=d["equity_irr_p10"], equity_irr_p50=d["equity_irr_p50"],
        equity_irr_p90=d["equity_irr_p90"],
        irr_prob_below_hurdle=d["irr_prob_below_hurdle"],
        npv_p10=d["npv_p10"], npv_p50=d["npv_p50"], npv_p90=d["npv_p90"],
        tornado=[], revenue_paths=[], equity_irr_paths=[], npv_paths=[],
    )
    return EconomicsResult(
        mc=mc,
        monthly_price=[tuple(r) for r in d.get("monthly_price", [])],
        n_price_hours=d.get("n_price_hours", 0),
        n_simulations=d.get("n_simulations", 0),
        model=d.get("model", "ou"),
    )


def sections_from_dicts(rows: list[dict]) -> list[SectionResult]:
    return [
        SectionResult(
            key=r["key"], title=r["title"], markdown=r.get("markdown", ""),
            status=r.get("status", "ok"), error=r.get("error", ""),
        )
        for r in (rows or [])
    ]


def result_to_record(result: CommitteeResult) -> dict:
    """JSONB-safe record consumed by library.save_result."""
    return {
        "brief": result.brief.model_dump(mode="json"),
        "sections": [
            {"key": s.key, "title": s.title, "markdown": s.markdown,
             "status": s.status, "error": s.error}
            for s in result.sections
        ],
        "economics": economics_to_dict(result.economics),
        "synthesis": result.synthesis,
        "recommendation": result.recommendation,
    }
