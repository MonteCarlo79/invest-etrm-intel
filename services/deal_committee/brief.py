# services/deal_committee/brief.py
"""DealBrief — confirmed deal parameters driving the committee analysis."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

# Fields the intake form warns about when extraction confidence is low.
CORE_FIELDS: tuple[str, ...] = (
    "deal_name", "asset_type", "province", "capacity_mw", "capacity_mwh",
    "installed_mw", "capex_total_yuan", "commissioning_year",
)


class DealBrief(BaseModel):
    # Identity
    deal_name: str = ""
    asset_type: Literal["bess", "wind", "solar", "wind_bess", "solar_bess"] = "bess"
    # Site
    province: str = ""
    node: Optional[str] = None
    # Technical
    capacity_mw: float = 0.0
    capacity_mwh: float = 0.0
    efficiency: float = Field(0.85, gt=0, le=1.0)
    cycles_per_day: float = Field(1.0, gt=0)
    installed_mw: float = 0.0
    # Commercial
    capex_total_yuan: Optional[float] = None
    commissioning_year: int = 2027
    tenor_years: int = Field(20, ge=1, le=40)
    counterparty: str = ""
    structure_notes: str = ""
    # Financing
    debt_ratio: float = Field(0.70, ge=0.0, le=0.95)
    loan_rate: float = Field(0.05, ge=0.0, le=0.30)
    loan_term_years: int = Field(10, ge=1, le=30)
    # Meta
    field_confidence: dict[str, float] = Field(default_factory=dict)
    confirmed: bool = False
    source_files: list[str] = Field(default_factory=list)


def parse_brief_json(payload: dict, source_files: list[str] | None = None) -> DealBrief:
    """Tolerant parser for LLM extraction output: unknown keys dropped, missing → defaults."""
    known = {k: v for k, v in (payload or {}).items() if k in DealBrief.model_fields}
    conf = known.pop("field_confidence", {}) or {}
    brief = DealBrief(**known)
    brief.field_confidence = {str(k): float(v) for k, v in conf.items()
                              if isinstance(v, (int, float))}
    if source_files:
        brief.source_files = list(source_files)
    return brief


def low_confidence_fields(brief: DealBrief, threshold: float = 0.6) -> list[str]:
    """Core fields whose extraction confidence is below threshold (or unrecorded-but-empty)."""
    low = [f for f in CORE_FIELDS
           if f in brief.field_confidence and brief.field_confidence[f] < threshold]
    return sorted(low)
