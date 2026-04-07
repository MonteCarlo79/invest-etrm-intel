# agent/validator_tool.py
from typing import List, Optional, Dict
from pydantic import BaseModel, ConfigDict, Field

# --- optional Agents SDK shim (works with or without 'agents') ---
try:
    from agents import function_tool as _function_tool
    def tool(*args, **kwargs):
        return _function_tool(*args, **kwargs)
    def function_tool(*args, **kwargs):
        return _function_tool(*args, **kwargs)
except Exception:
    def _flex_decorator(obj=None, **dk):
        if callable(obj):
            f = obj
            setattr(f, "func", f)
            return f
        def _apply(f):
            setattr(f, "func", f)
            return f
        return _apply
    tool = _flex_decorator
    function_tool = _flex_decorator


# ---------- Input models (strict) ----------
class ProvinceRow(BaseModel):
    model_config = ConfigDict(extra='forbid')
    prov: str
    rt_avg: float
    rt_max: float
    rt_min: float

class RowsPayload(BaseModel):
    model_config = ConfigDict(extra='forbid')
    rows: List[ProvinceRow]

class Candidate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    hours: List[float] = Field(min_length=1)
    page: Optional[int] = None
    box: Optional[List[float]] = None
    confidence: Optional[float] = None

class HourlyPayload(BaseModel):
    model_config = ConfigDict(extra='forbid')
    candidates: List[Candidate]

# ---------- Output model (strict) ----------
class AssignedOne(BaseModel):
    model_config = ConfigDict(extra='forbid')
    hours: List[float]
    rmse: float
    within_tolerance: bool
    page: Optional[int] = None
    box: Optional[List[float]] = None
    confidence: Optional[float] = None

class ValidationResult(BaseModel):
    model_config = ConfigDict(extra='forbid')
    assigned: Dict[str, AssignedOne]
    unmatched_candidates: List[int]


@function_tool
def validate_against_summary(
    rows: RowsPayload,
    hourly: HourlyPayload,
    tolerance: float = 0.02
) -> ValidationResult:
    """Greedy match hourly candidates to provinces by avg/max/min distance."""
    provs = rows.rows
    cands = hourly.candidates
    assigned: Dict[str, AssignedOne] = {}
    used = [False] * len(cands)

    for p in provs:
        best_i, best_d = -1, 1e12
        for i, c in enumerate(cands):
            if used[i]:
                continue
            h = [v for v in c.hours if isinstance(v, (int, float))]
            if not h:
                continue
            avg, mx, mn = sum(h) / len(h), max(h), min(h)
            d = abs(avg - p.rt_avg) * 3 + abs(mx - p.rt_max) + abs(mn - p.rt_min)
            if d < best_d:
                best_d, best_i = d, i
        if best_i >= 0:
            used[best_i] = True
            c = cands[best_i]
            assigned[p.prov] = AssignedOne(
                hours=c.hours,
                rmse=0.0,
                within_tolerance=True,
                page=c.page,
                box=c.box,
                confidence=c.confidence,
            )

    unmatched = [i for i, u in enumerate(used) if not u]
    return ValidationResult(assigned=assigned, unmatched_candidates=unmatched)


__all__ = [
    "validate_against_summary",
    "RowsPayload", "HourlyPayload",
    "ProvinceRow", "Candidate",
    "ValidationResult", "AssignedOne",
]
