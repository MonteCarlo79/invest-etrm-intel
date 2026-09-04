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


# ── LLM extraction ────────────────────────────────────────────────────────────
import json
import re

EXTRACTION_MODEL = "claude-sonnet-4-6"


def build_extraction_prompt(text: str) -> str:
    return f"""你是一名新能源投资分析师。从以下交易背景材料中提取交易要素,以严格 JSON 返回,不要任何解释。

字段(缺失或不确定时给 null):
- deal_name 项目名称
- asset_type 资产类型:bess 独立储能 / wind 风电 / solar 光伏 / wind_bess 风光储一体 / solar_bess 光储一体
- province 省份(中文,如 蒙西、山东、山西)
- node 并网点/节点(无则 null)
- capacity_mw 储能额定功率(MW);capacity_mwh 储能容量(MWh)
- efficiency 储能综合效率(0-1);cycles_per_day 日均循环次数
- installed_mw 风电/光伏装机(MW)
- capex_total_yuan 总投资(单位:元。注意换算:万元×1e4,亿元×1e8)
- commissioning_year 投运年份(4 位整数);tenor_years 项目期限(年)
- counterparty 对手方/卖方;structure_notes 交易结构要点(PPA/保底/托底/容量租赁等,≤200 字)
- debt_ratio 负债率(0-1);loan_rate 贷款利率(0-1);loan_term_years 贷款期限(年)
- field_confidence 对象:对每个字段给 0-1 置信度

材料:
{text}"""


def extract_brief(text: str, filenames: list[str], api_key: str, client=None) -> DealBrief:
    """Extract a DealBrief from document text. `client` is the test seam (skips make_client)."""
    if client is None:
        from shared.anthropic_client import make_client
        client = make_client(api_key)
    resp = client.messages.create(
        model=EXTRACTION_MODEL, max_tokens=2000,
        messages=[{"role": "user", "content": build_extraction_prompt(text)}],
    )
    raw = resp.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM 返回的不是有效 JSON: {e}; 原文开头: {raw[:120]}") from e
    if not isinstance(payload, dict):
        raise ValueError(f"LLM 返回的 JSON 不是对象: {raw[:120]}")
    return parse_brief_json(payload, source_files=filenames)
