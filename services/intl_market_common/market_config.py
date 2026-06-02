"""Shared MarketConfig dataclass for international BESS market apps."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List


@dataclass
class MarketConfig:
    name: str
    code: str
    table_prefix: str
    port: int
    app_slug: str
    app_key: str
    currency_sym: str
    currency_code: str
    timezone: str
    flag_emoji: str
    flag_url: str
    intervals_per_day: int
    system_operator: str
    wholesale_label: str
    ancillary_label: str
    modo_api_prefix: str = ""
    standard_questions: List[str] = field(default_factory=list)
    foundational_questions: List[str] = field(default_factory=list)
    research_questions: List[str] = field(default_factory=list)
