"""MarketConfig — per-market parameter bag consumed by app_template and connectors."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class MarketConfig:
    # Identity
    name: str               # "Australia (NEM)"
    code: str               # "au"  — used in URL schemes and log prefixes
    table_prefix: str       # "au_" — DB table prefix in intl_market schema

    # Deployment
    port: int               # 8509
    app_slug: str           # "au-market"  — ALB path prefix + Streamlit baseUrlPath
    app_key: str            # "au_market"  — marketdata.agent_memory.app column value

    # Display
    currency_sym: str       # "A$"
    currency_code: str      # "AUD"
    timezone: str           # "Australia/Sydney"  — APScheduler + display
    flag_emoji: str         # "🇦🇺"
    flag_url: str           # "https://flagcdn.com/w40/au.png"

    # Market structure
    intervals_per_day: int  # 48 for 30-min markets; 96 for 15-min (ERCOT)
    system_operator: str    # "AEMO"
    wholesale_label: str    # "NEM Spot"
    ancillary_label: str    # "FCAS"

    # Modo integration
    modo_api_prefix: str    # "/au/modo"  — prefix for Modo REST API endpoints

    # Modo AI question sets (populated by each market's config.py)
    standard_questions: list[str] = field(default_factory=list)
    foundational_questions: list[str] = field(default_factory=list)
    research_questions: list[str] = field(default_factory=list)
