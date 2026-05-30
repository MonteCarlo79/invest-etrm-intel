"""Philippines power market configuration — MarketConfig instance for investment advisory."""
from services.intl_market_common.market_config import MarketConfig

_STANDARD_QUESTIONS = [
    "What are the most significant recent developments in the Philippines renewable energy market affecting investment decisions?",
    "How are GEAP auction results trending — are GET prices moving up or down, and which technologies are winning?",
    "What is the current outlook for BESS investment in the Philippines — regulatory support, revenue streams, and market maturity?",
    "What major policy changes from DOE or ERC have been announced recently that affect RE developers?",
    "How is electricity demand growth and grid expansion progressing across Luzon, Visayas, and Mindanao?",
    "What are the key risks for onshore wind and solar projects in the Philippines right now?",
    "Which new renewable energy projects have been announced or are under development in the Philippines?",
    "How competitive is the Philippines renewable energy market compared to regional peers (Vietnam, Indonesia, Thailand)?",
]

_FOUNDATIONAL_QUESTIONS = [
    "Explain the full GEAP auction process — from Notice of Auction through issuance of COE-GET, including the GET price determination methodology (goal-seek equity IRR) and ERC approval.",
    "How does the WESM wholesale market work in the Philippines — market structure, price formation, settlement, and how RE generators with priority dispatch earn revenue?",
    "What are the full legal and regulatory requirements for a foreign developer to set up and operate a renewable energy project in the Philippines? (EPIRA, RE Act, foreign ownership, ERC permits, environmental compliance.)",
    "Explain the NGCP ancillary services market — reserve types (regulating/contingency/dispatchable), procurement mechanisms (ASPA firm contracts vs. reserve market), and how BESS assets can participate and stack revenues.",
    "What are the transmission constraints and congestion risks for RE projects in the Philippines? How do inter-island interconnections, local grid capacity, and NGCP TDP infrastructure plans affect curtailment risk by region?",
    "How does the Philippine energy project finance market work — typical debt/equity structures, active DFI lenders, WACC ranges, leverage ratios, and key bankability requirements for solar/wind/BESS projects?",
]

_RESEARCH_QUESTIONS = [
    "Based on available research (AFRY AIMR, Baringa, Aurora), what are the WESM price projections for Luzon, Visayas, and Mindanao through 2030–2040, and what does this mean for merchant RE revenue expectations?",
    "What does independent market research say about optimal GEA-4 bid pricing for onshore wind — considering LCOE benchmarks, construction costs, competitor pricing, and grid congestion risk?",
    "What offshore wind development pipeline exists in the Philippines — developer names, project locations, installed capacity, and expected commissioning dates?",
    "How does Philippines BESS investment compare to other APAC markets (Australia, Japan, Taiwan) in terms of revenue stack depth, regulatory maturity, and risk-adjusted returns?",
    "What does the NGCP Transmission Development Plan (TDP 2024–2050) mean for RE project developers — key transmission upgrades, grid bottlenecks, and development windows by region?",
    "What are the key lessons from GEA-1 and GEA-2 winning projects — post-auction permitting timelines, interconnection challenges, financing hurdles, and construction cost overruns?",
]

MARKET_CONFIG = MarketConfig(
    name="Philippines",
    code="ph",
    table_prefix="ph_",
    port=8510,
    app_slug="ph-market",
    app_key="ph_market",
    currency_sym="₱",
    currency_code="PHP",
    timezone="Asia/Manila",
    flag_emoji="🇵🇭",
    flag_url="https://flagcdn.com/w40/ph.png",
    intervals_per_day=48,
    system_operator="NGCP",
    wholesale_label="WESM Spot",
    ancillary_label="Regulating / Contingency / Dispatchable Reserves",
    modo_api_prefix="/ph/modo",  # not used — no Modo Energy for Philippines
    standard_questions=_STANDARD_QUESTIONS,
    foundational_questions=_FOUNDATIONAL_QUESTIONS,
    research_questions=_RESEARCH_QUESTIONS,
)
