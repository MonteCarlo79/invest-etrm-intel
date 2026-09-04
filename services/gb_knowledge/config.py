"""GB market configuration — MarketConfig instance for Great Britain (NEM/Elexon)."""
from services.intl_market_common.market_config import MarketConfig

# Questions imported from modo_ai.py question lists
_STANDARD_QUESTIONS = [
    "What are the most important GB BESS market developments from the last 24 hours?",
    "Which revenue streams are performing best for GB BESS assets right now — BM, FFR, DCL, DCH, or EPEX day-ahead?",
    "What is the current short-term outlook for GB BESS merchant revenues?",
    "Are there any significant regulatory or policy changes currently affecting GB BESS?",
    "How is GB system price and net imbalance volume (NIV) trending, and what does it mean for BESS dispatch strategy?",
    "Which GB BESS assets or operators are showing standout performance this week and why?",
    "What are the key market risks and opportunities for GB BESS investors right now?",
    "How is the GB grid stability and curtailment environment affecting BESS revenues?",
]

_FOUNDATIONAL_QUESTIONS = [
    "Give me a detailed explanation of how the UK power market mechanism works, covering the wholesale market, "
    "balancing mechanism, system operator role, settlement, and how prices are formed.",
    "Give me a comprehensive list of all BESS-relevant policies and regulations in the UK with a brief "
    "description of each — including capacity market, ancillary services framework, grid connection rules, "
    "planning policy, and any storage-specific legislation.",
    "How does NIV chasing work for GB BESS — what is net imbalance volume, how do batteries exploit it, "
    "what are the risks, and how has National Grid/NESO responded?",
    "Explain the GB Dynamic Containment, Dynamic Moderation, and Dynamic Regulation ancillary services — "
    "how they differ, procurement mechanisms, and typical BESS revenues.",
    "What is the GB Capacity Market and how does a BESS asset participate — T-4/T-1 auctions, de-rating "
    "factors, obligations, and penalty regime?",
    "How does EPEX day-ahead price formation work in Great Britain — auction timeline, participants, "
    "relationship to system price, and implications for BESS wholesale trading?",
]

_RESEARCH_QUESTIONS = [
    "Based on Modo's latest GB BESS revenue forecast, what is the outlook for total merchant revenues "
    "over the next 12 months, and which markets are expected to grow or decline?",
    "What are the key findings from Modo's most recent GB storage market report or outlook — including "
    "any changes to Modo's revenue index or forward curve assumptions?",
    "According to Modo's pipeline and deployment data, how much new GB BESS capacity is expected to come "
    "online in the next 12–24 months, and what does this mean for per-MW revenues?",
    "What does Modo's research say about optimal BESS duration strategy in the current GB market — "
    "is 1h, 2h, or 4h duration showing better risk-adjusted returns?",
    "According to Modo's data, which ancillary service markets (DC, DR, FFR) currently offer the best "
    "risk-adjusted returns for GB BESS, and how have clearing prices trended?",
    "What does Modo's research show about the long-term impact of increasing BESS penetration on GB "
    "ancillary service prices — at what fleet size does Modo expect significant price cannibalisation?",
]

MARKET_CONFIG = MarketConfig(
    name="Great Britain",
    code="gb",
    table_prefix="gb_",
    port=8508,
    app_slug="gb-market",
    app_key="gb_market",
    currency_sym="£",
    currency_code="GBP",
    timezone="Europe/London",
    flag_emoji="🇬🇧",
    flag_url="https://flagcdn.com/w40/gb.png",
    intervals_per_day=48,
    system_operator="National Grid ESO",
    wholesale_label="EPEX DA / System Price",
    ancillary_label="DC/DM/DR",
    modo_api_prefix="/gb/modo",
    standard_questions=_STANDARD_QUESTIONS,
    foundational_questions=_FOUNDATIONAL_QUESTIONS,
    research_questions=_RESEARCH_QUESTIONS,
)
