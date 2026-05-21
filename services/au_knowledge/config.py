"""Australia (NEM/AEMO) market configuration and Modo AI question sets."""
from services.intl_market_common.market_config import MarketConfig

MARKET_CONFIG = MarketConfig(
    name="Australia (NEM)",
    code="au",
    table_prefix="au_",
    port=8509,
    app_slug="au-market",
    app_key="au_market",
    currency_sym="A$",
    currency_code="AUD",
    timezone="Australia/Sydney",
    flag_emoji="🇦🇺",
    flag_url="https://flagcdn.com/w40/au.png",
    intervals_per_day=48,
    system_operator="AEMO",
    wholesale_label="NEM Spot",
    ancillary_label="FCAS",
    modo_api_prefix="/au/modo",
    standard_questions=[
        "What are the most important NEM/AEMO BESS market developments from the last 24 hours?",
        "Which revenue streams are performing best for NEM BESS assets right now — "
        "wholesale spot, FCAS regulation raise/lower, FCAS contingency, or capacity services?",
        "What is the current short-term outlook for Australian BESS merchant revenues in the NEM?",
        "Are there any significant regulatory or policy changes currently affecting Australian NEM BESS?",
        "How is NEM spot price and FCAS procurement trending across regions (QLD, NSW, VIC, SA), "
        "and what does it mean for BESS dispatch strategy?",
        "Which Australian NEM BESS assets or operators are showing standout performance this week and why?",
        "What are the key market risks and opportunities for NEM BESS investors right now?",
        "How is renewable curtailment, network congestion, and MLF changes affecting NEM BESS revenues?",
    ],
    foundational_questions=[
        "Give me a detailed explanation of how the Australian NEM power market works — "
        "5-minute dispatch, 30-minute settlement, price setting (VOLL, MPC), AEMO's role, "
        "and how BESS earns revenue across energy, FCAS, and market ancillary services.",
        "What are all FCAS services in the NEM — regulation raise/lower and contingency services "
        "(6-sec, 60-sec, 5-min fast/slow) — how are they co-optimised, procured, and what "
        "do BESS assets typically earn per MW?",
        "How does BESS participate in NEM energy arbitrage — charge/discharge cycles, MLF "
        "adjustments, how settlement prices work, and what basis risk means in practice?",
        "What is the Capacity Investment Scheme (CIS) in Australia, how does it work, "
        "and how does it affect BESS investment decisions and merchant revenue expectations?",
        "Explain NEM network constraints — how constraints create regional price separation "
        "(SNI, QNI, VIC-SA interconnectors), how FCAS regional requirements work, "
        "and how BESS exploits or is exposed to these dynamics.",
        "What is the NEM's approach to BESS registration, dispatch bidding (energy + FCAS bid stacks), "
        "rebidding rules, and how operators optimise their co-dispatch strategy?",
    ],
    research_questions=[
        "Based on Modo's latest AU NEM BESS revenue forecast, what is the outlook for total "
        "merchant revenues over the next 12 months, and which markets (spot, FCAS reg, FCAS "
        "contingency) are expected to grow or decline?",
        "What are the key findings from Modo's most recent Australian storage market report — "
        "including any changes to NEM revenue index, FCAS price forecasts, or capacity assumptions?",
        "According to Modo's NEM pipeline data, how much new BESS capacity is expected to come "
        "online in the next 12–24 months, and what does this mean for per-MW revenues as "
        "the market matures?",
        "What does Modo's research say about optimal BESS duration strategy in the current NEM — "
        "is 1h, 2h, or 4h duration showing better risk-adjusted returns and how is this expected "
        "to shift as FCAS cannibalisation continues?",
        "According to Modo's data, which NEM FCAS service types currently offer the best "
        "risk-adjusted returns for BESS, and how have clearing prices trended across regions?",
        "What does Modo's research show about the long-term impact of increasing BESS penetration "
        "on NEM FCAS prices — at what fleet size does Modo expect significant price cannibalisation?",
    ],
)
