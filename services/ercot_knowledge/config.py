"""ERCOT (Texas) market configuration and Modo AI question sets."""
from services.intl_market_common.market_config import MarketConfig

MARKET_CONFIG = MarketConfig(
    name="ERCOT (Texas)",
    code="ercot",
    table_prefix="ercot_",
    port=8510,
    app_slug="ercot-market",
    app_key="ercot_market",
    currency_sym="$",
    currency_code="USD",
    timezone="US/Central",
    flag_emoji="🇺🇸",
    flag_url="https://flagcdn.com/w40/us.png",
    intervals_per_day=96,  # 15-min settlement intervals
    system_operator="ERCOT",
    wholesale_label="ERCOT RT",
    ancillary_label="Reg/RRS/ECRS",
    modo_api_prefix="/ercot/modo",
    standard_questions=[
        "What are the most important ERCOT BESS market developments from the last 24 hours?",
        "Which revenue streams are performing best for ERCOT BESS assets right now — "
        "real-time energy, RegUp, RegDown, RRS, ECRS, or NSRS?",
        "What is the current short-term outlook for ERCOT BESS merchant revenues?",
        "Are there any significant regulatory or policy changes at PUCT/ERCOT currently "
        "affecting BESS operations or investment?",
        "How are ERCOT real-time prices and ancillary service clearing prices trending, "
        "and what does it mean for BESS dispatch strategy?",
        "Which ERCOT BESS assets or operators are showing standout performance this week and why?",
        "What are the key market risks and opportunities for ERCOT BESS investors right now?",
        "How are ERCOT grid conditions, renewable curtailment, and scarcity pricing events "
        "affecting BESS revenue opportunities?",
    ],
    foundational_questions=[
        "Give me a detailed explanation of how the ERCOT power market works — "
        "real-time dispatch, nodal prices (SPPs/LMPs), DAM, ancillary service markets, "
        "ERCOT's role as ISO, and how BESS earns revenue.",
        "What are all ERCOT ancillary services — Regulation Up/Down (RegUp/RegDown), "
        "Responsive Reserve Service (RRS), ECRS, and NSRS — how are they procured, "
        "cleared via capacity auctions, and what do BESS assets typically earn?",
        "How does BESS participate in ERCOT energy arbitrage — RT vs DA price differentials, "
        "charging during negative/low prices (solar over-generation), discharging at "
        "scarcity events, and the typical revenue stacking strategy?",
        "What is ERCOT's resource adequacy framework — Planning Reserve Margin, "
        "ORDC pricing, capacity procurement (if any), and how it affects BESS economics?",
        "Explain ERCOT's transition to 5-minute settlement and nodal pricing — how this "
        "affects BESS bidding strategies, price volatility exposure, and co-optimisation?",
        "What are ERCOT's rules for ESR (Energy Storage Resource) registration, "
        "telemetry requirements, performance standards, and how BESS qualifies for "
        "ancillary service obligations?",
    ],
    research_questions=[
        "Based on Modo's latest ERCOT BESS revenue forecast, what is the outlook for total "
        "merchant revenues over the next 12 months, and which markets are expected to grow?",
        "What are the key findings from Modo's most recent ERCOT storage market report — "
        "including any changes to revenue index, AS price forecasts, or capacity assumptions?",
        "According to Modo's ERCOT pipeline data, how much new BESS capacity is expected to "
        "come online in the next 12–24 months, and what does this mean for per-MW revenues?",
        "What does Modo's research say about optimal BESS duration strategy in ERCOT — "
        "is 1h, 2h, or 4h duration showing better risk-adjusted returns given ERCOT's "
        "scarcity pricing and ancillary service structure?",
        "According to Modo's data, which ERCOT ancillary service types currently offer "
        "the best risk-adjusted returns for BESS, and how have clearing prices trended?",
        "What does Modo's research show about the long-term impact of increasing BESS "
        "penetration on ERCOT ancillary service prices and scarcity event frequency?",
    ],
)
