"""PJM (US East) market configuration and Modo AI question sets."""
from services.intl_market_common.market_config import MarketConfig

MARKET_CONFIG = MarketConfig(
    name="PJM (US East)",
    code="pjm",
    table_prefix="pjm_",
    port=8511,
    app_slug="pjm-market",
    app_key="pjm_market",
    currency_sym="$",
    currency_code="USD",
    timezone="US/Eastern",
    flag_emoji="🇺🇸",
    flag_url="https://flagcdn.com/w40/us.png",
    intervals_per_day=48,
    system_operator="PJM",
    wholesale_label="PJM RT LMP",
    ancillary_label="Reg/Sync/Black Start",
    modo_api_prefix="/pjm/modo",
    standard_questions=[
        "What are the most important PJM BESS market developments from the last 24 hours?",
        "Which revenue streams are performing best for PJM BESS assets right now — "
        "real-time energy, regulation (RegA/RegD), synchronised reserve, or capacity (RPM)?",
        "What is the current short-term outlook for PJM BESS merchant revenues?",
        "Are there any significant regulatory or policy changes at FERC/PJM currently "
        "affecting BESS operations, capacity market rules, or regulation market design?",
        "How are PJM real-time LMPs and ancillary service clearing prices trending, "
        "and what does it mean for BESS dispatch strategy?",
        "Which PJM BESS assets or operators are showing standout performance this week and why?",
        "What are the key market risks and opportunities for PJM BESS investors right now?",
        "How are PJM grid conditions, transmission constraints (LMPs), and renewable "
        "build-out affecting BESS revenue opportunities?",
    ],
    foundational_questions=[
        "Give me a detailed explanation of how the PJM power market works — "
        "day-ahead and real-time markets, LMP pricing (energy + congestion + loss), "
        "ancillary services, RPM capacity market, and how BESS earns revenue.",
        "What are PJM's ancillary services — Regulation (RegA traditional, RegD dynamic), "
        "Synchronised Reserve, Primary Reserve, and Black Start — how are they procured, "
        "performance scoring (MILEAGE), and what do BESS assets typically earn?",
        "How does BESS participate in PJM energy arbitrage — DA vs RT LMP arbitrage, "
        "congestion management, and co-optimised storage dispatch?",
        "What is PJM's Capacity Market (RPM) — how do BESS assets qualify (capacity "
        "performance), accreditation (ELCC), obligation structure, and how much does "
        "capacity revenue contribute to total BESS revenue?",
        "Explain PJM's storage-specific rules under FERC Order 841 and Order 2222 — "
        "how ESRs are registered, interconnection requirements, and participation in "
        "energy, capacity, and ancillary service markets.",
        "What is PJM's performance score (ACE * performance factor) system for regulation, "
        "how does RegD differ from RegA for BESS, and what are the mileage multiplier "
        "implications for BESS revenue?",
    ],
    research_questions=[
        "Based on Modo's latest PJM BESS revenue forecast, what is the outlook for total "
        "merchant revenues over the next 12 months, and which markets (regulation, "
        "capacity, energy) are expected to grow or decline?",
        "What are the key findings from Modo's most recent PJM storage market report — "
        "including any changes to RPM capacity prices, regulation market trends, "
        "or forward revenue assumptions?",
        "According to Modo's PJM pipeline data, how much new BESS capacity is expected "
        "to come online in the next 12–24 months, and what does this mean for per-MW revenues?",
        "What does Modo's research say about optimal BESS duration strategy in PJM — "
        "is 1h, 2h, or 4h duration showing better risk-adjusted returns given PJM's "
        "regulation mileage and capacity accreditation structure?",
        "According to Modo's data, which PJM revenue streams currently offer the best "
        "risk-adjusted returns for BESS, and how have clearing prices and capacity "
        "auction results trended?",
        "What does Modo's research show about the long-term impact of increasing BESS "
        "penetration on PJM regulation prices, RPM clearing prices, and mileage payouts?",
    ],
)
