"""CAISO (California) market configuration and Modo AI question sets."""
from services.intl_market_common.market_config import MarketConfig

MARKET_CONFIG = MarketConfig(
    name="CAISO (California)",
    code="caiso",
    table_prefix="caiso_",
    port=8512,
    app_slug="caiso-market",
    app_key="caiso_market",
    currency_sym="$",
    currency_code="USD",
    timezone="US/Pacific",
    flag_emoji="🇺🇸",
    flag_url="https://flagcdn.com/w40/us.png",
    intervals_per_day=48,
    system_operator="CAISO",
    wholesale_label="CAISO RT LMP",
    ancillary_label="Reg/Spin/Non-Spin",
    modo_api_prefix="/caiso/modo",
    standard_questions=[
        "What are the most important CAISO BESS market developments from the last 24 hours?",
        "Which revenue streams are performing best for CAISO BESS assets right now — "
        "real-time energy, regulation, spinning reserve, non-spinning reserve, or RA capacity?",
        "What is the current short-term outlook for CAISO BESS merchant revenues?",
        "Are there any significant regulatory or policy changes at CPUC/CAISO/CARB currently "
        "affecting BESS operations, IRP requirements, or RA procurement?",
        "How are CAISO real-time LMPs and ancillary service clearing prices trending, "
        "and what does it mean for BESS dispatch strategy (including duck curve dynamics)?",
        "Which CAISO BESS assets or operators are showing standout performance this week and why?",
        "What are the key market risks and opportunities for CAISO BESS investors right now?",
        "How are CAISO grid conditions, solar curtailment, net load ramp requirements, "
        "and transmission constraints affecting BESS revenue opportunities?",
    ],
    foundational_questions=[
        "Give me a detailed explanation of how the CAISO power market works — "
        "day-ahead (DAM) and real-time (RTM) markets, 5-minute intervals, LMP pricing, "
        "ancillary service markets, Resource Adequacy (RA), and how BESS earns revenue.",
        "What are CAISO's ancillary services — Regulation Up/Down, Spinning Reserve, "
        "Non-Spinning Reserve — how are they procured via CAISO co-optimisation, "
        "performance requirements, and what do BESS assets typically earn?",
        "How does BESS participate in CAISO energy arbitrage — duck curve (morning charge, "
        "evening discharge), negative price events, RT vs DA spread trading, and the "
        "interplay with solar over-generation curtailment?",
        "What is CAISO Resource Adequacy (RA) — how does BESS qualify (effective load "
        "carrying capability / ELCC), RA obligations, and how does RA revenue compare "
        "to energy and AS revenues?",
        "Explain CAISO's storage-specific participation rules under FERC Order 841 — "
        "Storage Participation Model (SPM), State of Charge (SOC) management, "
        "and how BESS operators optimise between energy, RA, and AS co-dispatch.",
        "What is California's long-duration storage mandate (AB2514, IRP targets), "
        "how has CPUC procurement driven deployment, and what are the policy-driven "
        "vs merchant revenue dynamics for new CAISO BESS projects?",
    ],
    research_questions=[
        "Based on Modo's latest CAISO BESS revenue forecast, what is the outlook for total "
        "merchant revenues over the next 12 months, and which markets (energy, AS, RA) "
        "are expected to grow or decline?",
        "What are the key findings from Modo's most recent CAISO storage market report — "
        "including any changes to RA value, duck curve evolution, or AS price trends?",
        "According to Modo's CAISO pipeline data, how much new BESS capacity is expected "
        "to come online in the next 12–24 months, and what does this mean for per-MW revenues?",
        "What does Modo's research say about optimal BESS duration strategy in CAISO — "
        "is 2h, 4h, or longer-duration showing better risk-adjusted returns given "
        "CAISO's duck curve and RA accreditation structure?",
        "According to Modo's data, which CAISO revenue streams currently offer the best "
        "risk-adjusted returns for BESS, and how have RA clearing prices and AS "
        "clearing prices trended?",
        "What does Modo's research show about the long-term impact of increasing BESS "
        "penetration on CAISO ancillary service prices, RA value, and duck curve depth?",
    ],
)
