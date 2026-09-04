"""Portugal / Iberia (MIBEL) power market configuration."""
from services.intl_market_common.market_config import MarketConfig

MARKET_CONFIG = MarketConfig(
    name="Portugal (MIBEL)",
    code="po",
    table_prefix="po_",
    port=8512,
    app_slug="po-market",
    app_key="po_market",
    currency_sym="€",
    currency_code="EUR",
    timezone="Europe/Lisbon",
    flag_emoji="🇵🇹",
    flag_url="https://flagcdn.com/w40/pt.png",
    intervals_per_day=24,
    system_operator="REN",
    wholesale_label="MIBEL Spot (OMIE)",
    ancillary_label="Primary / Secondary / Tertiary Reserve",
    modo_api_prefix="/po/modo",
    standard_questions=[
        "What are the most significant recent developments in Portugal's renewable energy "
        "and storage market?",
        "How are MIBEL spot prices trending — is Portugal at a premium or discount to Spain?",
        "What is the current outlook for BESS investment in Portugal — regulatory support, "
        "revenue streams, and market maturity?",
        "What major policy changes from DGEG or ERSE have been announced recently?",
        "How are Portugal's electricity demand and renewable penetration trending?",
        "What are the key risks for utility-scale solar and wind projects in Portugal now?",
        "Which new battery storage or hybrid RE projects have been announced in Portugal?",
        "How does Portugal compare to Spain for BESS investment attractiveness?",
    ],
    foundational_questions=[
        "Explain the MIBEL wholesale electricity market — day-ahead (OMIE), intraday, "
        "and balancing market structure; how prices are formed; and how Portuguese generators "
        "and storage assets earn revenue.",
        "What ancillary services does REN procure — primary frequency response, secondary "
        "reserve (aFRR), tertiary reserve (mFRR), and interruptibility — and how can BESS "
        "assets participate and stack revenues?",
        "What are the regulatory requirements and licensing steps for a utility-scale BESS "
        "or hybrid RE+storage project in Portugal?",
        "How does Portugal's capacity mechanism work — are there capacity payments or "
        "strategic reserves that BESS can access?",
        "What is the current state of Portugal's grid infrastructure and what are the key "
        "transmission constraints affecting RE development, particularly in the Alentejo and "
        "Algarve regions?",
        "How does project finance for utility-scale RE projects work in Portugal — typical "
        "structures, key lenders (EIB, commercial banks), leverage, and bankability requirements?",
    ],
    research_questions=[
        "What do independent consultants (Aurora, Montel, ENTSO-E) forecast for MIBEL "
        "spot prices and BESS revenues in Portugal over the next 5 years?",
        "What are the key findings from recent BESS market reports for the Iberian Peninsula?",
        "How has Portugal's RE auction (SERUP) programme evolved — pricing, technologies, "
        "volumes, and lessons for future auctions?",
        "What is the state of Portugal-Spain interconnection capacity and what are the "
        "long-term plans — implications for price convergence and merchant BESS?",
    ],
)
