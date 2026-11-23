"""Poland power market configuration — MarketConfig instance for investment advisory."""
from services.intl_market_common.market_config import MarketConfig

_STANDARD_QUESTIONS = [
    "What are the most significant recent developments in the Polish power market affecting BESS investment?",
    "How are FCR and aFRR auction prices trending in Poland and what does this mean for BESS revenue stacks?",
    "What is the current outlook for BESS in Poland's capacity market (Rynek Mocy) — T-4/T-1 auction eligibility and pricing?",
    "What major policy changes from URE or government have been announced affecting RE and storage developers in Poland?",
    "How is Poland's renewable energy build-out progressing — solar, onshore wind, offshore wind pipelines?",
    "What are the key risks for BESS investment in Poland — regulatory, market, and grid connectivity?",
    "Which new BESS or flexible energy storage projects have been announced or are under development in Poland?",
    "How competitive is the Polish BESS market compared to DACH and Nordic peers in terms of FCR/aFRR revenue depth?",
]

_FOUNDATIONAL_QUESTIONS = [
    "Explain the full Polish balancing market structure — how does the Rynek Bilansujący (RB) work, how are bids submitted to PSE, and how are BESS assets settled?",
    "How does FCR (Primary Control Reserve) work in Poland under the ENTSO-E framework — bid requirements, activation, pricing, and BESS participation rules?",
    "Explain aFRR (Secondary Reserve) procurement in Poland — symmetrical vs asymmetrical products, weekly/monthly auctions, activation energy settlement.",
    "What are the full legal and regulatory requirements for a foreign developer to build and operate a BESS project in Poland? (Grid connection, environmental, URE licensing, land use.)",
    "How does the Polish capacity market (Rynek Mocy) work — T-4 and T-1 auctions, BESS eligibility, duration requirements, obligation periods, and derating factors?",
    "What are the transmission constraints and congestion patterns in Poland? How do N/S price zone differences and cross-border interconnection affect storage dispatch strategy?",
]

_RESEARCH_QUESTIONS = [
    "Based on Aurora Energy Research forecasts (Q1/Q2 2026), what are the Polish power price projections through 2030-2040, and what does this mean for BESS energy arbitrage revenue?",
    "What does Aurora's Monthly Flexible Energy Market Summary say about FCR and aFRR price trends in Poland for early 2026?",
    "What is the optimal duration for a Polish BESS project — 1h, 2h, or 4h — given FCR/aFRR requirements and energy arbitrage spreads?",
    "How does the Baltic offshore wind build-out (2030-2040) affect Polish power prices and BESS revenue prospects?",
    "What are the key lessons from early Polish BESS projects — grid connection timelines, capacity market performance, FCR revenue realisation?",
    "How does Polish BESS investment compare to German and UK markets in terms of risk-adjusted returns and regulatory maturity?",
]

MARKET_CONFIG = MarketConfig(
    name="Poland",
    code="po",
    table_prefix="po_",
    port=8511,
    app_slug="po-market",
    app_key="po_market",
    currency_sym="zł",
    currency_code="PLN",
    timezone="Europe/Warsaw",
    flag_emoji="🇵🇱",
    flag_url="https://flagcdn.com/w40/pl.png",
    intervals_per_day=96,       # 15-min intervals (ENTSO-E)
    system_operator="PSE",
    wholesale_label="TGE Day-Ahead / Balancing Market (RB)",
    ancillary_label="FCR / aFRR / mFRR",
    modo_api_prefix="/po/modo",  # not used
    standard_questions=_STANDARD_QUESTIONS,
    foundational_questions=_FOUNDATIONAL_QUESTIONS,
    research_questions=_RESEARCH_QUESTIONS,
)
