# Handoff: Philippines + Poland Market Investment Advisory Apps

**Branch:** `feat/ph-po-market-apps` (on GitHub MonteCarlo79/invest-etrm-intel)  
**Last updated:** 2026-05-30  
**Scope:** Two new Streamlit investment advisory apps — Philippines (ph-market) and Poland (po-market) — plus PyPSA grid analysis tab for both.

---

## What Is Already Done

### Philippines `services/ph_knowledge/` — COMMITTED + PUSHED ✅

| File | Status | Purpose |
|------|--------|---------|
| `services/ph_knowledge/__init__.py` | ✅ done | package init |
| `services/ph_knowledge/config.py` | ✅ done | `MarketConfig(code="ph", port=8510, currency_sym="₱", table_prefix="ph_", system_operator="NGCP")` + 18 investment-advisory question sets |
| `services/ph_knowledge/ingest.py` | ✅ done | `LocalReportsConnector` (scans `data/market-fundamentals-ph/`), `DOENewsConnector`, `IEMOPNoticesConnector`, `run_knowledge_ingest()` |

### Poland `services/po_knowledge/` — NOT STARTED ❌

---

## What Still Needs To Be Built

### 1. `apps/ph-market/app.py` ← MAIN OUTSTANDING PIECE

This is a **standalone** Streamlit app (~1400 lines). It does NOT call `run_market_app()` from `intl_market_common/app_template.py` because the tab structure is completely different (investment-focused, no Modo Energy data).

**Imports to use:**
```python
from services.intl_market_common.advanced_retrieval_base import retrieve_for_agent
from services.intl_market_common.expert_memory_base import (
    extract_insights, get_insights, inject_memory, digest_kb_docs
)
from services.ph_knowledge.config import MARKET_CONFIG
from services.ph_knowledge.ingest import run_knowledge_ingest
```

**Tabs (7 + optional PyPSA tab 8):**

| Tab | Content |
|-----|---------|
| Market Structure | Static KPIs (19.1 GW peak, 29,962 MW installed, 8.1% demand growth), 3-grid bar chart (Luzon 73%/Visayas 12%/Mindanao 15%), generation mix pie (coal 56-63%, renewables 23%), routes-to-market comparison table, key players |
| Green Energy Auctions | GEAP rounds bar chart (GEA-1: 1,967 MW; GEA-2: 10,653 MW; GEA-4: 3,441 MW target; GEA-5: 3,300 MW offshore), technology breakdown, GET pricing comparison table |
| BESS Opportunity | NGCP AS framework (regulating/contingency/dispatchable reserves), reserve market (started Jan 2024), ASPA procurement, BESS revenue stack, IEMOP CAPER context |
| Investment Analysis | IRR calculator (Solar/Wind/BESS/IRESS presets + custom), outputs: unlevered IRR, equity IRR, LCOE, NPV, 3×3 sensitivity table (CAPEX × revenue) |
| Investment Advisor | Main AI agent chat — Claude sonnet-4-6, 7 tools (see below), expert memory, knowledge gap interview |
| Knowledge Base | KB stats, search, upload files, fetch URL, "Auto-ingest local reports" button |
| Data Management | Table coverage, KB ingest, KB digest → expert insights, expert memory view, agent memory |
| PyPSA (Tab 8) | Placeholder: "Upload NGCP network CSV to enable power flow analysis"; when data present run `network.lopf()`, show dispatch + congestion map |

**7 Agent Tools:**

```python
_TOOLS = [
    {
        "name": "search_knowledge_base",
        "description": "Semantic search (HyDE + FTS + rerank) over Philippines market reports and documents.",
        "input_schema": {"type": "object", "properties": {"query": {"type": "string"}, "sources": {"type": "array", "items": {"type": "string"}}}, "required": ["query"]}
    },
    {
        "name": "get_geap_data",
        "description": "Returns Green Energy Auction Program (GEAP) data — rounds GEA-1 through GEA-5, target vs awarded capacity, GET pricing, technologies.",
        "input_schema": {"type": "object", "properties": {"round": {"type": "string"}}, "required": []}
    },
    {
        "name": "get_wesm_price_context",
        "description": "WESM spot price projections and routes-to-market price ranges (PHP/kWh) by grid (Luzon/Visayas/Mindanao) from AFRY AIMR 2024Q4.",
        "input_schema": {"type": "object", "properties": {"grid": {"type": "string"}}, "required": []}
    },
    {
        "name": "get_ancillary_services_context",
        "description": "Philippines ancillary services market — NGCP reserve types, ASPA procurement, reserve market mechanics, BESS revenue stack.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "estimate_re_irr",
        "description": "Parametric IRR/LCOE model for Philippine RE projects (Solar, Onshore Wind, BESS, IRESS). Returns unlevered IRR, equity IRR, LCOE, NPV, sensitivity table.",
        "input_schema": {
            "type": "object",
            "properties": {
                "technology": {"type": "string", "enum": ["solar", "onshore_wind", "bess_2h", "bess_4h", "iress"]},
                "capacity_mw": {"type": "number"},
                "capex_usd_per_kw": {"type": "number"},
                "revenue_php_per_kwh": {"type": "number"},
                "capacity_factor_pct": {"type": "number"},
                "wacc_pct": {"type": "number"},
                "project_life_yrs": {"type": "integer"},
                "leverage_pct": {"type": "number"},
                "cost_of_debt_pct": {"type": "number"}
            },
            "required": ["technology", "capacity_mw"]
        }
    },
    {
        "name": "get_market_structure",
        "description": "Philippines power market structure — installed capacity, demand, generation mix, key players, market participants.",
        "input_schema": {"type": "object", "properties": {"topic": {"type": "string"}}, "required": []}
    },
    {
        "name": "get_policy_snapshot",
        "description": "Philippines energy policy and regulatory landscape — EPIRA, RE Act, foreign ownership rules, DOE Philippine Energy Plan 2023-2050, GEAP rules.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
]
```

**Agent System Prompt (key excerpts):**
```
You are a senior Philippines Renewable Power Investment Expert at a global infrastructure fund.

GROUNDING RULE: For specific current prices, project data, and recent developments → use your tools.
For regulatory framework, market mechanics, and historical context → use embedded knowledge below.

MARKET CONTEXT:
- WESM: Wholesale Electricity Spot Market, operated by IEMOP/PEMC
- Three grids: Luzon (73% of capacity), Visayas (12%), Mindanao (15%)
- Peak demand: 19.1 GW (2024); demand growth 8.1%/yr; GDP growth 5.6%
- Generation mix: coal 56-63%, geothermal 6% (highest utilisation at 66% CF), renewables 23%
- RE target: 35% by 2030, 50% by 2040 (DOE Philippine Energy Plan 2023-2050)

ROUTES TO MARKET:
- WESM Spot: 4.26–8.25 PHP/kWh (2025–2060); wind has priority dispatch
- PSA with DU/EC: 5.19–6.52 PHP/kWh; ERC-regulated; 1-2yr approval timeline
- Retail/GEOP: 4.10–7.97 PHP/kWh; no ERC approval; short contracts
- GEAP: ~6.00 PHP/kWh avg for wind (GEA-1/2); 20-year COE-GET; government-backed
- FiT: 8.84–11.14 PHP/kWh; FULLY SUBSCRIBED since 2019

BESS OPPORTUNITY:
- Reserve market started January 2024 (regulating, contingency, dispatchable)
- NGCP procures via ASPA (firm contracts) + real-time reserve market
- BESS excels at regulating reserves (fast response premium)
- Revenue stack: regulating reserves + contingency reserves + WESM arbitrage + IRESS GEAP tariff

FOREIGN OWNERSHIP:
- RE Act allows 100% foreign ownership for RE project developers (exception to 40% FDI cap)
- NGCP (transmission) is 40% foreign-owned max
- Distribution: 40% FDI cap applies
```

**IRR Model Reference Values:**
```python
_TECH_PRESETS = {
    "solar":        {"capex_usd_kw": 720,  "cf_pct": 20.0, "om_pct": 1.5, "degradation": 0.005, "life": 25},
    "onshore_wind": {"capex_usd_kw": 1550, "cf_pct": 31.0, "om_pct": 2.0, "degradation": 0.000, "life": 25},
    "bess_2h":      {"capex_usd_kwh": 300, "cf_pct": 0.0,  "om_pct": 2.0, "degradation": 0.020, "life": 15},
    "bess_4h":      {"capex_usd_kwh": 280, "cf_pct": 0.0,  "om_pct": 2.0, "degradation": 0.020, "life": 15},
    "iress":        {"capex_usd_kw": 1000, "cf_pct": 19.0, "om_pct": 2.0, "degradation": 0.005, "life": 25},
}
_USD_PHP = 58.0  # approximate FX rate
```

**DB Tables created by `_ensure_tables()` in app.py:**
```sql
intl_market.ph_knowledge_docs       -- KB with FTS (already created by ingest.py)
intl_market.ph_expert_insights      -- expert insights
intl_market.ph_analyst_sessions     -- chat history
marketdata.agent_memory             -- already exists
```

**Scheduler (APScheduler, Asia/Manila):**
- 03:30 daily: `run_knowledge_ingest()` (local reports + DOE + IEMOP)
- 03:45 daily: `digest_kb_docs()` (extract insights from undigested docs)

---

### 2. `apps/ph-market/Dockerfile`

Copy from `apps/au-market/Dockerfile`, change:
- Port: `8510`
- App path: `apps/ph-market/app.py`
- baseUrlPath: `ph-market`
- Replace `apps/au-market/` → `apps/ph-market/`
- Replace `services/au_knowledge/` → `services/ph_knowledge/`
- Replace `services/aemo/` → (remove, not needed for PH)
- Add: `python-pptx>=0.6` to pip installs (for PPTX extraction in ingest.py)
- Add: `pypsa` to pip installs (for PyPSA tab)

---

### 3. Poland App — `services/po_knowledge/` + `apps/po-market/`

**Source data:** `data/market-fundamentals-po/`
- `Aurora_Q1_26_POL_Power_Renewables_Market_Forecast_Report.pdf`
- `Aurora_Q2_26_POL_Power_Renewables_Market_Forecast_Report.pdf`
- `Aurora_Q1_26_POL_Power_Renewables_Market_Forecast_Data.xlsx`
- `Aurora_Q2_26_POL_Power_Renewables_Market_Forecast_Data_v1.2.xlsx`
- `Aurora_Apr26_POL_Monthly_Flexible_Energy_Market_Summary.pdf`
- `Aurora_Mar26_POL_Monthly_Flexible_Energy_Market_Summary.pdf`
- 3 WXWork screenshot PNGs

**Architecture:** Same pattern as PH app but:
```python
MarketConfig(
    name="Poland",
    code="po",
    table_prefix="po_",
    port=8511,
    currency_sym="zł",
    currency_code="PLN",
    timezone="Europe/Warsaw",
    flag_emoji="🇵🇱",
    system_operator="PSE",
    wholesale_label="TGE Day-Ahead / Balancing Market",
    ancillary_label="FCR / aFRR / mFRR",
)
```

**Polish Market Key Facts (for system prompt):**
- TSO: PSE (Polskie Sieci Elektroenergetyczne)
- Market operator: TGE (Towarowa Giełda Energii) for day-ahead/intraday
- Balancing: Rynek Bilansujący (RB) — PSE-operated
- AS: FCR (Primary), aFRR (Secondary), mFRR (Tertiary) — ENTSO-E framework
- BESS revenue stack: FCR + aFRR + energy arbitrage + capacity market (Rynek Mocy)
- RE policy: Polish Energy Policy 2040 (PEP2040); offshore wind in Baltic Sea

**Agent tools (Poland):**
1. `search_knowledge_base`
2. `get_aurora_forecast_data` — returns key projections from Aurora Excel/PDF
3. `get_balancing_market_context` — PSE RB, AS services, BESS participation
4. `get_capacity_market_context` — Rynek Mocy, BESS T-4/T-1 auction data
5. `estimate_bess_irr` — IRR model for Polish BESS (PLN, FCR+aFRR revenue stack)
6. `get_market_structure` — installed capacity, generation mix, RE pipeline
7. `get_policy_snapshot` — PEP2040, offshore wind law, RES support mechanisms

**Port:** 8511

---

### 4. PyPSA Tab (Both Apps)

Add as last tab in both ph-market and po-market apps.

**For Philippines (ph-market):**
```python
# Tab: Grid Analysis (PyPSA)
with tab_pypsa:
    st.header("Grid Analysis — PyPSA Power Flow")
    st.info("Upload NGCP network data (buses CSV + lines CSV + generators CSV) to run power flow analysis.")
    
    uploaded_buses = st.file_uploader("Buses CSV", type=["csv"], key="ph_pypsa_buses")
    uploaded_lines = st.file_uploader("Lines CSV", type=["csv"], key="ph_pypsa_lines")
    uploaded_gens  = st.file_uploader("Generators CSV", type=["csv"], key="ph_pypsa_gens")
    
    if uploaded_buses and uploaded_lines:
        import pypsa
        n = pypsa.Network()
        n.import_from_csv_folder(...)  # or build manually
        n.lopf(pyomo=False)
        # show dispatch chart + marginal prices map
```

**For Poland (po-market):**
- PSE publishes SCADA/grid data; Aurora Excel likely has zonal marginal prices
- Start with simplified 4-zone model (North/South/East/West Poland)
- Use `pypsa.Network()` with zonal prices from Aurora data

---

## How To Run Locally (once app.py exists)

```bash
cd bess-platform
streamlit run apps/ph-market/app.py --server.port=8510
```

Requires `.env` with `PGURL` and `ANTHROPIC_API_KEY`.

First run: go to Knowledge Base tab → click "Auto-ingest Local Reports" to seed the KB from `data/market-fundamentals-ph/`.

---

## Summary Checklist for Next Session

- [ ] Write `apps/ph-market/app.py` (~1400 lines, see architecture above)
- [ ] Write `apps/ph-market/Dockerfile` (copy au-market, port 8510, add python-pptx + pypsa)
- [ ] Commit + push to `feat/ph-po-market-apps`
- [ ] Create `services/po_knowledge/__init__.py`, `config.py`, `ingest.py`
- [ ] Write `apps/po-market/app.py`
- [ ] Write `apps/po-market/Dockerfile`
- [ ] Test locally: `streamlit run apps/ph-market/app.py`
- [ ] Build + push Docker images to ECR
- [ ] Deploy to ECS (Terraform or AWS CLI)
