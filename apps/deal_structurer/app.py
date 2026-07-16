"""apps/deal_structurer/app.py — Shell: sidebar, session state, tab routing."""
from __future__ import annotations
import os
import sys
sys.path.insert(0, "/app")

import streamlit as st
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(page_title="Deal Structurer", layout="wide", page_icon="📊")

# ── Session state defaults ────────────────────────────────────────────────────
_DEFAULTS = {
    "price_paths": None,          # np.ndarray (n_sim, 8760)
    "price_sim_req": None,        # PriceSimRequest
    "dispatch_result": None,      # DispatchResult
    "mc_result": None,            # MCResult
    "last_dispatch_req": None,    # DispatchRequest
    "last_financials": None,      # ProjectFinancials
    "last_cf_result": None,       # CashFlowResult
    "dp_result": None,            # DealPricingResult
    "agent_messages": [],
    "agent_display": [],
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📊 Deal Structurer")
    st.caption("Quant deal pricing platform")
    st.divider()
    tab_choice = st.radio(
        "Navigate",
        ["1 · Price Simulation", "2 · Dispatch Revenue", "3 · Project Cash Flow",
         "4 · Monte Carlo", "5 · Deal Pricing", "💬 Strategist"],
        label_visibility="collapsed",
    )
    st.divider()
    st.caption("Province → Price Paths → Dispatch → Cashflow → MC → Deal Pricing")

# ── Route to tabs ─────────────────────────────────────────────────────────────
if tab_choice == "1 · Price Simulation":
    from apps.deal_structurer import price_tab; price_tab.render()
elif tab_choice == "2 · Dispatch Revenue":
    from apps.deal_structurer import dispatch_tab; dispatch_tab.render()
elif tab_choice == "3 · Project Cash Flow":
    from apps.deal_structurer import cashflow_tab; cashflow_tab.render()
elif tab_choice == "4 · Monte Carlo":
    from apps.deal_structurer import mc_tab; mc_tab.render()
elif tab_choice == "5 · Deal Pricing":
    from apps.deal_structurer import deal_tab; deal_tab.render()
else:
    from apps.deal_structurer import strategist; strategist.render()
