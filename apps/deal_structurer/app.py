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
    "ov_result": None,            # option-value cell dict (option_tab contract)
    "acq_result": None,           # acquisition panel result (option_tab)
    "last_financials": None,      # ProjectFinancials
    "last_cf_result": None,       # CashFlowResult
    "dp_result": None,            # DealPricingResult
    "agent_messages": [],
    "agent_display": [],
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# Restore persisted results from previous session (only on first run)
from apps.deal_structurer import session_cache as _sc
if not st.session_state.get("_cache_loaded"):
    _sc.load(st.session_state)
    st.session_state["_cache_loaded"] = True

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📊 Deal Structurer")
    st.caption("Quant deal pricing platform")
    st.link_button("← Portal", "https://pjh-etrm.ai/portal/", use_container_width=True)
    st.divider()
    tab_choice = st.radio(
        "Navigate",
        ["0 · Deal Intake", "期权价值", "项目财务", "Deal Pricing",
         "价值筛选", "投委会", "💬 Structurer"],
        label_visibility="collapsed",
        key="nav",
    )
    st.divider()
    st.caption("Intake → 期权价值 → 项目财务 → Deal Pricing → 投委会")
    st.divider()
    if st.button("🗑 Clear saved session", use_container_width=True):
        _sc.clear()
        for k, v in _DEFAULTS.items():
            st.session_state[k] = v
        st.rerun()

# ── Route to tabs ─────────────────────────────────────────────────────────────
if tab_choice == "0 · Deal Intake":
    from apps.deal_structurer import intake_tab; intake_tab.render()
elif tab_choice == "期权价值":
    from apps.deal_structurer import option_tab; option_tab.render()
elif tab_choice == "项目财务":
    from apps.deal_structurer import cashflow_tab; cashflow_tab.render()
elif tab_choice == "Deal Pricing":
    from apps.deal_structurer import deal_tab; deal_tab.render()
elif tab_choice == "价值筛选":
    from apps.deal_structurer import screen_tab; screen_tab.render()
elif tab_choice == "投委会":
    from apps.deal_structurer import committee_tab; committee_tab.render()
else:
    from apps.deal_structurer import strategist; strategist.render()
