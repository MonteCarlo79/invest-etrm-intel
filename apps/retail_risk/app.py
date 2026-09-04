"""
Retail Risk Management — Streamlit Application

Risk cockpit for retail electricity customers: CRM, settlement, P&L, positions, VaR, agent.

Run locally:
    streamlit run apps/retail_risk/app.py --server.port=8513
"""
from __future__ import annotations

import os
import sys

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _repo_root)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_repo_root, "config", ".env"), override=False)
except ImportError:
    pass

import streamlit as st

st.set_page_config(
    page_title="Retail Risk Management",
    layout="wide",
    initial_sidebar_state="expanded",
)

from sqlalchemy import create_engine


@st.cache_resource
def _get_engine():
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        st.error("Database URL not configured (PGURL or DB_DSN)")
        st.stop()
    return create_engine(url, pool_pre_ping=True)


engine = _get_engine()

# Auth (optional in dev)
try:
    from auth.rbac import require_role
    require_role(["Admin", "Trader", "Quant", "RiskOfficer"])
except Exception:
    pass

st.title("Retail Risk Management")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "CRM", "Settlement", "Realised P&L",
    "Positions & MtM", "VaR & Greeks", "Agent"
])

with tab1:
    from apps.retail_risk.tab_crm import render_crm
    render_crm(engine)

with tab2:
    from apps.retail_risk.tab_settlement import render_settlement
    render_settlement(engine)

with tab3:
    from apps.retail_risk.tab_pnl import render_pnl
    render_pnl(engine)

with tab4:
    from apps.retail_risk.tab_positions import render_positions
    render_positions(engine)

with tab5:
    from apps.retail_risk.tab_var import render_var
    render_var(engine)

with tab6:
    from apps.retail_risk.tab_agent import render_agent
    render_agent(engine)
