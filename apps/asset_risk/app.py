"""
Asset Risk Management — Streamlit Application

Risk cockpit for asset-backed trading books: wind, solar, BESS, thermal.

Run locally:
    streamlit run apps/asset_risk/app.py --server.port=8512
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
    page_title="Asset Risk Management",
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

st.title("Asset Risk Management")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Asset Config", "Settlement", "Realised P&L",
    "Positions & MtM", "VaR & Greeks", "Agent"
])

with tab1:
    from apps.asset_risk.tab_asset_config import render_asset_config
    render_asset_config(engine)

with tab2:
    from apps.asset_risk.tab_settlement import render_settlement
    render_settlement(engine)

with tab3:
    from apps.asset_risk.tab_pnl import render_pnl
    render_pnl(engine)

with tab4:
    from apps.asset_risk.tab_positions import render_positions
    render_positions(engine)

with tab5:
    from apps.asset_risk.tab_var import render_var
    render_var(engine)

with tab6:
    from apps.asset_risk.tab_agent import render_agent
    render_agent(engine)
