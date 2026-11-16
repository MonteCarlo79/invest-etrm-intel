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

LABELS = [
    "Asset Config", "Settlement", "Realised P&L", "Agent",
    "Dispatch Diagnostics", "收益瀑布 P&L Waterfall", "复盘 Review",
]

# st.tabs executes every tab body on each rerun; a radio renders only the
# selected page, so the waterfall LP / chain load run only when selected.
page = st.radio("页面", LABELS, horizontal=True, key="asset_risk_page",
                label_visibility="collapsed")

if page == "Asset Config":
    from apps.asset_risk.tab_asset_config import render_asset_config
    render_asset_config(engine)

elif page == "Settlement":
    from apps.asset_risk.tab_settlement import render_settlement
    render_settlement(engine)

elif page == "Realised P&L":
    from apps.asset_risk.tab_pnl import render_pnl
    render_pnl(engine)

elif page == "Agent":
    from apps.asset_risk.tab_agent import render_agent
    render_agent(engine)

elif page == "Dispatch Diagnostics":
    from apps.asset_risk.tab_diagnostics import render_diagnostics
    render_diagnostics(engine)

elif page == "收益瀑布 P&L Waterfall":
    from apps.asset_risk.tab_waterfall import render_waterfall
    render_waterfall(engine)

elif page == "复盘 Review":
    from apps.asset_risk.tab_review import render_review
    render_review(engine)
