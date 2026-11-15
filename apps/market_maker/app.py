"""
Market Maker — central risk book (Streamlit).

Run locally:
    streamlit run apps/market_maker/app.py --server.port=8530
"""
from __future__ import annotations

import os
import sys
from datetime import date

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _repo_root)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_repo_root, "config", ".env"), override=False)
except ImportError:
    pass

import streamlit as st

st.set_page_config(page_title="Market Maker Risk Book", layout="wide",
                   initial_sidebar_state="expanded")

from sqlalchemy import create_engine


@st.cache_resource
def _get_engine():
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        st.error("Database URL not configured (PGURL or DB_DSN)")
        st.stop()
    return create_engine(url, pool_pre_ping=True)


engine = _get_engine()

try:
    from auth.rbac import require_role
    require_role(["Admin", "Trader", "Quant", "RiskOfficer"])
except Exception:
    pass

from apps.market_maker.data import get_books, sidebar_selection  # noqa: E402

st.title("Market Maker — Central Risk Book")

sel = sidebar_selection(engine, get_books(engine), date.today())

LABELS = ["Positions", "Exposure & MtM", "VaR & Greeks", "Curves", "Risk Agent"]

# A radio renders only the selected page (st.tabs would run every page each rerun).
page = st.radio("页面", LABELS, horizontal=True, key="mm_page", label_visibility="collapsed")

if page == "Positions":
    from apps.market_maker.page_positions import render_positions
    render_positions(engine, sel)

elif page == "Exposure & MtM":
    from apps.market_maker.page_exposure import render_exposure
    render_exposure(engine, sel)

elif page == "VaR & Greeks":
    from apps.market_maker.page_var import render_var
    render_var(engine, sel)

elif page == "Curves":
    from apps.market_maker.page_curves import render_curves
    render_curves(engine, sel)

elif page == "Risk Agent":
    from apps.market_maker.page_agent import render_agent
    render_agent(engine)
