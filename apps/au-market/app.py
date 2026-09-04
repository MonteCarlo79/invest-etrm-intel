"""Australia (NEM) Market Intelligence — Streamlit app.  Port 8509."""
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"))

import streamlit as st
st.set_page_config(
    page_title="Australia (NEM) Market Intelligence",
    page_icon="🇦🇺",
    layout="wide",
    initial_sidebar_state="expanded",
)

from services.au_knowledge.config import MARKET_CONFIG
from services.intl_market_common.app_template import run_market_app

run_market_app(MARKET_CONFIG, _app_file=__file__)
