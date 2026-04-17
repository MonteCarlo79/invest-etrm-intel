"""
libs/decision_models/adapters/app/catalogue_app.py

Standalone Streamlit entry point for the decision model catalogue.

Run:
    cd bess-platform
    streamlit run libs/decision_models/adapters/app/catalogue_app.py

The page has no auth, no DB connection, no external dependencies beyond
the model library itself (numpy, pandas, pulp are lazy-imported by the
model modules only when run_fn is called — not at catalogue load time).
"""
import sys
from pathlib import Path

# Ensure bess-platform repo root is on PYTHONPATH (project-wide convention)
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import streamlit as st

st.set_page_config(
    page_title="Decision Model Catalogue",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="collapsed",
)

from libs.decision_models.adapters.app.catalogue_page import render_catalogue_page

render_catalogue_page()
