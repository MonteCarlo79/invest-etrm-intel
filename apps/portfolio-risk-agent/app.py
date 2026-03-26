import streamlit as st
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from auth.rbac import require_role, get_user
from shared.agents.portfolio_risk_agent import (
    build_candidate_portfolio,
    stress_test_portfolio,
    build_risk_flags,
    simple_portfolio_memo,
)
from shared.agents.logging_utils import ensure_agent_log_table, log_agent_request

st.set_page_config(page_title="Portfolio & Risk Agent", layout="wide")

role = require_role(["Admin", "Trader", "Quant"])
user = get_user()
user_email = user.get("email", "unknown") if user else "unknown"

ensure_agent_log_table()

st.title("🛡️ Portfolio & Risk Agent")
st.caption(f"User: {user_email} | Role: {role}")

tab1, tab2 = st.tabs(["Portfolio Builder", "Prompt"])

with tab1:
    candidate_size = st.slider("Candidate pool size", 4, 12, 8)
    spread_down = st.slider("Stress: spread compression", 0.00, 0.50, 0.15, 0.01)

    if st.button("Build Portfolio View"):
        base = build_candidate_portfolio(limit=candidate_size)
        stressed = stress_test_portfolio(base, spread_down_pct=spread_down)
        flags = build_risk_flags(base)

        st.markdown("### Candidate Portfolio")
        st.dataframe(base, use_container_width=True)

        st.markdown("### Stress Test")
        st.dataframe(stressed, use_container_width=True)

        st.markdown("### Risk Flags")
        st.dataframe(flags, use_container_width=True)

with tab2:
    prompt = st.text_area(
        "Enter portfolio/risk question",
        height=140,
        placeholder="Example: Build a balanced portfolio across top provinces under 15% spread compression.",
    )
    if st.button("Run Portfolio & Risk Agent"):
        base = build_candidate_portfolio(limit=8)
        stressed = stress_test_portfolio(base, spread_down_pct=0.15)
        log_agent_request(user_email, "Portfolio & Risk Agent", prompt, status="submitted")
        st.success(simple_portfolio_memo(prompt, base, stressed))