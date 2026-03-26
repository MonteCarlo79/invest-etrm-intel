import streamlit as st
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from auth.rbac import require_role, get_user
from shared.agents.strategy_agent import (
    build_strategy_summary,
    simple_strategy_memo,
)
from shared.agents.logging_utils import ensure_agent_log_table, log_agent_request

st.set_page_config(page_title="Strategy Agent", layout="wide")

role = require_role(["Admin", "Trader", "Quant", "Analyst"])
user = get_user()
user_email = user.get("email", "unknown") if user else "unknown"

ensure_agent_log_table()

st.title("🧠 Strategy Agent")
st.caption(f"User: {user_email} | Role: {role}")

tab1, tab2 = st.tabs(["Strategy Dashboard", "Prompt"])

with tab1:
    if st.button("Load Strategy Summary"):
        summary = build_strategy_summary()

        st.markdown("### Top Provinces")
        st.dataframe(summary["top_provinces"], use_container_width=True)

        st.markdown("### Spread Statistics")
        st.dataframe(summary["spread_stats"], use_container_width=True)

        st.markdown("### Structural Capacity Bias")
        st.dataframe(
            summary["capacity_bias"][
                ["province", "solar_ratio", "wind_ratio", "thermal_ratio", "structural_spread_bias"]
            ],
            use_container_width=True,
        )

        st.markdown("### Mengxi Rank Delta")
        st.dataframe(summary["mengxi_rank_delta"], use_container_width=True)

with tab2:
    prompt = st.text_area(
        "Enter strategy question",
        height=140,
        placeholder="Example: Which provinces should we prioritise for 4h BESS deployment?",
    )
    if st.button("Run Strategy Agent"):
        summary = build_strategy_summary()
        log_agent_request(user_email, "Strategy Agent", prompt, status="submitted")
        st.success(simple_strategy_memo(prompt, summary))