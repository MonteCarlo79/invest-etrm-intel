import streamlit as st
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from auth.rbac import require_role, get_user
from shared.agents.it_dev_agent import (
    scan_repo_overview,
    detect_candidate_targets,
    propose_change,
)
from shared.agents.logging_utils import ensure_agent_log_table, log_agent_request

st.set_page_config(page_title="IT Developer Agent", layout="wide")

role = require_role(["Admin", "Quant"])
user = get_user()
user_email = user.get("email", "unknown") if user else "unknown"
ROOT = Path(__file__).resolve().parents[2]

ensure_agent_log_table()

st.title("💻 IT Developer Agent")
st.caption(f"User: {user_email} | Role: {role}")

tab1, tab2 = st.tabs(["Repo Scan", "Change Intake"])

with tab1:
    if st.button("Scan Repository"):
        st.dataframe(scan_repo_overview(ROOT), use_container_width=True)

with tab2:
    request_text = st.text_area(
        "Describe requested change",
        height=180,
        placeholder="Example: Add province filter to BESS Map and expose route from portal.",
    )
    if st.button("Generate Change Proposal"):
        targets = detect_candidate_targets(request_text)
        log_agent_request(user_email, "IT Developer Agent", request_text, status="submitted")

        st.markdown("### Likely Target Files")
        if targets:
            for t in targets:
                st.write(f"- {t}")
        else:
            st.write("- No obvious target files detected")

        st.markdown("### Proposed Workflow")
        st.code(propose_change(request_text, targets))