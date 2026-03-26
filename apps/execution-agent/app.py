

import streamlit as st
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from auth.rbac import require_role, get_user
from shared.agents.execution_agent import (
    build_execution_queue,
    build_execution_plan,
    build_daily_operations_report,
    build_report_summary_text,
    render_report_pdf_bytes,
    save_pdf_to_disk,
    generate_and_send_daily_report,
)

st.set_page_config(page_title="Execution Agent", layout="wide")

role = require_role(["Admin", "Trader", "Quant"])
user = get_user()
user_email = user.get("email", "unknown") if user else "unknown"

st.title("⚙️ Execution Agent")
st.caption(f"User: {user_email} | Role: {role}")

tab1, tab2, tab3 = st.tabs(["Action Queue", "Execution Plan", "Routine Reports"])

with tab1:
    if st.button("Load Action Queue"):
        st.dataframe(build_execution_queue(), use_container_width=True)

with tab2:
    region = st.selectbox("Region", ["Mengxi", "All Provinces", "Inner Mongolia"])
    objective = st.text_area("Execution objective", height=140)
    if st.button("Generate Execution Plan"):
        st.code(build_execution_plan(region, objective))

with tab3:
    st.subheader("Daily Routine Report")

    if st.button("Preview Daily Report"):
        report = build_daily_operations_report()
        summary = build_report_summary_text(report)

        st.markdown("### Summary")
        st.text(summary)

        st.markdown("### Top Provinces")
        st.dataframe(report["top_provinces"], use_container_width=True)

        st.markdown("### Spread Monitor")
        st.dataframe(report["spread_monitor"], use_container_width=True)

        st.markdown("### Mengxi Performance")
        st.dataframe(report["mengxi"].head(30), use_container_width=True)

        pdf_bytes = render_report_pdf_bytes(report, summary)
        st.download_button(
            "Download PDF Report",
            data=pdf_bytes,
            file_name=f"execution_report_{report['generated_at']:%Y%m%d}.pdf",
            mime="application/pdf",
        )

    send_slack = st.checkbox("Send to Slack", value=True)
    send_email = st.checkbox("Send by Email", value=True)

    if st.button("Generate and Send Daily Report"):
        result = generate_and_send_daily_report(
            send_slack=send_slack,
            send_email=send_email,
        )
        st.success("Report sent successfully.")
        st.code(result["summary"])
        st.write("Saved PDF:", result["pdf_path"])