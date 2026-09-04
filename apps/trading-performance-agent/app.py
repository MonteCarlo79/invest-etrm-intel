"""
apps/trading-performance-agent/app.py

Operator-grade BESS Trading Performance Agent — Streamlit chat interface.

Provides two modes:
  1. Daily Review — one-click full analysis of all 4 IM assets with streaming output
  2. Chat — multi-turn operator Q&A backed by the full 17-tool decision model suite

Run:
    streamlit run apps/trading-performance-agent/app.py

Environment variables required:
    DB_DSN or PGURL     — PostgreSQL connection string
    ANTHROPIC_API_KEY   — Claude API key
"""
import datetime
import os
import sys
from pathlib import Path

import streamlit as st

# Project root on path
sys.path.append(str(Path(__file__).resolve().parents[2]))

# DB env var alias
_url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
if _url:
    os.environ.setdefault("DB_DSN", _url)
    os.environ.setdefault("PGURL", _url)

from auth.rbac import require_role, get_user  # noqa: E402
from libs.decision_models.adapters.agent.trading_performance_agent import (  # noqa: E402
    TradingPerformanceAgent,
)

# ---------------------------------------------------------------------------
# Page config and auth
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Trading Performance Agent",
    page_icon=":bar_chart:",
    layout="wide",
)

role = require_role(["Admin", "Trader", "Quant"])
user = get_user()
user_email = user.get("email", "unknown") if user else "unknown"

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

if "conversation_history" not in st.session_state:
    st.session_state.conversation_history = []

if "last_review_date" not in st.session_state:
    st.session_state.last_review_date = None

if "review_running" not in st.session_state:
    st.session_state.review_running = False

if "download_report_key" not in st.session_state:
    st.session_state.download_report_key = None

if "download_report_bytes" not in st.session_state:
    st.session_state.download_report_bytes = None

if "download_report_date" not in st.session_state:
    st.session_state.download_report_date = None

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Trading Performance Agent")
    st.caption(f"User: {user_email} | Role: {role}")
    st.divider()

    # Date selector
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    selected_date = st.date_input(
        "Analysis date",
        value=yesterday,
        max_value=datetime.date.today(),
        help="Date to analyse — should be a day for which ops ingestion has completed.",
    )
    date_str = selected_date.isoformat()

    # Asset scope
    asset_scope = st.selectbox(
        "Asset scope",
        ["All 4 assets", "suyou", "hangjinqi", "siziwangqi", "gushanliang"],
    )

    st.divider()

    # Daily review button
    run_review = st.button(
        "Run Daily Review",
        type="primary",
        use_container_width=True,
        help="Run the full daily strategy review for the selected date and assets",
    )

    # Email button — only shown once a review has been run
    if st.session_state.last_review_date:
        if st.button("Send Email Report", use_container_width=True):
            _send_email_report(date_str)

    st.divider()

    # Clear conversation
    if st.button("Clear conversation", use_container_width=True):
        st.session_state.conversation_history = []
        st.rerun()

    # API key check
    if not os.environ.get("ANTHROPIC_API_KEY"):
        st.error("ANTHROPIC_API_KEY not set")
    else:
        st.success("API key configured")

    # -----------------------------------------------------------------------
    # Report History
    # -----------------------------------------------------------------------
    st.divider()
    st.subheader("Report History")

    bucket = os.environ.get("UPLOADS_BUCKET_NAME", "")
    if not bucket:
        st.caption("UPLOADS_BUCKET_NAME not configured.")
    else:
        reports = _list_s3_reports(bucket)
        if not reports:
            st.caption("No reports saved yet.")
        else:
            for report in reports[:15]:
                c1, c2 = st.columns([3, 1])
                with c1:
                    st.write(report["date"])
                with c2:
                    if st.button("PDF", key=f"fetch_{report['date']}"):
                        st.session_state.download_report_key = report["key"]
                        st.session_state.download_report_date = report["date"]
                        st.session_state.download_report_bytes = _fetch_s3_report(
                            bucket, report["key"]
                        )

        if st.session_state.download_report_bytes:
            dl_date = st.session_state.download_report_date or "report"
            st.download_button(
                label=f"Save {dl_date}.pdf",
                data=st.session_state.download_report_bytes,
                file_name=f"trading_performance_{dl_date}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )

# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------

st.title("BESS Trading Performance Agent")
st.caption(
    "4 Inner Mongolia BESS assets: suyou · hangjinqi · siziwangqi · gushanliang  |  "
    "Powered by Claude claude-sonnet-4-6 with 17 decision model tools"
)

# Suggestion chips (only shown when chat is empty)
if not st.session_state.conversation_history:
    st.markdown("**Quick actions:**")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Review all assets today", use_container_width=True):
            _handle_user_message(
                f"Run the daily trading performance review for all 4 assets on {date_str}.",
                date_str,
            )
    with col2:
        if st.button("Which assets are on ALERT?", use_container_width=True):
            _handle_user_message(
                f"Check realization and fragility monitoring status for all 4 IM assets "
                f"as of {date_str}. List any ALERT or CRITICAL assets with their dominant loss buckets.",
                date_str,
            )
    with col3:
        if st.button("Suyou strategy breakdown", use_container_width=True):
            _handle_user_message(
                f"Show me the full strategy breakdown for suyou on {date_str}: "
                f"perfect foresight, forecast, nominated, and actual P&L with gaps.",
                date_str,
            )

# ---------------------------------------------------------------------------
# Render conversation history
# ---------------------------------------------------------------------------

for message in st.session_state.conversation_history:
    role_label = message["role"]
    content = message["content"]

    with st.chat_message(role_label):
        if isinstance(content, list):
            # Tool result blocks — show as collapsed expanders
            for block in content:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    with st.expander(f"Tool result (id: {block.get('tool_use_id', '?')[:8]}...)"):
                        st.code(str(block.get("content", ""))[:2000], language="json")
        else:
            st.markdown(str(content))

# ---------------------------------------------------------------------------
# Handle Daily Review button
# ---------------------------------------------------------------------------

if run_review:
    scope_label = asset_scope if asset_scope != "All 4 assets" else "all 4 assets"
    question = (
        f"Run the daily trading performance review for {scope_label} on {date_str}. "
        f"Follow the DAILY REVIEW PROTOCOL: load strategy analysis, check monitoring "
        f"status, and produce the full structured report."
    )
    _handle_user_message(question, date_str, is_daily_review=True)

# ---------------------------------------------------------------------------
# Chat input
# ---------------------------------------------------------------------------

if prompt := st.chat_input("Ask about trading performance..."):
    _handle_user_message(prompt, date_str)


# ---------------------------------------------------------------------------
# Helper functions (defined after their first reference — Streamlit re-runs
# the full script so forward references work at call time)
# ---------------------------------------------------------------------------

def _handle_user_message(
    question: str,
    date: str,
    is_daily_review: bool = False,
) -> None:
    """Append user message, stream agent response, update history."""
    # Show user message
    with st.chat_message("user"):
        st.markdown(question)

    # Append to history
    user_content = question
    if not st.session_state.conversation_history:
        user_content = f"Context: Reviewing BESS trading performance for date {date}.\n\n{question}"

    st.session_state.conversation_history.append(
        {"role": "user", "content": question}
    )

    # Stream agent response
    agent = TradingPerformanceAgent()

    history_for_agent = []
    # Rebuild agent-compatible history (exclude tool-result display blocks)
    for msg in st.session_state.conversation_history[:-1]:
        if isinstance(msg["content"], str):
            history_for_agent.append({"role": msg["role"], "content": msg["content"]})

    # Add the new user message with date context
    history_for_agent.append({"role": "user", "content": user_content})

    with st.chat_message("assistant"):
        response_text = st.write_stream(
            agent.stream_query(
                question=question,
                date=date,
                conversation_history=history_for_agent[:-1] if history_for_agent else None,
            )
        )

    # Append assistant response to display history
    st.session_state.conversation_history.append(
        {"role": "assistant", "content": response_text}
    )

    if is_daily_review:
        st.session_state.last_review_date = date

    st.rerun()


def _list_s3_reports(bucket: str) -> list:
    """Return list of historical reports from S3 sorted newest first.

    Each entry: {"date": "2026-04-17", "key": "trading-performance/...", "size": int}
    """
    try:
        import boto3

        s3 = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="trading-performance/")
        items = []
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]
            # filename pattern: trading_performance_YYYYMMDD.pdf
            if filename.startswith("trading_performance_") and filename.endswith(".pdf"):
                raw = filename[len("trading_performance_"):-4]  # "20260417"
                if len(raw) == 8 and raw.isdigit():
                    display_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
                else:
                    display_date = filename
                items.append({"date": display_date, "key": key, "size": obj["Size"]})
        items.sort(key=lambda x: x["date"], reverse=True)
        return items
    except Exception:
        return []


def _fetch_s3_report(bucket: str, key: str) -> bytes | None:
    """Download a single report PDF from S3. Returns bytes or None on error."""
    try:
        import boto3

        s3 = boto3.client("s3")
        resp = s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except Exception:
        return None


def _send_email_report(date: str) -> None:
    """Send the last review narrative as an email report."""
    # Find the last assistant message as the narrative
    narrative = ""
    for msg in reversed(st.session_state.conversation_history):
        if msg["role"] == "assistant" and isinstance(msg["content"], str):
            narrative = msg["content"]
            break

    if not narrative:
        st.warning("No review narrative found — run a Daily Review first.")
        return

    try:
        from shared.agents.execution_agent import send_email_report
        from services.ops.run_trading_agent import _build_portfolio_pdf

        pdf_bytes = _build_portfolio_pdf(narrative, date)
        filename = f"trading_performance_{date.replace('-', '')}.pdf"
        subject = f"BESS Trading Performance — {date}"
        send_email_report(
            subject=subject,
            body=narrative,
            pdf_bytes=pdf_bytes,
            filename=filename,
        )
        st.success(f"Email sent: {subject}")
    except Exception as exc:
        st.error(f"Email failed: {exc}")
