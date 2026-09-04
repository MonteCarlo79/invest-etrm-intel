"""
Strategy Agent — v2
=====================
Full-stack strategy analyst with:
  - Knowledge pool intelligence (4,000+ synthesized docs)
  - Expert memory from validated prior sessions
  - Policy timeline (temporal regulatory context)
  - Quantitative market data (IRR, spreads, capacity mix)

Run:
    py -m streamlit run apps/strategy-agent/app.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv
for _env in [_REPO / "config" / ".env", _REPO / ".env"]:
    if _env.exists():
        load_dotenv(_env)
        break

from auth.rbac import require_role, get_user
from shared.agents.logging_utils import ensure_agent_log_table, log_agent_request

st.set_page_config(page_title="Strategy Agent", layout="wide")

role = require_role(["Admin", "Trader", "Quant", "Analyst"])
user = get_user()
user_email = user.get("email", "unknown") if user else "unknown"

ensure_agent_log_table()

st.title("Strategy Agent")
st.caption(f"User: {user_email} | Role: {role} | Knowledge-aware v2")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_chat, tab_dashboard, tab_policy, tab_memory = st.tabs([
    "Chat", "Market Dashboard", "Policy Timeline", "Expert Memory"
])

# ── Chat tab ─────────────────────────────────────────────────────────────────
with tab_chat:
    st.markdown(
        "Ask strategy questions. The agent has access to ~4,000 synthesized "
        "policy documents, market data, and accumulated expert insights."
    )

    # Chat history
    if "strategy_messages" not in st.session_state:
        st.session_state.strategy_messages = []

    for msg in st.session_state.strategy_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ask a strategy question..."):
        st.session_state.strategy_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Retrieving knowledge and reasoning..."):
                try:
                    from shared.agents.strategy_agent import run_strategy_agent
                    response = run_strategy_agent(
                        user_prompt=prompt,
                        api_key=os.environ.get("ANTHROPIC_API_KEY"),
                        app="shared",
                        use_knowledge=True,
                        use_memory=True,
                    )
                    st.markdown(response)
                    st.session_state.strategy_messages.append(
                        {"role": "assistant", "content": response}
                    )
                    log_agent_request(user_email, "Strategy Agent v2", prompt, status="ok")

                    # Log conversation for expert memory extraction
                    try:
                        from services.knowledge_pool.knowledge_docs import log_conversation_turn
                        log_conversation_turn(prompt, response)
                    except Exception:
                        pass

                except Exception as exc:
                    st.error(f"Agent error: {exc}")

    if st.session_state.strategy_messages:
        col1, col2 = st.columns([1, 5])
        with col1:
            if st.button("Clear chat"):
                st.session_state.strategy_messages = []
                st.rerun()
        with col2:
            if st.button("Extract insights from this session"):
                api_key = os.environ.get("ANTHROPIC_API_KEY", "")
                if api_key:
                    from services.knowledge_pool.expert_memory import extract_and_store_insights
                    n = extract_and_store_insights(api_key=api_key)
                    st.success(f"Extracted {n} expert insight(s) from this session.")
                else:
                    st.warning("ANTHROPIC_API_KEY not set.")

# ── Market Dashboard tab ──────────────────────────────────────────────────────
with tab_dashboard:
    if st.button("Load Market Summary"):
        with st.spinner("Loading..."):
            try:
                from shared.agents.strategy_agent import build_strategy_summary
                summary = build_strategy_summary()

                st.markdown("### Top Provinces by BESS IRR")
                st.dataframe(summary["top_provinces"], use_container_width=True)

                st.markdown("### DA-RT Spread Statistics (180d)")
                st.dataframe(summary["spread_stats"], use_container_width=True)

                st.markdown("### Structural Capacity Bias")
                cap = summary["capacity_bias"]
                cols = [c for c in [
                    "province", "solar_ratio", "wind_ratio",
                    "thermal_ratio", "structural_spread_bias"
                ] if c in cap.columns]
                st.dataframe(cap[cols], use_container_width=True)

                st.markdown("### Mengxi Rank Delta (30d)")
                st.dataframe(summary["mengxi_rank_delta"], use_container_width=True)
            except Exception as exc:
                st.error(f"Data load failed: {exc}")

# ── Policy Timeline tab ───────────────────────────────────────────────────────
with tab_policy:
    st.markdown("Query the regulatory timeline — what rules were active on a given date.")

    col1, col2 = st.columns(2)
    with col1:
        province_filter = st.text_input("Province (optional)", placeholder="e.g. Shanxi")
        policy_type_filter = st.selectbox(
            "Policy type",
            ["All", "market_rule", "regulation", "notice", "standard", "guideline"],
        )
    with col2:
        date_filter = st.date_input("Active on date (optional)")
        include_superseded = st.checkbox("Include superseded policies")

    if st.button("Query Policy Timeline"):
        with st.spinner("Querying..."):
            try:
                from services.knowledge_pool.knowledge_graph import query_policy_timeline
                results = query_policy_timeline(
                    province=province_filter or None,
                    effective_on=str(date_filter) if date_filter else None,
                    policy_type=policy_type_filter if policy_type_filter != "All" else None,
                    include_superseded=include_superseded,
                    limit=30,
                )
                if results:
                    import pandas as pd
                    df = pd.DataFrame(results)
                    display_cols = [c for c in [
                        "policy_name", "policy_type", "province", "issuing_body",
                        "effective_date", "superseded_date", "bess_relevance"
                    ] if c in df.columns]
                    st.dataframe(df[display_cols], use_container_width=True)

                    # Expandable detail view
                    for r in results[:5]:
                        with st.expander(f"{r['policy_name']} ({r['effective_date']})"):
                            changes = r.get("key_changes") or []
                            if changes:
                                st.markdown("**Key changes:**")
                                for c in changes:
                                    st.markdown(f"- {c}")
                            if r.get("bess_relevance"):
                                st.markdown(f"**BESS relevance:** {r['bess_relevance']}")
                else:
                    st.info("No policies found for the given filters.")
            except Exception as exc:
                st.error(f"Policy query failed: {exc}")

    st.divider()
    st.markdown("**Knowledge synthesis status**")
    if st.button("Show synthesis progress"):
        try:
            from services.knowledge_pool.db import get_conn
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            d.app,
                            COUNT(d.id) AS total_docs,
                            COUNT(s.doc_id) AS synthesized,
                            COUNT(pt.doc_id) AS in_timeline
                        FROM staging.spot_knowledge_docs d
                        LEFT JOIN staging.kp_doc_summaries s ON s.doc_id = d.id
                        LEFT JOIN staging.kp_policy_timeline pt ON pt.doc_id = d.id
                        WHERE d.active = TRUE
                        GROUP BY d.app
                        """
                    )
                    rows = cur.fetchall()
                    if rows:
                        import pandas as pd
                        df = pd.DataFrame(rows, columns=["app", "total_docs", "synthesized", "in_timeline"])
                        df["synthesis_%"] = (df["synthesized"] / df["total_docs"] * 100).round(1)
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.info("Run the synthesis pipeline first.")
        except Exception as exc:
            st.error(f"Status query failed: {exc}")

# ── Expert Memory tab ─────────────────────────────────────────────────────────
with tab_memory:
    st.markdown(
        "Accumulated expert insights extracted from validated agent sessions. "
        "These are automatically injected as context into every agent query."
    )

    col1, col2 = st.columns(2)
    with col1:
        memory_query = st.text_input("Search insights", placeholder="e.g. ancillary market Shanxi")
        memory_province = st.text_input("Filter by province", placeholder="optional")
    with col2:
        memory_type = st.selectbox(
            "Insight type",
            ["All", "market_structure", "price_driver", "regulation",
             "risk", "opportunity", "dispatch_economics", "investment", "operations"],
        )
        min_conf = st.selectbox("Min confidence", ["medium", "high", "low"])

    if st.button("Search Expert Memory"):
        try:
            from services.knowledge_pool.expert_memory import get_relevant_insights
            results = get_relevant_insights(
                query=memory_query or "market",
                province=memory_province or None,
                insight_type=memory_type if memory_type != "All" else None,
                min_confidence=min_conf,
                limit=20,
            )
            if results:
                import pandas as pd
                df = pd.DataFrame(results)
                display_cols = [c for c in [
                    "insight_type", "province", "confidence",
                    "source_session", "insight_text"
                ] if c in df.columns]
                st.dataframe(df[display_cols], use_container_width=True)
            else:
                st.info("No insights found. Run strategy sessions to accumulate memory.")
        except Exception as exc:
            st.error(f"Memory search failed: {exc}")

    st.divider()
    try:
        from services.knowledge_pool.expert_memory import get_memory_stats
        stats = get_memory_stats()
        if stats.get("total"):
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Insights", stats.get("total", 0))
            col2.metric("High Confidence", stats.get("high_conf", 0))
            col3.metric("Insight Types", stats.get("type_count", 0))
            col4.metric("Provinces Covered", stats.get("province_count", 0))
    except Exception:
        pass
