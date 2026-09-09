"""Strategist agent — Claude tool-use chat with session persistence."""
from __future__ import annotations
import os
import streamlit as st
from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available
from libs.deal_models.adapters.agent_tools import AGENT_TOOLS, dispatch_tool

_SYSTEM = """你是 Structurer — 电力资产交易结构化与投委会分析助手,服务中国电力现货市场。

## 铁律(grounding)
数字只能来自 DB 读取(list_deals/get_deal_result)或工具重算(rerun_analysis/金融模型工具)。
禁止凭记忆给出任何价格、收益、IRR、容量补偿费率。不知道就说没有数据。

## 工具使用
- 用户上传的文档(投委会 DAF、可研报告等)用 read_uploaded_doc 按页或关键词读取。
- 质疑/挑战流程:先 get_deal_result 读当前口径 → update_deal_parameters 改参数(白名单)
  → rerun_analysis("economics") 重算(默认;章节级用 "section:<key>",综合意见用 "synthesis",
  PDF 用 "daf")。全量 7 章重算("full")必须先请用户明确说「确认」,再以 confirmed=true 调用。
- 每次修订必须在回复中引用 原口径 → 新口径 数字对比。
- 中文回答,金额用 ¥M/年 或 亿元 表达。
"""

_TOOL_ICONS = {
    "run_price_simulation": "📈",
    "run_dispatch_valuation": "⚡",
    "run_project_cashflow": "💰",
    "run_monte_carlo": "🎲",
    "price_deal_structure": "🤝",
}


def _run_agent_turn(messages: list, text_ph) -> tuple[str, list]:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not is_llm_available(api_key):
        return "No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION).", messages

    client = _make_anthropic_client(api_key)
    status_ph = st.empty()

    while True:
        streamed = ""
        status_ph.caption("⏳ Thinking…")
        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=_SYSTEM,
            tools=AGENT_TOOLS,
            messages=messages,
        ) as stream:
            for chunk in stream.text_stream:
                streamed += chunk
                status_ph.empty()
                text_ph.markdown(streamed + "▌")
            final = stream.get_final_message()

        messages = messages + [{"role": "assistant", "content": final.content}]

        if final.stop_reason == "end_turn":
            status_ph.empty()
            text_ph.markdown(streamed)
            return streamed, messages

        if final.stop_reason != "tool_use":
            status_ph.empty()
            return f"Unexpected stop: {final.stop_reason}", messages

        tool_results = []
        for block in final.content:
            if block.type == "tool_use":
                icon = _TOOL_ICONS.get(block.name, "⚙️")
                status_ph.caption(f"{icon} Calling `{block.name}`…")
                result_str = dispatch_tool(block.name, block.input)
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_str})

        status_ph.empty()
        messages = messages + [{"role": "user", "content": tool_results}]


def render() -> None:
    st.header("💬 Structurer")
    st.caption("上传投委会文档提问,挑战各 tab 结果,改参重算并生成修订版 DAF。")

    from services.deal_structurer import structurer_agent as _sa

    uploads = st.file_uploader(
        "上传文档(投委会 DAF / 可研 / 条款,可多选)",
        type=["pdf", "pptx", "docx", "xlsx", "xls", "txt", "png", "jpg", "jpeg", "webp"],
        accept_multiple_files=True, key="structurer_uploads")
    if uploads:
        from services.deal_committee.intake_parser import extract_text
        ingested = st.session_state.setdefault("_structurer_ingested", set())
        new_docs = []
        for f in uploads:
            fp = (f.name, f.size)
            if fp in ingested:
                continue
            try:
                text = extract_text(f.getvalue(), f.name,
                                    api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
                _sa.add_uploaded_doc(f.name, text)
                ingested.add(fp)
                new_docs.append(f.name)
            except Exception as e:
                st.error(f"{f.name}: {e}")
        if new_docs:
            st.success(f"已载入:{', '.join(_sa.list_uploaded_docs())}")

    revisions = _sa.get_session_revisions()
    if revisions:
        with st.expander(f"📝 本次修订记录 ({len(revisions)})", expanded=True):
            for r in revisions:
                st.markdown(f"- result `#{r['result_id']}` — {r['label']}")

    if "agent_messages" not in st.session_state:
        st.session_state["agent_messages"] = []
    if "agent_display" not in st.session_state:
        st.session_state["agent_display"] = [{"role": "assistant", "content": (
            "你好!我可以:\n"
            "- 读取上传的投委会文档并回答细节问题\n"
            "- 针对 tab 1-6 的结果接受挑战(如「容量补偿降到 280 重算」)\n"
            "- 改参重算并生成修订版 DAF(历史库中留痕)\n"
        )}]

    if st.button("🗑 Clear Chat", key="strat_clear"):
        st.session_state["agent_messages"] = []
        st.session_state["agent_display"] = []
        st.rerun()

    for msg in st.session_state["agent_display"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    user_input = st.chat_input("Ask the Strategist…")
    if user_input:
        st.session_state["agent_display"].append({"role": "user", "content": user_input})
        st.session_state["agent_messages"].append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            text_ph = st.empty()
            reply, new_msgs = _run_agent_turn(st.session_state["agent_messages"], text_ph)
            st.session_state["agent_messages"] = new_msgs
            st.session_state["agent_display"].append({"role": "assistant", "content": reply})
