"""Tab 6 · 投委会 — run committee analysis, synthesize, generate DAF PDF."""
from __future__ import annotations

import os

import streamlit as st

from services.deal_committee.orchestrator import (
    CommitteeResult, default_query_fn, run_committee, run_single_section,
)


def _api_key() -> str:
    return os.environ.get("ANTHROPIC_API_KEY", "")


def _rerun_section(key: str) -> None:
    result: CommitteeResult = st.session_state["committee_result"]
    with st.spinner("重新生成中…"):
        sec, econ = run_single_section(key, result.brief, default_query_fn, _api_key())
    for i, s in enumerate(result.sections):
        if s.key == key:
            result.sections[i] = sec
    if econ is not None:
        result.economics = econ
    result.synthesis = ""
    result.recommendation = ""


def render() -> None:
    st.header("6 · 投委会 — 投资决策建议书 (DAF)")
    brief = st.session_state.get("deal_brief")
    if brief is None or not brief.confirmed:
        st.warning("请先在 **0 · Deal Intake** 确认交易要素。")
        return

    st.caption(f"项目:**{brief.deal_name or '(未命名)'}** · {brief.province} · "
               f"{brief.asset_type} · {brief.capacity_mw:g}MW/{brief.capacity_mwh:g}MWh")

    if st.button("▶ 运行投委会分析", type="primary"):
        result = CommitteeResult(brief=brief, sections=[])
        with st.status("投委会分析运行中…", expanded=True) as status:
            def _done(sec):
                icon = "✅" if sec.status == "ok" else "❌"
                st.write(f"{icon} {sec.title}")
            result = run_committee(brief, api_key=_api_key(), on_section_done=_done)
            status.update(label="分析完成", state="complete")
        st.session_state["committee_result"] = result

    result: CommitteeResult | None = st.session_state.get("committee_result")
    if result is None:
        st.info("点击 **▶ 运行投委会分析** 开始。各章节将依次调用市场/量化/运营代理。")
        return

    for sec in result.sections:
        icon = "✅" if sec.status == "ok" else "❌"
        with st.expander(f"{icon} {sec.title}", expanded=False):
            if sec.status == "ok":
                st.markdown(sec.markdown)
            else:
                st.error(sec.error)
            if st.button("↻ 重新生成", key=f"rerun_{sec.key}"):
                _rerun_section(sec.key)
                st.rerun()

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        synth_btn = st.button("🧠 生成综合意见", use_container_width=True,
                              disabled=not any(s.status == "ok" for s in result.sections))
    with c2:
        pdf_btn = st.button("📄 生成并保存 DAF PDF", use_container_width=True,
                            disabled=not result.synthesis)

    if synth_btn:
        from services.deal_committee.synthesis import run_synthesis
        with st.spinner("综合意见生成中…"):
            try:
                result.synthesis, result.recommendation = run_synthesis(
                    result.brief, result.sections, result.economics, _api_key())
            except Exception as e:
                st.error(f"综合意见生成失败:{e}")
    if result.synthesis:
        if result.recommendation:
            st.metric("投资结论", result.recommendation)
        st.markdown(result.synthesis)

    if pdf_btn and result.synthesis:
        from services.deal_committee.daf_builder import build_daf
        from services.deal_committee.library import save_daf
        try:
            pdf = build_daf(result)
            st.session_state["_daf_pdf"] = pdf
            fname = f"DAF_{result.brief.deal_name or 'deal'}_{result.brief.province}.pdf"
            try:
                from services.common.db_utils import get_engine
                save_daf(get_engine(), st.session_state.get("deal_brief_id"),
                         result.brief, pdf, fname, result.recommendation)
                st.success("DAF 已保存到报告库")
            except Exception as e:
                st.warning(f"PDF 已生成,但保存到数据库失败:{e}")
        except Exception as e:
            st.error(f"PDF 生成失败:{e}")

    pdf = st.session_state.get("_daf_pdf")
    if pdf:
        st.download_button("⬇ 下载 DAF PDF", pdf,
                           file_name=f"DAF_{result.brief.deal_name or 'deal'}.pdf",
                           mime="application/pdf", use_container_width=True)

    st.divider()
    with st.expander("📚 历史 DAF"):
        try:
            from services.common.db_utils import get_engine
            from services.deal_committee.library import list_dafs, load_daf
            rows = list_dafs(get_engine())
            if not rows:
                st.caption("暂无历史 DAF。")
            for r in rows:
                c1, c2 = st.columns([4, 1])
                c1.write(f"**{r['deal_name']}** · {r['created_at'][:10]} · "
                         f"{r['recommendation'] or '—'} · {r['file_size_kb']} KB")
                if c2.button("下载", key=f"daf_{r['id']}"):
                    data, fname = load_daf(get_engine(), r["id"])
                    st.download_button("⬇", data, file_name=fname,
                                       mime="application/pdf", key=f"dl_{r['id']}")
        except Exception as e:
            st.caption(f"报告库不可用:{e}")
