"""Tab 0 · Deal Intake — upload deal docs or manual entry → confirmed DealBrief."""
from __future__ import annotations

import os

import streamlit as st

from apps.deal_structurer import geo
from services.deal_committee.brief import DealBrief, extract_brief, low_confidence_fields
from services.deal_committee.intake_parser import SUPPORTED_EXTS, extract_text


def _node_widget(nodes: list[str], draft_node: str | None, province: str) -> str | None:
    """Province-dependent node picker; keyed by province so a province switch
    never leaves a stale value from the previous province's node list."""
    if not nodes:
        raw = st.text_input("节点(可选)", draft_node or "", key=f"intake_node_text_{province}")
        return raw.strip() or None
    options, idx, prefill = geo.node_select_state(nodes, draft_node)
    sel = st.selectbox("节点(可选)", options, index=idx, key=f"intake_node_sel_{province}")
    if sel == geo.NODE_MANUAL:
        raw = st.text_input("节点(手工输入)", prefill, key=f"intake_node_manual_{province}")
        return raw.strip() or None
    return None if sel == geo.NODE_NONE else sel


def _brief_form(draft: DealBrief) -> DealBrief | None:
    low = set(low_confidence_fields(draft))

    def _warn(field: str):
        if field in low:
            st.caption(f"⚠️ 提取置信度较低,请核对 {field}")

    # 省份/节点 sit outside the form: the node list depends on the selected
    # province, and form widgets only rerun on submit.
    loc1, loc2 = st.columns(2)
    with loc1:
        prov_opts, prov_idx = geo.province_options(geo.load_provinces(), draft.province)
        province = st.selectbox("省份", prov_opts, index=prov_idx, key="intake_province")
        _warn("province")
    with loc2:
        node = _node_widget(geo.load_nodes(province), draft.node, province)

    with st.form("deal_brief_form"):
        c1, c2 = st.columns(2)
        with c1:
            deal_name = st.text_input("项目名称", draft.deal_name); _warn("deal_name")
            asset_type = st.selectbox("资产类型",
                                      ["bess", "wind", "solar", "wind_bess", "solar_bess"],
                                      index=["bess", "wind", "solar", "wind_bess",
                                             "solar_bess"].index(draft.asset_type))
            capacity_mw = st.number_input("储能功率 (MW)", 0.0, 2000.0,
                                          float(draft.capacity_mw)); _warn("capacity_mw")
            capacity_mwh = st.number_input("储能容量 (MWh)", 0.0, 8000.0,
                                           float(draft.capacity_mwh)); _warn("capacity_mwh")
            efficiency = st.number_input("综合效率", 0.5, 1.0, float(draft.efficiency), 0.01)
            cycles = st.number_input("日均循环次数", 0.1, 4.0, float(draft.cycles_per_day), 0.1)
        with c2:
            installed_mw = st.number_input("新能源装机 (MW)", 0.0, 5000.0,
                                           float(draft.installed_mw)); _warn("installed_mw")
            capex_yi = st.number_input("总投资 (亿元)", 0.0, 200.0,
                                       (draft.capex_total_yuan or 0.0) / 1e8, 0.1)
            _warn("capex_total_yuan")
            commissioning = st.number_input("投运年份", 2024, 2035,
                                            int(draft.commissioning_year))
            tenor = st.number_input("项目期限 (年)", 1, 40, int(draft.tenor_years))
            counterparty = st.text_input("对手方", draft.counterparty)
            debt = st.number_input("负债率", 0.0, 0.95, float(draft.debt_ratio), 0.05)
            rate = st.number_input("贷款利率", 0.0, 0.30, float(draft.loan_rate), 0.005,
                                   format="%.3f")
            term = st.number_input("贷款期限 (年)", 1, 30, int(draft.loan_term_years))
        notes = st.text_area("交易结构要点", draft.structure_notes, height=80)
        submitted = st.form_submit_button("✅ 确认交易要素", type="primary",
                                          use_container_width=True)
    if not submitted:
        return None
    return DealBrief(
        deal_name=deal_name, asset_type=asset_type, province=province,
        node=node, capacity_mw=capacity_mw, capacity_mwh=capacity_mwh,
        efficiency=efficiency, cycles_per_day=cycles, installed_mw=installed_mw,
        capex_total_yuan=capex_yi * 1e8 or None, commissioning_year=int(commissioning),
        tenor_years=int(tenor), counterparty=counterparty, structure_notes=notes,
        debt_ratio=debt, loan_rate=rate, loan_term_years=int(term),
        field_confidence=draft.field_confidence, confirmed=True,
        source_files=draft.source_files,
    )


def _persist(brief: DealBrief) -> None:
    try:
        from services.common.db_utils import get_engine
        from services.deal_committee.library import save_brief
        st.session_state["deal_brief_id"] = save_brief(get_engine(), brief)
    except Exception as e:
        st.session_state["deal_brief_id"] = None
        st.warning(f"要素已保存在会话中,但写入数据库失败:{e}")


def render() -> None:
    st.header("0 · Deal Intake — 交易要素录入")
    st.caption("上传交易背景材料(docx / pptx / pdf / xlsx / txt),自动提取交易要素;"
               "确认后进入 6 · 投委会 生成投资建议书(DAF)。")

    from shared.anthropic_client import is_llm_available
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    llm_ok = is_llm_available(api_key)

    uploaded = st.file_uploader("交易背景材料(可多选)", type=list(SUPPORTED_EXTS),
                                accept_multiple_files=True)
    c1, c2 = st.columns(2)
    with c1:
        extract_btn = st.button("📄 解析文档并提取要素", disabled=not uploaded or not llm_ok,
                                use_container_width=True)
    with c2:
        manual_btn = st.button("✏️ 手工录入", use_container_width=True)
    if not llm_ok:
        st.warning("未检测到 LLM 配置(ANTHROPIC_API_KEY 或 BEDROCK_REGION)——文档提取不可用,可手工录入。")

    if extract_btn:
        texts, names = [], []
        for f in uploaded:
            try:
                texts.append(extract_text(f.getvalue(), f.name, api_key=api_key))
                names.append(f.name)
            except Exception as e:
                st.error(f"{f.name}:{e}")
        if texts:
            with st.spinner("正在提取交易要素…"):
                try:
                    draft = extract_brief("\n\n---\n\n".join(texts), names, api_key)
                    st.session_state["_draft_brief"] = draft
                except Exception as e:
                    st.error(f"要素提取失败:{e}")
    if manual_btn:
        st.session_state["_draft_brief"] = DealBrief()

    draft = st.session_state.get("_draft_brief")
    if draft is not None:
        st.divider()
        st.subheader("交易要素确认")
        brief = _brief_form(draft)
        if brief is not None:
            st.session_state["deal_brief"] = brief
            st.session_state["_draft_brief"] = None
            _persist(brief)
            st.success(f"交易要素已确认:{brief.deal_name or '(未命名)'} —— 请切换到 6 · 投委会")

    existing = st.session_state.get("deal_brief")
    if existing is not None and draft is None:
        st.info(f"当前已确认要素:**{existing.deal_name or '(未命名)'}** "
                f"({existing.province} · {existing.asset_type})。重新上传或手工录入可覆盖。")
