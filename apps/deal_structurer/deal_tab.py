"""Tab Deal Pricing: structure selector, params, payout distribution.

Revenue distribution source (post-consolidation contract): the Option Value
tab's ``ov_result["revenue_paths_yuan"]`` — absolute ¥ per simulated year,
scaled to ``ov_result["power_mw"]``. This tab's own power_mw input re-scales
the paths (× dp_power / ov_power) when the deal size differs from the valued
cell. Snapshot-sourced cells carry no paths (None) — the on-demand recompute
on the Option Value tab is the only producer.
"""
from __future__ import annotations
import numpy as np
import plotly.graph_objects as go
import streamlit as st
import libs.deal_models.deal_structures as _ds_mod  # triggers registration
from libs.deal_models.registry import list_structures, get
from libs.deal_models.deal_structures import price_structure
from apps.deal_structurer import session_cache


def render() -> None:
    st.header("Deal Pricing")
    ov = st.session_state.get("ov_result")
    if ov is None:
        st.warning("请先在 **期权价值** 页选择一个单元（省份 × 资产类型）— "
                   "Deal Pricing 的收入分布取自该单元的价值结果。")
        return

    paths_raw = ov.get("revenue_paths_yuan")
    if paths_raw is None:
        st.info(
            f"当前单元（{ov.get('province')} · {ov.get('asset_type')} · "
            f"快照 {ov.get('asof_date')}）来自快照表，不含收入路径分布。"
            "请在 **期权价值** 页运行 **「🔄 重算该单元」** 生成路径分布后返回本页。"
        )
        return

    ov_power = float(ov.get("power_mw") or 100.0)

    col1, col2 = st.columns([1, 2])
    with col1:
        power_mw = st.number_input(
            "交易资产功率 power (MW)", 1.0, 2000.0, ov_power, key="dp_power_mw",
        )
        scale = float(power_mw) / ov_power
        revenue_paths = np.asarray(paths_raw, dtype=float) * scale
        p50_rev = float(np.percentile(revenue_paths, 50))
        if scale != 1.0:
            st.caption(
                f"路径分布由期权价值页的 {ov_power:,.0f} MW 按 ×{scale:.3f} "
                f"线性缩放至 {power_mw:,.0f} MW（收入与功率近似线性）。"
            )

        structure = st.selectbox("Deal Structure", list_structures(), key="dp_struct")

        spec = get(structure)
        st.caption(spec.description)
        st.subheader("Parameters")

        param_vals = {}
        for field_name, field_info in spec.params_schema.model_fields.items():
            if "yuan" in field_name and "price" not in field_name:
                default = p50_rev
            elif "price" in field_name:
                default = 300.0
            else:
                default = 1e5
            label = field_name.replace("_", " ").title()
            val = st.number_input(label, value=float(default), key=f"dp_{field_name}")
            param_vals[field_name] = val

        price_btn = st.button("▶ Price Structure", type="primary", key="dp_price")

    with col2:
        if price_btn:
            try:
                params = spec.params_schema(**param_vals)
                result = price_structure(structure, revenue_paths, params)
                st.session_state["dp_result"] = result
                session_cache.save(st.session_state)
            except Exception as e:
                st.error(f"Pricing failed: {e}")

        dp = st.session_state.get("dp_result")
        if dp is not None:
            m = st.columns(2)
            m[0].metric("Expected Cost", f"¥{dp.expected_cost/1e6:.2f}M/yr")
            m[1].metric("P95 Cost", f"¥{dp.p95_cost/1e6:.2f}M/yr")
            m2 = st.columns(2)
            m2[0].metric("Min Premium", f"¥{dp.min_premium/1e6:.2f}M/yr")
            m2[1].metric("Suggested Premium", f"¥{dp.suggested_premium/1e6:.2f}M/yr",
                         delta=f"+¥{(dp.suggested_premium-dp.min_premium)/1e6:.2f}M risk charge")

            fig = go.Figure()
            fig.add_trace(go.Histogram(x=dp.payout_paths / 1e6, nbinsx=40, marker_color="rgb(239,85,59)", name="Payout"))
            fig.add_vline(x=dp.expected_cost / 1e6, line_dash="dash", line_color="blue", annotation_text="Expected")
            fig.add_vline(x=dp.p95_cost / 1e6, line_dash="dot", line_color="red", annotation_text="P95")
            fig.update_layout(title="Payout Distribution", xaxis_title="Payout (¥M)", height=380)
            st.plotly_chart(fig, use_container_width=True)

            first_param = list(param_vals.values())[0]
            st.info(
                f"**Pricing recommendation:** Floor at ¥{first_param/1e6:.1f}M/yr — "
                f"expected cost ¥{dp.expected_cost/1e6:.2f}M/yr · suggest charging ¥{dp.suggested_premium/1e6:.2f}M/yr"
            )
        else:
            st.info("Configure structure parameters and click **▶ Price Structure**.")
