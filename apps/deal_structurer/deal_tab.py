"""Tab 5 — Deal Pricing: structure selector, params, payout distribution."""
from __future__ import annotations
import numpy as np
import plotly.graph_objects as go
import streamlit as st
import libs.deal_models.deal_structures as _ds_mod  # triggers registration
from libs.deal_models.registry import list_structures, get
from libs.deal_models.deal_structures import price_structure
from apps.deal_structurer import session_cache


def render() -> None:
    st.header("5 · Deal Pricing")
    mc = st.session_state.get("mc_result")
    if mc is None:
        st.warning("Run **Tab 4 · Monte Carlo** first to get revenue paths.")
        return

    revenue_paths = mc.revenue_paths
    p50_rev = mc.revenue_p50

    col1, col2 = st.columns([1, 2])
    with col1:
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
