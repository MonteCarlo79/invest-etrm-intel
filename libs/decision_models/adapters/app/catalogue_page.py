"""
libs/decision_models/adapters/app/catalogue_page.py

Reusable Streamlit component: decision model catalogue.

Reads exclusively from registry.summarize() — no hardcoded model descriptions.
Each registered model is rendered as a card showing all standard metadata keys.

Usage (embed in any Streamlit app):
    from libs.decision_models.adapters.app.catalogue_page import render_catalogue_page
    render_catalogue_page()

Standalone:
    streamlit run libs/decision_models/adapters/app/catalogue_app.py

Data layer (no Streamlit — usable in tests or agents):
    from libs.decision_models.adapters.app.catalogue_page import load_catalogue
    models = load_catalogue()   # list of describe_model() dicts
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# All model modules to register at load time.
# Add new model modules here when extending the library.
# ---------------------------------------------------------------------------
_MODEL_MODULES = [
    "libs.decision_models.bess_dispatch_optimization",
    "libs.decision_models.bess_dispatch_simulation_multiday",
    "libs.decision_models.price_forecast_dayahead",
    "libs.decision_models.revenue_scenario_engine",
]

_CATEGORY_ORDER = ["optimization", "simulation", "forecast", "analytics"]
_STATUS_ORDER = ["production", "experimental"]

# Category → short label for display
_CATEGORY_LABELS = {
    "optimization": "Optimization",
    "simulation":   "Simulation",
    "forecast":     "Forecast",
    "analytics":    "Analytics",
}


# ---------------------------------------------------------------------------
# Data layer (no Streamlit)
# ---------------------------------------------------------------------------

def load_catalogue() -> List[Dict[str, Any]]:
    """
    Import all registered model modules (idempotent — Python module cache
    prevents re-registration), then return registry.summarize().

    Safe to call multiple times. Returns models sorted by name.
    """
    import importlib
    for mod in _MODEL_MODULES:
        importlib.import_module(mod)
    from libs.decision_models.registry import registry
    return registry.summarize()


def apply_filters(
    models: List[Dict[str, Any]],
    categories: List[str],
    markets: List[str],
    statuses: List[str],
) -> List[Dict[str, Any]]:
    """
    Filter a list of describe_model() dicts by category, market, and status.

    ``markets`` may contain "—" to match models where market is None.
    Empty lists mean "no filter applied" (return all).
    """
    out = models
    if categories:
        out = [m for m in out if m["metadata"].get("category") in categories]
    if markets:
        selected = {m if m != "—" else None for m in markets}
        out = [m for m in out if m["metadata"].get("market") in selected]
    if statuses:
        out = [m for m in out if m["metadata"].get("status") in statuses]
    return out


def fmt(value: Any, none_label: str = "—") -> str:
    """Format a metadata value for display in the catalogue."""
    if value is None or value == "":
        return none_label
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    return str(value)


# ---------------------------------------------------------------------------
# Card renderer
# ---------------------------------------------------------------------------

def _render_model_card(desc: Dict[str, Any]) -> None:
    """Render one model as a bordered card. Must be called inside a Streamlit app."""
    import streamlit as st

    md = desc["metadata"]

    with st.container(border=True):
        # --- Header ---
        h_left, h_right = st.columns([5, 1])
        with h_left:
            st.markdown(f"**{desc['name']}**")
        with h_right:
            st.caption(f"v{desc['version']}")

        # Short description
        description = desc.get("description", "")
        if description:
            # Trim to first sentence for card display
            first_sentence = description.split(".")[0] + "."
            st.caption(first_sentence)

        st.divider()

        # --- Metadata grid (two columns) ---
        col_l, col_r = st.columns(2)

        with col_l:
            st.markdown(f"**Category** &nbsp; {_CATEGORY_LABELS.get(md.get('category', ''), fmt(md.get('category')))}")
            st.markdown(f"**Scope** &nbsp; `{fmt(md.get('scope'))}`")
            st.markdown(f"**Granularity** &nbsp; `{fmt(md.get('granularity'))}`")
            st.markdown(f"**Horizon** &nbsp; `{fmt(md.get('horizon'))}`")
            st.markdown(f"**Deterministic** &nbsp; {fmt(md.get('deterministic'))}")

        with col_r:
            st.markdown(f"**Model family** &nbsp; `{fmt(md.get('model_family'))}`")
            st.markdown(f"**Market** &nbsp; {fmt(md.get('market'))}")
            st.markdown(f"**Asset type** &nbsp; {fmt(md.get('asset_type'))}")
            st.markdown(f"**Status** &nbsp; {fmt(md.get('status'))}")
            st.markdown(f"**Owner** &nbsp; {fmt(md.get('owner'))}")

        # --- Source ---
        st.markdown(
            f"**Source** &nbsp; `{fmt(md.get('source_of_truth_module'))}`"
        )
        fns = md.get("source_of_truth_functions") or []
        if fns:
            st.markdown(f"**Functions** &nbsp; `{', '.join(fns)}`")

        # --- Fallback ---
        fallback = md.get("fallback_behavior")
        if fallback:
            st.markdown(f"**Fallback** &nbsp; {fallback}")

        # --- Limitations expander ---
        limitations = md.get("limitations") or []
        if limitations:
            with st.expander(f"Limitations ({len(limitations)})"):
                for lim in limitations:
                    st.markdown(f"- {lim}")

        # --- Assumptions expander ---
        assumptions = md.get("assumptions")
        if assumptions:
            with st.expander("Assumptions"):
                _render_assumptions(assumptions)

        # --- Tags ---
        tags = desc.get("tags") or []
        if tags:
            st.caption("Tags: " + " · ".join(tags))


def _render_assumptions(assumptions: Any) -> None:
    """Render an assumptions value (dict or list) inside an expander."""
    import streamlit as st

    if isinstance(assumptions, list):
        for item in assumptions:
            st.markdown(f"- {item}")
        return

    if isinstance(assumptions, dict):
        for key, value in assumptions.items():
            if key == "limitations":
                continue  # already shown in the Limitations expander
            if isinstance(value, dict):
                st.markdown(f"**{key}**")
                for k2, v2 in value.items():
                    st.markdown(f"&nbsp;&nbsp;- **{k2}**: {v2}")
            elif isinstance(value, list):
                st.markdown(f"**{key}**")
                for item in value:
                    st.markdown(f"&nbsp;&nbsp;- {item}")
            else:
                st.markdown(f"**{key}** &nbsp; {value}")
        return

    # Fallback: plain string
    st.write(str(assumptions))


# ---------------------------------------------------------------------------
# Page entry point
# ---------------------------------------------------------------------------

def render_catalogue_page() -> None:
    """
    Render the full model catalogue page.

    Embed in any Streamlit app:
        from libs.decision_models.adapters.app.catalogue_page import render_catalogue_page
        render_catalogue_page()

    Run standalone:
        streamlit run libs/decision_models/adapters/app/catalogue_app.py
    """
    import streamlit as st

    st.header("Decision Model Catalogue")
    st.caption(
        "All models registered in libs/decision_models. "
        "Data source: registry.summarize() — no hardcoded content. "
        "See libs/decision_models/OVERVIEW.md for the full standard."
    )

    # --- Load ---
    try:
        all_models = load_catalogue()
    except Exception as exc:
        st.error(f"Failed to load model catalogue: {exc}")
        raise

    # --- Summary metrics ---
    n_categories = len({m["metadata"].get("category") for m in all_models if m["metadata"].get("category")})
    n_markets = len({m["metadata"].get("market") for m in all_models if m["metadata"].get("market") is not None})
    n_production = sum(1 for m in all_models if m["metadata"].get("status") == "production")

    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    col_m1.metric("Registered models", len(all_models))
    col_m2.metric("Categories", n_categories)
    col_m3.metric("Markets", n_markets)
    col_m4.metric("Production", n_production)

    st.divider()

    # --- Filter controls (collapsed by default — only 4 models now) ---
    markets_available = sorted(
        {(m["metadata"].get("market") or "—") for m in all_models}
    )

    with st.expander("Filters", expanded=False):
        fc1, fc2, fc3 = st.columns(3)
        sel_categories = fc1.multiselect(
            "Category",
            options=_CATEGORY_ORDER,
            default=[],
            placeholder="All categories",
            key="_catalogue_filter_category",
        )
        sel_markets = fc2.multiselect(
            "Market",
            options=markets_available,
            default=[],
            placeholder="All markets",
            key="_catalogue_filter_market",
        )
        sel_statuses = fc3.multiselect(
            "Status",
            options=_STATUS_ORDER,
            default=[],
            placeholder="All statuses",
            key="_catalogue_filter_status",
        )

    filtered = apply_filters(all_models, sel_categories, sel_markets, sel_statuses)

    if not filtered:
        st.info("No models match the current filters.")
        return

    if len(filtered) < len(all_models):
        st.caption(f"Showing {len(filtered)} of {len(all_models)} models")

    # --- Model cards (2-column grid) ---
    left_col, right_col = st.columns(2)
    col_cycle = [left_col, right_col]

    for i, desc in enumerate(filtered):
        with col_cycle[i % 2]:
            _render_model_card(desc)

    # --- Raw metadata table (collapsed) ---
    st.divider()
    with st.expander("Raw metadata table"):
        import pandas as pd

        rows = []
        for desc in filtered:
            md = desc["metadata"]
            rows.append({
                "name":                    desc["name"],
                "version":                 desc["version"],
                "category":                md.get("category"),
                "model_family":            md.get("model_family"),
                "scope":                   md.get("scope"),
                "granularity":             md.get("granularity"),
                "horizon":                 md.get("horizon"),
                "market":                  fmt(md.get("market")),
                "asset_type":              md.get("asset_type"),
                "deterministic":           md.get("deterministic"),
                "status":                  md.get("status"),
                "owner":                   md.get("owner"),
                "source_of_truth_module":  md.get("source_of_truth_module"),
                "fallback_behavior":       fmt(md.get("fallback_behavior")),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
