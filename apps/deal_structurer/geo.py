"""apps/deal_structurer/geo.py — Province/node option sources for Deal Intake.

Province values must match `marketdata.spot_prices_hourly.province` (Chinese
names like 蒙西 / 山东) because the committee economics path queries that table
with brief.province verbatim. Node values are prompt/display-only; canonical
node names come from the nodal price tables where coverage exists (蒙西, 山西).
"""
from __future__ import annotations

import streamlit as st

# Same coverage as Tab 1 price simulation; used when the DB is unreachable
# (local dev without RDS).
FALLBACK_PROVINCES = ["蒙西", "蒙东", "山西", "河北南网", "山东", "陕西", "甘肃", "新疆"]

_SUFFIXES = ("壮族自治区", "回族自治区", "维吾尔自治区", "自治区", "省", "市")

NODE_NONE = "（不指定）"
NODE_MANUAL = "✏️ 手工输入"


def normalize_province(value: str | None) -> str:
    """Strip whitespace and administrative suffixes from an extracted province.

    Never guesses aliases (内蒙古 could be 蒙西 or 蒙东) — the caller decides
    what to do with a value that matches no option.
    """
    v = (value or "").strip()
    for suffix in _SUFFIXES:
        if v.endswith(suffix):
            return v[: -len(suffix)]
    return v


def province_options(options: list[str], draft: str | None) -> tuple[list[str], int]:
    """(options, default_index) with the draft value preselected.

    A draft value matching no option is prepended so the selectbox can still
    display it — the user corrects it via the dropdown.
    """
    norm = normalize_province(draft)
    if norm and norm in options:
        return list(options), options.index(norm)
    if norm:
        return [norm, *options], 0
    return list(options), 0


def node_select_state(nodes: list[str], draft_node: str | None) -> tuple[list[str], int, str]:
    """(options, default_index, manual_prefill) for the node selectbox."""
    options = [NODE_NONE, *nodes, NODE_MANUAL]
    if draft_node and draft_node in nodes:
        return options, options.index(draft_node), ""
    if draft_node:
        return options, len(options) - 1, draft_node
    return options, 0, ""


@st.cache_data(ttl=3600, show_spinner=False)
def load_provinces() -> list[str]:
    """Distinct provinces present in spot_prices_hourly; fallback list offline."""
    try:
        from sqlalchemy import text

        from services.common.db_utils import get_engine

        with get_engine().connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT province FROM marketdata.spot_prices_hourly"
                " ORDER BY province"
            )).fetchall()
        provinces = [r[0] for r in rows if r[0]]
        return provinces or list(FALLBACK_PROVINCES)
    except Exception:
        return list(FALLBACK_PROVINCES)


@st.cache_data(ttl=3600, show_spinner=False)
def load_nodes(province: str) -> list[str]:
    """Canonical node names where nodal price coverage exists; [] otherwise."""
    try:
        from sqlalchemy import text

        from services.common.db_utils import get_engine

        if province == "山西":
            sql = text("SELECT DISTINCT node_name FROM marketdata.md_shanxi_nodal_price_96"
                       " ORDER BY node_name")
            params = {}
        else:
            sql = text("SELECT DISTINCT node_name FROM reports.nodal_pf_node_daily"
                       " WHERE province = :p ORDER BY node_name")
            params = {"p": province}
        with get_engine().connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [r[0] for r in rows if r[0]]
    except Exception:
        return []
