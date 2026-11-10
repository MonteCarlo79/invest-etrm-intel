"""Source-level regression checks for the bess-map Quant agent + 跨省绿电 tab
(2026-11-10 review). Mirrors the grep-style tests in this folder: app.py cannot
be imported without a live DB."""
from pathlib import Path

_APP = Path(__file__).resolve().parents[2] / "apps" / "bess-map" / "app.py"
_SRC = _APP.read_text(encoding="utf-8")
_AGENT = _SRC[_SRC.index("with tab_agent:"):_SRC.index("# ── 跨省绿电 (Interconnector Green) tab")]
_ICP = _SRC[_SRC.index("# ── 跨省绿电 (Interconnector Green) tab"):]


def test_irr_tool_payback_includes_initial_outlay():
    assert "payback = _compute_payback(cfs)" in _AGENT


def test_avg_economics_nan_safe():
    # AVG over daily P&L must skip literal-NaN warmup/forecast-hole days,
    # like load_province_ranking already does (one NaN poisons AVG).
    assert _SRC.count("AVG(NULLIF(realized_profit_per_mwh_day, 'NaN'::double precision))") >= 2
    assert "AVG(NULLIF(theoretical_profit_per_mwh_day, 'NaN'::double precision))" in _SRC


def test_missing_llm_key_does_not_stop_script():
    # st.stop() inside tab_agent blanked every tab rendered after it (跨省绿电).
    assert "st.stop()" not in _AGENT
    assert "disabled=not _agent_ready" in _AGENT


def test_tool_loops_are_bounded():
    assert "while True:" not in _AGENT
    assert "_MAX_TOOL_ROUNDS" in _AGENT
    for key in ('"agent_round_cap"', '"agent_empty_reply"'):
        assert _SRC.count(key) >= 2, f"{key} missing from one locale dict"


def test_memory_extraction_validates_shape():
    assert "isinstance(items, list)" in _AGENT


def test_tool_descriptions_state_correct_unit_conversion():
    assert "subsidy_per_mwh = -50" in _AGENT          # 0.05 ¥/kWh sysop fee example
    assert "÷ duration_h ÷ 1000" not in _AGENT          # old, wrong by ~3000x


def test_shanghai_routes_to_proxy_series():
    # 上海 is in RECEIVER_PROVINCES but has no LingFeng RT series; membership
    # must be tested against the 29 live markets, not the receiver list.
    assert "if recv in _ICP_LINGFENG_29:" in _ICP
    assert "if recv in _provs:" not in _ICP
