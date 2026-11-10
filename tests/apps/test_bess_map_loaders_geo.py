"""Source-level regression checks for bess-map loaders / ranking / geo / aux
tabs (2026-11-10 review). app.py cannot be imported without a live DB."""
from pathlib import Path

_APP = Path(__file__).resolve().parents[2] / "apps" / "bess-map" / "app.py"
_SRC = _APP.read_text(encoding="utf-8")


def test_no_timestamp_between_date_string_truncation():
    # `datetime BETWEEN :start AND :end` with a 'YYYY-MM-DD' end casts to
    # end 00:00 → the last selected day contributes hour 0 only.
    assert "datetime BETWEEN :start AND :end" not in _SRC
    assert _SRC.count("datetime < CAST(:end AS date) + 1") == 5


def test_theoretical_avgs_nan_safe():
    # ranking annual_theo, monthly theo_avg, avg_economics theo
    assert _SRC.count("AVG(NULLIF(theoretical_profit_per_mwh_day, 'NaN'::double precision))") >= 3
    assert "AVG(theoretical_profit_per_mwh_day)" not in _SRC
    assert "AVG(realized_profit_per_mwh_day)" not in _SRC


def test_geo_map_respects_ranking_exclusions_and_duration():
    assert 'geo_rank_df[~geo_rank_df["province"].isin(_RANKING_EXCLUDE)]' in _SRC
    # cap_comp/fr ¥/MWh adjustments scale with duration → one map per duration
    assert "_geo_extra_map[2.0]" in _SRC and "_geo_extra_map[4.0]" in _SRC
    assert 'cycles_df[~cycles_df["province"].isin(_RANKING_EXCLUDE)]' in _SRC


def test_installed_gap_query_uses_date_column():
    assert "TO_DATE(year_month" not in _SRC
    assert "TO_CHAR(year_month, 'YYYY-MM')" in _SRC


def test_aux_conflict_styling_and_resolution_reachable():
    assert 'row.get(_t("aux_status")) == "conflict"' in _SRC
    # resolution expander must not be nested under the installed-capacity else:
    line = next(l for l in _SRC.splitlines() if "# ── Conflict resolution" in l)
    assert line.startswith("    #"), "conflict resolution block is indented inside Section 3"


def test_rt_price_axes_labelled_mwh():
    # spot_prices_hourly.rt_price is ¥/MWh (run_capture_pipeline.py: "Prices are stored in ¥/MWh")
    for bad in ('"rank_spread_title":    "Intraday RT Spread by Province (¥/kWh)"',
                '"pca_mean_title":          "Mean Daily Price Profile (¥/kWh)"',
                '"disp_rt_price":        "RT Price (¥/kWh)"',
                'labels={"spread": "RT Intraday Spread (¥/kWh)"',
                'xaxis_title=_t("pca_hour"), yaxis_title="¥/kWh"',
                'title_text="¥/kWh", row=1, col=1, secondary_y=True'):
        assert bad not in _SRC, bad
    assert "元/千瓦时" not in _SRC
    # the 系统运行费 line chart is genuinely ¥/kWh (fee_yuan_kwh) — must stay
    assert 'yaxis_title="¥/kWh"' in _SRC


def test_aux_year_default_is_an_available_option():
    assert 'if st.session_state.get("aux_year") not in _aux_years:' in _SRC


def test_scan_progress_clamped():
    assert "st.progress(min(_done / _total, 1.0) if _total else 0" in _SRC
