"""F1/F2/F7: tab-local Streamlit caches (bare-mode st.cache_data) and the
sign guard scoped to what each tab shows."""
import inspect

import pytest

import apps.asset_risk.tab_diagnostics as dx
import apps.asset_risk.tab_waterfall as tw
from tests.asset_risk.test_chain_loader import _base_engine, _chain


@pytest.fixture()
def bad_feb_engine():
    """Asset 1: Σ actual·nominated < 0 in 2026-02, clean in 2026-11."""
    from sqlalchemy import text
    eng = _base_engine()
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO marketdata.rm_assets VALUES "
                          "(1, '裕昭沙子坝', 'bess', 'test', 100, 4.0, 'gushanliang')"))
        _chain(conn, 1, "2026-02-01 00:00:00", 10, -10)
        _chain(conn, 1, "2026-02-01 00:15:00", -10, 10)
        _chain(conn, 1, "2026-11-01 00:00:00", 10, 9)
        _chain(conn, 1, "2026-11-01 00:15:00", -10, -9)
    yield eng
    eng.dispose()


@pytest.fixture(autouse=True)
def _clear_caches():
    for f in (tw._cached_chain, tw._cached_benchmark_leg, tw._cached_forecast_leg,
              dx._cached_chain):
        f.clear()
    yield


def _spy_load_chain(monkeypatch, mod):
    calls = []
    real = mod._load_chain

    def spy(*a, **k):
        calls.append(k.get("check_sign", True))
        return real(*a, **k)

    monkeypatch.setattr(mod, "_load_chain", spy)
    return calls


def test_waterfall_cached_chain_defers_sign_check_and_caches(bad_feb_engine, monkeypatch):
    calls = _spy_load_chain(monkeypatch, tw)
    df = tw._cached_chain(bad_feb_engine, (1,))
    tw._cached_chain(bad_feb_engine, (1,))
    assert calls == [False] and len(df) == 4


def test_waterfall_sign_guard_scoped_to_selected_month(bad_feb_engine):
    from libs.asset_risk.chain import _check_sign
    chain = tw._cached_chain(bad_feb_engine, (1,))
    month = chain["ts"].dt.strftime("%Y-%m")
    _check_sign(chain[month == "2026-11"])           # shown month clean → no raise
    with pytest.raises(ValueError, match="裕昭沙子坝 2026-02"):
        _check_sign(chain[month == "2026-02"])


def test_diagnostics_cached_chain_keyed_on_range(bad_feb_engine, monkeypatch):
    calls = _spy_load_chain(monkeypatch, dx)
    nov = dx._cached_chain(bad_feb_engine, (1,), "2026-11-01", "2026-12-01")
    dx._cached_chain(bad_feb_engine, (1,), "2026-11-01", "2026-12-01")
    dx._cached_chain(bad_feb_engine, (1,), "2026-02-01", "2026-03-01")
    assert calls == [False, False] and len(nov) == 2


def test_cached_legs_delegate_and_cache(monkeypatch):
    n = {"b": 0, "f": 0}

    def bench(engine, *a, **k):
        n["b"] += 1
        return {"arb_cny": 1.0, "args": a, "kw": k}

    def fc(engine, *a, **k):
        n["f"] += 1
        return {"arb_cny": 2.0}

    monkeypatch.setattr(tw._wf, "benchmark_leg", bench)
    monkeypatch.setattr(tw._wf, "forecast_optimal_leg", fc)
    for _ in range(2):
        r = tw._cached_benchmark_leg(object(), 1, "2026-11", 100.0, asset_name="A")
        tw._cached_forecast_leg(object(), 1, "2026-11", 100.0)
    assert r["args"] == (1, "2026-11", 100.0) and r["kw"] == {"asset_name": "A"}
    assert n == {"b": 1, "f": 1}
    tw._cached_benchmark_leg.clear()
    tw._cached_benchmark_leg(object(), 1, "2026-11", 100.0, asset_name="A")
    assert n["b"] == 2


def test_render_functions_use_cached_chain_and_scoped_guard():
    for fn in (tw.render_waterfall, dx.render_diagnostics):
        src = inspect.getsource(fn)
        assert "_load_chain(" not in src and "_cached_chain(" in src
        assert "_check_sign(" in src and "st.stop()" in src


def test_recompute_bypasses_and_clears_leg_caches():
    src = inspect.getsource(tw.render_waterfall)
    assert "_cached_benchmark_leg.clear()" in src and "_cached_forecast_leg.clear()" in src
    assert "_wf.benchmark_leg if force_legs else _cached_benchmark_leg" in src
    assert "_wf.forecast_optimal_leg if force_legs else _cached_forecast_leg" in src


# --- F10: captions -----------------------------------------------------------

def test_caveats_no_times_four_on_cleared_energy():
    assert "×4" not in tw._CAVEATS_MD
    assert "交易所出清电量（md_id_cleared_energy）补齐" in tw._CAVEATS_MD


def test_bench_note_captions_split_by_note():
    rows = [{"asset": "A", "month": "2026-09", "note": "出清价+节点RT价"},
            {"asset": "B", "month": "2026-09", "note": "no prices"},
            {"asset": "C", "month": "2026-10", "note": "no complete price day"},
            {"asset": "D", "month": "2026-09", "note": ""},
            {"asset": "E", "month": "2026-09", "note": "lp empty"}]
    filled, zero = tw._bench_note_captions(rows)
    assert filled.endswith("投资标准按节点价格（RT/填充）逐槽补缺计算：A") and "B" not in filled
    assert zero == "投资标准无可用电价，按 ¥0 显示：B 2026-09、C 2026-10"


def test_bench_note_captions_none_when_clean():
    assert tw._bench_note_captions([{"asset": "D", "month": "2026-09", "note": ""}]) == (None, None)
