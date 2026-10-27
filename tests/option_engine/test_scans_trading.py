# tests/option_engine/test_scans_trading.py
import numpy as np, pandas as pd
from services.option_engine.scout import scans_trading as st

def test_basis_zscore_flags_step_shift():
    rng = np.random.default_rng(1)
    dates = pd.date_range("2025-10-27", periods=200*24, freq="h")
    basis = np.concatenate([rng.normal(10, 5, 170*24), rng.normal(60, 5, 30*24)])  # 30d regime shift
    df = pd.DataFrame({"datetime": dates, "rt_price": 300 + basis, "da_price": 300.0})
    out = st.compute_da_rt_basis({"山东": df})
    assert len(out) == 1 and out[0]["evidence"]["z"] >= 2.0 and out[0]["evidence"]["direction"] == "rt_premium"

def test_basis_thin_history_skipped():
    df = pd.DataFrame({"datetime": pd.date_range("2026-09-01", periods=50*24, freq="h"),
                       "rt_price": 300.0, "da_price": 290.0})
    assert st.compute_da_rt_basis({"新疆": df}) == []

def test_nodal_outlier_ranking():
    rows = [{"data_date": f"2026-10-{d:02d}", "province": "蒙西", "node_name": n,
             "revenue_cny": 50000.0 if n != "hot_node" else 200000.0, "discharge_mwh": 100.0}
            for d in range(1, 25) for n in ("a", "b", "hot_node")]
    out = st.compute_nodal_pf_outliers(pd.DataFrame(rows))
    assert [c["evidence"]["node_name"] for c in out] == ["hot_node"]
    assert out[0]["evidence"]["multiple"] > 1.5

def test_interconnector_name_mapping_both_directions():
    assert st.map_province("冀南") == "河北南网" and st.map_province("河北南网") == "河北南网"
    assert st.map_province("内蒙古（蒙西）") == "蒙西"
