# tests/option_engine/test_batch.py
import numpy as np, pandas as pd
from datetime import date
import services.option_engine.batch as batch
from libs.option_value.contracts import PolicyLevers

class _FakeEng: pass

def test_run_one_chain(monkeypatch):
    idx = pd.date_range("2025-11-01", periods=390*24, freq="h")
    s = pd.Series(300 + 120*np.sin(2*np.pi*(idx.hour-6)/24) + np.random.default_rng(1).normal(0, 25, len(idx)), index=idx)
    monkeypatch.setattr(batch.readers, "read_price_series", lambda *a, **k: s)
    monkeypatch.setattr(batch.readers, "read_policy_levers", lambda *a, **k: PolicyLevers(capacity_price_yuan_kw_yr=50.0))
    monkeypatch.setattr(batch.readers, "read_capture_rate", lambda *a, **k: 0.75)
    written = {}
    monkeypatch.setattr(batch.snapshot, "write_snapshot", lambda eng, row: written.update(row))
    row = batch.run_one(_FakeEng(), "测试省", "bess", 2.0, n_paths=50, seed=42)
    assert row["total_yuan_kw"] > 0 and row["extrinsic_yuan_kw"] >= -1e-6
    assert row["capture_adj_yuan_kw"] < row["total_yuan_kw"]      # 0.75 haircut applied
    assert written["mc_params"]["seed"] == 42

def test_run_one_skips_insufficient_history():
    idx = pd.date_range("2026-06-01", periods=100*24, freq="h")
    s = pd.Series(300.0, index=idx)
    import services.option_engine.batch as b
    from unittest.mock import patch
    with patch.object(b.readers, "read_price_series", return_value=s):
        out = b.run_one(_FakeEng(), "福建", "bess", 2.0, n_paths=10)
    assert out["skipped"] == "福建"
