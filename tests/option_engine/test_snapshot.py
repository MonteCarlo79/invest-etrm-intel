# test_snapshot.py
import os, pytest
from sqlalchemy import create_engine

@pytest.mark.skipif(not os.environ.get("DB_DSN"), reason="integration: needs DB_DSN")
def test_snapshot_roundtrip():
    from services.option_engine.snapshot import ensure_table, write_snapshot, read_latest
    eng = create_engine(os.environ["DB_DSN"])
    ensure_table(eng)
    row = dict(province="测试省", asset_type="bess", duration_h=2.0, asof_date="2026-10-25",
               window_start="2025-10-26", window_end="2026-10-25",
               intrinsic_yuan_kw=1.0, extrinsic_yuan_kw=2.0, total_yuan_kw=3.0,
               capture_adj_yuan_kw=2.9, policy_json={}, greeks_json={}, mc_params={"seed": 42})
    write_snapshot(eng, row)
    got = read_latest(eng, "测试省", "bess", 2.0)
    assert got["total_yuan_kw"] == 3.0
