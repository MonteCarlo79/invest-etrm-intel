# tests/interconnector_pricing/test_params.py
from libs.interconnector_pricing import params as P


def test_table_sizes():
    assert len(P.PROVINCE_GRID) == 27
    assert len(P.REGIONAL_GRID) == 5
    assert len(P.SPECIAL_PROJECTS) == 40
    assert len(P.PATHS) == 19  # 18 Excel presets + 1 v1 extension (甘浙—华东)


def test_unpriced_and_noncomputable_stops():
    assert P.PROVINCE_GRID["西藏"]["priced"] is False
    assert P.SPECIAL_PROJECTS["辛洹线"]["computable"] is False
    assert P.SPECIAL_PROJECTS["云霄"]["computable"] is False


def test_spot_values():
    assert P.PROVINCE_GRID["青海"] == {"tariff": 33.9, "loss": 0.0139}
    assert P.REGIONAL_GRID["西北"] == {"tariff": 12.1, "loss": 0.019}
    assert P.SPECIAL_PROJECTS["宝合直流"]["tariff"] == 53.23
    assert P.SPECIAL_PROJECTS["宝合直流"]["loss"] == 0.05


def test_paths_reference_known_segments():
    known = {f"区域：{k}" for k in P.REGIONAL_GRID} | {f"专项：{k}" for k in P.SPECIAL_PROJECTS}
    for name, segs in P.PATHS.items():
        assert segs and len(segs) <= 5, name
        for s in segs:
            assert s in known, (name, s)
    assert P.PATHS["西北—宝合—华东"] == ["区域：西北", "专项：宝合直流", "区域：华东"]


def test_profile_buckets():
    assert P.PROFILE_BUCKETS["solar"] == [9, 10, 11, 12, 13, 14, 15, 16, 17]
    assert sorted(P.PROFILE_BUCKETS["wind"]) == [0, 1, 2, 3, 4, 5, 19, 20, 21, 22, 23]
    assert P.PROFILE_BUCKETS["mixed"] is None and P.PROFILE_BUCKETS["hydro"] is None
