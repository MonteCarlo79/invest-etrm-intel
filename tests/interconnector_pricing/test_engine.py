# tests/interconnector_pricing/test_engine.py
import pytest
from libs.interconnector_pricing.engine import compute_landed

GOLDEN = [
    # province, basis, path, green_total, env, landed, tariff_sum, loss_sum, composite
    ("青海", "发电侧", ["区域：西北", "专项：宝合直流", "区域：华东"], 240, 5,
     377.782511764198, 108.43, 29.3525117641982, 0.0949728426040001),
    ("甘肃", "发电侧", ["区域：西北", "专项：宝合直流", "区域：华东"], 240, 5,
     370.357208258708, 102.53, 27.8272082587081, 0.0914852620360001),
    ("甘肃", "发电侧", ["区域：西北", "专项：宝合直流", "区域：华东"], 240, 30,
     367.839768028707, 102.53, 25.3097680287065, 0.0914852620360001),
    ("青海", "发电侧", ["区域：西北", "专项：宝合直流", "区域：华东"], 240, 30,
     375.159031302096, 108.43, 26.7290313020963, 0.0949728426040001),
    ("甘肃", "出省节点", ["区域：西北", "专项：宝合直流", "区域：华东"], 240, 5,
     337.236446709552, 74.53, 22.7064467095517, 0.08221564),
    ("山西", "发电侧", ["专项：雁淮直流", "区域：华东"], 353, 20,
     436.751853292325, 66.8, 16.9518532923255, 0.044298249976),
    ("安徽", "发电侧", ["区域：华东"], 376.45, 0,
     421.923395613323, 39.2, 6.2733956133225, 0.0152),
]


@pytest.mark.parametrize("prov,basis,path,gt,env,landed,tsum,lsum,comp", GOLDEN)
def test_golden_values(prov, basis, path, gt, env, landed, tsum, lsum, comp):
    r = compute_landed(province=prov, basis=basis, path_segments=path,
                       green_total=gt, env_price=env)
    assert r.ok, r.stop_reason
    assert r.landed == pytest.approx(landed, abs=1e-9)
    assert r.tariff_sum == pytest.approx(tsum, abs=1e-9)
    assert r.loss_sum == pytest.approx(lsum, abs=1e-9)
    assert r.composite_loss == pytest.approx(comp, abs=1e-9)
    assert r.increment == pytest.approx(landed - gt, abs=1e-9)
    assert r.landed_kwh == pytest.approx(landed / 1000, abs=1e-12)


def test_stops():
    assert not compute_landed(province="西藏", basis="发电侧",
                              path_segments=["区域：华东"], green_total=240, env_price=5).ok
    assert not compute_landed(province="青海", basis="发电侧",
                              path_segments=["专项：辛洹线"], green_total=240, env_price=5).ok
    assert not compute_landed(province="青海", basis="发电侧",
                              path_segments=["专项：云霄"], green_total=240, env_price=5).ok
    assert not compute_landed(province="青海", basis="发电侧",
                              path_segments=["区域：华东"], green_total=240, env_price=250).ok
    assert not compute_landed(province="青海", basis="发电侧",
                              path_segments=[], green_total=240, env_price=5).ok
    assert not compute_landed(province="青海", basis="发电侧",
                              path_segments=["专项：不存在直流"], green_total=240, env_price=5).ok


def test_env_equals_total_is_valid():
    r = compute_landed(province="青海", basis="发电侧", path_segments=["区域：华东"],
                       green_total=240, env_price=240)
    # 电能量价=0: seg1 省网青海 D=0 → post=33.9; seg2 区域华东 D=33.9 → G=33.9×0.0152/(1−0.0152)
    expected = 33.9 + 33.9 * 0.0152 / (1 - 0.0152) + 9.2 + 240
    assert r.ok and r.landed == pytest.approx(expected, abs=1e-9)


def test_zero_loss_segment():
    r = compute_landed(province="北京", basis="发电侧", path_segments=["区域：华北"],
                       green_total=240, env_price=0)
    assert r.ok
    assert r.segments[0].loss_cost == 0.0


def test_duplicate_segment_computed_as_given():
    r = compute_landed(province="安徽", basis="发电侧",
                       path_segments=["区域：华东", "区域：华东"], green_total=376.45, env_price=0)
    assert r.ok and len(r.segments) == 3  # 省网 + two 区域：华东
