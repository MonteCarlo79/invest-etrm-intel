"""Region mapping and path filtering for receiver-aware S1."""
from libs.interconnector_pricing import params as P


def test_province_region_mapping():
    assert P.PROVINCE_TO_REGION["甘肃"] == "西北"
    assert P.PROVINCE_TO_REGION["上海"] == "华东"
    assert P.PROVINCE_TO_REGION["山西"] == "华北"
    assert P.PROVINCE_TO_REGION["四川"] == "华中（西南）"
    assert P.PROVINCE_TO_REGION["蒙西"] == "华北"
    assert set(P.UNPRICEABLE_RECEIVERS) == {"广州", "广西", "云南", "贵州", "海南"}
    # all 27 送出省 have a region (蒙西 is receiver-only, correctly absent below)
    for prov in P.PROVINCE_GRID:
        if prov == "西藏":
            continue
        assert prov in P.PROVINCE_TO_REGION, prov


def test_path_regions():
    assert P.path_regions(["区域：西北", "专项：宝合直流", "区域：华东"]) == ("西北", "华东")
    assert P.path_regions(["专项：雁淮直流", "区域：华东"]) == ("华北", "华东")
    assert P.path_regions(["区域：华北"]) == ("华北", "华北")
    assert P.path_regions(["专项：金永直流", "区域：华东"]) == ("西北", "华东")


def test_filter_paths_gansu_shanghai():
    out = P.filter_paths("甘肃", "上海")
    assert "西北—宝合—华东" in out
    assert "西北—灵绍—华东" in out
    assert "灵绍—华东" in out            # 专项 first, send region 西北
    assert "甘浙—华东" in out
    assert "雁淮—华东" not in out        # starts 华北
    assert "华东区域" not in out         # 华东→华东 only
    assert all(P.path_regions(P.PATHS[n]) == ("西北", "华东") for n in out)


def test_filter_paths_shanxi_shanghai():
    out = P.filter_paths("山西", "上海")
    assert "雁淮—华东" in out
    assert "西北—宝合—华东" not in out


def test_filter_paths_unpriceable_receiver():
    assert P.filter_paths("甘肃", "广州") == []


def test_filter_paths_intra_region():
    assert P.filter_paths("安徽", "江苏") == ["华东区域"]


def test_receiver_list_canonical():
    receivers = P.RECEIVER_PROVINCES
    assert receivers[0] == "上海"
    assert len(receivers) == 30
    for dirty in ("豫中东", "豫北", "豫南", "豫西", "海南礼记", "海南那悦", "甘肃西河", "甘肃河西"):
        assert dirty not in receivers, dirty
    for r in receivers:
        assert r in P.PROVINCE_TO_REGION or r in P.UNPRICEABLE_RECEIVERS, r
