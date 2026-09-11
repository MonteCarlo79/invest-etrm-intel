import pytest
from services.interconnector import topology

_CHANNELS = [
    dict(name="锡泰直流", send="锡林浩特", send_prov="内蒙古", sc=[116.07,43.94],
         recv="泰州", recv_prov="江苏", rc=[119.93,32.46], kv="±800", gw=10.0,
         category="大基地型", formal="f", commissioned="2017", km=1, note=""),
    dict(name="云霄直流", send="云霄", send_prov="福建", sc=[117.34,23.96],
         recv="梅州", recv_prov="广东", rc=[116.12,24.29], kv="±100", gw=2.0,
         category="制度创新型", formal="f", commissioned="2022", km=1, note=""),
]

def test_physical_options_series_per_kv_class_and_widths():
    opts = topology.physical_options(_CHANNELS, {}, None)
    line_series = [s for s in opts["series"] if s["type"] == "lines"]
    assert {s["name"] for s in line_series} == {"±800", "±100"}
    s800 = next(s for s in line_series if s["name"] == "±800")
    assert s800["data"][0]["lineStyle"]["width"] == pytest.approx(1 + 5.5)  # max gw
    assert s800["data"][0]["meta"]["name"] == "锡泰直流"
    assert opts["legend"]["data"] == topology.KV_ORDER

def test_physical_options_dim_when_province_selected():
    opts = topology.physical_options(_CHANNELS, {}, "福建")
    s800 = next(s for s in opts["series"] if s.get("name") == "±800")
    s100 = next(s for s in opts["series"] if s.get("name") == "±100")
    assert s800["data"][0]["lineStyle"]["opacity"] == pytest.approx(0.07)   # not linked
    assert s100["data"][0]["lineStyle"]["opacity"] == pytest.approx(0.85)   # linked

def test_flows_options_widths_and_meta():
    flows = [dict(send="蒙东", recv="江苏", vol_gwh=100.0, land=300, sendp=150, fee=120,
                  trades=2, channels=["鲁固直流"], send_raws=["蒙东"], months=1),
             dict(send="山西", recv="江苏", vol_gwh=25.0, land=380, sendp=None, fee=70,
                  trades=1, channels=["雁淮直流"], send_raws=["山西"], months=1)]
    coords = {"蒙东":[122.24,43.62], "山西":[112.55,37.87], "江苏":[119.78,32.96]}
    opts = topology.flows_options(flows, coords)
    s = next(x for x in opts["series"] if x["name"] == "江苏")
    w_big = s["data"][0]["lineStyle"]["width"]
    assert w_big == pytest.approx(0.8 + 6.2)     # max vol flow
    assert s["data"][1]["lineStyle"]["width"] < w_big
    assert s["data"][0]["meta"]["channels"] == ["鲁固直流"]
