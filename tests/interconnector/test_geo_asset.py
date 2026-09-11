# tests/interconnector/test_geo_asset.py
import json
from pathlib import Path

def test_china_geojson_loads_with_provinces_and_centers():
    p = Path("assets/geo/china_provinces.json")
    g = json.loads(p.read_text())
    names = {f["properties"]["name"] for f in g["features"]}
    assert len(g["features"]) >= 34
    assert {"江苏省","内蒙古自治区","新疆维吾尔自治区","上海市"} <= names
    located = sum(1 for f in g["features"]
                  if f["properties"].get("centroid") or f["properties"].get("center"))
    assert located >= 34
