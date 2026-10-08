# scripts/openinfra_preview_mp.py
"""Multi-province OpenInfraMap × 网架图 preview data builder.

Provinces:
  - mengxi: 蒙西 (extent 100-117.5E/37-44.8N; vision stations VISION_500KV)
  - guangxi: 广西 (extent 104-112.8E/20.4-26.8N; 500kV nodes + adjacency from
    广西500KV画图.xlsx — the map author's own data, no vision needed)

Output: data/nodal/open-infra-map/preview_mp.json
Match semantics: OK (normalized hit) / COLLAPSED (city-prefix substring
fallback onto a less-specific OIM station) / MISSING.
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from openinfra_preview import (VISION_500KV, norm_name, _parse_wkb_coords,
                               _read_gpb_wkb, _downsample)

GPKG = Path("data/nodal/open-infra-map/CHN.gpkg")
OUT = Path("data/nodal/open-infra-map/preview_mp.json")

PROVINCES = {
    "mengxi": {"label": "蒙西", "extent": (100.0, 117.5, 37.0, 44.8)},
    "guangxi": {"label": "广西", "extent": (104.0, 112.8, 20.4, 26.8)},
    "liaoning": {"label": "辽宁", "extent": (118.5, 126.0, 38.5, 44.0), "pending": True},
    "heilongjiang": {"label": "黑龙江", "extent": (121.0, 135.5, 43.0, 53.8), "pending": True},
    "mengdong": {"label": "蒙东", "extent": (114.0, 126.5, 41.5, 53.5), "pending": True},
}

# Guangxi 500kV nodes whose OIM presence should be tried in the generator
# layers as well (power plants, not substations)
_GX_PLANT_RE = re.compile(r"(电厂|厂|核电)$")


def load_substations(db, extent):
    lon_min, lon_max, lat_min, lat_max = extent
    subs = {}
    for table, rtree in (("power_substation_point", "rtree_power_substation_point_geometry"),
                         ("power_substation_polygon", "rtree_power_substation_polygon_geometry")):
        for row in db.execute(
                f"""SELECT p.name, p.voltages, p.max_voltage, p.operator,
                           (r.minx + r.maxx)/2.0 AS lon, (r.miny + r.maxy)/2.0 AS lat
                    FROM {table} p JOIN {rtree} r ON p.fid = r.id
                    WHERE r.minx >= ? AND r.maxx <= ? AND r.miny >= ? AND r.maxy <= ?
                      AND p.name IS NOT NULL AND p.name != ''""",
                (lon_min, lon_max, lat_min, lat_max)):
            key = norm_name(row["name"])
            if key and (key not in subs or (row["max_voltage"] or 0) > (subs[key]["max_voltage"] or 0)):
                subs[key] = dict(row)
    return subs


def load_lines(db, extent, min_voltage=500000):
    lon_min, lon_max, lat_min, lat_max = extent
    lines = []
    for row in db.execute(
            f"""SELECT p.geometry, p.name, p.max_voltage
                FROM power_line p JOIN rtree_power_line_geometry r ON p.fid = r.id
                WHERE r.minx >= ? AND r.maxx <= ? AND r.miny >= ? AND r.maxy <= ?
                  AND p.max_voltage >= ?""",
            (lon_min, lon_max, lat_min, lat_max, min_voltage)):
        for path in _parse_wkb_coords(_read_gpb_wkb(row["geometry"])):
            if len(path) >= 2:
                lines.append({"path": _downsample(path, 100),
                              "name": row["name"] or "", "max_voltage": row["max_voltage"]})
    return lines


_NON_GRID_RE = re.compile(r"(管理局|管理分局|公司|政府|学校|医院|园区管委会)")


def match_one(name: str, by_norm: dict, city_prefix: bool):
    """Return (status, hit). OK exact/normalized; COLLAPSED via prefix-drop or
    shorter-key substring; MISSING. Fallback hits onto non-grid entities
    (管理局/公司/政府…) are rejected."""
    keys = [norm_name(name)]
    if city_prefix and len(keys[0]) > 2:
        keys.append(keys[0][2:])
    for k in keys:
        if k in by_norm:
            return "OK", by_norm[k]
    for k in keys:
        if not k:
            continue
        cands = [kk for kk in by_norm if k in kk and not _NON_GRID_RE.search(by_norm[kk]["name"])]
        if cands:
            return "COLLAPSED", by_norm[min(cands, key=len)]
        cands = [kk for kk in by_norm if len(kk) >= 2 and kk in k
                 and not _NON_GRID_RE.search(by_norm[kk]["name"])]
        if cands:
            return "COLLAPSED", by_norm[max(cands, key=len)]
    return "MISSING", None


def build_mengxi(db):
    cfg = PROVINCES["mengxi"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in VISION_500KV:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {
        "stations": [{"name": s["name"], "key": norm_name(s["name"]),
                      "voltages": s["voltages"], "max_voltage": s["max_voltage"],
                      "lon": s["lon"], "lat": s["lat"]} for s in subs.values()],
        "lines": load_lines(db, cfg["extent"]),
        "match": match,
        "adjacency": [],
    }


def build_guangxi(db):
    cfg = PROVINCES["guangxi"]
    gx = json.load(open("data/nodal/open-infra-map/guangxi_grid.json", encoding="utf-8"))
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    coord = {}
    for node in gx["nodes_500kv"]:
        st, hit = match_one(node, by_norm, city_prefix=True)
        match.append({"vision": node, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
        if hit:
            coord[node] = [hit["lon"], hit["lat"]]
    adj = [[a, b] for a, b in gx["adjacency"] if a in coord and b in coord]
    return {
        "stations": [{"name": s["name"], "key": norm_name(s["name"]),
                      "voltages": s["voltages"], "max_voltage": s["max_voltage"],
                      "lon": s["lon"], "lat": s["lat"]} for s in subs.values()],
        "lines": load_lines(db, cfg["extent"]),
        "match": match,
        "adjacency": adj,
    }


MD_500 = ["荣泰", "海北", "兴隆", "岭东", "铝都", "巴林", "阿拉坦", "兴安",
          "科尔沁", "青山", "金沙", "红城", "巴彦托海", "伊敏换流站",
          "扎鲁特", "开鲁", "通辽"]


def build_mengdong(db):
    cfg = PROVINCES["mengdong"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in MD_500:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {
        "stations": [{"name": s["name"], "key": norm_name(s["name"]),
                      "voltages": s["voltages"], "max_voltage": s["max_voltage"],
                      "lon": s["lon"], "lat": s["lat"]} for s in subs.values()],
        "lines": load_lines(db, cfg["extent"]),
        "match": match,
        "adjacency": [],
    }


LN_500 = ["川州", "阜新", "北宁", "鹤乡", "辽滨", "营口", "历林", "京诚",
          "北海", "南海", "辽中", "白清寨", "抚顺", "徐家", "程家", "张台",
          "辽阳", "鞍山", "唐家", "王石", "析木", "虎官", "丹东北", "黄海",
          "瓦房店", "登台", "金家", "南关岭", "大连湾", "甘井子", "玉华", "雁水",
          "清河", "石岭", "东港", "龙王", "凤城", "冷家", "庄河"]


def build_liaoning(db):
    cfg = PROVINCES["liaoning"]
    subs = load_substations(db, cfg["extent"])
    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    match = []
    for v in LN_500:
        st, hit = match_one(v, by_norm, city_prefix=False)
        match.append({"vision": v, "status": st,
                      "oim_name": hit["name"] if hit else None,
                      "voltages": hit["voltages"] if hit else None})
    return {
        "stations": [{"name": s["name"], "key": norm_name(s["name"]),
                      "voltages": s["voltages"], "max_voltage": s["max_voltage"],
                      "lon": s["lon"], "lat": s["lat"]} for s in subs.values()],
        "lines": load_lines(db, cfg["extent"]),
        "match": match,
        "adjacency": [],
    }


def main() -> None:
    db = sqlite3.connect(GPKG)
    db.row_factory = sqlite3.Row
    out = {"provinces": {}}
    out["provinces"]["mengxi"] = {**PROVINCES["mengxi"], **build_mengxi(db)}
    out["provinces"]["guangxi"] = {**PROVINCES["guangxi"], **build_guangxi(db)}
    out["provinces"]["mengdong"] = {**PROVINCES["mengdong"], **build_mengdong(db)}
    out["provinces"]["liaoning"] = {**PROVINCES["liaoning"], **build_liaoning(db)}
    out["provinces"]["heilongjiang"] = PROVINCES["heilongjiang"]
    for k, p in out["provinces"].items():
        if "match" in p:
            p["counts"] = {
                "stations": len(p["stations"]),
                "lines": len(p["lines"]),
                "ok": sum(1 for m in p["match"] if m["status"] == "OK"),
                "collapsed": sum(1 for m in p["match"] if m["status"] == "COLLAPSED"),
                "missing": sum(1 for m in p["match"] if m["status"] == "MISSING"),
                "adjacency": len(p["adjacency"]),
            }
    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    for k, p in out["provinces"].items():
        print(k, p.get("counts", "pending"))


if __name__ == "__main__":
    sys.exit(main())
