# scripts/openinfra_preview.py
"""Extract Mengxi grid preview data from the OpenInfraMap CHN.gpkg purchase.

Outputs preview_data.json next to the gpkg with:
  - substations: named stations in the 蒙西 region (rtree centroid, voltages)
  - lines_500kv: 500kV+ line paths (WKB-decoded, downsampled)
  - vision_match: vision-map 500kV stations x OIM match status
  - oim_extras: named 500kV+ OIM stations NOT in the vision map

GeoPackage geometry = GP header + WKB; decoded with struct (no shapely in env).
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
import struct
import sys
from pathlib import Path

GPKG = Path("data/nodal/open-infra-map/CHN.gpkg")
OUT = Path("data/nodal/open-infra-map/preview_data.json")

# 蒙西 (Inner Mongolia western grid) working extent: 阿拉善 .. 锡林郭勒
LON_MIN, LON_MAX = 100.0, 117.5
LAT_MIN, LAT_MAX = 37.0, 44.8

_SUFFIX_RE = re.compile(r"(\d+千伏)?(升压站|开关站|变电站|换流站|电厂.*|电站|站)$|[（(].*?[）)]|（分）|（II）|[ⅠⅡ]+|#.*$")
_PREFIX_RE = re.compile(r"^(±?\d+kV|\d+千伏)")

# vision-map abbreviations -> full OIM names
ALIASES = {"包北": "包头北", "塔拉": "塔拉", "乌海": "乌海",
           "保定": "雄安", "黑换": "黑河"}


def norm_name(name: str) -> str:
    """谷山梁500千伏变电站 / 500kV鸿沁湖变电站 / 苏尼特（分） -> 谷山梁 / 鸿沁湖 / 苏尼特."""
    n = (name or "").strip()
    n = _PREFIX_RE.sub("", n).strip()
    n = _SUFFIX_RE.sub("", n).strip()
    return ALIASES.get(n, n)


def match_name(vision: str, keys: dict) -> str | None:
    """Vision station -> OIM key: exact normalized, then substring both ways."""
    v = norm_name(vision)
    if v in keys:
        return v
    cands = [k for k in keys if v and (v in k or k in v)]
    return min(cands, key=len) if cands else None


def _read_gpb_wkb(gpb: bytes):
    """GeoPackageBinary -> WKB payload (skip GP header)."""
    if gpb[:2] != b"GP":
        return None
    flags = gpb[3]
    env_type = (flags >> 1) & 0x07
    header = 8 + [0, 32, 48, 48, 64, 64][env_type] if env_type else 8
    # envelope sizes: 0=0, 1=32, 2/3=48, 4=64
    env_sizes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}
    header = 8 + env_sizes.get(env_type, 0)
    return gpb[header:]


def _parse_wkb_coords(wkb: bytes):
    """LineString/MultiLineString WKB -> list of paths, each [(lon,lat),...]."""
    if not wkb:
        return []
    endian = "<" if wkb[0] == 1 else ">"
    gtype = struct.unpack_from(endian + "I", wkb, 1)[0]

    def read_ls(off):
        n = struct.unpack_from(endian + "I", wkb, off)[0]
        off += 4
        pts = [struct.unpack_from(endian + "dd", wkb, off + 16 * i) for i in range(n)]
        return pts, off + 16 * n

    if gtype == 2:  # LineString
        pts, _ = read_ls(5)
        return [pts]
    if gtype == 5:  # MultiLineString
        n = struct.unpack_from(endian + "I", wkb, 5)[0]
        off = 9
        out = []
        for _ in range(n):
            sub_endian = "<" if wkb[off] == 1 else ">"
            assert struct.unpack_from(sub_endian + "I", wkb, off + 1)[0] == 2
            pts, off = read_ls(off + 5)
            out.append(pts)
        return out
    return []


def _downsample(path: list, max_pts: int = 200) -> list:
    if len(path) <= max_pts:
        return path
    step = len(path) / max_pts
    return [path[int(i * step)] for i in range(max_pts)]


def _region_clause(r_alias: str) -> str:
    return (f"{r_alias}.minx >= {LON_MIN} AND {r_alias}.maxx <= {LON_MAX} "
            f"AND {r_alias}.miny >= {LAT_MIN} AND {r_alias}.maxy <= {LAT_MAX}")


VISION_500KV = [
    "河套", "乌后旗", "阿拉腾", "定远营", "吉兰太", "金湖", "祥泰", "千里山",
    "乌海", "沙井", "谷山梁", "响沙湾", "耳字壕", "布日都", "甘迪尔", "芒哈图",
    "阿勒泰", "高新", "土默特", "永圣域", "常胜", "渡口", "宁格尔", "春坤山",
    "包北", "百灵", "克仁珠", "威俊", "昆都仑", "英华", "开林河", "武川",
    "红梁", "旗下营", "赛罕", "瑞升", "察右中", "苏尼特", "涌泉", "努如",
    "汗海", "德义", "白音高勒", "苏敦", "庆云", "灰腾梁", "塔拉", "巨宝庄",
    "丰泉", "鸿沁湖", "敖瑞", "浩雅", "城川", "双井", "马兰", "鹰骏",
    "伊克昭换流站", "德岭山", "黄旗海", "杜尔伯特", "高新",
]


def main() -> None:
    db = sqlite3.connect(GPKG)
    db.row_factory = sqlite3.Row

    subs: dict[str, dict] = {}
    for table, rtree in (("power_substation_point", "rtree_power_substation_point_geometry"),
                         ("power_substation_polygon", "rtree_power_substation_polygon_geometry")):
        for row in db.execute(
                f"""SELECT p.name, p.voltages, p.max_voltage, p.operator,
                           (r.minx + r.maxx) / 2.0 AS lon, (r.miny + r.maxy) / 2.0 AS lat
                    FROM {table} p JOIN {rtree} r ON p.fid = r.id
                    WHERE {_region_clause('r')} AND p.name IS NOT NULL AND p.name != ''"""):
            key = norm_name(row["name"])
            if not key:
                continue
            cur = subs.get(key)
            # prefer the higher-voltage / polygon entry for display
            if cur is None or (row["max_voltage"] or 0) > (cur["max_voltage"] or 0):
                subs[key] = dict(row)

    hv_subs = [s for s in subs.values() if (s["max_voltage"] or 0) >= 220000]
    hv_subs.sort(key=lambda s: (-(s["max_voltage"] or 0), s["name"]))

    lines = []
    for row in db.execute(
            f"""SELECT p.geometry, p.name, p.voltages, p.max_voltage
                FROM power_line p JOIN rtree_power_line_geometry r ON p.fid = r.id
                WHERE {_region_clause('r')} AND p.max_voltage >= 500000"""):
        wkb = _read_gpb_wkb(row["geometry"])
        for path in _parse_wkb_coords(wkb):
            if len(path) >= 2:
                lines.append({"path": _downsample(path, 120),
                              "name": row["name"] or "",
                              "max_voltage": row["max_voltage"]})

    by_norm = {norm_name(s["name"]): s for s in subs.values()}
    vision_match = []
    for v in VISION_500KV:
        key = match_name(v, by_norm)
        hit = by_norm.get(key) if key else None
        if hit is None:
            vision_match.append({"vision": v, "status": "MISSING", "oim_name": None,
                                 "voltages": None, "lon": None, "lat": None})
        else:
            vision_match.append({"vision": v, "status": "OK", "oim_name": hit["name"],
                                 "voltages": hit["voltages"], "lon": hit["lon"], "lat": hit["lat"]})

    vision_norm = {norm_name(v) for v in VISION_500KV}
    extras = [s for s in hv_subs
              if norm_name(s["name"]) not in vision_norm and (s["max_voltage"] or 0) >= 500000]
    extras.sort(key=lambda s: s["name"])

    out = {
        "extent": {"lon_min": LON_MIN, "lon_max": LON_MAX, "lat_min": LAT_MIN, "lat_max": LAT_MAX},
        "substations": [
            {"name": s["name"], "key": norm_name(s["name"]), "voltages": s["voltages"],
             "max_voltage": s["max_voltage"], "operator": s["operator"],
             "lon": s["lon"], "lat": s["lat"]}
            for s in subs.values()
        ],
        "lines_500kv": lines,
        "vision_match": vision_match,
        "oim_extras": [{"name": s["name"], "voltages": s["voltages"],
                        "lon": s["lon"], "lat": s["lat"]} for s in extras],
        "counts": {
            "named_substations_region": len(subs),
            "hv_named_substations_region": len(hv_subs),
            "lines_500kv_paths": len(lines),
            "vision_ok": sum(1 for m in vision_match if m["status"] == "OK"),
            "vision_missing": sum(1 for m in vision_match if m["status"] == "MISSING"),
            "extras_500kv": len(extras),
        },
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(out["counts"], ensure_ascii=False, indent=1))


if __name__ == "__main__":
    sys.exit(main())
