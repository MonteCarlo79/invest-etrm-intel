"""GeoPackage geometry decode (GeoPackageBinary header + WKB), no shapely.

Moved from scripts/openinfra_preview.py — that script remains the preview-page
builder; this module is the runtime copy for services (RDS extraction).
"""
from __future__ import annotations

import struct

_ENV_SIZES = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}


def read_gpb_wkb(gpb: bytes) -> bytes | None:
    """GeoPackageBinary -> WKB payload (skip GP header + envelope)."""
    if gpb[:2] != b"GP":
        return None
    env_type = (gpb[3] >> 1) & 0x07
    return gpb[8 + _ENV_SIZES.get(env_type, 0):]


def parse_wkb_coords(wkb: bytes) -> list[list[tuple[float, float]]]:
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
            if struct.unpack_from(sub_endian + "I", wkb, off + 1)[0] != 2:
                break
            pts, off = read_ls(off + 5)
            out.append(pts)
        return out
    return []


def downsample(path: list, max_pts: int = 100) -> list:
    if len(path) <= max_pts:
        return path
    step = len(path) / max_pts
    return [path[int(i * step)] for i in range(max_pts)]
