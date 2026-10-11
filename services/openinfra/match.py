"""网架图 name -> OIM substation matching, services copy.

Same algorithm as scripts/openinfra_preview_mp.py::match_one (kept in sync):
OK on exact/normalized hit; COLLAPSED via prefix-drop or >=3-char reverse
substring; fallback rejects non-grid entities and sub-220 kV stations.
"""
from __future__ import annotations

import re

_SUFFIX_RE = re.compile(r"(\d+千伏)?(升压站|开关站|变电站|换流站|电厂.*|电站|站)$|[（(].*?[）)]|（分）|（II）|[ⅠⅡ]+|#.*$")
_PREFIX_RE = re.compile(r"^(±?\d+kV|\d+千伏)")
_NON_GRID_RE = re.compile(r"(管理局|管理分局|公司|政府|学校|医院|园区管委会|牵引)")

ALIASES = {"包北": "包头北", "塔拉": "塔拉", "乌海": "乌海",
           "保定": "雄安", "黑换": "黑河"}


def norm_name(name: str) -> str:
    n = (name or "").strip()
    n = _PREFIX_RE.sub("", n).strip()
    n = _SUFFIX_RE.sub("", n).strip()
    return ALIASES.get(n, n)


def match_one(name: str, by_norm: dict) -> tuple[str, dict | None]:
    keys = [norm_name(name)]

    def ok_fallback(s):
        return not _NON_GRID_RE.search(s["name"]) and (s["max_voltage"] or 0) >= 220000

    for k in keys:
        if k in by_norm:
            return "OK", by_norm[k]
    for k in keys:
        if not k:
            continue
        cands = [kk for kk in by_norm if k in kk and ok_fallback(by_norm[kk])]
        if cands:
            return "COLLAPSED", by_norm[min(cands, key=len)]
        cands = [kk for kk in by_norm if len(kk) >= 3 and kk in k and ok_fallback(by_norm[kk])]
        if cands:
            return "COLLAPSED", by_norm[max(cands, key=len)]
    return "MISSING", None


def match_vision(vision: list[str], subs_df) -> dict[str, dict]:
    """vision station list x staging substations ->
    {oim_name: {"vision": short_name, "status": OK|COLLAPSED}}.
    MISSING entries carry no coordinates, so they are not returned."""
    by_norm = {}
    for r in subs_df.itertuples():
        key = norm_name(r.name)
        if key and (key not in by_norm or (r.max_voltage or 0) > (by_norm[key]["max_voltage"] or 0)):
            by_norm[key] = {"name": r.name, "max_voltage": r.max_voltage}
    out = {}
    for v in vision:
        st, hit = match_one(v, by_norm)
        if hit is not None:
            out[hit["name"]] = {"vision": v, "status": st}
    return out
