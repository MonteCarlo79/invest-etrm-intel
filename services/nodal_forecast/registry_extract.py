# services/nodal_forecast/registry_extract.py
"""Extract the BESS asset registry draft from md_* dispatch data.
Capacity ≈ max |cleared_energy_mwh| × 4 (15-min → MW); duration ≈ longest
run of same-sign intervals ÷ 4. Zone guessed from plant-name prefix."""
from __future__ import annotations

from collections import defaultdict
from datetime import date

_ZONE_PREFIX = {"远景": "—", "谷山梁": "乌兰察布", "苏尼特": "锡林郭勒",
                "四子王": "乌兰察布", "杭锦旗": "鄂尔多斯", "乌尔图": "锡林郭勒",
                "巴盟": "巴彦淖尔", "乌梁素海": "巴彦淖尔"}

def extract_bess_plants(rows: list[dict]) -> list[dict]:
    by_plant: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if r["cleared_energy_mwh"] is not None:
            by_plant[r["plant_name"]].append(r)
    out = []
    for plant, rs in sorted(by_plant.items()):
        cap = max(abs(r["cleared_energy_mwh"]) for r in rs) * 4
        series = sorted(rs, key=lambda r: (r["data_date"], r["datetime"]))
        best = cur = 0
        prev_sign = 0
        for r in series:
            v = r["cleared_energy_mwh"]
            s = 1 if v > 0 else (-1 if v < 0 else prev_sign)
            cur = cur + 1 if s == prev_sign and s != 0 else (1 if s != 0 else 0)
            best = max(best, cur)
            prev_sign = s
        dur = round(best / 4, 1) if best else None
        zone = next((z for p, z in _ZONE_PREFIX.items() if plant.startswith(p)), None)
        dates = [r["data_date"] for r in rs]
        out.append(dict(plant_name=plant, capacity_mw_est=round(cap, 1),
                        duration_h_est=dur, zone_guess=zone,
                        first_seen=min(dates), last_seen=max(dates)))
    return out

def write_registry_md(plants: list[dict], path) -> None:
    lines = ["# BESS Asset Registry — DRAFT (REVIEW REQUIRED before DB seed)",
             "",
             "Each row needs: node (Fengxing node_name or 母线), substation,",
             "substation_cap_mw, zone, settle_node. Edit inline; do not rename plant_name.",
             "",
             "| plant_name | capacity_mw_est | duration_h_est | zone_guess | node (FILL) | substation (FILL) | cap_mw (FILL) |",
             "|---|---:|---:|---|---|---|---|"]
    for p in plants:
        lines.append(f"| {p['plant_name']} | {p['capacity_mw_est']} | {p['duration_h_est'] or '—'} "
                     f"| {p['zone_guess'] or '—'} |  |  |  |")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
