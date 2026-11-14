"""Diff the registry-derived asset identity frame against the legacy maps.

Usage: PGURL=postgresql://... python scripts/asset_risk/probe_identity.py
Expected exceptions: 阿拉善储能 capcomp 350→280, 武川 (new), 阿拉善储能 (new),
and the NULL-code books. Any other line is a defect.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402

from libs.asset_risk.diagnostics import AUTO_NOM_CODE, CAPCOMP_RATE, CAPCOMP_RATE_DEFAULT, PLANT_MAP  # noqa: E402
from libs.asset_risk.registry import (  # noqa: E402
    asset_identity, capcomp_rate_map, nom_code_map, plant_map, rm_name_map,
)
from libs.trading_book.waterfall import _ATTR_CODE_ALIAS  # noqa: E402


def _optional(modpath: str, attr: str):
    try:
        mod = __import__(modpath, fromlist=[attr])
        return getattr(mod, attr)
    except Exception as exc:  # noqa: BLE001 - probe must keep going
        print(f"[skip] {modpath}.{attr} unavailable: {exc}")
        return None


# Seeded codes that are new to the derived maps (not in the legacy six-station maps)
NEW_NAMES = {"武川", "阿拉善储能", "零碳46风电", "乌海康富"}
ALASHAN_RATE_NOTE = "EXPECTED (user decision 2026-11-14: 阿拉善 rate 280)"


def _map_diff(label, legacy, new, out, by_value=False):
    """Generic key diff: new-only is EXPECTED for NEW_NAMES keys; all else DEFECT."""
    for k in sorted(set(legacy) | set(new), key=str):
        if k not in new:
            out.append(("DEFECT", f"{label}: {k} (legacy only: {legacy[k]!r})"))
        elif k not in legacy:
            tag = "EXPECTED" if (new[k] if by_value else k) in NEW_NAMES else "DEFECT"
            out.append((tag, f"{label}: {k} (new)"))
        elif legacy[k] != new[k]:
            out.append(("DEFECT", f"{label}: {k} {legacy[k]!r}→{new[k]!r}"))


def classify_diffs(df, legacy: dict) -> list[tuple[str, str]]:
    """Return (tag, line) for every difference; tag is 'EXPECTED' or 'DEFECT'.

    legacy keys: plant_map {name: plant}, nom_code {name: code}, capcomp {name: effective rate},
    rm_name {code: name}, folder_map {folder: name|None}, attr_alias {code: code}.
    """
    out: list[tuple[str, str]] = []
    null_names = [r.name for r in df.itertuples() if not r.asset_code]
    if null_names:
        out.append(("EXPECTED", f"NULL asset_code (non-registry books): {', '.join(null_names)}"))
    _map_diff("PLANT_MAP", legacy["plant_map"], {k: v[0] for k, v in plant_map(df).items() if v}, out)
    _map_diff("AUTO_NOM_CODE", legacy["nom_code"], nom_code_map(df), out)
    _map_diff("_RM_ASSET_NAME", legacy["rm_name"], rm_name_map(df), out, by_value=True)
    _map_diff("_ATTR_CODE_ALIAS", legacy["attr_alias"], legacy.get("attr_new", legacy["attr_alias"]), out)
    for name, rate in capcomp_rate_map(df).items():
        old = legacy["capcomp"].get(name, CAPCOMP_RATE_DEFAULT)
        if old == rate:
            continue
        if name == "阿拉善储能" and old == 350.0 and rate == 280.0:
            out.append(("EXPECTED", f"CAPCOMP_RATE: 阿拉善储能 capcomp 350→280  {ALASHAN_RATE_NOTE}"))
        else:
            out.append(("DEFECT", f"CAPCOMP_RATE: {name} {old!r}→{rate!r}"))
    coded = {r.name for r in df.itertuples() if r.asset_code}
    db_folder = {r.invoice_folder: r.name for r in df.itertuples() if r.invoice_folder}
    for folder, name in legacy["folder_map"].items():
        if name is None:
            out.append(("EXPECTED", f"folder_mapper: {folder} explicit skip (None)"))
        elif name not in coded:
            out.append(("EXPECTED", f"folder_mapper: {folder} -> {name} (no registry code)"))
        elif folder in db_folder and db_folder[folder] != name:
            out.append(("DEFECT", f"folder_mapper: {folder} fallback {name!r} vs DB {db_folder[folder]!r}"))
    return out


def main() -> int:
    pgurl = os.environ.get("PGURL")
    if not pgurl:
        print("PGURL not set", file=sys.stderr)
        return 2
    df = asset_identity(create_engine(pgurl))
    with pd.option_context("display.width", 200, "display.max_columns", None):
        print(df)
    from libs.trading_book.registry import get_stations
    legacy = {
        "plant_map": PLANT_MAP, "nom_code": AUTO_NOM_CODE,
        "capcomp": {n: CAPCOMP_RATE.get(n, CAPCOMP_RATE_DEFAULT) for n in df["name"]},
        "rm_name": _optional("services.mengxi_nodal.settlement_bridge", "_RM_ASSET_NAME") or {},
        "folder_map": _optional("services.settlement_ingest.folder_mapper", "_FALLBACK_MAP") or {},
        "attr_alias": _ATTR_CODE_ALIAS,
        "attr_new": {s.asset_code: s.attribution_code for s in get_stations() if s.attribution_code},
    }
    print("\n-- diff vs legacy --")
    results = classify_diffs(df, legacy)
    for tag, line in results:
        print(f"{tag}: {line}" if tag == "DEFECT" else f"{tag}  {line}")
    n_def = sum(t == "DEFECT" for t, _ in results)
    print(f"PROBE: {len(results) - n_def} expected, {n_def} defect")
    return 1 if n_def else 0


if __name__ == "__main__":
    raise SystemExit(main())
