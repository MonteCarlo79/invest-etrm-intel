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


def _diff(label: str, legacy: dict, new: dict) -> None:
    for k in sorted(set(legacy) | set(new), key=str):
        if k not in new:
            print(f"{label}: {k} (legacy only: {legacy[k]!r})")
        elif k not in legacy:
            print(f"{label}: {k} (new)")
        elif legacy[k] != new[k]:
            print(f"{label}: {k} {legacy[k]!r}→{new[k]!r}")


def main() -> int:
    pgurl = os.environ.get("PGURL")
    if not pgurl:
        print("PGURL not set", file=sys.stderr)
        return 2
    df = asset_identity(create_engine(pgurl))
    with pd.option_context("display.width", 200, "display.max_columns", None):
        print(df)
    print("\n-- diff vs legacy (exceptions only) --")
    for r in df[df.asset_code.isna()].itertuples():
        print(f"{r.name} (NULL asset_code)")
    # PLANT_MAP holds a single plant name; compare against the first registry name
    _diff("PLANT_MAP", PLANT_MAP, {k: v[0] for k, v in plant_map(df).items() if v})
    _diff("AUTO_NOM_CODE", AUTO_NOM_CODE, nom_code_map(df))
    # capcomp: legacy effective rate = override or default, for every identity name
    legacy_rate = {n: CAPCOMP_RATE.get(n, CAPCOMP_RATE_DEFAULT) for n in df["name"]}
    _diff("CAPCOMP_RATE", legacy_rate, capcomp_rate_map(df))
    bridge = _optional("services.mengxi_nodal.settlement_bridge", "_RM_ASSET_NAME")
    if bridge is not None:
        _diff("_RM_ASSET_NAME", bridge, rm_name_map(df))
    fm = _optional("services.settlement_ingest.folder_mapper", "_FALLBACK_MAP")
    if fm is not None:
        legacy_f = {k: v for k, v in fm.items()}
        new_f = {r.invoice_folder: r.name for r in df.itertuples() if r.invoice_folder}
        _diff("folder_mapper._FALLBACK_MAP", legacy_f, new_f)
    from libs.trading_book.registry import get_stations
    _diff("_ATTR_CODE_ALIAS", _ATTR_CODE_ALIAS,
          {s.asset_code: s.attribution_code for s in get_stations() if s.attribution_code})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
