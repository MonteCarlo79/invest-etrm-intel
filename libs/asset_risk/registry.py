"""Asset identity frame derived from the trading-book registry.

One row per marketdata.rm_assets row; registry-derived fields (plant_names,
capcomp_rate) come from libs.trading_book.registry via rm_assets.asset_code.
Rows without asset_code get plant_names=() and the default capcomp rate.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import inspect, text

from libs.trading_book.registry import get_station

IDENTITY_COLUMNS = ["asset_id", "name", "asset_code", "asset_type", "plant_names", "capcomp_rate",
                    "invoice_folder", "capacity_mw", "bess_duration_h", "commission_date"]

# rm_assets.name -> registry asset_code, used by the migration UPDATE
SEED_CODES: dict[str, str] = {
    "悦杭独贵": "hangjinqi", "景怡查干哈达": "bameng", "景蓝乌尔图": "suyou", "裕昭沙子坝": "gushanliang",
    "远景乌拉特": "wulate", "四子王旗": "siziwangqi", "武川": "wuchuan", "阿拉善储能": "alashan",
    "零碳46风电": "lingt46", "乌海康富": "wuhai"}
CAPCOMP_DEFAULT = 350.0

# rm_assets columns passed through as-is (selected only if present in the table)
_PASSTHROUGH = ["asset_type", "invoice_folder", "capacity_mw", "bess_duration_h", "commission_date"]


def has_asset_code(engine) -> bool:
    """True when marketdata.rm_assets carries the asset_code column (added by the
    Task 9 migration; absent in production until it runs). Consumers fall back to
    their legacy literal maps when False."""
    cols = inspect(engine).get_columns("rm_assets", schema="marketdata")
    return any(c["name"] == "asset_code" for c in cols)


def asset_identity(engine) -> pd.DataFrame:
    raw = pd.read_sql(text("SELECT * FROM marketdata.rm_assets ORDER BY id"), engine)
    rows = []
    for r in raw.to_dict("records"):
        code = r.get("asset_code")
        code = code if isinstance(code, str) and code else None
        st = get_station(code) if code else None
        rate = st.subsidy_cny_per_mwh if st is not None and st.subsidy_cny_per_mwh is not None else CAPCOMP_DEFAULT
        row = {"asset_id": r["id"], "name": r["name"], "asset_code": code,
               "plant_names": tuple(st.cleared_names) if st is not None else (),
               "capcomp_rate": float(rate)}
        for c in _PASSTHROUGH:
            v = r.get(c)
            row[c] = None if v is not None and pd.isna(v) else v
        rows.append(row)
    # dtype=object keeps None (not NaN) for missing asset_code and tuples intact
    return pd.DataFrame(rows, columns=IDENTITY_COLUMNS, dtype=object).astype({"capcomp_rate": float})


def plant_map(identity: pd.DataFrame) -> dict[str, tuple[str, ...]]:
    return {r.name: r.plant_names for r in identity.itertuples() if r.asset_code}


def nom_code_map(identity: pd.DataFrame) -> dict[str, str]:
    return {r.name: r.asset_code for r in identity.itertuples() if r.asset_code}


def capcomp_rate_map(identity: pd.DataFrame) -> dict[str, float]:
    return {r.name: float(r.capcomp_rate) for r in identity.itertuples()}


def rm_name_map(identity: pd.DataFrame) -> dict[str, str]:
    return {r.asset_code: r.name for r in identity.itertuples() if r.asset_code}
