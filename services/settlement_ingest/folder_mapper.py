"""Map invoice folder names to asset names in rm_assets.

Reads mapping from rm_assets.invoice_folder column (configurable via UI).
Falls back to hardcoded map if DB is unavailable.
"""
from __future__ import annotations

# Fallback hardcoded map (used if DB unavailable)
_FALLBACK_MAP: dict[str, str | None] = {
    "B-8 内蒙杭锦旗": "悦杭独贵",
    "B-7 内蒙乌拉特": "远景乌拉特",
    "B-11 内蒙四子王旗": "四子王旗",
    "B-6 内蒙苏右": "景蓝乌尔图",
    "B-9 内蒙巴盟": "景怡查干哈达",
    "B-10谷山梁": "裕昭沙子坝",
    "B-1 乌兰察布": "乌兰察布储能",
    "B-【外】乌海储能": None,
}


def _load_map_from_db() -> dict[str, str]:
    """Load folder→asset mapping from rm_assets.invoice_folder."""
    try:
        from shared.agents.db import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT invoice_folder, name FROM marketdata.rm_assets WHERE invoice_folder IS NOT NULL")
                return {row[0]: row[1] for row in cur.fetchall()}
    except Exception:
        return {}


def resolve_folder_to_asset(folder_name: str) -> str | None:
    """Resolve a folder name to an asset name.

    Reads from DB (rm_assets.invoice_folder) first, falls back to hardcoded map.
    """
    # Load from DB
    db_map = _load_map_from_db()
    full_map = {**_FALLBACK_MAP, **db_map}

    # Exact match
    if folder_name in full_map:
        return full_map[folder_name]

    # Partial match
    for key, asset in full_map.items():
        if key and asset and (key in folder_name or folder_name in key):
            return asset

    return None
