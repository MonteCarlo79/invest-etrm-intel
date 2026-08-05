"""Map invoice folder names to asset names in rm_assets."""
from __future__ import annotations

# Folder name → rm_assets.name
FOLDER_ASSET_MAP: dict[str, str | None] = {
    "B-8 内蒙杭锦旗": "悦杭独贵",
    "B-7 内蒙乌拉特": "远景乌拉特",
    "B-11 内蒙四子王旗": "四子王旗",
    "B-6 内蒙苏右": "景蓝乌尔图",
    "B-9 内蒙巴盟": "景怡查干哈达",
    "B-10谷山梁": "裕昭沙子坝",
    "B-1 乌兰察布": "景通四益堂",
    "B-【外】乌海储能": None,  # external asset, skip
}


def resolve_folder_to_asset(folder_name: str) -> str | None:
    """Resolve a folder name to an asset name.

    Tries exact match first, then partial match on the key.
    Returns None if no match or if the asset is marked as skip (None value).
    """
    # Exact match
    if folder_name in FOLDER_ASSET_MAP:
        return FOLDER_ASSET_MAP[folder_name]

    # Partial match (folder_name contains key or key contains folder_name)
    for key, asset in FOLDER_ASSET_MAP.items():
        if key in folder_name or folder_name in key:
            return asset

    return None
