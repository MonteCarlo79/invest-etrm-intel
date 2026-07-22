"""Map uploaded filenames to asset IDs.

Configuration-driven (not hardcoded). Patterns checked in order;
first match wins.
"""
from __future__ import annotations

import re
from typing import Any

FILENAME_PATTERNS: list[dict[str, Any]] = [
    {"pattern": r"零碳46|零碳46风电经营统计", "asset_name": "零碳46风电", "asset_type": "wind"},
    {"pattern": r"裕昭沙子坝|220kV裕昭", "asset_name": "裕昭沙子坝", "asset_type": "bess"},
    {"pattern": r"远景乌拉特", "asset_name": "远景乌拉特", "asset_type": "bess"},
    {"pattern": r"景怡查干哈达", "asset_name": "景怡查干哈达", "asset_type": "bess"},
    {"pattern": r"景通四益堂", "asset_name": "景通四益堂", "asset_type": "bess"},
    {"pattern": r"四子王旗", "asset_name": "四子王旗", "asset_type": "bess"},
    {"pattern": r"悦杭独贵", "asset_name": "悦杭独贵", "asset_type": "bess"},
    {"pattern": r"景蓝乌尔图", "asset_name": "景蓝乌尔图", "asset_type": "bess"},
]


def resolve_asset(filename: str) -> dict[str, str] | None:
    """Match filename to an asset entry.

    Args:
        filename: Original filename (e.g. '裕昭沙子坝_20260715.xlsx')

    Returns:
        Dict with asset_name and asset_type, or None if no match.
    """
    for entry in FILENAME_PATTERNS:
        if re.search(entry["pattern"], filename):
            return {"asset_name": entry["asset_name"], "asset_type": entry["asset_type"]}
    return None
