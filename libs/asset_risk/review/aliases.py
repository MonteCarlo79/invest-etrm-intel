"""Station alias map (Global Constraints). Single source: ic_case.py imports it from here.

Unknown names are reported by callers, never guessed.
"""
from __future__ import annotations

STATION_ALIASES: dict[str, list[str]] = {
    "suyou": ["苏右", "远景景蓝", "景蓝乌尔图"],
    "hangjinqi": ["杭锦旗", "悦杭独贵"],
    "siziwangqi": ["四子王旗", "四子王", "景通四益堂"],
    "gushanliang": ["谷山梁", "固山梁", "裕昭新能源", "裕昭沙子坝"],
    "wuhai": ["乌海", "乌海康富"],
    "wulate": ["乌拉特", "远景乌拉特"],
    "bameng": ["巴盟", "景怡查干哈达"],
    "wuchuan": ["武川"],
    "alashan": ["阿拉善"],
}


def asset_code_for(name: str) -> str | None:
    """Asset code of the longest alias that is a substring of `name`; None if no alias matches."""
    best, best_len = None, 0
    for code, aliases in STATION_ALIASES.items():
        for a in aliases:
            if a in (name or "") and len(a) > best_len:
                best, best_len = code, len(a)
    return best
