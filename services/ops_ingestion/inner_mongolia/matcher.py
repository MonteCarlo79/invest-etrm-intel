"""
services/ops_ingestion/inner_mongolia/matcher.py

Match Excel sheet names to BESS asset codes and dispatch unit names.

Sheet name format: <nickname>（<bracket_name>）
  Full-width brackets （）are normalised to ASCII () before matching.
  Example: '苏右（景蓝乌尔图）' → nickname='苏右', bracket='景蓝乌尔图'

Disambiguation: two assets share the same bracket name root; use the nickname
to tell them apart.  See STATIC_ASSET_SHEET_MAP for the authoritative list.

Pluggable design
----------------
load_asset_map(engine=None) is the single entry point for callers.
  - When engine is provided, first tries marketdata.ops_asset_sheet_config.
  - Falls back to STATIC_ASSET_SHEET_MAP if DB table is absent or empty.
  - Callers never need to import STATIC_ASSET_SHEET_MAP directly.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional


# ---------------------------------------------------------------------------
# Asset map dataclass
# ---------------------------------------------------------------------------

@dataclass
class AssetSheetMapping:
    nickname_cn: str            # text before bracket, e.g. '苏右'
    bracket_cn: str             # text in bracket, e.g. '景蓝乌尔图'
    asset_code: str             # internal identifier, e.g. 'suyou'
    dispatch_unit_name: str     # full CN name in md_id_cleared_energy
    plant_name: Optional[str] = None


# ---------------------------------------------------------------------------
# Static asset map
# ---------------------------------------------------------------------------

STATIC_ASSET_SHEET_MAP: List[AssetSheetMapping] = [
    AssetSheetMapping(
        nickname_cn='苏右',
        bracket_cn='景蓝乌尔图',
        asset_code='suyou',
        dispatch_unit_name='景蓝乌尔图储能电站',
    ),
    AssetSheetMapping(
        nickname_cn='杭锦旗',
        bracket_cn='悦杭独贵',
        asset_code='hangjinqi',
        dispatch_unit_name='悦杭独贵储能电站',
    ),
    AssetSheetMapping(
        nickname_cn='四子王旗',
        bracket_cn='景通四益堂储',
        asset_code='siziwangqi',
        dispatch_unit_name='景通四益堂储能电站',
    ),
    AssetSheetMapping(
        nickname_cn='谷山梁',
        bracket_cn='裕昭沙子坝',
        asset_code='gushanliang',
        dispatch_unit_name='裕昭沙子坝储能电站',
    ),
]


# ---------------------------------------------------------------------------
# Match result
# ---------------------------------------------------------------------------

@dataclass
class MatchResult:
    sheet_name: str
    asset_code: Optional[str]           # None if unmatched
    dispatch_unit_name: Optional[str]   # None if unmatched
    plant_name: Optional[str]
    match_method: str                   # 'exact' | 'partial' | 'nickname' | 'db_config' | 'unmatched'
    nickname_cn: Optional[str] = None
    bracket_cn: Optional[str] = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_asset_map(engine=None) -> List[AssetSheetMapping]:
    """
    Return the active asset→sheet mapping.

    When engine is provided, first checks marketdata.ops_asset_sheet_config;
    falls back to STATIC_ASSET_SHEET_MAP if table is absent or empty.
    This allows the map to move into a governed DB table later without
    changing parser or matcher call sites.
    """
    if engine is not None:
        try:
            rows = _load_from_db(engine)
            if rows:
                return rows
        except Exception:
            pass   # DB table not yet created — fall through to static
    return STATIC_ASSET_SHEET_MAP


def match_sheet(sheet_name: str, asset_map: Optional[List[AssetSheetMapping]] = None) -> MatchResult:
    """
    Match a sheet name to an AssetSheetMapping.

    Parameters
    ----------
    sheet_name : str
        The raw sheet name from the workbook (e.g. '苏右（景蓝乌尔图）').
    asset_map : list | None
        Asset map to use.  Defaults to STATIC_ASSET_SHEET_MAP.

    Returns
    -------
    MatchResult
        match_method='unmatched' and asset_code=None if no match found.
    """
    if asset_map is None:
        asset_map = STATIC_ASSET_SHEET_MAP

    # Normalise: full-width brackets → ASCII; strip whitespace
    normalised = _normalise_brackets(sheet_name.strip())

    # Try to extract nickname and bracket text
    nickname, bracket = _split_nickname_bracket(normalised)

    # Strategy 1: exact match on (nickname, bracket)
    if nickname and bracket:
        for mapping in asset_map:
            if (mapping.nickname_cn == nickname and mapping.bracket_cn == bracket):
                return MatchResult(
                    sheet_name=sheet_name,
                    asset_code=mapping.asset_code,
                    dispatch_unit_name=mapping.dispatch_unit_name,
                    plant_name=mapping.plant_name,
                    match_method='exact',
                    nickname_cn=nickname,
                    bracket_cn=bracket,
                )

    # Strategy 2: partial bracket match (bracket text appears in mapping.bracket_cn or vice versa)
    if bracket:
        for mapping in asset_map:
            if (bracket in mapping.bracket_cn or mapping.bracket_cn in bracket):
                # Disambiguation: if nickname is available, require it to match too
                if nickname and mapping.nickname_cn and nickname != mapping.nickname_cn:
                    continue
                return MatchResult(
                    sheet_name=sheet_name,
                    asset_code=mapping.asset_code,
                    dispatch_unit_name=mapping.dispatch_unit_name,
                    plant_name=mapping.plant_name,
                    match_method='partial',
                    nickname_cn=nickname,
                    bracket_cn=bracket,
                )

    # Strategy 3: nickname-only match (no bracket present in sheet name)
    if nickname:
        for mapping in asset_map:
            if mapping.nickname_cn == nickname:
                return MatchResult(
                    sheet_name=sheet_name,
                    asset_code=mapping.asset_code,
                    dispatch_unit_name=mapping.dispatch_unit_name,
                    plant_name=mapping.plant_name,
                    match_method='nickname',
                    nickname_cn=nickname,
                    bracket_cn=bracket,
                )

    return MatchResult(
        sheet_name=sheet_name,
        asset_code=None,
        dispatch_unit_name=None,
        plant_name=None,
        match_method='unmatched',
        nickname_cn=nickname,
        bracket_cn=bracket,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalise_brackets(s: str) -> str:
    """Replace full-width parentheses （）with ASCII ()."""
    return s.replace('（', '(').replace('）', ')')


def _split_nickname_bracket(normalised: str):
    """
    Split '苏右(景蓝乌尔图)' into ('苏右', '景蓝乌尔图').
    Returns (None, None) if no bracket pattern found.
    Returns (None, bracket) if no text before bracket.
    Returns (nickname, None) if no bracket present.
    """
    m = re.match(r'^(.*?)\((.+?)\)\s*$', normalised)
    if m:
        nickname = m.group(1).strip() or None
        bracket = m.group(2).strip() or None
        return nickname, bracket
    # No bracket — whole string is the nickname candidate
    return normalised.strip() or None, None


def _load_from_db(engine) -> List[AssetSheetMapping]:
    """
    Load asset sheet mappings from marketdata.ops_asset_sheet_config.
    Returns empty list if table is absent or has no active rows.
    """
    from sqlalchemy import text
    sql = text("""
        SELECT asset_code, dispatch_unit_name, plant_name,
               sheet_nickname_cn, sheet_bracket_cn
        FROM marketdata.ops_asset_sheet_config
        WHERE active = TRUE
        ORDER BY asset_code
    """)
    with engine.connect() as conn:
        result = conn.execute(sql)
        rows = result.fetchall()

    return [
        AssetSheetMapping(
            nickname_cn=row.sheet_nickname_cn,
            bracket_cn=row.sheet_bracket_cn,
            asset_code=row.asset_code,
            dispatch_unit_name=row.dispatch_unit_name,
            plant_name=row.plant_name,
        )
        for row in rows
    ]
