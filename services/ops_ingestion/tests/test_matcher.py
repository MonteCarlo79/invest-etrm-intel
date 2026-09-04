"""
tests/test_matcher.py

Unit tests for services/ops_ingestion/inner_mongolia/matcher.py

Covers:
  - All 4 known asset sheets match correctly (exact)
  - Full-width brackets （）normalised to ASCII ()
  - Partial bracket match
  - Nickname-only match (no bracket)
  - siziwangqi not confused with wulanchabu (disambiguation)
  - Unmatched sheet returns method='unmatched', asset_code=None
  - load_asset_map(engine=None) returns static map
"""
from __future__ import annotations

import sys
import os
import pytest

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')))

from inner_mongolia.matcher import (
    match_sheet,
    load_asset_map,
    STATIC_ASSET_SHEET_MAP,
    _normalise_brackets,
    _split_nickname_bracket,
)


# ---------------------------------------------------------------------------
# _normalise_brackets
# ---------------------------------------------------------------------------

class TestNormaliseBrackets:
    def test_fullwidth_to_ascii(self):
        assert _normalise_brackets('苏右（景蓝乌尔图）') == '苏右(景蓝乌尔图)'

    def test_ascii_unchanged(self):
        assert _normalise_brackets('苏右(景蓝乌尔图)') == '苏右(景蓝乌尔图)'

    def test_no_brackets_unchanged(self):
        assert _normalise_brackets('苏右') == '苏右'


# ---------------------------------------------------------------------------
# _split_nickname_bracket
# ---------------------------------------------------------------------------

class TestSplitNicknameBracket:
    def test_standard_split(self):
        nick, bracket = _split_nickname_bracket('苏右(景蓝乌尔图)')
        assert nick == '苏右'
        assert bracket == '景蓝乌尔图'

    def test_no_bracket_returns_nickname_none(self):
        nick, bracket = _split_nickname_bracket('苏右')
        assert nick == '苏右'
        assert bracket is None

    def test_empty_nickname(self):
        nick, bracket = _split_nickname_bracket('(景蓝乌尔图)')
        assert nick is None
        assert bracket == '景蓝乌尔图'


# ---------------------------------------------------------------------------
# match_sheet — all 4 known assets
# ---------------------------------------------------------------------------

class TestMatchSheetKnownAssets:
    @pytest.mark.parametrize("sheet_name, expected_code", [
        ('苏右（景蓝乌尔图）',    'suyou'),
        ('杭锦旗（悦杭独贵）',    'hangjinqi'),
        ('四子王旗（景通四益堂储）', 'siziwangqi'),
        ('谷山梁（裕昭沙子坝）',   'gushanliang'),
    ])
    def test_exact_match(self, sheet_name, expected_code):
        result = match_sheet(sheet_name)
        assert result.asset_code == expected_code
        assert result.match_method == 'exact'

    def test_exact_match_dispatch_unit_suyou(self):
        result = match_sheet('苏右（景蓝乌尔图）')
        assert result.dispatch_unit_name == '景蓝乌尔图储能电站'

    def test_exact_match_ascii_brackets(self):
        result = match_sheet('苏右(景蓝乌尔图)')
        assert result.asset_code == 'suyou'
        assert result.match_method == 'exact'


# ---------------------------------------------------------------------------
# match_sheet — partial / nickname matching
# ---------------------------------------------------------------------------

class TestMatchSheetPartial:
    def test_partial_bracket_match(self):
        # Truncated bracket name still matches
        result = match_sheet('谷山梁（裕昭沙子坝储能）')
        assert result.asset_code == 'gushanliang'
        assert result.match_method == 'partial'

    def test_nickname_only_match(self):
        result = match_sheet('苏右')
        assert result.asset_code == 'suyou'
        assert result.match_method == 'nickname'


# ---------------------------------------------------------------------------
# match_sheet — disambiguation (siziwangqi vs wulanchabu)
# ---------------------------------------------------------------------------

class TestMatchSheetDisambiguation:
    def test_siziwangqi_matched_by_nickname(self):
        result = match_sheet('四子王旗（景通四益堂储）')
        assert result.asset_code == 'siziwangqi'

    def test_unknown_nickname_with_similar_bracket_is_unmatched(self):
        # A different nickname + a bracket that doesn't appear in the map
        result = match_sheet('乌兰察布（景通四益堂储）')
        # The bracket partially matches siziwangqi, but nickname 乌兰察布 != 四子王旗
        # → should not match siziwangqi; may return unmatched
        # The key assertion: must NOT return siziwangqi
        assert result.asset_code != 'siziwangqi' or result.asset_code is None


# ---------------------------------------------------------------------------
# match_sheet — unmatched sheet
# ---------------------------------------------------------------------------

class TestMatchSheetUnmatched:
    def test_unknown_sheet(self):
        result = match_sheet('总览')
        assert result.asset_code is None
        assert result.match_method == 'unmatched'

    def test_summary_sheet(self):
        result = match_sheet('汇总')
        assert result.asset_code is None
        assert result.match_method == 'unmatched'

    def test_sheet_name_preserved(self):
        result = match_sheet('未知资产（测试）')
        assert result.sheet_name == '未知资产（测试）'


# ---------------------------------------------------------------------------
# load_asset_map
# ---------------------------------------------------------------------------

class TestLoadAssetMap:
    def test_no_engine_returns_static(self):
        result = load_asset_map(engine=None)
        assert result is STATIC_ASSET_SHEET_MAP

    def test_static_map_has_4_assets(self):
        result = load_asset_map(engine=None)
        assert len(result) == 4

    def test_static_map_asset_codes(self):
        codes = {m.asset_code for m in load_asset_map(engine=None)}
        assert codes == {'suyou', 'hangjinqi', 'siziwangqi', 'gushanliang'}

    def test_broken_engine_falls_back_to_static(self):
        class BrokenEngine:
            def connect(self):
                raise RuntimeError("No DB")
        result = load_asset_map(engine=BrokenEngine())
        assert result is STATIC_ASSET_SHEET_MAP
