"""Regression tests for the null-amount guard (灵山 2025-04 incident).

A vision-parsed section header ("现货结算-其他费用", all numerics null) reached
the rm_settlement_items INSERT with amount_cny=None, violating NOT NULL and
rolling back the entire file's transaction. Two defenses:
1. parser_vision._row_has_no_numbers — header rows never become items
2. scanner.split_insertable — any residual None-amount row is dropped before insert
"""
from services.settlement_ingest.parser_vision import _row_has_no_numbers
from services.settlement_ingest.scanner import split_insertable


class TestRowHasNoNumbers:
    def test_all_nulls_is_header(self):
        # The exact 灵山 2025-04 failing row shape
        row = {"item_cn": "现货结算-其他费用", "volume": None, "volume_unit": None,
               "price": None, "price_unit": None, "amount_cny": None, "month": "2025-04"}
        assert _row_has_no_numbers(row) is True

    def test_missing_keys_count_as_null(self):
        assert _row_has_no_numbers({"item_cn": "某章节标题"}) is True

    def test_zero_amount_is_a_number(self):
        # Real 0.00 fee lines (基数电费) must survive
        assert _row_has_no_numbers({"volume": None, "price": None, "amount_cny": 0.0}) is False

    def test_volume_alone_is_not_header(self):
        assert _row_has_no_numbers({"volume": 12322.2, "price": None, "amount_cny": None}) is False

    def test_full_row_is_not_header(self):
        assert _row_has_no_numbers({"volume": 12322.2, "price": 351.9, "amount_cny": 4336577.2}) is False


class TestSplitInsertable:
    def test_drops_none_amount_keeps_rest(self):
        items = [
            {"category": "discharge_energy", "volume_mwh": 12322.2, "amount_cny": 4336577.2,
             "notes": "放电结算: 区内电能量电费"},
            {"category": "other", "volume_mwh": None, "amount_cny": None,
             "notes": "放电结算: 现货结算-其他费用"},
        ]
        valid, dropped = split_insertable(items)
        assert len(valid) == 1 and valid[0]["category"] == "discharge_energy"
        assert len(dropped) == 1 and dropped[0]["notes"] == "放电结算: 现货结算-其他费用"

    def test_zero_amount_is_insertable(self):
        items = [{"category": "other", "amount_cny": 0.0, "notes": "放电结算: 基数电费"}]
        valid, dropped = split_insertable(items)
        assert len(valid) == 1 and not dropped

    def test_missing_amount_key_is_dropped(self):
        valid, dropped = split_insertable([{"category": "other", "notes": "x"}])
        assert not valid and len(dropped) == 1

    def test_empty_input(self):
        assert split_insertable([]) == ([], [])
