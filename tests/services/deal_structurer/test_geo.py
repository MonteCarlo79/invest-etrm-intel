"""Pure-helper tests for apps/deal_structurer/geo.py — province/node option logic."""
from apps.deal_structurer.geo import (
    FALLBACK_PROVINCES,
    NODE_MANUAL,
    NODE_NONE,
    node_select_state,
    normalize_province,
    province_options,
)


class TestNormalizeProvince:
    def test_exact_match_unchanged(self):
        assert normalize_province("山东") == "山东"

    def test_strips_sheng_suffix(self):
        assert normalize_province("山东省") == "山东"

    def test_strips_whitespace(self):
        assert normalize_province(" 蒙西 ") == "蒙西"

    def test_autonomous_region_no_false_match(self):
        # 内蒙古 could be 蒙西 or 蒙东 — never guess; return the stripped value
        assert normalize_province("内蒙古自治区") == "内蒙古"

    def test_empty(self):
        assert normalize_province("") == ""
        assert normalize_province(None) == ""


class TestProvinceOptions:
    def test_matched_draft_keeps_position(self):
        opts, idx = province_options(["蒙西", "山东"], "山东省")
        assert opts == ["蒙西", "山东"]
        assert idx == 1

    def test_unmatched_draft_prepended(self):
        opts, idx = province_options(["蒙西", "山东"], "内蒙古")
        assert opts == ["内蒙古", "蒙西", "山东"]
        assert idx == 0

    def test_empty_draft_defaults_first(self):
        opts, idx = province_options(["蒙西", "山东"], "")
        assert opts == ["蒙西", "山东"]
        assert idx == 0


class TestNodeSelectState:
    def test_options_shape(self):
        options, _, _ = node_select_state(["A", "B"], None)
        assert options == [NODE_NONE, "A", "B", NODE_MANUAL]

    def test_draft_in_nodes_preselected(self):
        options, idx, prefill = node_select_state(["节点A", "节点B"], "节点B")
        assert options[idx] == "节点B"
        assert prefill == ""

    def test_unknown_draft_goes_manual(self):
        options, idx, prefill = node_select_state(["节点A"], "自定义节点")
        assert options[idx] == NODE_MANUAL
        assert prefill == "自定义节点"

    def test_empty_draft_defaults_none(self):
        options, idx, prefill = node_select_state(["节点A"], None)
        assert options[idx] == NODE_NONE
        assert prefill == ""
