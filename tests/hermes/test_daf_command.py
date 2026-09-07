"""Tests for services/hermes/daf_command.py — /daf chat command (no DB, no API)."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import services.hermes.daf_command as daf


def _msg(text, source="feishu", sender="owner-open-id"):
    return SimpleNamespace(text=text, source=source, sender_id=sender)


class TestParseCompact:
    def test_full_form(self):
        b = daf.parse_compact("乌兰察布二期 | 蒙西 | 500/2000 | 12.5亿")
        assert b is not None
        assert b.deal_name == "乌兰察布二期"
        assert b.province == "蒙西"
        assert b.capacity_mw == 500.0 and b.capacity_mwh == 2000.0
        assert b.capex_total_yuan == 12.5e8
        assert b.asset_type == "bess" and b.confirmed

    def test_with_asset_type_and_defaults(self):
        b = daf.parse_compact("项目X | 山西 | 100/200 | 5 | wind_bess")
        assert b.asset_type == "wind_bess"
        assert b.capex_total_yuan == 5e8
        assert b.efficiency == 0.85 and b.debt_ratio == 0.70

    def test_rejects_malformed(self):
        assert daf.parse_compact("只有名称") is None
        assert daf.parse_compact("a | b | notcap | 5亿") is None
        assert daf.parse_compact("a | b | 500/2000") is None  # missing capex


class TestApplyOverride:
    def test_numeric_field(self):
        b = daf.parse_compact("X | 蒙西 | 500/2000 | 13亿")
        assert daf.apply_override(b, "效率=0.88") is not None
        assert b.efficiency == 0.88

    def test_capex_yi_scaling(self):
        b = daf.parse_compact("X | 蒙西 | 500/2000 | 13亿")
        daf.apply_override(b, "总投资=14亿")
        assert b.capex_total_yuan == 14e8

    def test_text_field(self):
        b = daf.parse_compact("X | 蒙西 | 500/2000 | 13亿")
        assert daf.apply_override(b, "节点:杜尔伯特220kV") is not None
        assert b.node == "杜尔伯特220kV"

    def test_rejects_unknown(self):
        b = daf.parse_compact("X | 蒙西 | 500/2000 | 13亿")
        assert daf.apply_override(b, "随便说句话") is None
        assert daf.apply_override(b, "未知字段=1") is None


class TestStateMachine:
    def setup_method(self):
        daf._pending.clear()
        self.env = patch.dict("os.environ",
                              {"FEISHU_OWNER_OPEN_ID": "owner-open-id",
                               "TELEGRAM_OWNER_CHAT_ID": "12345"})
        self.env.start()

    def teardown_method(self):
        self.env.stop()

    def _brief_row(self, name="谷山梁二期", province="蒙西"):
        return {"id": 1, "deal_name": name, "confirmed": True,
                "created_at": "2026-09-06 00:31",
                "brief": {"deal_name": name, "asset_type": "bess",
                          "province": province, "capacity_mw": 500.0,
                          "capacity_mwh": 2000.0, "capex_total_yuan": 13e8,
                          "confirmed": True},
                "result_id": None, "recommendation": None, "daf_id": None}

    def test_deny_non_owner_echoes_id(self):
        feishu = MagicMock()
        handled = daf.try_handle(_msg("/daf", sender="intruder"), feishu, None, "k")
        assert handled is True
        text = feishu.send_text.call_args.kwargs["text"]
        assert "intruder" in text and "仅限管理员" in text

    def test_pick_flow(self):
        feishu = MagicMock()
        with patch("services.common.db_utils.get_engine", return_value=MagicMock()), \
             patch("services.deal_committee.library.list_briefs",
                   return_value=[self._brief_row()]):
            assert daf.try_handle(_msg("/daf"), feishu, None, "k") is True
            assert "谷山梁二期" in feishu.send_text.call_args.kwargs["text"]

            # reply with number → confirm card
            assert daf.try_handle(_msg("1"), feishu, None, "k") is True
            card = feishu.send_text.call_args.kwargs["text"]
            assert "交易要素确认" in card and "蒙西" in card and "13.0亿" in card

    def test_compact_then_confirm_runs_committee(self):
        feishu = MagicMock()
        assert daf.try_handle(
            _msg("/daf 乌兰察布二期 | 蒙西 | 500/2000 | 12.5亿"), feishu, None, "k")
        card = feishu.send_text.call_args.kwargs["text"]
        assert "乌兰察布二期" in card and "500MW / 2000MWh" in card

        with patch.object(daf.threading, "Thread") as th:
            assert daf.try_handle(_msg("确认"), feishu, None, "k") is True
            th.assert_called_once()
            assert daf._pending.get("owner-open-id") is None  # state consumed

    def test_override_then_confirm(self):
        feishu = MagicMock()
        daf.try_handle(_msg("/daf X | 蒙西 | 500/2000 | 13亿"), feishu, None, "k")
        assert daf.try_handle(_msg("效率=0.9"), feishu, None, "k") is True
        card = feishu.send_text.call_args.kwargs["text"]
        assert "0.9" in card

    def test_cancel_clears(self):
        feishu = MagicMock()
        daf.try_handle(_msg("/daf X | 蒙西 | 500/2000 | 13亿"), feishu, None, "k")
        assert daf.try_handle(_msg("取消"), feishu, None, "k") is True
        assert daf._pending.get("owner-open-id") is None

    def test_unrelated_text_passes_through(self):
        assert daf.try_handle(_msg("今天蒙西价格怎么样?"), MagicMock(), None, "k") is False

    def test_telegram_owner_gate(self):
        tg = MagicMock()
        assert daf.try_handle(
            _msg("/daf X | 蒙西 | 500/2000 | 13亿", source="telegram", sender="12345"),
            None, tg, "k") is True
        assert "交易要素确认" in tg.send_text.call_args.kwargs["text"]

        tg2 = MagicMock()
        assert daf.try_handle(
            _msg("/daf", source="telegram", sender="999"),
            None, tg2, "k") is True
        assert "999" in tg2.send_text.call_args.kwargs["text"]
