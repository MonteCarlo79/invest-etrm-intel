"""Tests for shared/usage_meter.py and the metering hook in shared/anthropic_client.py."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from shared.usage_meter import current_caller, log_usage, usage_tag, with_usage_tag
from shared.anthropic_client import _BedrockMessages, _DirectMessages, _DirectWrapper


class TestUsageTag:
    def test_default_unknown(self):
        assert current_caller() == "unknown"

    def test_tag_sets_and_restores(self):
        with usage_tag("news_screener"):
            assert current_caller() == "news_screener"
        assert current_caller() == "unknown"

    def test_nesting(self):
        with usage_tag("outer"):
            with usage_tag("inner"):
                assert current_caller() == "inner"
            assert current_caller() == "outer"

    def test_decorator(self):
        @with_usage_tag("kb_digest")
        def job():
            return current_caller()
        assert job() == "kb_digest"
        assert current_caller() == "unknown"


class TestLogUsage:
    def test_never_raises_on_db_failure(self):
        with patch("shared.agents.db.get_conn", side_effect=RuntimeError("db down")):
            log_usage("claude-sonnet-4-6", SimpleNamespace(input_tokens=10, output_tokens=5))

    def test_skips_empty_usage(self):
        with patch("shared.agents.db.get_conn") as gc:
            log_usage("m", SimpleNamespace(input_tokens=None, output_tokens=None))
        gc.assert_not_called()

    def test_writes_row_with_caller(self):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        with patch("shared.agents.db.get_conn") as gc:
            gc.return_value.__enter__.return_value = conn  # `with get_conn() as conn` binds __enter__'s result
            with usage_tag("patrol"):
                log_usage("claude-sonnet-4-6", SimpleNamespace(input_tokens=123, output_tokens=45))
        sql, params = cur.execute.call_args[0]
        assert "llm_usage_log" in sql
        assert params == ("patrol", "claude-sonnet-4-6", 123, 45)


class TestClientMeteringHook:
    def test_bedrock_messages_create_meters(self):
        inner = MagicMock()
        inner.messages.create.return_value = SimpleNamespace(
            usage=SimpleNamespace(input_tokens=7, output_tokens=3)
        )
        msgs = _BedrockMessages(inner)
        with patch("shared.usage_meter.log_usage") as log:
            resp = msgs.create(model="claude-sonnet-4-6", messages=[])
        assert log.call_args[0][0] == "global.anthropic.claude-sonnet-4-6"
        assert resp is inner.messages.create.return_value

    def test_direct_messages_create_meters(self):
        inner = MagicMock()
        inner.messages.create.return_value = SimpleNamespace(
            usage=SimpleNamespace(input_tokens=1, output_tokens=1)
        )
        msgs = _DirectMessages(inner)
        with patch("shared.usage_meter.log_usage") as log:
            msgs.create(model="claude-sonnet-4-6", messages=[])
        assert log.call_args[0][0] == "claude-sonnet-4-6"

    def test_direct_wrapper_passthrough(self):
        inner = MagicMock()
        w = _DirectWrapper(inner)
        assert w.some_attr is inner.some_attr

    def test_meter_failure_does_not_break_create(self):
        inner = MagicMock()
        inner.messages.create.return_value = SimpleNamespace(usage=None)
        msgs = _BedrockMessages(inner)
        with patch("shared.usage_meter.log_usage", side_effect=RuntimeError("boom")):
            resp = msgs.create(model="claude-sonnet-4-6", messages=[])
        assert resp is inner.messages.create.return_value
