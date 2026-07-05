import os
import pytest
from unittest.mock import MagicMock, patch

from services.hermes.thinking_agent import _DEV_REQUESTS_FOLDER


def _make_agent(**kwargs):
    from services.hermes.thinking_agent import ThinkingAgent
    defaults = dict(
        anthropic_api_key="test-key",
        pg_url="postgresql://localhost/test",
        feishu=MagicMock(),
        feishu_owner_open_id="owner123",
        onedrive=None,
    )
    defaults.update(kwargs)
    return ThinkingAgent(**defaults)


class TestModelResolution:
    def test_health_defaults_to_haiku(self):
        agent = _make_agent()
        assert agent._resolve_model("health") == "claude-haiku-4-5-20251001"

    def test_design_defaults_to_sonnet(self):
        agent = _make_agent()
        assert agent._resolve_model("design") == "claude-sonnet-4-6"

    def test_health_model_overridden_by_env(self, monkeypatch):
        monkeypatch.setenv("HERMES_THINK_HEALTH_MODEL", "sonnet")
        agent = _make_agent()
        assert agent._resolve_model("health") == "claude-sonnet-4-6"

    def test_design_model_overridden_by_env(self, monkeypatch):
        monkeypatch.setenv("HERMES_THINK_DESIGN_MODEL", "haiku")
        agent = _make_agent()
        assert agent._resolve_model("design") == "claude-haiku-4-5-20251001"

    def test_passthrough_unknown_alias(self, monkeypatch):
        monkeypatch.setenv("HERMES_THINK_HEALTH_MODEL", "deepseek-chat")
        agent = _make_agent()
        assert agent._resolve_model("health") == "deepseek-chat"


class TestQueryDb:
    def test_rejects_insert(self):
        agent = _make_agent()
        result = agent._tool_query_db("INSERT INTO foo VALUES (1)")
        assert "rejected" in result.lower() or "not allowed" in result.lower()

    def test_rejects_drop(self):
        agent = _make_agent()
        result = agent._tool_query_db("DROP TABLE foo")
        assert "rejected" in result.lower() or "not allowed" in result.lower()

    def test_rejects_update(self):
        agent = _make_agent()
        result = agent._tool_query_db("UPDATE foo SET bar=1")
        assert "rejected" in result.lower() or "not allowed" in result.lower()

    def test_accepts_select(self):
        agent = _make_agent()
        with patch("psycopg2.connect") as mock_conn:
            mock_cur = MagicMock()
            mock_cur.description = [("col",)]
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_conn.return_value)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_conn.return_value.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
            mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)
            mock_cur.fetchmany.return_value = [("value",)]
            result = agent._tool_query_db("SELECT 1")
            assert isinstance(result, str)


class TestReadSourceFile:
    def test_rejects_path_traversal(self):
        agent = _make_agent()
        result = agent._tool_read_source_file("../../etc/passwd")
        assert "not allowed" in result.lower() or "invalid" in result.lower()

    def test_rejects_non_py(self):
        agent = _make_agent()
        result = agent._tool_read_source_file("services/hermes/requirements.txt")
        assert "not allowed" in result.lower() or "only .py" in result.lower()

    def test_reads_existing_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "services.hermes.thinking_agent._REPO_ROOT", str(tmp_path)
        )
        py_file = tmp_path / "services" / "hermes" / "test.py"
        py_file.parent.mkdir(parents=True)
        py_file.write_text("def foo(): pass\n" * 10)
        agent = _make_agent()
        result = agent._tool_read_source_file("services/hermes/test.py")
        assert "def foo" in result

    def test_truncates_at_300_lines(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "services.hermes.thinking_agent._REPO_ROOT", str(tmp_path)
        )
        py_file = tmp_path / "big.py"
        py_file.write_text("x = 1\n" * 400)
        agent = _make_agent()
        result = agent._tool_read_source_file("big.py")
        assert result.count("x = 1") == 300


class TestListAppFiles:
    def test_returns_py_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "services.hermes.thinking_agent._REPO_ROOT", str(tmp_path)
        )
        app_dir = tmp_path / "apps" / "spot-market"
        app_dir.mkdir(parents=True)
        (app_dir / "app.py").write_text("")
        (app_dir / "utils.py").write_text("")
        agent = _make_agent()
        result = agent._tool_list_app_files("spot-market")
        assert "apps/spot-market/app.py" in result
        assert "apps/spot-market/utils.py" in result

    def test_falls_back_to_services(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "services.hermes.thinking_agent._REPO_ROOT", str(tmp_path)
        )
        svc_dir = tmp_path / "services" / "hermes"
        svc_dir.mkdir(parents=True)
        (svc_dir / "agent.py").write_text("")
        agent = _make_agent()
        result = agent._tool_list_app_files("hermes")
        assert "services/hermes/agent.py" in result


class TestSendFeishuMessage:
    def test_sends_message(self):
        feishu = MagicMock()
        agent = _make_agent(feishu=feishu)
        with patch.object(agent, "_is_duplicate", return_value=False):
            result = agent._tool_send_feishu_message("hello")
        feishu.send_text.assert_called_once_with(open_id="owner123", text="hello")
        assert "sent" in result

    def test_empty_text_skips_send(self):
        feishu = MagicMock()
        agent = _make_agent(feishu=feishu)
        result = agent._tool_send_feishu_message("")
        feishu.send_text.assert_not_called()
        assert "healthy" in result.lower() or "no message" in result.lower()

    def test_suppresses_duplicate(self):
        feishu = MagicMock()
        agent = _make_agent(feishu=feishu)
        with patch.object(agent, "_is_duplicate", return_value=True):
            result = agent._tool_send_feishu_message("same message")
        feishu.send_text.assert_not_called()
        assert "suppressed" in result.lower()


class TestWriteDevRequest:
    def test_writes_to_onedrive(self):
        onedrive = MagicMock()
        feishu = MagicMock()
        agent = _make_agent(onedrive=onedrive, feishu=feishu)
        result = agent._tool_write_dev_request("fix-irr", "# Dev Request\ncontent here")
        onedrive.upload_file.assert_called_once()
        call_args = onedrive.upload_file.call_args
        assert call_args[0][0] == _DEV_REQUESTS_FOLDER
        assert call_args[0][1].endswith("-fix-irr.md")
        assert b"# Dev Request" in call_args[0][2]
        feishu.send_text.assert_called_once()
        assert "fix-irr" in feishu.send_text.call_args[1]["text"] or \
               "fix-irr" in str(feishu.send_text.call_args)

    def test_skips_onedrive_if_not_configured(self):
        feishu = MagicMock()
        agent = _make_agent(onedrive=None, feishu=feishu)
        result = agent._tool_write_dev_request("test", "content")
        assert "onedrive" in result.lower() or "not configured" in result.lower()


class TestRun:
    def _mock_tool_response(self, tool_name, tool_input, tool_use_id="t1"):
        block = MagicMock()
        block.type = "tool_use"
        block.name = tool_name
        block.input = tool_input
        block.id = tool_use_id
        response = MagicMock()
        response.stop_reason = "tool_use"
        response.content = [block]
        return response

    def _mock_end_response(self):
        response = MagicMock()
        response.stop_reason = "end_turn"
        response.content = []
        return response

    def test_health_run_sends_message(self):
        feishu = MagicMock()
        agent = _make_agent(feishu=feishu)
        send_response = self._mock_tool_response(
            "send_feishu_message", {"text": "山东数据已过期"}
        )
        end_response = self._mock_end_response()
        agent._client = MagicMock()
        agent._client.messages.create.side_effect = [send_response, end_response]
        with patch.object(agent, "_is_duplicate", return_value=False), \
             patch.object(agent, "_log_run"):
            agent.run("health")
        feishu.send_text.assert_called_once()
        assert "山东数据已过期" in feishu.send_text.call_args[1]["text"]

    def test_run_respects_max_iterations(self):
        feishu = MagicMock()
        agent = _make_agent(feishu=feishu)
        from services.hermes.thinking_agent import _MAX_ITER
        query_response = self._mock_tool_response("query_db", {"sql": "SELECT 1"})
        agent._client = MagicMock()
        agent._client.messages.create.return_value = query_response
        with patch.object(agent, "_tool_query_db", return_value="| col |\n| --- |\n| 1 |"), \
             patch.object(agent, "_log_run"):
            agent.run("health")
        assert agent._client.messages.create.call_count <= _MAX_ITER + 1

    def test_run_raises_on_invalid_mode(self):
        agent = _make_agent()
        with pytest.raises(ValueError, match="Unknown mode"):
            agent.run("invalid")
