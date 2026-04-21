"""
libs/decision_models/tests/test_trading_performance_agent.py

Unit tests for the Claude agentic loop and trading performance agent.

All tests use mocks — no real Claude API calls, no database calls.
anthropic must be installed (pip install anthropic>=0.40).

Patching strategy
-----------------
Rather than mocking sys.modules["anthropic"] or using dotted-string patch paths
on unloaded submodules, we patch _get_client() to inject a fake Anthropic client
and patch handle_tool_call via patch.object on the already-imported runner module.
This avoids AttributeError from Python's attribute-traversal in patch().

Test classes
------------
TestRunnerAgentLoop             (runner.py)
TestRunnerAgentLoopStreaming    (runner.py)
TestTradingPerformanceAgent     (trading_performance_agent.py)
TestExtractSectionHelpers       (internal helpers)
TestRunTradingAgentCLI          (run_trading_agent.py CLI)
"""
from __future__ import annotations

import datetime
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Pre-import the modules under test so patch() can resolve attribute paths
import libs.decision_models.adapters.agent.runner as _runner_mod
import libs.decision_models.adapters.agent.trading_performance_agent as _agent_mod
import services.ops.run_trading_agent as _cli_mod


# ---------------------------------------------------------------------------
# Helpers to build fake Anthropic response objects
# ---------------------------------------------------------------------------

def _make_text_block(text: str):
    block = MagicMock()
    block.type = "text"
    block.text = text
    return block


def _make_tool_use_block(name: str, tool_id: str = "tu_001", input_data: dict = None):
    block = MagicMock()
    block.type = "tool_use"
    block.name = name
    block.id = tool_id
    block.input = input_data or {"asset_code": "suyou", "date": "2026-04-17"}
    return block


def _make_response(content_blocks: list, stop_reason: str = "end_turn"):
    resp = MagicMock()
    resp.content = content_blocks
    resp.stop_reason = stop_reason
    return resp


def _fake_client(side_effects=None, single_return=None):
    """Return a MagicMock Anthropic client with messages.create pre-configured."""
    client = MagicMock()
    if side_effects is not None:
        client.messages.create.side_effect = side_effects
    elif single_return is not None:
        client.messages.create.return_value = single_return
    return client


# ---------------------------------------------------------------------------
# TestRunnerAgentLoop
# ---------------------------------------------------------------------------

class TestRunnerAgentLoop:

    def test_returns_text_when_no_tools_called(self):
        """When Claude replies with end_turn and no tool use, response_text is returned."""
        response = _make_response([_make_text_block("All assets performing well.")], "end_turn")
        client = _fake_client(single_return=response)

        with patch.object(_runner_mod, "_get_client", return_value=client):
            with patch.object(_runner_mod, "handle_tool_call") as mock_htc:
                result = _runner_mod.run_agent_loop(
                    messages=[{"role": "user", "content": "Hello"}],
                    system_prompt="You are an analyst.",
                )

        assert result["response_text"] == "All assets performing well."
        assert result["turns"] == 1
        assert result["tool_calls"] == []
        mock_htc.assert_not_called()

    def test_dispatches_tool_call_and_continues(self):
        """When stop_reason is tool_use, handle_tool_call is called and loop continues."""
        tool_block = _make_tool_use_block("run_bess_daily_strategy_analysis")
        text_block = _make_text_block("Analysis complete.")
        client = _fake_client(side_effects=[
            _make_response([tool_block], "tool_use"),
            _make_response([text_block], "end_turn"),
        ])

        with patch.object(_runner_mod, "_get_client", return_value=client):
            with patch.object(_runner_mod, "handle_tool_call", return_value='{"result": "ok"}') as mock_htc:
                result = _runner_mod.run_agent_loop(
                    messages=[{"role": "user", "content": "Run analysis"}],
                    system_prompt="Analyst",
                )

        assert result["response_text"] == "Analysis complete."
        assert result["turns"] == 2
        assert "run_bess_daily_strategy_analysis" in result["tool_calls"]
        mock_htc.assert_called_once_with(
            "run_bess_daily_strategy_analysis",
            {"asset_code": "suyou", "date": "2026-04-17"},
        )

    def test_raises_on_max_turns_exceeded(self):
        """RuntimeError raised when max_turns reached without end_turn."""
        tool_block = _make_tool_use_block("some_tool")
        # Always return tool_use — never ends
        client = _fake_client(single_return=_make_response([tool_block], "tool_use"))

        with patch.object(_runner_mod, "_get_client", return_value=client):
            with patch.object(_runner_mod, "handle_tool_call", return_value='{"ok": true}'):
                with pytest.raises(RuntimeError, match="max_turns"):
                    _runner_mod.run_agent_loop(
                        messages=[{"role": "user", "content": "Go"}],
                        system_prompt="Analyst",
                        max_turns=3,
                    )

        assert client.messages.create.call_count == 3

    def test_raises_when_api_key_missing(self, monkeypatch):
        """RuntimeError raised immediately if ANTHROPIC_API_KEY is not set."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        # Don't patch _get_client — let it run the real check
        with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
            _runner_mod.run_agent_loop(
                messages=[{"role": "user", "content": "Hello"}],
                system_prompt="Analyst",
            )

    def test_multiple_tool_calls_in_one_turn(self):
        """Multiple tool_use blocks in one response are all dispatched."""
        tool1 = _make_tool_use_block("run_all_assets_daily_strategy_analysis", "tu_001")
        tool2 = _make_tool_use_block("query_realization_status", "tu_002", {})
        text_block = _make_text_block("Done.")
        client = _fake_client(side_effects=[
            _make_response([tool1, tool2], "tool_use"),
            _make_response([text_block], "end_turn"),
        ])

        with patch.object(_runner_mod, "_get_client", return_value=client):
            with patch.object(_runner_mod, "handle_tool_call", return_value='{"data": []}') as mock_htc:
                result = _runner_mod.run_agent_loop(
                    messages=[{"role": "user", "content": "Go"}],
                    system_prompt="Analyst",
                )

        assert mock_htc.call_count == 2
        assert result["tool_calls"] == [
            "run_all_assets_daily_strategy_analysis",
            "query_realization_status",
        ]

    def test_unexpected_stop_reason_treated_as_end_turn(self):
        """An unexpected stop_reason returns the text without raising."""
        text_block = _make_text_block("Partial response.")
        client = _fake_client(single_return=_make_response([text_block], "max_tokens"))

        with patch.object(_runner_mod, "_get_client", return_value=client):
            result = _runner_mod.run_agent_loop(
                messages=[{"role": "user", "content": "Hi"}],
                system_prompt="Analyst",
            )

        assert result["response_text"] == "Partial response."
        assert result["turns"] == 1


# ---------------------------------------------------------------------------
# TestRunnerAgentLoopStreaming
# ---------------------------------------------------------------------------

class TestRunnerAgentLoopStreaming:

    def _make_stream_cm(self, text_chunks: list, stop_reason: str = "end_turn", content: list = None):
        """Build a mock streaming context manager."""
        stream = MagicMock()
        stream.__enter__ = MagicMock(return_value=stream)
        stream.__exit__ = MagicMock(return_value=False)
        stream.text_stream = iter(text_chunks)
        if content is None:
            content = [_make_text_block("".join(text_chunks))]
        stream.get_final_message.return_value = _make_response(content, stop_reason)
        return stream

    def test_yields_text_chunks(self):
        """run_agent_loop_streaming yields text deltas from the final response."""
        client = MagicMock()
        stream = self._make_stream_cm(["All ", "assets ", "normal."], "end_turn")
        client.messages.stream.return_value = stream

        with patch.object(_runner_mod, "_get_client", return_value=client):
            chunks = list(_runner_mod.run_agent_loop_streaming(
                messages=[{"role": "user", "content": "Hi"}],
                system_prompt="Analyst",
            ))

        assert "".join(chunks) == "All assets normal."

    def test_yields_tool_status_marker(self):
        """A `[Tool: name]` status chunk is yielded when a tool is dispatched."""
        tool_block = _make_tool_use_block("run_bess_daily_strategy_analysis")
        text_block = _make_text_block("Done.")

        # First stream ends with tool_use, second ends with end_turn
        stream1 = self._make_stream_cm([], "tool_use", content=[tool_block])
        stream2 = self._make_stream_cm(["Done."], "end_turn", content=[text_block])

        client = MagicMock()
        client.messages.stream.side_effect = [stream1, stream2]

        with patch.object(_runner_mod, "_get_client", return_value=client):
            with patch.object(_runner_mod, "handle_tool_call", return_value='{"ok": true}'):
                chunks = list(_runner_mod.run_agent_loop_streaming(
                    messages=[{"role": "user", "content": "Go"}],
                    system_prompt="Analyst",
                ))

        full_text = "".join(chunks)
        assert "run_bess_daily_strategy_analysis" in full_text
        assert "Done." in full_text

    def test_raises_when_max_turns_exceeded_streaming(self):
        """RuntimeError raised when streaming loop hits max_turns."""
        tool_block = _make_tool_use_block("loop_tool")
        # Always returns tool_use
        stream = self._make_stream_cm([], "tool_use", content=[tool_block])
        stream.__enter__ = MagicMock(return_value=stream)
        stream.__exit__ = MagicMock(return_value=False)
        stream.text_stream = iter([])
        stream.get_final_message.return_value = _make_response([tool_block], "tool_use")

        client = MagicMock()
        client.messages.stream.return_value = stream

        with patch.object(_runner_mod, "_get_client", return_value=client):
            with patch.object(_runner_mod, "handle_tool_call", return_value='{"ok": true}'):
                with pytest.raises(RuntimeError, match="max_turns"):
                    list(_runner_mod.run_agent_loop_streaming(
                        messages=[{"role": "user", "content": "Go"}],
                        system_prompt="Analyst",
                        max_turns=2,
                    ))


# ---------------------------------------------------------------------------
# TestTradingPerformanceAgent
# ---------------------------------------------------------------------------

_MOCK_NARRATIVE = """\
## Portfolio Overview
All 4 assets ran successfully today. Total forecast P&L: 993,148 CNY.

## Per-Asset Highlights
- suyou: best strategy forecast_ols_da_time_v1, P&L 248,287 CNY, 96 ops rows loaded.
- hangjinqi: best strategy forecast_ols_da_time_v1, P&L 312,000 CNY.
- siziwangqi: ops data available, forecast P&L 215,000 CNY.
- gushanliang: ops data available, forecast P&L 217,861 CNY.

## Alerts & Flags
- suyou: ALERT — dominant_loss_bucket: forecast_error, realization ratio 0.45.

## Recommendations
1. Investigate forecast error for suyou — check DA price proxy divergence.
2. Verify ops ingestion completeness for all assets.
3. Schedule compensation rate update for suyou (currently using default 350 CNY/MWh).
"""


def _mock_loop_result(narrative: str = _MOCK_NARRATIVE):
    return {
        "response_text": narrative,
        "messages": [],
        "turns": 4,
        "tool_calls": [
            "run_all_assets_daily_strategy_analysis",
            "query_realization_status",
            "query_fragility_status",
        ],
    }


class TestTradingPerformanceAgent:

    def test_run_daily_review_returns_dataclass(self):
        """run_daily_review returns a DailyOpsReviewResult with expected fields."""
        with patch.object(_agent_mod, "run_agent_loop", return_value=_mock_loop_result()):
            with patch.object(_agent_mod, "_log_request"):
                agent = _agent_mod.TradingPerformanceAgent()
                result = agent.run_daily_review("2026-04-17")

        assert result.date == "2026-04-17"
        assert result.n_assets_reviewed == 4
        assert result.narrative == _MOCK_NARRATIVE
        assert "run_all_assets_daily_strategy_analysis" in result.tool_calls
        assert result.generated_at is not None

    def test_alert_count_extracted_correctly(self):
        """n_alerts counts bullet items from the Alerts & Flags section."""
        with patch.object(_agent_mod, "run_agent_loop", return_value=_mock_loop_result()):
            with patch.object(_agent_mod, "_log_request"):
                result = _agent_mod.TradingPerformanceAgent().run_daily_review("2026-04-17")

        assert result.n_alerts == 1
        assert len(result.alerts) == 1
        assert "suyou" in result.alerts[0]

    def test_recommendations_extracted(self):
        """Numbered recommendations are extracted from the Recommendations section."""
        with patch.object(_agent_mod, "run_agent_loop", return_value=_mock_loop_result()):
            with patch.object(_agent_mod, "_log_request"):
                result = _agent_mod.TradingPerformanceAgent().run_daily_review("2026-04-17")

        assert len(result.recommendations) == 3
        assert "forecast error" in result.recommendations[0].lower()

    def test_run_daily_review_logs_request(self):
        """run_daily_review calls _log_request with correct agent_name and status."""
        with patch.object(_agent_mod, "run_agent_loop", return_value=_mock_loop_result()):
            with patch.object(_agent_mod, "_log_request") as mock_log:
                _agent_mod.TradingPerformanceAgent().run_daily_review("2026-04-17")

        mock_log.assert_called_once()
        kw = mock_log.call_args[1]
        assert kw["agent_name"] == "trading_performance_agent"
        assert kw["status"] == "completed"

    def test_zero_alerts_when_no_alert_section(self):
        """n_alerts is 0 when Alerts & Flags section contains the none sentinel."""
        no_alert_narrative = """\
## Portfolio Overview
Normal day.

## Per-Asset Highlights
- suyou: normal.

## Alerts & Flags
- None — all assets within normal operating range.

## Recommendations
1. Continue monitoring.
"""
        with patch.object(_agent_mod, "run_agent_loop", return_value={
            "response_text": no_alert_narrative,
            "messages": [],
            "turns": 3,
            "tool_calls": [],
        }):
            with patch.object(_agent_mod, "_log_request"):
                result = _agent_mod.TradingPerformanceAgent().run_daily_review("2026-04-17")

        assert result.n_alerts == 0

    def test_answer_query_appends_to_history(self):
        """answer_query returns updated history with user + assistant turns appended."""
        with patch.object(_agent_mod, "run_agent_loop", return_value={
            "response_text": "Suyou underperformed due to forecast error.",
            "messages": [],
            "turns": 2,
            "tool_calls": ["run_bess_daily_strategy_analysis"],
        }):
            with patch.object(_agent_mod, "_log_request"):
                response, history = _agent_mod.TradingPerformanceAgent().answer_query(
                    "Why did suyou underperform?",
                    date="2026-04-17",
                )

        assert "forecast error" in response.lower()
        assert history[-1]["role"] == "assistant"
        assert history[-1]["content"] == response

    def test_answer_query_multi_turn_continues_history(self):
        """Subsequent answer_query calls extend the existing conversation history."""
        existing_history = [
            {"role": "user", "content": "What happened today?"},
            {"role": "assistant", "content": "All assets performed well."},
        ]
        with patch.object(_agent_mod, "run_agent_loop", return_value={
            "response_text": "Hangjinqi had 312,000 CNY P&L.",
            "messages": [],
            "turns": 2,
            "tool_calls": [],
        }):
            with patch.object(_agent_mod, "_log_request"):
                _, new_history = _agent_mod.TradingPerformanceAgent().answer_query(
                    "What about hangjinqi?",
                    date="2026-04-17",
                    conversation_history=existing_history,
                )

        # original 2 + new user + new assistant = 4
        assert len(new_history) == 4
        assert new_history[2]["role"] == "user"
        assert new_history[3]["role"] == "assistant"


# ---------------------------------------------------------------------------
# TestExtractSectionHelpers
# ---------------------------------------------------------------------------

class TestExtractSectionHelpers:

    def test_extracts_bullet_items(self):
        narrative = """\
## Alerts & Flags
- suyou: ALERT, forecast_error
- hangjinqi: CRITICAL, grid_restriction

## Recommendations
1. Fix forecast.
"""
        items = _agent_mod._extract_section_items(narrative, "Alerts & Flags")
        assert len(items) == 2
        assert "suyou" in items[0]
        assert "hangjinqi" in items[1]

    def test_extracts_numbered_items(self):
        narrative = """\
## Recommendations
1. Check DA prices.
2. Update compensation rate.
3. Review nominations.
"""
        items = _agent_mod._extract_section_items(narrative, "Recommendations")
        assert len(items) == 3
        assert "DA prices" in items[0]

    def test_returns_empty_when_section_missing(self):
        items = _agent_mod._extract_section_items("No sections here", "Alerts & Flags")
        assert items == []

    def test_count_alert_assets_zero_for_none_sentinel(self):
        assert _agent_mod._count_alert_assets(
            ["None — all assets within normal operating range."]
        ) == 0

    def test_count_alert_assets_correct_count(self):
        assert _agent_mod._count_alert_assets(
            ["suyou: ALERT", "hangjinqi: CRITICAL"]
        ) == 2

    def test_count_alert_assets_empty_list(self):
        assert _agent_mod._count_alert_assets([]) == 0


# ---------------------------------------------------------------------------
# TestRunTradingAgentCLI
# ---------------------------------------------------------------------------

def _make_review_result(n_alerts: int = 1):
    return _agent_mod.DailyOpsReviewResult(
        date="2026-04-17",
        generated_at="2026-04-17T00:00:00+00:00",
        narrative=_MOCK_NARRATIVE,
        alerts=["suyou: ALERT"] if n_alerts else [],
        recommendations=["Fix forecast"],
        n_assets_reviewed=4,
        n_alerts=n_alerts,
        tool_calls=["run_all_assets_daily_strategy_analysis"],
    )


class TestRunTradingAgentCLI:

    def test_cli_dry_run_no_pdf_no_email(self, tmp_path, monkeypatch):
        """--dry-run skips PDF write and email."""
        monkeypatch.setenv("REPORT_OUTPUT_DIR", str(tmp_path))

        with patch("sys.argv", ["run_trading_agent.py", "--date", "2026-04-17", "--dry-run"]):
            with patch.object(
                _agent_mod.TradingPerformanceAgent, "run_daily_review",
                return_value=_make_review_result(),
            ):
                with patch("shared.agents.execution_agent.send_email_report") as mock_email:
                    _cli_mod.main()

        mock_email.assert_not_called()
        assert list(tmp_path.glob("*.pdf")) == []

    def test_cli_send_email_calls_send_email_report(self, tmp_path, monkeypatch):
        """--send-email calls send_email_report with the date in the subject."""
        monkeypatch.setenv("REPORT_OUTPUT_DIR", str(tmp_path))

        with patch("sys.argv", ["run_trading_agent.py", "--date", "2026-04-17", "--send-email"]):
            with patch.object(
                _agent_mod.TradingPerformanceAgent, "run_daily_review",
                return_value=_make_review_result(),
            ):
                with patch("shared.agents.execution_agent.send_email_report") as mock_email:
                    _cli_mod.main()

        mock_email.assert_called_once()
        subject = mock_email.call_args.kwargs.get("subject") or mock_email.call_args[0][0]
        assert "2026-04-17" in subject
        assert "alert" in subject.lower()

    def test_cli_default_date_is_yesterday(self, monkeypatch):
        """When --date is omitted, the date defaults to yesterday UTC."""
        with patch("sys.argv", ["run_trading_agent.py", "--dry-run"]):
            with patch.object(
                _agent_mod.TradingPerformanceAgent, "run_daily_review",
                return_value=_make_review_result(),
            ) as mock_review:
                _cli_mod.main()

        called_date = mock_review.call_args[0][0]
        yesterday = (
            datetime.datetime.now(datetime.timezone.utc).date()
            - datetime.timedelta(days=1)
        ).isoformat()
        assert called_date == yesterday

    def test_cli_no_alerts_subject(self, tmp_path, monkeypatch):
        """Zero alerts reflected in email subject."""
        monkeypatch.setenv("REPORT_OUTPUT_DIR", str(tmp_path))

        with patch("sys.argv", ["run_trading_agent.py", "--date", "2026-04-17", "--send-email"]):
            with patch.object(
                _agent_mod.TradingPerformanceAgent, "run_daily_review",
                return_value=_make_review_result(n_alerts=0),
            ):
                with patch("shared.agents.execution_agent.send_email_report") as mock_email:
                    _cli_mod.main()

        subject = mock_email.call_args.kwargs.get("subject") or mock_email.call_args[0][0]
        assert "0 alert" in subject.lower()
