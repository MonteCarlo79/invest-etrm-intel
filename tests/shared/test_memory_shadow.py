"""Tests for shared.memory_shadow (Ollama shadow-mode pilot).

ollama_complete itself is never tested against a live Ollama — no network in
tests; it is always patched.
"""

import json
from unittest.mock import patch

from shared.memory_shadow import (
    _EXTRACT_SYSTEM,
    parse_extraction_json,
    shadow_memory_extraction,
)


class TestParseExtractionJson:
    def test_clean_array(self):
        raw = '[{"category": "market_view", "subject": "s", "content": "c"}]'
        assert parse_extraction_json(raw) == [
            {"category": "market_view", "subject": "s", "content": "c"}
        ]

    def test_fenced_json_array(self):
        raw = '```json\n[{"category": "methodology", "subject": "s", "content": "c"}]\n```'
        items = parse_extraction_json(raw)
        assert items == [{"category": "methodology", "subject": "s", "content": "c"}]

    def test_garbage_returns_empty(self):
        assert parse_extraction_json("not json at all") == []

    def test_empty_array(self):
        assert parse_extraction_json("[]") == []


class TestShadowGate:
    def test_gate_off_no_call_no_file(self, tmp_path, monkeypatch):
        monkeypatch.delenv("LOCAL_LLM_SHADOW", raising=False)
        with patch(
            "shared.memory_shadow.ollama_complete",
            side_effect=AssertionError("must not be called when gate is off"),
        ):
            assert shadow_memory_extraction("bess_map", "u", "a", [], log_dir=tmp_path) is None
        assert list(tmp_path.iterdir()) == []

    def test_gate_on_logs_both_outputs(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOCAL_LLM_SHADOW", "1")
        fenced = (
            '```json\n[{"category": "market_view", "subject": "shandong", '
            '"content": "spread widening"}]\n```'
        )
        haiku_items = [
            {"category": "market_view", "subject": "shandong", "content": "spread widening"}
        ]
        with patch("shared.memory_shadow.ollama_complete", return_value=fenced) as mock_oc:
            assert shadow_memory_extraction(
                "bess_map", "what about shandong?", "spread widened", haiku_items,
                log_dir=tmp_path,
            ) is None
        assert mock_oc.call_count == 1
        files = list(tmp_path.glob("bess_map-*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
        rec = json.loads(lines[0])
        assert rec["app"] == "bess_map"
        assert rec["user_msg_head"] == "what about shandong?"
        assert rec["haiku_items"] == haiku_items
        assert rec["ollama_items"] == haiku_items
        assert rec["error"] is None
        assert isinstance(rec["ollama_latency_ms"], int)
        assert rec["ollama_model"]  # non-empty

    def test_never_raises_on_ollama_failure(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOCAL_LLM_SHADOW", "1")
        with patch(
            "shared.memory_shadow.ollama_complete", side_effect=RuntimeError("boom")
        ):
            assert shadow_memory_extraction(
                "spot_market", "u", "a", [], log_dir=tmp_path
            ) is None
        files = list(tmp_path.glob("spot_market-*.jsonl"))
        assert len(files) == 1
        rec = json.loads(files[0].read_text(encoding="utf-8").strip())
        assert "boom" in rec["error"]
        assert rec["ollama_items"] == []


class TestPromptSelection:
    def test_provided_prompt_forwarded_verbatim(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOCAL_LLM_SHADOW", "1")
        with patch("shared.memory_shadow.ollama_complete", return_value="[]") as mock_oc:
            shadow_memory_extraction(
                "mengxi_trader", "u", "a", [],
                log_dir=tmp_path, system="SYS-X", user="USER-Y",
            )
        mock_oc.assert_called_once_with("SYS-X", "USER-Y")

    def test_default_prompt_when_omitted(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOCAL_LLM_SHADOW", "1")
        with patch("shared.memory_shadow.ollama_complete", return_value="[]") as mock_oc:
            shadow_memory_extraction(
                "bess_map", "hello", "reply text", [], log_dir=tmp_path
            )
        args, _kwargs = mock_oc.call_args
        assert args[0] == _EXTRACT_SYSTEM
        assert "User said: hello" in args[1]
        assert "Agent replied: reply text" in args[1]

    def test_partial_prompt_falls_back_to_default(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOCAL_LLM_SHADOW", "1")
        with patch("shared.memory_shadow.ollama_complete", return_value="[]") as mock_oc:
            shadow_memory_extraction(
                "bess_map", "hi", "reply", [], log_dir=tmp_path, system="SYS-ONLY"
            )
        args, _kwargs = mock_oc.call_args
        assert args[0] == _EXTRACT_SYSTEM
