"""Tests for the DeepSeek→Bedrock credit fallback (metrics_extractor + ingestor)."""
import datetime as dt
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from services.exchange_reports.metrics_extractor import (
    _is_credit_error,
    _bedrock_client,
    extract_metrics,
)
from services.exchange_reports.ingestor import _detect_province_via_llm


def _credit_exc():
    return RuntimeError("402 Insufficient Balance")


def _anthropic_tool_resp(metrics: dict):
    block = SimpleNamespace(type="tool_use", name="store_market_metrics", input=metrics)
    return SimpleNamespace(content=[block])


def _deepseek_tool_resp(metrics: dict):
    import json
    fn = SimpleNamespace(arguments=json.dumps(metrics))
    tc = SimpleNamespace(function=fn)
    msg = SimpleNamespace(tool_calls=[tc], content="")
    choice = SimpleNamespace(message=msg, finish_reason="tool_calls")
    return SimpleNamespace(choices=[choice])


class TestCreditErrorClassifier:
    def test_status_code_402(self):
        e = SimpleNamespace(status_code=402)
        assert _is_credit_error(e) is True

    def test_insufficient_balance_message(self):
        assert _is_credit_error(RuntimeError("402 Insufficient Balance")) is True

    def test_quota_message(self):
        assert _is_credit_error(RuntimeError("quota exceeded")) is True

    def test_unrelated_error(self):
        assert _is_credit_error(ValueError("parse error")) is False


class TestBedrockClient:
    def test_returns_none_without_region(self):
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("BEDROCK_REGION", None)
            client, model = _bedrock_client()
        assert client is None and model is None

    def test_builds_with_region(self):
        with patch.dict("os.environ", {"BEDROCK_REGION": "ap-southeast-1"}):
            client, model = _bedrock_client()
        assert client is not None and model


class TestExtractMetricsFallback:
    def _clients(self):
        ds = MagicMock()
        ds.chat.completions.create.side_effect = _credit_exc()
        return ds

    def test_credit_failure_falls_back_to_bedrock(self):
        metrics = {"spot_avg_price": 350.0, "spot_volume": 100.0}
        bedrock = MagicMock()
        bedrock.messages.create.return_value = _anthropic_tool_resp(metrics)
        with patch("services.exchange_reports.metrics_extractor._get_client",
                   return_value=(self._clients(), "deepseek-chat", "deepseek")), \
             patch("services.exchange_reports.metrics_extractor._bedrock_client",
                   return_value=(bedrock, "claude-sonnet-4-6")):
            result = extract_metrics("报告全文", "甘肃", dt.date(2026, 6, 1), api_key="k")
        assert result is not None and result.get("spot_avg_price") == 350.0
        bedrock.messages.create.assert_called_once()

    def test_non_credit_error_does_not_fallback(self):
        ds = MagicMock()
        ds.chat.completions.create.side_effect = ValueError("weird input")
        with patch("services.exchange_reports.metrics_extractor._get_client",
                   return_value=(ds, "deepseek-chat", "deepseek")), \
             patch("services.exchange_reports.metrics_extractor._bedrock_client") as bc:
            result = extract_metrics("报告全文", "甘肃", dt.date(2026, 6, 1), api_key="k")
        assert result is None
        bc.assert_not_called()

    def test_anthropic_path_unchanged(self):
        metrics = {"spot_avg_price": 300.0}
        client = MagicMock()
        client.messages.create.return_value = _anthropic_tool_resp(metrics)
        with patch("services.exchange_reports.metrics_extractor._get_client",
                   return_value=(client, "claude-sonnet-4-6", "bedrock")):
            result = extract_metrics("报告全文", "甘肃", dt.date(2026, 6, 1), api_key="k")
        assert result is not None and result.get("spot_avg_price") == 300.0


class TestProvinceDetectionFallback:
    def test_credit_failure_falls_back(self):
        ds = MagicMock()
        ds.chat.completions.create.side_effect = _credit_exc()
        bedrock = MagicMock()
        bedrock.messages.create.return_value = SimpleNamespace(
            content=[SimpleNamespace(text="甘肃")]
        )
        pages = [(1, "甘肃电力市场信息披露文件 甘肃 2026年6月信息披露报告 一、电网概况")]
        with patch("services.exchange_reports.ingestor.extract_pages", return_value=pages), \
             patch("services.exchange_reports.metrics_extractor._get_client",
                   return_value=(ds, "deepseek-chat", "deepseek")), \
             patch("services.exchange_reports.metrics_extractor._bedrock_client",
                   return_value=(bedrock, "claude-sonnet-4-6")):
            result = _detect_province_via_llm(b"%PDF", "甘肃月报.pdf", "k")
        assert result == "甘肃"
