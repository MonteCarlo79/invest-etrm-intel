# services/hermes/tests/test_kb_digest.py
"""
Unit tests for _run_kb_digest() in services/hermes/app.py.
No DB, no API calls — all external dependencies mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _import_helper():
    """Import _run_kb_digest from app.py."""
    from services.hermes.app import _run_kb_digest
    return _run_kb_digest


class TestRunKbDigest:

    def test_returns_dict_with_both_counts(self):
        """Happy path: both stages succeed, counts returned."""
        mock_pipeline = MagicMock()
        mock_pipeline.return_value.run.return_value = {"ok": 5, "error": 0, "skipped": 2}

        with patch("services.hermes.app._synthesis_pipeline_cls", mock_pipeline), \
             patch("services.hermes.app._digest_spot_kb_docs", return_value=3):
            from services.hermes.app import _run_kb_digest
            result = _run_kb_digest("test-api-key", limit=10)

        assert result == {"synthesized": 5, "insights": 3}

    def test_synthesis_failure_still_runs_digest(self):
        """If synthesis raises, digest still runs and partial result returned."""
        mock_pipeline = MagicMock()
        mock_pipeline.return_value.run.side_effect = RuntimeError("synthesis boom")

        with patch("services.hermes.app._synthesis_pipeline_cls", mock_pipeline), \
             patch("services.hermes.app._digest_spot_kb_docs", return_value=2):
            from services.hermes.app import _run_kb_digest
            result = _run_kb_digest("test-api-key")

        assert result["synthesized"] == 0
        assert result["insights"] == 2

    def test_digest_failure_still_returns_synthesis_count(self):
        """If digest raises, synthesis count is preserved."""
        mock_pipeline = MagicMock()
        mock_pipeline.return_value.run.return_value = {"ok": 4, "error": 0, "skipped": 0}

        with patch("services.hermes.app._synthesis_pipeline_cls", mock_pipeline), \
             patch("services.hermes.app._digest_spot_kb_docs",
                   side_effect=RuntimeError("digest boom")):
            from services.hermes.app import _run_kb_digest
            result = _run_kb_digest("test-api-key")

        assert result["synthesized"] == 4
        assert result["insights"] == 0

    def test_empty_api_key_returns_zeros_without_calling_apis(self):
        """Empty API key: neither stage is called, both counts are zero."""
        mock_pipeline = MagicMock()
        mock_digest = MagicMock(return_value=99)

        with patch("services.hermes.app._synthesis_pipeline_cls", mock_pipeline), \
             patch("services.hermes.app._digest_spot_kb_docs", mock_digest):
            from services.hermes.app import _run_kb_digest
            result = _run_kb_digest("")

        mock_pipeline.assert_not_called()
        mock_digest.assert_not_called()
        assert result == {"synthesized": 0, "insights": 0}

    def test_both_stages_fail_returns_zero_zero(self):
        """Both stages explode: result is zeros, no exception propagates."""
        mock_pipeline = MagicMock()
        mock_pipeline.return_value.run.side_effect = Exception("synthesis dead")

        with patch("services.hermes.app._synthesis_pipeline_cls", mock_pipeline), \
             patch("services.hermes.app._digest_spot_kb_docs",
                   side_effect=Exception("digest dead")):
            from services.hermes.app import _run_kb_digest
            result = _run_kb_digest("key")

        assert result == {"synthesized": 0, "insights": 0}
