"""Shared intl-market LLM calls must go through the Bedrock-aware factory.

A direct anthropic.Anthropic(api_key=...) ignores BEDROCK_REGION; with a dead key
every call 401s and digest_kb_docs silently returned 0 (found 2026-11-23).
"""
import os
import pathlib
import sys
from unittest.mock import MagicMock, patch

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


@pytest.mark.parametrize("mod", ["expert_memory_base", "advanced_retrieval_base", "audio_ingest"])
def test_no_direct_anthropic_client(mod):
    src = (ROOT / "services" / "intl_market_common" / f"{mod}.py").read_text()
    assert "anthropic.Anthropic(" not in src
    assert "make_client(" in src


def test_digest_raises_when_every_llm_call_fails():
    from services.intl_market_common import expert_memory_base as m

    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [(1, "t", "c"), (2, "t", "c")]
    client = MagicMock()
    client.messages.create.side_effect = RuntimeError("401")
    with patch.object(m, "_conn", return_value=conn), patch.object(m, "make_client", return_value=client):
        with pytest.raises(RuntimeError, match="all 2"):
            m.digest_kb_docs("k", "ph_", "Philippines")
