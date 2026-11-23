"""Shared intl-market LLM calls must go through the Bedrock-aware factory.

A direct anthropic.Anthropic(api_key=...) ignores BEDROCK_REGION; with a dead key
every call 401s and the caller sees nothing but zero results (found 2026-11-23).
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


@pytest.mark.parametrize("mod", ["expert_memory_base", "advanced_retrieval_base", "audio_ingest"])
def test_no_direct_anthropic_client(mod):
    src = (ROOT / "services" / "intl_market_common" / f"{mod}.py").read_text()
    assert "anthropic.Anthropic(" not in src
    assert "shared.anthropic_client import make_client" in src

