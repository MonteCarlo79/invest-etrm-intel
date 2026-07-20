"""
shared/anthropic_client.py — Bedrock-aware Anthropic client factory.

Usage:
    from shared.anthropic_client import make_client, is_llm_available

    client = make_client(api_key)          # same interface as anthropic.Anthropic()
    client.messages.create(model="claude-haiku-4-5-20251001", ...)

Selection order (first match wins):
  1. BEDROCK_REGION env var set  → AnthropicBedrock via IAM role (no API key needed)
  2. api_key or ANTHROPIC_API_KEY set → direct Anthropic API

When Bedrock is active, direct model IDs (e.g. 'claude-haiku-4-5-20251001') are
auto-mapped to Bedrock model IDs — call sites do NOT need to change model strings.
"""
from __future__ import annotations

import logging
import os

import anthropic as _anthropic

logger = logging.getLogger(__name__)

# Maps direct Anthropic model IDs → Bedrock model IDs.
# Uses global inference profiles — available in all regions, no AWS Marketplace subscription required.
# Override any entry at runtime via BEDROCK_MODEL_<DIRECT_ID_UPPERCASED_UNDERSCORED> env var (checked first).
# IMPORTANT: us.anthropic.* cross-region profiles require an AWS Marketplace subscription that
# cannot be auto-completed in most accounts → use global.anthropic.* instead.
_BEDROCK_MODEL_MAP: dict[str, str] = {
    # Claude 4.x — global inference profiles
    "claude-sonnet-4-6":               "global.anthropic.claude-sonnet-4-6",
    "claude-opus-4-6":                 "global.anthropic.claude-opus-4-6-v1",
    # Claude 4.5 — global inference profiles
    "claude-haiku-4-5":                "global.anthropic.claude-haiku-4-5-20251001-v1:0",
    "claude-haiku-4-5-20251001":       "global.anthropic.claude-haiku-4-5-20251001-v1:0",
    "claude-sonnet-4-5-20250929":      "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
    # Claude 3.x — on-demand
    "claude-3-5-haiku-20241022":       "anthropic.claude-3-5-haiku-20241022-v1:0",
    "claude-3-opus-20240229":          "anthropic.claude-3-opus-20240229-v1:0",
}


class _BedrockMessages:
    """Wraps AnthropicBedrock.messages with transparent direct→Bedrock model mapping."""

    def __init__(self, client: _anthropic.AnthropicBedrock) -> None:
        self._client = client

    def _map(self, model: str) -> str:
        # Env var override takes precedence — change model IDs without rebuilding image
        env_key = "BEDROCK_MODEL_" + model.upper().replace("-", "_").replace(".", "_")
        env_override = os.environ.get(env_key)
        if env_override:
            return env_override
        mapped = _BEDROCK_MODEL_MAP.get(model)
        return mapped if mapped else model

    def create(self, *, model: str, **kwargs):
        bedrock_id = self._map(model)
        logger.debug("Bedrock create: %s → %s", model, bedrock_id)
        return self._client.messages.create(model=bedrock_id, **kwargs)

    def stream(self, *, model: str, **kwargs):
        bedrock_id = self._map(model)
        logger.debug("Bedrock stream: %s → %s", model, bedrock_id)
        return self._client.messages.stream(model=bedrock_id, **kwargs)


class _BedrockWrapper:
    """Thin wrapper that makes AnthropicBedrock look like Anthropic to call sites."""

    def __init__(self, client: _anthropic.AnthropicBedrock) -> None:
        self._client = client
        self.messages = _BedrockMessages(client)

    def __getattr__(self, name: str):
        return getattr(self._client, name)


def make_client(api_key: str | None = None) -> _anthropic.Anthropic | _BedrockWrapper:
    """Return an Anthropic-compatible client, preferring Bedrock when available.

    Args:
        api_key: Direct Anthropic API key. Ignored when BEDROCK_REGION is set.

    Returns:
        Either a real ``anthropic.Anthropic`` instance or a ``_BedrockWrapper``
        with the same ``.messages.create()`` / ``.messages.stream()`` interface.
    """
    region = os.environ.get("BEDROCK_REGION", "").strip()
    if region:
        logger.debug("LLM client: Bedrock region=%s", region)
        client = _anthropic.AnthropicBedrock(aws_region=region)
        return _BedrockWrapper(client)
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    logger.debug("LLM client: direct Anthropic API")
    return _anthropic.Anthropic(api_key=key)


def is_llm_available(api_key: str | None = None) -> bool:
    """Return True if either Bedrock or a direct API key is configured."""
    return bool(os.environ.get("BEDROCK_REGION")) or bool(
        api_key or os.environ.get("ANTHROPIC_API_KEY")
    )
