"""Local LLM access via Ollama's OpenAI-compatible endpoint.

Local development only — ECS tasks have no Ollama. All functions return None
on connection failure so callers can no-op when Ollama isn't running.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://localhost:11434/v1"
DEFAULT_MODEL = "qwen3:8b"


def _base_url() -> str:
    return os.environ.get("OLLAMA_BASE_URL", DEFAULT_BASE_URL)


def ollama_available() -> bool:
    """Quick health check: GET {base}/models with a 2s timeout."""
    try:
        import requests  # lazy import — house style
        resp = requests.get(f"{_base_url()}/models", timeout=2)
        return resp.status_code == 200
    except Exception:
        return False


def ollama_complete(
    system: str,
    user: str,
    *,
    max_tokens: int = 800,
    timeout: int = 60,
) -> str | None:
    """One-shot chat completion against the local Ollama model.

    Returns the assistant message content, or None on any failure (Ollama not
    running, model missing, timeout, ...). Model from OLLAMA_MODEL env var.
    """
    try:
        from openai import OpenAI  # lazy import — house style
        client = OpenAI(base_url=_base_url(), api_key="ollama")
        resp = client.chat.completions.create(
            model=os.environ.get("OLLAMA_MODEL", DEFAULT_MODEL),
            max_tokens=max_tokens,
            timeout=timeout,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        return resp.choices[0].message.content
    except Exception as exc:
        logger.debug("ollama_complete failed: %s", exc)
        return None
