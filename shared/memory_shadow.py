"""Shadow-mode evaluation of local-LLM memory extraction.

When LOCAL_LLM_SHADOW=1, every Haiku memory extraction in the pillar apps is
duplicated against local Ollama qwen3:8b; both outputs land in a JSONL log for
offline comparison. The Haiku result is always the only one used by the app.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from shared.local_llm import DEFAULT_MODEL, ollama_complete

logger = logging.getLogger(__name__)

# Same prompt as apps/bess-map/app.py::_extract_memories — the extraction task
# being evaluated. One fixed prompt across all three apps so qwen3:8b is
# measured on a single, stable task definition.
_EXTRACT_SYSTEM = (
    "You extract key investment facts, views, and methodology decisions from "
    "BESS analyst conversations to build a persistent memory. "
    "Output ONLY a JSON array (no markdown). Each item: "
    "{\"category\": one of [market_view, methodology, province_note, red_flag, investment_thesis], "
    "\"subject\": short title (≤60 chars), \"content\": the key fact or view (≤200 chars)}. "
    "Return [] if nothing worth persisting."
)

_EXTRACT_USER_TEMPLATE = (
    "User said: {user_msg}\n\nAgent replied: {agent_reply}\n\n"
    "What facts, views, or decisions from this exchange are worth remembering?"
)


def parse_extraction_json(raw: str) -> list[dict]:
    """Parse a model's JSON-array extraction output; [] on any error.

    Same fence-stripping logic as apps/bess-map/app.py::_extract_memories.
    """
    try:
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        items = json.loads(raw)
        return items if isinstance(items, list) else []
    except Exception:
        return []


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def shadow_memory_extraction(
    app_key: str,
    user_msg: str,
    agent_reply: str,
    haiku_items: list[dict],
    log_dir: Path | None = None,
) -> None:
    """Re-run this turn's memory extraction against local Ollama and log both.

    No-op unless LOCAL_LLM_SHADOW=1. Never raises — a shadow failure must not
    affect the app, and the Haiku result remains the only one used.
    """
    if os.environ.get("LOCAL_LLM_SHADOW") != "1":
        return
    now = datetime.now(timezone.utc)
    record = {
        "ts": now.isoformat(),
        "app": app_key,
        "user_msg_head": user_msg[:200],
        "haiku_items": haiku_items,
        "ollama_items": [],
        "ollama_latency_ms": None,
        "ollama_model": os.environ.get("OLLAMA_MODEL", DEFAULT_MODEL),
        "error": None,
    }
    try:
        t0 = time.monotonic()
        raw = ollama_complete(
            _EXTRACT_SYSTEM,
            _EXTRACT_USER_TEMPLATE.format(
                user_msg=user_msg, agent_reply=agent_reply[:1500]
            ),
        )
        record["ollama_latency_ms"] = int((time.monotonic() - t0) * 1000)
        if raw is None:
            record["error"] = "ollama_complete returned None"
        else:
            record["ollama_items"] = parse_extraction_json(raw)
    except Exception as exc:  # ollama_complete swallows, but belt-and-braces
        record["error"] = f"{type(exc).__name__}: {exc}"
    try:
        out_dir = Path(log_dir) if log_dir is not None else _repo_root() / "logs" / "memory_shadow"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{app_key}-{now.strftime('%Y-%m-%d')}.jsonl"
        with open(out_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.debug("shadow_memory_extraction log write failed: %s", exc)
