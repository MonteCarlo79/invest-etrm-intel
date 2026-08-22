"""Token usage metering for LLM calls — cost attribution per job.

Every messages.create() through shared.anthropic_client.make_client is logged
to marketdata.llm_usage_log with a caller tag (when LLM_USAGE_METER is not "0").
Set the tag around a job:

    from shared.usage_meter import usage_tag
    with usage_tag("news_screener"):
        run_screener(...)

Untagged calls land in 'unknown'. Metering is best-effort and never raises —
a metering failure must never break the caller.
"""
from __future__ import annotations

import contextlib
import contextvars
import logging

logger = logging.getLogger(__name__)

_caller = contextvars.ContextVar("llm_usage_caller", default="unknown")

_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.llm_usage_log (
    id            BIGSERIAL PRIMARY KEY,
    ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
    caller        TEXT NOT NULL,
    model         TEXT NOT NULL,
    input_tokens  INT,
    output_tokens INT
)
"""

_ensure_done = False


@contextlib.contextmanager
def usage_tag(name: str):
    """Tag all LLM calls in the block with caller=name (nests correctly)."""
    tok = _caller.set(name)
    try:
        yield
    finally:
        _caller.reset(tok)


def with_usage_tag(name: str):
    """Decorator form of usage_tag — @with_usage_tag("news_screener")."""
    import functools

    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with usage_tag(name):
                return fn(*args, **kwargs)
        return wrapper
    return deco


def current_caller() -> str:
    return _caller.get()


def enabled() -> bool:
    return __import__("os").environ.get("LLM_USAGE_METER", "1") != "0"


def _ensure_table() -> None:
    global _ensure_done
    if _ensure_done:
        return
    from shared.agents.db import get_conn
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
    _ensure_done = True


def log_usage(model: str, usage, caller: str | None = None) -> None:
    """Write one LLM call's token usage. Never raises."""
    try:
        inp = getattr(usage, "input_tokens", None)
        outp = getattr(usage, "output_tokens", None)
        if inp is None and outp is None:
            return
        _ensure_table()
        from shared.agents.db import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO marketdata.llm_usage_log (caller, model, input_tokens, output_tokens)"
                    " VALUES (%s, %s, %s, %s)",
                    (caller or current_caller(), model, inp, outp),
                )
            conn.commit()
    except Exception as exc:
        logger.debug("usage_meter: log failed: %s", exc)
