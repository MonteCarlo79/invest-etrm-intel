"""
libs/decision_models/adapters/agent/runner.py

Claude API agentic loop for the BESS decision model tool suite.

Provides two entry points:
  run_agent_loop()           — synchronous, returns final response dict
  run_agent_loop_streaming() — generator yielding text chunks (for Streamlit)

Both functions share the same tool dispatch logic via handle_tool_call() from
libs.decision_models.adapters.agent.tools.

Environment variables
---------------------
ANTHROPIC_API_KEY  : required — Claude API key
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Iterator, List, Optional

import anthropic

from libs.decision_models.adapters.agent.tools import ALL_TOOLS, handle_tool_call

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "claude-sonnet-4-6"
_DEFAULT_MAX_TOKENS = 8192
_DEFAULT_MAX_TURNS = 10


def _get_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY is not set. "
            "Export it before running: export ANTHROPIC_API_KEY=sk-ant-..."
        )
    return anthropic.Anthropic(api_key=api_key)


def _dispatch_tool_uses(content_blocks: list) -> list:
    """
    Dispatch all tool_use blocks in a response content list.

    Returns a list of tool_result content blocks ready to be added to
    a user message.
    """
    results = []
    for block in content_blocks:
        if block.type != "tool_use":
            continue
        t0 = time.perf_counter()
        logger.debug("tool_call name=%s input_keys=%s", block.name, list(block.input.keys()))
        try:
            result_json = handle_tool_call(block.name, block.input)
        except Exception as exc:
            result_json = f'{{"error": "{exc}"}}'
            logger.warning("tool_call name=%s raised %s", block.name, exc)
        elapsed = time.perf_counter() - t0
        logger.debug("tool_call name=%s elapsed=%.2fs", block.name, elapsed)
        results.append({
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": result_json,
        })
    return results


def run_agent_loop(
    messages: List[Dict[str, Any]],
    system_prompt: str,
    tools: Optional[List[Dict[str, Any]]] = None,
    max_turns: int = _DEFAULT_MAX_TURNS,
    model: str = _DEFAULT_MODEL,
    max_tokens: int = _DEFAULT_MAX_TOKENS,
) -> Dict[str, Any]:
    """
    Run a synchronous Claude API agentic loop until end_turn or max_turns.

    Parameters
    ----------
    messages     : initial conversation messages in Anthropic API format
    system_prompt: system prompt string
    tools        : tool schemas; defaults to ALL_TOOLS from tools.py
    max_turns    : maximum number of assistant turns (raises if exceeded)
    model        : Claude model ID
    max_tokens   : max tokens per response

    Returns
    -------
    dict with keys:
      response_text : str  — final assistant text response
      messages      : list — full conversation history including tool calls
      turns         : int  — number of assistant turns used
      tool_calls    : list[str] — names of all tools dispatched
    """
    if tools is None:
        tools = ALL_TOOLS

    client = _get_client()
    working_messages = list(messages)
    tool_calls_log: List[str] = []
    turns = 0

    while turns < max_turns:
        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system_prompt,
            tools=tools,
            messages=working_messages,
        )
        turns += 1

        # Append assistant response to history
        working_messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            # Extract text from final response
            text_blocks = [b.text for b in response.content if hasattr(b, "text")]
            response_text = "\n".join(text_blocks)
            return {
                "response_text": response_text,
                "messages": working_messages,
                "turns": turns,
                "tool_calls": tool_calls_log,
            }

        if response.stop_reason == "tool_use":
            tool_results = _dispatch_tool_uses(response.content)
            tool_calls_log.extend(
                b.name for b in response.content if b.type == "tool_use"
            )
            working_messages.append({"role": "user", "content": tool_results})
            continue

        # Unexpected stop reason — treat as terminal
        logger.warning("Unexpected stop_reason=%r — treating as end_turn", response.stop_reason)
        text_blocks = [b.text for b in response.content if hasattr(b, "text")]
        response_text = "\n".join(text_blocks)
        return {
            "response_text": response_text,
            "messages": working_messages,
            "turns": turns,
            "tool_calls": tool_calls_log,
        }

    raise RuntimeError(
        f"Agent loop reached max_turns={max_turns} without stopping. "
        "Increase max_turns or simplify the task."
    )


def run_agent_loop_streaming(
    messages: List[Dict[str, Any]],
    system_prompt: str,
    tools: Optional[List[Dict[str, Any]]] = None,
    model: str = _DEFAULT_MODEL,
    max_tokens: int = _DEFAULT_MAX_TOKENS,
    max_turns: int = _DEFAULT_MAX_TURNS,
) -> Iterator[str]:
    """
    Run the agentic loop with streaming output (for Streamlit st.write_stream).

    Yields text delta strings as Claude generates them.  Tool-call turns are
    dispatched synchronously; a status line is yielded before each tool call
    so the operator can see progress.

    Parameters
    ----------
    Same as run_agent_loop().

    Yields
    ------
    str chunks — text deltas from the assistant, plus "[Tool: name...]" status lines
    """
    if tools is None:
        tools = ALL_TOOLS

    client = _get_client()
    working_messages = list(messages)
    turns = 0

    while turns < max_turns:
        tool_uses_in_turn: list = []
        text_in_turn: list = []

        with client.messages.stream(
            model=model,
            max_tokens=max_tokens,
            system=system_prompt,
            tools=tools,
            messages=working_messages,
        ) as stream:
            for text_chunk in stream.text_stream:
                yield text_chunk
                text_in_turn.append(text_chunk)

            final_message = stream.get_final_message()

        turns += 1
        working_messages.append({"role": "assistant", "content": final_message.content})

        if final_message.stop_reason == "end_turn":
            return

        if final_message.stop_reason == "tool_use":
            # Emit a status line per tool call before dispatching
            for block in final_message.content:
                if block.type == "tool_use":
                    tool_uses_in_turn.append(block)
                    yield f"\n\n`[Tool: {block.name}]` "

            tool_results = _dispatch_tool_uses(final_message.content)
            working_messages.append({"role": "user", "content": tool_results})
            # Start the next streaming turn
            continue

        # Unexpected stop reason
        logger.warning("Streaming: unexpected stop_reason=%r", final_message.stop_reason)
        return

    raise RuntimeError(
        f"Streaming agent loop reached max_turns={max_turns} without stopping."
    )
