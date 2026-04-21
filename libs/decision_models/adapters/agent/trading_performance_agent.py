"""
libs/decision_models/adapters/agent/trading_performance_agent.py

Operator-grade BESS trading performance monitoring agent.

Wraps the Claude API agentic loop (runner.py) with a domain-specific system
prompt for the 4 Inner Mongolia BESS assets and exposes two callable interfaces:

  agent.run_daily_review(date)
      → DailyOpsReviewResult — full structured report with narrative, alerts,
        and recommendations.  Designed for the automated daily batch job.

  agent.answer_query(question, date, conversation_history)
      → (response_text, updated_history) — multi-turn operator chat.
        Designed for the Streamlit interactive interface.

All agent requests are written to agent_request_log via logging_utils.

Environment variables
---------------------
ANTHROPIC_API_KEY : required (passed through to runner.py)
DB_DSN            : required for all tool calls that hit the database
"""
from __future__ import annotations

import dataclasses
import datetime
import logging
import re
from typing import Any, Dict, Iterator, List, Optional, Tuple

from libs.decision_models.adapters.agent.runner import (
    run_agent_loop,
    run_agent_loop_streaming,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are an operator-grade trading performance analyst for a 4-asset Inner Mongolia BESS portfolio.

Assets: suyou, hangjinqi, siziwangqi, gushanliang (all Mengxi grid, Inner Mongolia, China).

You have access to 17 tools covering:
  - Daily strategy analysis (run_bess_daily_strategy_analysis, run_all_assets_daily_strategy_analysis)
  - Realization and fragility monitoring (query_realization_status, query_fragility_status)
  - Perfect-foresight dispatch, forecast suite, strategy ranking, attribution
  - Revenue scenario engine, P&L attribution, dispatch optimization
  - Report and dashboard generation

─── DAILY REVIEW PROTOCOL ───────────────────────────────────────────────────
When asked to run a daily review:
1. Call run_all_assets_daily_strategy_analysis with the given date to get strategy
   performance for all 4 assets in one call.
2. Call query_realization_status (no filters) to get 30-day rolling realization
   ratios and status levels (NORMAL/WARN/ALERT/CRITICAL) for all assets.
3. Call query_fragility_status (no filters) to get composite fragility scores
   (LOW/MEDIUM/HIGH/CRITICAL) for all assets.
4. Synthesize the data and write a structured operator report in markdown with
   these exact sections:

## Portfolio Overview
One paragraph. Summarise total forecast P&L, average capture rate vs PF benchmark,
number of ops-dispatch rows loaded, and any portfolio-level data gaps. Use CNY.

## Per-Asset Highlights
For each of the 4 assets, 3–5 bullet points covering:
  - Best strategy and its P&L vs PF benchmark
  - Whether ops dispatch data was available (96 rows = full day)
  - Dominant loss bucket if attribution is available
  - Realization status (30d) and fragility level

## Alerts & Flags
List every asset with realization status ALERT/CRITICAL or fragility HIGH/CRITICAL.
For each: asset name, status level, dominant_loss_bucket, narrative from the monitor.
Write "None — all assets within normal operating range." if no alerts.

## Recommendations
3–5 numbered, actionable items for the ops/trading team.
Examples: follow up on data gaps, adjust nomination strategy, investigate curtailment.

─── AD-HOC QUERY PROTOCOL ───────────────────────────────────────────────────
For operator questions: use whatever tools are needed. Quote data before conclusions.
Always surface key comparability caveats:
  - Hourly PF/forecast P&L is NOT directly comparable to 15-min ops P&L
  - nominated_dispatch_mw (申报) ≠ md_id_cleared_energy.cleared_energy_mwh
  - actual_dispatch_mw (实际) ≠ md_id_cleared_energy.cleared_energy_mwh
  - Province-level DA price proxy may diverge from asset-level nodal prices

Use CNY for monetary figures. Be concise. Prefer tables over prose for data.
"""

# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class DailyOpsReviewResult:
    """Structured output from a daily trading performance review."""
    date: str
    generated_at: str
    narrative: str              # full Claude markdown report
    alerts: List[str]           # lines from the Alerts & Flags section
    recommendations: List[str]  # items from the Recommendations section
    n_assets_reviewed: int
    n_alerts: int               # count of ALERT/CRITICAL assets
    tool_calls: List[str]       # tool names dispatched (for audit)


# ---------------------------------------------------------------------------
# Agent class
# ---------------------------------------------------------------------------

class TradingPerformanceAgent:
    """
    Operator-grade BESS trading performance monitoring agent.

    Usage
    -----
    agent = TradingPerformanceAgent()
    result = agent.run_daily_review("2026-04-17")
    print(result.narrative)

    # Multi-turn chat
    response, history = agent.answer_query(
        "Why did suyou underperform today?",
        date="2026-04-17",
    )
    response2, history = agent.answer_query(
        "What about hangjinqi?",
        date="2026-04-17",
        conversation_history=history,
    )
    """

    def __init__(
        self,
        model: str = "claude-sonnet-4-6",
        max_turns: int = 10,
    ) -> None:
        self.model = model
        self.max_turns = max_turns

    # ------------------------------------------------------------------
    # Daily batch review
    # ------------------------------------------------------------------

    def run_daily_review(self, date: str) -> DailyOpsReviewResult:
        """
        Run the full daily trading performance review for all 4 IM assets.

        Calls the Claude agentic loop with the daily review protocol,
        parses the structured narrative, and returns a DailyOpsReviewResult.

        Parameters
        ----------
        date : ISO date string, e.g. "2026-04-17"

        Returns
        -------
        DailyOpsReviewResult
        """
        generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        initial_message = (
            f"Run the daily trading performance review for all 4 Inner Mongolia BESS "
            f"assets (suyou, hangjinqi, siziwangqi, gushanliang) on {date}.\n\n"
            f"Follow the DAILY REVIEW PROTOCOL: load strategy analysis, check monitoring "
            f"status, and produce the structured markdown report with Portfolio Overview, "
            f"Per-Asset Highlights, Alerts & Flags, and Recommendations sections."
        )

        messages = [{"role": "user", "content": initial_message}]

        logger.info("Starting daily review for date=%s", date)
        loop_result = run_agent_loop(
            messages=messages,
            system_prompt=SYSTEM_PROMPT,
            model=self.model,
            max_turns=self.max_turns,
        )

        narrative = loop_result["response_text"]
        tool_calls = loop_result["tool_calls"]

        alerts = _extract_section_items(narrative, "Alerts & Flags")
        recommendations = _extract_section_items(narrative, "Recommendations")
        n_alerts = _count_alert_assets(alerts)

        # Audit log (silently skip if DB not available)
        _log_request(
            agent_name="trading_performance_agent",
            request_text=f"daily_review date={date}",
            status="completed",
            metadata={
                "date": date,
                "turns": loop_result["turns"],
                "tool_calls": tool_calls,
                "n_alerts": n_alerts,
            },
        )

        logger.info(
            "Daily review complete date=%s turns=%d tools=%d n_alerts=%d",
            date, loop_result["turns"], len(tool_calls), n_alerts,
        )

        return DailyOpsReviewResult(
            date=date,
            generated_at=generated_at,
            narrative=narrative,
            alerts=alerts,
            recommendations=recommendations,
            n_assets_reviewed=4,
            n_alerts=n_alerts,
            tool_calls=tool_calls,
        )

    # ------------------------------------------------------------------
    # Interactive operator query (multi-turn)
    # ------------------------------------------------------------------

    def answer_query(
        self,
        question: str,
        date: str,
        conversation_history: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Answer an ad-hoc operator question using the full tool suite.

        Parameters
        ----------
        question             : the operator's question
        date                 : context date for tool calls (e.g. "2026-04-17")
        conversation_history : prior turns from st.session_state; None for new conversation

        Returns
        -------
        (response_text, updated_conversation_history)
          response_text is the final Claude response (markdown).
          updated_conversation_history includes the new turn and can be passed
          back on the next call for multi-turn continuity.
        """
        history = list(conversation_history) if conversation_history else []

        # Prepend context for fresh conversations
        user_content = question
        if not history:
            user_content = (
                f"Context: We are reviewing BESS trading performance for date {date}.\n\n"
                f"{question}"
            )

        history.append({"role": "user", "content": user_content})

        loop_result = run_agent_loop(
            messages=history,
            system_prompt=SYSTEM_PROMPT,
            model=self.model,
            max_turns=self.max_turns,
        )

        response_text = loop_result["response_text"]
        # Append assistant response to history for next turn
        history.append({"role": "assistant", "content": response_text})

        _log_request(
            agent_name="trading_performance_agent",
            request_text=question[:500],
            status="answered",
            metadata={"date": date, "turns": loop_result["turns"]},
        )

        return response_text, history

    # ------------------------------------------------------------------
    # Streaming query (for Streamlit st.write_stream)
    # ------------------------------------------------------------------

    def stream_query(
        self,
        question: str,
        date: str,
        conversation_history: Optional[List[Dict[str, Any]]] = None,
    ) -> Iterator[str]:
        """
        Stream an ad-hoc operator query response chunk by chunk.

        Yields text delta strings suitable for Streamlit's st.write_stream().
        Tool calls are dispatched synchronously; a status marker is yielded
        for each tool call so the operator sees progress.

        Parameters
        ----------
        question             : operator question
        date                 : context date
        conversation_history : prior turns (pass None for new conversation)

        Yields
        ------
        str chunks — text deltas and tool status markers
        """
        history = list(conversation_history) if conversation_history else []

        user_content = question
        if not history:
            user_content = (
                f"Context: We are reviewing BESS trading performance for date {date}.\n\n"
                f"{question}"
            )

        history.append({"role": "user", "content": user_content})

        yield from run_agent_loop_streaming(
            messages=history,
            system_prompt=SYSTEM_PROMPT,
            model=self.model,
            max_turns=self.max_turns,
        )

    def stream_daily_review(self, date: str) -> Iterator[str]:
        """
        Stream the daily review protocol, yielding text deltas for Streamlit.
        """
        initial_message = (
            f"Run the daily trading performance review for all 4 Inner Mongolia BESS "
            f"assets (suyou, hangjinqi, siziwangqi, gushanliang) on {date}.\n\n"
            f"Follow the DAILY REVIEW PROTOCOL: load strategy analysis, check monitoring "
            f"status, and produce the structured markdown report with Portfolio Overview, "
            f"Per-Asset Highlights, Alerts & Flags, and Recommendations sections."
        )
        messages = [{"role": "user", "content": initial_message}]
        yield from run_agent_loop_streaming(
            messages=messages,
            system_prompt=SYSTEM_PROMPT,
            model=self.model,
            max_turns=self.max_turns,
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_section_items(narrative: str, section_title: str) -> List[str]:
    """
    Extract bullet / numbered items from a markdown section.

    Looks for "## {section_title}" and collects list items until the next
    "##" heading or end of string.  Returns an empty list if the section
    is not found.
    """
    pattern = rf"##\s+{re.escape(section_title)}\s*\n(.*?)(?=\n##\s|\Z)"
    match = re.search(pattern, narrative, re.DOTALL | re.IGNORECASE)
    if not match:
        return []
    body = match.group(1)
    items = []
    for line in body.splitlines():
        stripped = line.strip()
        # Accept bullet points (- / *) and numbered items (1. 2. etc.)
        if re.match(r"^[-*•]\s+.+", stripped) or re.match(r"^\d+\.\s+.+", stripped):
            # Strip the leading marker
            text = re.sub(r"^[-*•]\s+|^\d+\.\s+", "", stripped)
            items.append(text)
    return items


def _count_alert_assets(alerts: List[str]) -> int:
    """Count distinct ALERT/CRITICAL mentions in extracted alert lines."""
    if not alerts:
        return 0
    # Check for the "none" sentinel
    if len(alerts) == 1 and "none" in alerts[0].lower():
        return 0
    return len(alerts)


def _log_request(
    agent_name: str,
    request_text: str,
    status: str,
    metadata: Dict[str, Any],
) -> None:
    """Write to agent_request_log; silently ignore failures."""
    try:
        import json
        from shared.agents.logging_utils import log_agent_request
        log_agent_request(
            user_email="system",
            agent_name=agent_name,
            request_text=request_text,
            status=status,
            metadata_json=json.dumps(metadata, default=str),
        )
    except Exception:
        pass
