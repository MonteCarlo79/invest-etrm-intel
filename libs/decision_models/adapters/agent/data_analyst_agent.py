"""
libs/decision_models/adapters/agent/data_analyst_agent.py

BESS Data Analyst Agent — data quality auditing, gap detection, and gap-filling.

Wraps the Claude API agentic loop (runner.py) with a system prompt grounded in
the platform design docs and the 10 BESS MCP tool functions.

Two modes of operation:

  1. Programmatic (no LLM):
       agent.audit(start_date, end_date)   → structured gap report (dict)
       agent.fill_gaps(start_date, end_date) → triggers ETL + LP batch, returns dict

  2. Interactive (LLM-powered):
       agent.answer_query(question, history) → (response_text, updated_history)
       agent.stream_query(question, history) → Iterator[str] for Streamlit

Environment variables
---------------------
ANTHROPIC_API_KEY : required for LLM methods
DB_DSN / PGURL    : required for all DB-backed tool calls
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

_REPO = Path(__file__).resolve().parents[4]
_DOCS_DIR = _REPO / "docs" / "platform-design"

# The 4 actively managed IM assets
_IM_ASSETS = ["suyou", "hangjinqi", "siziwangqi", "gushanliang"]


# ---------------------------------------------------------------------------
# System prompt (loaded once at module import)
# ---------------------------------------------------------------------------

def _load_platform_context() -> str:
    """Read data_contracts.md and agent_skills.md for grounding."""
    snippets: list[str] = []
    for doc_name in ("data_contracts", "agent_skills", "decision_modules"):
        p = _DOCS_DIR / f"{doc_name}.md"
        if p.exists():
            try:
                snippets.append(f"=== {doc_name}.md ===\n{p.read_text(encoding='utf-8')[:3000]}")
            except Exception:
                pass
    return "\n\n".join(snippets)


_PLATFORM_CONTEXT = _load_platform_context()

SYSTEM_PROMPT = f"""\
You are the BESS Data Analyst Agent for the Inner Mongolia (Mengxi grid) \
four-asset BESS portfolio: suyou, hangjinqi, siziwangqi, gushanliang.

Your primary responsibility is data quality auditing, gap detection, and
triggering data fills so that the LP optimisation and P&L analysis workflows
always have complete inputs.

─── PLATFORM ARCHITECTURE (from design docs) ───────────────────────────────
{_PLATFORM_CONTEXT[:4000]}

─── DATA LAYERS YOU MONITOR ────────────────────────────────────────────────
1. RT nodal prices        : canon.nodal_rt_price_15min
   Backed by ETL from marketdata.md_id_cleared_energy (cleared_price).
   Fill gaps with: bess_canon_etl(start_date, end_date)

2. Ops dispatch (Excel)   : marketdata.ops_bess_dispatch_15min
   Source: Inner Mongolia daily Excel ops reports (nominated + actual dispatch,
   nodal_price_excel). Gaps = Excel file not yet ingested.

3. LP pre-computed results: reports.bess_strategy_dispatch_15min
                            reports.bess_asset_daily_scenario_pnl
   Two LP scenarios: perfect_foresight_hourly and forecast_ols_rt_time_v1.
   Fill gaps with: bess_lp_batch(asset_codes, start_date, end_date, force=True)

4. Ops P&L scenarios      : reports.bess_asset_daily_scenario_pnl
   Three ops-derived scenarios: nominated_dispatch, cleared_actual, trading_cleared.
   These are written by run_daily_strategy_batch.py alongside LP results.

5. Trading cleared energy : marketdata.md_id_cleared_energy
   Source: EnOS market data feed.  Cleared_energy_mwh × cleared_price = trading P&L.

─── AUDIT PROTOCOL ────────────────────────────────────────────────────────
When asked to audit or investigate data quality:
1. Call bess_data_quality_report to get a full picture of all gaps.
2. For each gap category, call the specific gap-list tool to get date lists.
3. Report the gaps clearly, grouped by severity:
   - CRITICAL: RT prices missing (LP cannot run without prices)
   - HIGH:     LP results missing (UI will re-run CBC solver on every click)
   - MEDIUM:   Ops dispatch missing (nominated/actual scenarios unavailable)
   - LOW:      Trading cleared gaps (secondary data source)
4. Provide explicit commands to fill the gaps.

─── GAP-FILLING PROTOCOL ──────────────────────────────────────────────────
When filling gaps:
1. First run bess_canon_etl for the date range if price gaps exist.
2. Then run bess_lp_batch per asset (individually) for LP gaps.
   Always pass force=true when re-running after a bug fix.
3. Report success/failure per asset with returncode and key log lines.

─── DATA CAVEATS ───────────────────────────────────────────────────────────
- Inner Mongolia Mengxi is a PURE RT spot market. There is no DA price.
  The correct LP forecast model is ols_rt_time_v1 (not ols_da_time_v1).
- cleared_energy_mwh in md_id_cleared_energy is intraday-cleared TRADING volume,
  NOT physical dispatch. Do not conflate with actual_dispatch_mw from ops files.
- Prices are in CNY/MWh. Dispatch is in MW. P&L is in CNY.
- gushanliang is 500 MW / 4h; the other 3 assets are 100 MW / 4h.

Use CNY (¥) for amounts. Be concise; prefer tables for data-heavy outputs.
"""

# ---------------------------------------------------------------------------
# Claude tool definitions for the 10 BESS MCP functions
# ---------------------------------------------------------------------------

DATA_ANALYST_TOOLS: List[Dict[str, Any]] = [
    {
        "name": "bess_check_completeness",
        "description": (
            "Build a per-asset × per-date coverage matrix across 5 data layers: "
            "RT prices, ops dispatch, LP PF, LP forecast, trading cleared."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_codes": {"type": ["array", "null"], "items": {"type": "string"},
                                "description": "null = all 4 IM assets"},
                "start_date":  {"type": "string", "description": "ISO date, e.g. '2026-03-01'"},
                "end_date":    {"type": "string", "description": "ISO date, e.g. '2026-04-23'"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "bess_price_gaps",
        "description": "List dates with no RT prices in canon.nodal_rt_price_15min for an asset.",
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {"type": "string"},
                "start_date": {"type": "string"},
                "end_date":   {"type": "string"},
            },
            "required": ["asset_code", "start_date", "end_date"],
        },
    },
    {
        "name": "bess_ops_gaps",
        "description": "List dates with no ops Excel dispatch data for an asset.",
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code": {"type": "string"},
                "start_date": {"type": "string"},
                "end_date":   {"type": "string"},
            },
            "required": ["asset_code", "start_date", "end_date"],
        },
    },
    {
        "name": "bess_lp_gaps",
        "description": "List dates missing LP pre-computed results (PF and/or forecast).",
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_codes": {"type": ["array", "null"], "items": {"type": "string"}},
                "start_date":  {"type": "string"},
                "end_date":    {"type": "string"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "bess_canon_etl",
        "description": (
            "Run the canon RT nodal price ETL (populate_canon_nodal_prices.py). "
            "Fills canon.nodal_rt_price_15min from md_id_cleared_energy.cleared_price."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string"},
                "end_date":   {"type": "string"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "bess_lp_batch",
        "description": (
            "Run the LP pre-computation batch for given assets and date range. "
            "Each asset is run in a separate subprocess. Results go to DB."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_codes": {"type": ["array", "null"], "items": {"type": "string"}},
                "start_date":  {"type": "string"},
                "end_date":    {"type": "string"},
                "force":       {"type": "boolean", "default": False},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "bess_portfolio_pnl",
        "description": "Retrieve all 5-strategy P&L rows from reports.bess_asset_daily_scenario_pnl.",
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_codes": {"type": ["array", "null"], "items": {"type": "string"}},
                "start_date":  {"type": "string"},
                "end_date":    {"type": "string"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "bess_dispatch_series",
        "description": (
            "Retrieve 15-min dispatch time series for one asset / date / scenario. "
            "Valid scenarios: perfect_foresight_hourly, forecast_ols_rt_time_v1, "
            "nominated_dispatch, cleared_actual, trading_cleared."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_code":    {"type": "string"},
                "trade_date":    {"type": "string"},
                "scenario_name": {"type": "string"},
            },
            "required": ["asset_code", "trade_date", "scenario_name"],
        },
    },
    {
        "name": "bess_platform_docs",
        "description": (
            "Read platform design documentation. "
            "Available: agent_skills, data_contracts, db_spot_market, "
            "decision_modules, implementation_design, platform_roadmap, "
            "platform_skills, ui_china_geo_map, ui_data_management_tab."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "doc_name": {"type": ["string", "null"],
                             "description": "Filename without .md; null to list all docs"},
            },
        },
    },
    {
        "name": "bess_data_quality_report",
        "description": (
            "Comprehensive data quality report with gap counts per layer and "
            "actionable recommendations."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_codes": {"type": ["array", "null"], "items": {"type": "string"}},
                "start_date":  {"type": "string"},
                "end_date":    {"type": "string"},
            },
            "required": ["start_date", "end_date"],
        },
    },
]


def _dispatch_tool(name: str, inputs: dict) -> Any:
    """Route a tool_use block to the correct BESS MCP tool function."""
    from services.bess_mcp.tools import (
        bess_check_data_completeness,
        bess_list_price_gaps,
        bess_list_ops_dispatch_gaps,
        bess_list_lp_gaps,
        bess_run_canon_etl,
        bess_run_lp_batch,
        bess_get_portfolio_pnl,
        bess_get_dispatch_series,
        bess_get_platform_docs,
        bess_get_data_quality_report,
    )
    _MAP = {
        "bess_check_completeness": lambda i: bess_check_data_completeness(
            i.get("asset_codes"), i["start_date"], i["end_date"]),
        "bess_price_gaps":  lambda i: bess_list_price_gaps(
            i["asset_code"], i["start_date"], i["end_date"]),
        "bess_ops_gaps":    lambda i: bess_list_ops_dispatch_gaps(
            i["asset_code"], i["start_date"], i["end_date"]),
        "bess_lp_gaps":     lambda i: bess_list_lp_gaps(
            i.get("asset_codes"), i["start_date"], i["end_date"]),
        "bess_canon_etl":   lambda i: bess_run_canon_etl(
            i["start_date"], i["end_date"]),
        "bess_lp_batch":    lambda i: bess_run_lp_batch(
            i.get("asset_codes"), i["start_date"], i["end_date"], i.get("force", False)),
        "bess_portfolio_pnl": lambda i: bess_get_portfolio_pnl(
            i.get("asset_codes"), i["start_date"], i["end_date"]),
        "bess_dispatch_series": lambda i: bess_get_dispatch_series(
            i["asset_code"], i["trade_date"], i["scenario_name"]),
        "bess_platform_docs": lambda i: bess_get_platform_docs(i.get("doc_name")),
        "bess_data_quality_report": lambda i: bess_get_data_quality_report(
            i.get("asset_codes"), i["start_date"], i["end_date"]),
    }
    if name not in _MAP:
        return {"error": f"Unknown tool: {name}"}
    try:
        return _MAP[name](inputs)
    except Exception as exc:
        logger.exception("Tool %s failed: %s", name, exc)
        return {"error": str(exc), "tool": name}


# ---------------------------------------------------------------------------
# Agent class
# ---------------------------------------------------------------------------

class BessDataAnalystAgent:
    """
    BESS Data Analyst Agent.

    Programmatic usage (no LLM):
        agent = BessDataAnalystAgent()
        gaps = agent.audit("2026-03-01", "2026-04-23")
        fills = agent.fill_gaps("2026-04-01", "2026-04-23")

    Interactive usage (LLM-powered):
        response, history = agent.answer_query(
            "Which assets have LP results missing for April?",
            start_date="2026-04-01", end_date="2026-04-30"
        )
    """

    def __init__(
        self,
        asset_codes: Optional[List[str]] = None,
        model: str = "claude-sonnet-4-6",
        max_turns: int = 12,
    ) -> None:
        self.asset_codes = asset_codes or _IM_ASSETS
        self.model = model
        self.max_turns = max_turns

    # ------------------------------------------------------------------
    # Programmatic: audit (no LLM)
    # ------------------------------------------------------------------

    def audit(
        self,
        start_date: str,
        end_date: str,
    ) -> dict:
        """
        Run a full data quality audit for all managed assets over the date range.

        Returns a structured gap report without invoking the LLM.

        Returns
        -------
        dict with keys: summary, price_gaps, ops_gaps, lp_pf_gaps,
                        lp_forecast_gaps, trading_cleared_gaps, recommendations
        """
        from services.bess_mcp.tools import bess_get_data_quality_report
        report = bess_get_data_quality_report(self.asset_codes, start_date, end_date)
        logger.info(
            "audit %s→%s: assets=%s", start_date, end_date, self.asset_codes,
        )
        return report

    # ------------------------------------------------------------------
    # Programmatic: fill_gaps (no LLM)
    # ------------------------------------------------------------------

    def fill_gaps(
        self,
        start_date: str,
        end_date: str,
        force: bool = True,
    ) -> dict:
        """
        Automatically fill data gaps by:
          1. Running canon ETL if any asset has RT price gaps
          2. Running LP batch per asset for dates with missing LP results

        Returns a summary of actions taken.
        """
        from services.bess_mcp.tools import (
            bess_get_data_quality_report,
            bess_run_canon_etl,
            bess_run_lp_batch,
        )

        report = bess_get_data_quality_report(self.asset_codes, start_date, end_date)
        actions: list[dict] = []

        # Step 1: fill price gaps
        any_price_gaps = any(
            len(gaps) > 0 for gaps in report.get("price_gaps", {}).values()
        )
        if any_price_gaps:
            logger.info("fill_gaps: running canon ETL for %s→%s", start_date, end_date)
            etl_result = bess_run_canon_etl(start_date, end_date)
            actions.append({"action": "canon_etl", "result": etl_result})

        # Step 2: fill LP gaps — run each asset individually
        lp_gaps = report.get("lp_pf_gaps", {})
        fc_gaps = report.get("lp_forecast_gaps", {})
        assets_needing_lp = [
            a for a in self.asset_codes
            if len(lp_gaps.get(a, [])) > 0 or len(fc_gaps.get(a, [])) > 0
        ]
        if assets_needing_lp:
            logger.info(
                "fill_gaps: running LP batch for assets=%s, %s→%s, force=%s",
                assets_needing_lp, start_date, end_date, force,
            )
            lp_result = bess_run_lp_batch(
                assets_needing_lp, start_date, end_date, force=force
            )
            actions.append({"action": "lp_batch", "result": lp_result})

        if not actions:
            return {
                "status": "no_gaps_found",
                "message": f"All data layers complete for {start_date}→{end_date}.",
                "actions": [],
            }

        return {
            "status": "fills_triggered",
            "assets": self.asset_codes,
            "period": {"start_date": start_date, "end_date": end_date},
            "actions": actions,
        }

    # ------------------------------------------------------------------
    # Interactive: answer_query (LLM + tools)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Internal: own agentic loop (uses BESS MCP tools, not decision model tools)
    # ------------------------------------------------------------------

    def _run_loop(
        self,
        messages: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Synchronous agentic loop using DATA_ANALYST_TOOLS and _dispatch_tool.
        Independent of runner.py so it does not inherit the decision model tool set.
        """
        import json as _json
        import anthropic

        client = anthropic.Anthropic()
        working = list(messages)
        tool_calls_log: list[str] = []

        for _turn in range(self.max_turns):
            response = client.messages.create(
                model=self.model,
                max_tokens=8192,
                system=SYSTEM_PROMPT,
                tools=DATA_ANALYST_TOOLS,
                messages=working,
            )

            tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
            text_blocks     = [b for b in response.content if hasattr(b, "text")]

            if response.stop_reason == "end_turn" or not tool_use_blocks:
                return {
                    "response_text": "\n".join(b.text for b in text_blocks),
                    "tool_calls": tool_calls_log,
                    "turns": _turn + 1,
                }

            # Append assistant message
            working.append({"role": "assistant", "content": response.content})

            # Dispatch tool calls
            tool_results = []
            for block in tool_use_blocks:
                tool_calls_log.append(block.name)
                logger.debug("DataAnalystAgent tool_call: %s", block.name)
                raw = _dispatch_tool(block.name, block.input)
                # Serialize result to JSON string (Anthropic API requires string content)
                result_str = _json.dumps(raw, default=str)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_str,
                })
            working.append({"role": "user", "content": tool_results})

        raise RuntimeError(
            f"DataAnalystAgent reached max_turns={self.max_turns} without stopping."
        )

    def _stream_loop(
        self,
        messages: List[Dict[str, Any]],
    ) -> Iterator[str]:
        """Streaming agentic loop yielding text delta strings."""
        import json as _json
        import anthropic

        client = anthropic.Anthropic()
        working = list(messages)

        for _turn in range(self.max_turns):
            tool_use_blocks_collected = []
            text_parts: list[str] = []
            stop_reason = None
            raw_content = []

            with client.messages.stream(
                model=self.model,
                max_tokens=8192,
                system=SYSTEM_PROMPT,
                tools=DATA_ANALYST_TOOLS,
                messages=working,
            ) as stream:
                for event in stream:
                    event_type = getattr(event, "type", None)
                    if event_type == "content_block_delta":
                        delta = getattr(event, "delta", None)
                        if delta and getattr(delta, "type", None) == "text_delta":
                            yield delta.text
                            text_parts.append(delta.text)
                final = stream.get_final_message()
                stop_reason = final.stop_reason
                raw_content = final.content

            tool_use_blocks_collected = [b for b in raw_content if b.type == "tool_use"]

            if stop_reason == "end_turn" or not tool_use_blocks_collected:
                return

            working.append({"role": "assistant", "content": raw_content})
            tool_results = []
            for block in tool_use_blocks_collected:
                yield f"\n\n*[Calling {block.name}...]*\n\n"
                logger.debug("DataAnalystAgent stream tool_call: %s", block.name)
                raw = _dispatch_tool(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": _json.dumps(raw, default=str),
                })
            working.append({"role": "user", "content": tool_results})

    # ------------------------------------------------------------------
    # Interactive: answer_query (LLM + tools)
    # ------------------------------------------------------------------

    def answer_query(
        self,
        question: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        conversation_history: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Answer a data-related question using the LLM + BESS MCP tools.

        Returns
        -------
        (response_text, updated_conversation_history)
        """
        history = list(conversation_history or [])
        user_content = question
        if not history and (start_date or end_date):
            ctx_parts = []
            if start_date:
                ctx_parts.append(f"start_date={start_date}")
            if end_date:
                ctx_parts.append(f"end_date={end_date}")
            user_content = (
                f"Context: {', '.join(ctx_parts)}. Assets: {self.asset_codes}.\n\n"
                f"{question}"
            )
        history.append({"role": "user", "content": user_content})
        result = self._run_loop(history)
        response_text = result["response_text"]
        history.append({"role": "assistant", "content": response_text})
        return response_text, history

    # ------------------------------------------------------------------
    # Interactive: stream_query (LLM + tools, for Streamlit)
    # ------------------------------------------------------------------

    def stream_query(
        self,
        question: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        conversation_history: Optional[List[Dict[str, Any]]] = None,
    ) -> Iterator[str]:
        """
        Stream a data query response chunk by chunk (for st.write_stream).
        """
        history = list(conversation_history or [])
        user_content = question
        if not history and (start_date or end_date):
            parts = []
            if start_date:
                parts.append(f"start_date={start_date}")
            if end_date:
                parts.append(f"end_date={end_date}")
            user_content = (
                f"Context: {', '.join(parts)}. Assets: {self.asset_codes}.\n\n"
                f"{question}"
            )
        history.append({"role": "user", "content": user_content})
        yield from self._stream_loop(history)
