"""
BayesianAnalystAgent — a thinking-mode agent that reasons via
Prior → Evidence → Posterior for any analytical question.

Triggered from HermesAgent via the BAYESIAN_ANALYSIS action.
"""
from __future__ import annotations

import logging
import re
from typing import Optional

import psycopg2
from anthropic import Anthropic

logger = logging.getLogger(__name__)

_MAX_ITER = 6

_SYSTEM_PROMPT = """You are a Bayesian reasoning agent embedded in a BESS energy trading platform.

Your job is to answer analytical questions by following a strict reasoning cycle:

═══════════════════════════════════════════════
STEP 1 — PRIOR  (always first, before any tool call)
═══════════════════════════════════════════════
State your prior belief explicitly:
• What would you expect, based on first principles or historical base rates?
• Confidence in the prior: Low / Medium / High (and roughly why)
• What specific evidence would most update this prior?

═══════════════════════════════════════════════
STEP 2 — EVIDENCE GATHERING  (use tools)
═══════════════════════════════════════════════
Use tools to gather evidence that can confirm, refute, or quantify the prior:
• search_kb      — search policy documents, reports, research in the knowledge base
• query_db       — query market data tables for quantitative evidence (SELECT only)
• query_market   — ask a specialist market agent for analysis or data

Be explicit: for each tool call, state what you are looking for and why.
Aim for evidence that is DIAGNOSTIC — i.e., it distinguishes between hypotheses.

═══════════════════════════════════════════════
STEP 3 — POSTERIOR  (final answer)
═══════════════════════════════════════════════
After gathering evidence, call give_posterior with:
• How the evidence updated your prior (likelihood ratios, directional shifts)
• The posterior estimate with a confidence range or probability
• Residual uncertainty — what you still don't know
• What would change your view further

Rules:
- ALWAYS state the prior before calling any tool.
- Do NOT skip straight to tools without articulating the prior.
- The posterior must reference specific evidence, not just repeat the prior.
- If evidence is absent or thin, say so explicitly and widen the uncertainty band.
- Match the language of the user's question (Chinese if Chinese, English if English).
"""

_TOOL_DEFS = [
    {
        "name": "search_kb",
        "description": (
            "Search the knowledge base (policy documents, market reports, regulatory filings, research). "
            "Returns the most relevant chunks. Use this to find policy context, historical precedents, "
            "or qualitative evidence relevant to the prior."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query in Chinese or English"},
                "top_k": {"type": "integer", "description": "Number of results to return (default 6, max 10)", "default": 6},
            },
            "required": ["query"],
        },
    },
    {
        "name": "query_db",
        "description": (
            "Run a read-only SELECT query against the platform database. "
            "Returns results as a markdown table (max 50 rows). "
            "Key tables: marketdata.md_id_cleared_energy (BESS 15-min dispatch), "
            "marketdata.md_da_cleared_energy (day-ahead prices), "
            "marketdata.province_installed_monthly (installed capacity by province/month), "
            "marketdata.province_cap_comp (capacity compensation rates), "
            "reports.nodal_pf_annual (annual BESS nodal PF scores)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SELECT SQL to execute"},
                "rationale": {"type": "string", "description": "Why this query is diagnostically useful"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "query_market",
        "description": (
            "Ask a specialist market agent a data or analysis question. "
            "Use for complex market questions that require multi-step reasoning over market data. "
            "Markets: spot (China spot prices), bess-map (BESS economics/IRR), mengxi (Inner Mongolia ops), "
            "gb (Great Britain), au (Australia), ercot, caiso, pjm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "market": {"type": "string", "description": "Market code: spot|bess-map|mengxi|gb|au|ercot|caiso|pjm"},
                "question": {"type": "string", "description": "The full question to ask the market agent"},
            },
            "required": ["market", "question"],
        },
    },
    {
        "name": "give_posterior",
        "description": (
            "Submit your final posterior estimate. Call this ONLY after you have stated a prior "
            "and gathered at least one piece of evidence. This ends the reasoning loop."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "posterior": {
                    "type": "string",
                    "description": (
                        "Full posterior answer. Must include: "
                        "(1) how evidence updated the prior, "
                        "(2) posterior estimate with confidence range, "
                        "(3) residual uncertainty and what would further change the view."
                    ),
                },
            },
            "required": ["posterior"],
        },
    },
]


class BayesianAnalystAgent:
    """
    Reasoning agent for the Prior → Evidence → Posterior thinking cycle.
    Called synchronously; returns the full analysis as a string.
    """

    _BLOCKED_SQL = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\b",
        re.IGNORECASE,
    )

    def __init__(
        self,
        anthropic_api_key: str,
        pg_url: str,
        model: Optional[str] = None,
    ) -> None:
        self._api_key = anthropic_api_key
        self._pg_url = pg_url
        self._model = model or "claude-sonnet-4-6"
        self._client = Anthropic(api_key=anthropic_api_key)

    # ── Tools ─────────────────────────────────────────────────────────────────

    def _tool_search_kb(self, query: str, top_k: int = 6) -> str:
        try:
            from services.knowledge_pool.advanced_retrieval import retrieve_for_agent
            top_k = min(int(top_k), 10)
            result = retrieve_for_agent(
                query=query,
                api_key=self._api_key,
                use_hyde=True,
                use_rerank=True,
                top_k=top_k,
            )
            return result or "No relevant knowledge found."
        except Exception as exc:
            logger.warning("BayesianAgent search_kb failed: %s", exc)
            return f"KB search error: {exc}"

    def _tool_query_db(self, sql: str) -> str:
        if self._BLOCKED_SQL.search(sql):
            return "ERROR: Only SELECT statements are permitted."
        try:
            conn = psycopg2.connect(self._pg_url, options="-c statement_timeout=15000")
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        rows = cur.fetchmany(50)
                        if not rows:
                            return "(no rows returned)"
                        cols = [d[0] for d in cur.description]
                        lines = [
                            "| " + " | ".join(cols) + " |",
                            "| " + " | ".join("---" for _ in cols) + " |",
                        ]
                        for row in rows:
                            lines.append("| " + " | ".join(str(v) for v in row) + " |")
                        return "\n".join(lines)
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("BayesianAgent query_db error: %s", exc)
            return f"DB error: {exc}"

    def _tool_query_market(self, market: str, question: str) -> str:
        try:
            from services.hermes.market_agent_bridge import run_market_query
            return run_market_query(market=market, question=question, api_key=self._api_key)
        except Exception as exc:
            logger.warning("BayesianAgent query_market failed: %s", exc)
            return f"Market agent error: {exc}"

    def _dispatch(self, name: str, tool_input: dict) -> str:
        if name == "search_kb":
            return self._tool_search_kb(
                tool_input["query"],
                int(tool_input.get("top_k", 6)),
            )
        if name == "query_db":
            return self._tool_query_db(tool_input["sql"])
        if name == "query_market":
            return self._tool_query_market(tool_input["market"], tool_input["question"])
        if name == "give_posterior":
            return "__POSTERIOR__"  # sentinel — caller extracts the param
        return f"ERROR: Unknown tool '{name}'"

    # ── Main entry point ──────────────────────────────────────────────────────

    def run(self, question: str) -> str:
        """
        Run the Bayesian reasoning loop and return the full analysis as a string.
        The string includes the prior, evidence trail, and posterior.
        """
        messages = [{"role": "user", "content": question}]
        prior_text: str = ""
        posterior_text: str = ""
        evidence_trail: list[str] = []
        iteration = 0

        while iteration < _MAX_ITER:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=2048,
                system=_SYSTEM_PROMPT,
                tools=_TOOL_DEFS,
                messages=messages,
            )

            # Collect any text blocks as running commentary
            text_blocks = [b.text for b in response.content if b.type == "text" and b.text.strip()]
            if text_blocks and iteration == 0:
                # First text block is expected to be the prior
                prior_text = "\n\n".join(text_blocks)

            if response.stop_reason == "end_turn":
                # No more tool calls — use last text as posterior if give_posterior wasn't called
                if text_blocks and not posterior_text:
                    posterior_text = "\n\n".join(text_blocks)
                break

            # Process tool calls
            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue

                if block.name == "give_posterior":
                    posterior_text = block.input.get("posterior", "")
                    # Return immediately — reasoning complete
                    return _format_output(prior_text, evidence_trail, posterior_text)

                result = self._dispatch(block.name, block.input)
                rationale = block.input.get("rationale", "")
                evidence_trail.append(
                    f"**[{block.name}]** {rationale or block.input.get('query', block.input.get('sql', ''))[:120]}\n{result[:800]}"
                )
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
            iteration += 1

        # Fallback if loop exhausted without give_posterior
        if not posterior_text:
            posterior_text = prior_text or "（分析未能完成，请重试）"
            prior_text = ""

        return _format_output(prior_text, evidence_trail, posterior_text)


def _format_output(prior: str, evidence: list[str], posterior: str) -> str:
    """Format the three-stage analysis into a readable Feishu message."""
    parts: list[str] = []

    if prior:
        parts.append(f"**🎯 先验 Prior**\n{prior}")

    if evidence:
        evidence_str = "\n\n".join(evidence)
        parts.append(f"**🔍 证据 Evidence**\n{evidence_str}")

    if posterior:
        parts.append(f"**📊 后验 Posterior**\n{posterior}")

    return "\n\n─\n\n".join(parts) if parts else posterior
