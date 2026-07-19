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
from shared.anthropic_client import make_client as _make_anthropic_client

logger = logging.getLogger(__name__)

_MAX_ITER = 8

def _build_system_prompt() -> str:
    from datetime import datetime, timezone, timedelta
    bj_now = datetime.now(tz=timezone(timedelta(hours=8)))
    current_date = bj_now.strftime("%Y-%m-%d")
    current_year = bj_now.year
    h2_start = f"{current_year}-07-01"
    h2_end = f"{current_year}-12-31"
    return f"""You are a Bayesian reasoning agent embedded in a BESS energy trading platform.

IMPORTANT — TEMPORAL CONTEXT:
  Today's date (Beijing time): {current_date}
  Current year: {current_year}
  "下半年" / "H2" / "second half of the year" = {h2_start} to {h2_end} — the FUTURE months to forecast.
  Historical data from BEFORE today is EVIDENCE, not the answer.
  Your posterior must address the FUTURE period (upcoming months), not recap historical data.

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

═══════════════════════════════════════════════
GUIDANCE: ELECTRICITY PRICE OUTLOOK QUESTIONS
═══════════════════════════════════════════════
When asked about future/H2 price levels for a Chinese province, follow this evidence hierarchy:

NOTE: Pre-fetched data (spot_daily + interprov_flow + exchange_monthly_metrics) is already injected
into your context if available. Do NOT re-query the same tables — use the pre-fetched data directly
and proceed to search_exchange_reports or give_posterior.

1. HISTORICAL SEASONALITY — use the pre-fetched public.spot_daily data (same province, same calendar
   months in prior years). Identify seasonal patterns (summer peak, winter trough, etc.).

2. OFFICIAL EXCHANGE REPORTS — call search_exchange_reports with the province name and topic
   (e.g. "上海 2025年夏季 现货价格 高温 用电量"). These reports contain official settlement prices,
   load levels, renewable percentages, and year-on-year comparisons.
   Also check pre-fetched staging.exchange_monthly_metrics for structured data.

3. CROSS-PROVINCIAL FLOWS — use the pre-fetched staging.spot_interprov_flow data.
   High import share (受端 province_share) suppresses local prices;
   rising import prices signal upward pressure.
   Example: Shanghai (上海) is a net importer; hydro inflows from Yunnan/Sichuan lower prices.

4. CAPACITY STRUCTURE — query marketdata.province_installed_monthly for recent solar/wind/BESS
   installed capacity only if not already in the pre-fetched data.

5. POLICY & RULES — use search_kb ONLY for market rules, price caps/floors, or policy changes.
   Do NOT use search_kb for price levels or market statistics — use search_exchange_reports instead.

5. POSTERIOR FORMAT for price outlook questions must include:
   - Monthly price range estimate (¥/kWh, DA and RT separately if asked)
   - Key upside risks (heat wave, hydro deficit, coal price spike)
   - Key downside risks (surplus imports, renewable curtailment period, demand slowdown)
   - Confidence level (Low/Medium/High) with reasoning

Rules:
- ALWAYS state the prior before calling any tool.
- Do NOT skip straight to tools without articulating the prior.
- The posterior must reference specific evidence, not just repeat the prior.
- If evidence is absent or thin, say so explicitly and widen the uncertainty band.
- You ARE permitted to give calibrated price estimates (ranges, not point forecasts). Do not refuse
  to estimate — instead, widen the range and state uncertainty clearly.
- MANDATORY: You MUST call give_posterior after 2–3 rounds of evidence. Do NOT keep gathering more
  evidence — 2–4 tool calls is sufficient. If pre-fetched data is already in the prompt, 1 round
  of supplemental queries is enough. Call give_posterior next.
- If the system forces tool_choice=give_posterior, call it immediately with your best estimate.
- Match the language of the user's question (Chinese if Chinese, English if English).
"""


_TOOL_DEFS = [
    {
        "name": "search_exchange_reports",
        "description": (
            "Search the indexed exchange monthly/quarterly reports (电力交易月报). "
            "These are official reports published by provincial electricity exchanges covering: "
            "market transaction volumes, settlement prices (spot/contract/BESS), load levels, "
            "renewable energy ratios, market participants, and year-on-year comparisons. "
            "Use this for qualitative and quantitative context FROM OFFICIAL EXCHANGE REPORTS. "
            "Provinces available: 上海, 山东, 安徽, 江苏, 浙江, 广东, 福建, 冀南, 蒙西."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query, include province name and topic (e.g. '上海 2025年夏季 现货价格 高温')"},
                "top_k": {"type": "integer", "description": "Number of results to return (default 5, max 10)", "default": 5},
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_kb",
        "description": (
            "Search the general knowledge base (policy documents, research reports, regulatory filings). "
            "Use ONLY for policy/regulatory context, market rules, or research — NOT for price data or "
            "exchange statistics (use search_exchange_reports or query_db for those)."
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
            "Key tables:\n"
            "  public.spot_daily — daily DA/RT spot prices per province "
            "(columns: report_date, province_en, province_cn, da_avg, da_max, da_min, rt_avg, rt_max, rt_min; unit ¥/kWh). "
            "Covers 2024-08 to present for most provinces. Use for historical price baselines.\n"
            "  staging.spot_interprov_flow — inter-provincial spot trading "
            "(columns: report_date, direction TEXT ['送端'|'受端'], metric_type, province_cn, "
            "province_share NUMERIC%, price_yuan_kwh, price_chg_pct, time_period, total_vol_100gwh). "
            "Covers 2025-01 to present. Use to analyse cross-border power flows and their price impact.\n"
            "  staging.exchange_monthly_metrics — structured metrics extracted from official exchange monthly reports "
            "(columns: province TEXT, report_month DATE, report_type TEXT, "
            "total_volume_gwh, spot_volume_gwh, medium_longterm_volume_gwh, "
            "avg_price_yuan_mwh, spot_avg_price_yuan_mwh, contract_avg_price_yuan_mwh, "
            "peak_price_yuan_mwh, valley_price_yuan_mwh, "
            "bess_settlement_price_yuan_mwh, bess_traded_volume_gwh, "
            "incoming_volume_gwh, outgoing_volume_gwh, "
            "max_load_gw, renewable_pct, wind_pct, solar_pct, "
            "thermal_settlement_price_yuan_mwh, wind_settlement_price_yuan_mwh, solar_settlement_price_yuan_mwh). "
            "Province is in Chinese (e.g. '上海'). Covers 2024-01 to present. "
            "Some columns may be NULL for older rows — use COALESCE or IS NOT NULL filters.\n"
            "  marketdata.province_installed_monthly — columns: province (TEXT), year_month (DATE), "
            "wind_mw, solar_mw, thermal_mw, hydro_mw, nuclear_mw, bess_mw, total_mw (all NUMERIC). "
            "Filter by province name in Chinese (e.g. '上海'). No province_cn or energy_type column.\n"
            "  marketdata.md_id_cleared_energy (BESS 15-min intraday dispatch), "
            "marketdata.md_da_cleared_energy (day-ahead cleared energy), "
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
        self._model = model or "claude-3-5-sonnet-20241022"  # on-demand Bedrock; 4.6 cross-region profile blocks tool use
        self._client = _make_anthropic_client(anthropic_api_key)

    # ── Tools ─────────────────────────────────────────────────────────────────

    def _tool_search_exchange_reports(self, query: str, top_k: int = 5) -> str:
        """Targeted search within monthly_report category only."""
        try:
            from services.knowledge_pool.knowledge_docs import search_reference_docs
            top_k = min(int(top_k), 10)
            results = search_reference_docs(
                query=query,
                category="monthly_report",
                limit=top_k,
            )
            if not results:
                return "No exchange reports found for this query."
            parts = []
            for i, hit in enumerate(results, 1):
                parts.append(
                    f"**[{i}] {hit['file_name']}** (page {hit.get('page_no', '?')}, rank {hit.get('rank', '—'):.1f})\n"
                    f"{hit['chunk_text'][:600]}"
                )
            return "\n\n".join(parts)
        except Exception as exc:
            logger.warning("BayesianAgent search_exchange_reports failed: %s", exc)
            return f"Exchange report search error: {exc}"

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
        if name == "search_exchange_reports":
            return self._tool_search_exchange_reports(
                tool_input["query"],
                int(tool_input.get("top_k", 5)),
            )
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

    # ── Pre-fetch helpers ─────────────────────────────────────────────────────

    # Province CN→EN mapping for prefetch queries
    _PROVINCE_EN = {
        "上海": "Shanghai", "北京": "Beijing", "广东": "Guangdong",
        "山东": "Shandong", "浙江": "Zhejiang", "江苏": "Jiangsu",
        "湖南": "Hunan", "湖北": "Hubei", "四川": "Sichuan",
        "云南": "Yunnan", "福建": "Fujian", "安徽": "Anhui",
        "广西": "Guangxi", "贵州": "Guizhou", "山西": "Shanxi",
        "陕西": "Shaanxi", "新疆": "Xinjiang", "青海": "Qinghai",
        "甘肃": "Gansu", "内蒙古": "Inner Mongolia", "辽宁": "Liaoning",
        "吉林": "Jilin", "黑龙江": "Heilongjiang", "河北": "Hebei",
        "河南": "Henan", "天津": "Tianjin", "重庆": "Chongqing",
        "江西": "Jiangxi", "宁夏": "Ningxia", "西藏": "Tibet", "海南": "Hainan",
    }

    def _prefetch_spot_data(self, question: str) -> str:
        """
        For spot price forecast questions, run key DB queries upfront and
        return a compact context block to inject into the initial prompt.
        This ensures the agent has enough data to conclude without burning
        all iterations on data gathering.
        """
        triggers = ["电价", "价格", "price", "spot", "现货", "均价"]
        if not any(t in question.lower() for t in triggers):
            return ""

        # Detect province from question
        province_cn = "上海"  # default
        province_en = "Shanghai"
        for cn, en in self._PROVINCE_EN.items():
            if cn in question:
                province_cn = cn
                province_en = en
                break

        try:
            # 1. Monthly averages (all available history)
            monthly = self._tool_query_db(
                "SELECT DATE_TRUNC('month', report_date)::date AS month, "
                "ROUND(AVG(da_avg)::numeric, 4) AS avg_da, "
                "ROUND(AVG(rt_avg)::numeric, 4) AS avg_rt, "
                "COUNT(*) FILTER (WHERE da_avg IS NOT NULL) AS days "
                "FROM public.spot_daily "
                f"WHERE province_en = '{province_en}' "
                "GROUP BY 1 ORDER BY 1"
            )
            # 2. Recent interprov flows (last 6 months)
            interprov = self._tool_query_db(
                "SELECT DATE_TRUNC('month', report_date)::date AS month, "
                "direction, "
                "ROUND(AVG(price_yuan_kwh)::numeric, 4) AS avg_price, "
                "ROUND(AVG(province_share)::numeric, 1) AS avg_share "
                "FROM staging.spot_interprov_flow "
                f"WHERE province_cn = '{province_cn}' "
                "AND report_date >= CURRENT_DATE - INTERVAL '6 months' "
                "GROUP BY 1, 2 ORDER BY 1, 2"
            )
            # 3. Exchange monthly metrics (official exchange report data)
            exchange = self._tool_query_db(
                "SELECT report_month, report_type, "
                "total_volume_gwh, spot_volume_gwh, incoming_volume_gwh, "
                "avg_price_yuan_mwh, spot_avg_price_yuan_mwh, contract_avg_price_yuan_mwh, "
                "peak_price_yuan_mwh, valley_price_yuan_mwh, "
                "bess_settlement_price_yuan_mwh, bess_traded_volume_gwh, "
                "max_load_gw, renewable_pct "
                "FROM staging.exchange_monthly_metrics "
                f"WHERE province = '{province_cn}' "
                "ORDER BY report_month DESC LIMIT 18"
            )
            return (
                "\n\n---\n**PRE-FETCHED DATA (use this — do NOT re-query the same tables):**\n\n"
                f"**{province_cn}月度现货均价 (public.spot_daily):**\n{monthly}\n\n"
                f"**{province_cn}近6个月省间流量 (staging.spot_interprov_flow):**\n{interprov}\n\n"
                f"**{province_cn}电力交易月报结构化指标 (staging.exchange_monthly_metrics, 单位MWh/GWh/GW):**\n{exchange}\n"
                "---\n"
            )
        except Exception as exc:
            logger.warning("Prefetch failed: %s", exc)
            return ""

    # ── Main entry point ──────────────────────────────────────────────────────

    def run(self, question: str) -> str:
        """
        Run the Bayesian reasoning loop and return the full analysis as a string.
        The string includes the prior, evidence trail, and posterior.
        """
        prefetch = self._prefetch_spot_data(question)
        initial_content = question + prefetch if prefetch else question

        messages = [{"role": "user", "content": initial_content}]
        prior_text: str = ""
        posterior_text: str = ""
        evidence_trail: list[str] = []
        iteration = 0
        _FORCE_POSTERIOR_AT = 2  # force give_posterior after this many tool rounds

        while iteration < _MAX_ITER:
            # After _FORCE_POSTERIOR_AT rounds, force the model to call give_posterior
            _tool_choice = (
                {"type": "tool", "name": "give_posterior"}
                if iteration >= _FORCE_POSTERIOR_AT
                else {"type": "auto"}
            )
            response = self._client.messages.create(
                model=self._model,
                max_tokens=2048,
                system=_build_system_prompt(),
                tools=_TOOL_DEFS,
                tool_choice=_tool_choice,
                messages=messages,
            )

            # Collect any text blocks as running commentary
            text_blocks = [b.text for b in response.content if b.type == "text" and b.text.strip()]
            if text_blocks and iteration == 0:
                # First text block is expected to be the prior
                prior_text = "\n\n".join(text_blocks)

            if response.stop_reason == "end_turn":
                if not posterior_text:
                    # Model ended without calling give_posterior — force one conclude call
                    messages.append({"role": "assistant", "content": response.content})
                    forced_resp = self._client.messages.create(
                        model=self._model,
                        max_tokens=2048,
                        system=_build_system_prompt(),
                        tools=_TOOL_DEFS,
                        tool_choice={"type": "tool", "name": "give_posterior"},
                        messages=messages,
                    )
                    for b in forced_resp.content:
                        if b.type == "tool_use" and b.name == "give_posterior":
                            posterior_text = b.input.get("posterior", "")
                            break
                    if not posterior_text:
                        posterior_text = "\n\n".join(text_blocks)
                break

            # Process tool calls
            tool_results = []
            posterior_received = False
            for block in response.content:
                if block.type != "tool_use":
                    continue

                if block.name == "give_posterior":
                    posterior_text = block.input.get("posterior", "")
                    posterior_received = True
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": "Posterior recorded.",
                    })
                    continue

                result = self._dispatch(block.name, block.input)
                rationale = block.input.get("rationale", "")
                evidence_trail.append(
                    f"**[{block.name}]** {rationale or block.input.get('query', block.input.get('sql', ''))[:120]}\n{result[:600]}"
                )
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result[:2000] or "(no result)",
                })

            if posterior_received:
                return _format_output(prior_text, evidence_trail, posterior_text)

            if not tool_results:
                # Model produced no tool calls — end loop
                if text_blocks and not posterior_text:
                    posterior_text = "\n\n".join(text_blocks)
                break

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})

            iteration += 1

        # Fallback if loop exhausted without give_posterior
        if not posterior_text:
            posterior_text = prior_text or "（分析未能完成，请重试）"
            prior_text = ""

        return _format_output(prior_text, evidence_trail, posterior_text)

    def run_pdf(self, question: str) -> bytes:
        """Run analysis and return a PDF byte string."""
        text = self.run(question)
        return _render_pdf(question, text)


def _render_pdf(title: str, body: str) -> bytes:
    """Render the analysis text as a PDF using ReportLab."""
    import io
    from datetime import datetime, timezone, timedelta
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    # Register CJK font if available
    _FONT = "Helvetica"
    for _path in [
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
    ]:
        try:
            pdfmetrics.registerFont(TTFont("CJK", _path))
            _FONT = "CJK"
            break
        except Exception:
            continue

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm,
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "HTitle", fontName=_FONT, fontSize=14, leading=20,
        textColor=colors.HexColor("#1a1a2e"), spaceAfter=6,
    )
    section_style = ParagraphStyle(
        "HSection", fontName=_FONT, fontSize=11, leading=15,
        textColor=colors.HexColor("#2c3e50"), spaceBefore=12, spaceAfter=4,
        fontWeight="bold",
    )
    body_style = ParagraphStyle(
        "HBody", fontName=_FONT, fontSize=9, leading=13,
        textColor=colors.HexColor("#333333"),
    )
    meta_style = ParagraphStyle(
        "HMeta", fontName=_FONT, fontSize=8, leading=11,
        textColor=colors.grey, spaceAfter=8,
    )

    bj_now = datetime.now(tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M 北京时间")
    story = [
        Paragraph("Hermes · 贝叶斯市场分析", title_style),
        Paragraph(title.replace("<", "&lt;").replace(">", "&gt;"), section_style),
        Paragraph(f"生成时间：{bj_now}", meta_style),
        HRFlowable(width="100%", thickness=0.5, color=colors.lightgrey, spaceAfter=8),
    ]

    # Split body into sections and paragraphs
    for line in body.split("\n"):
        stripped = line.strip()
        if not stripped:
            story.append(Spacer(1, 4))
            continue
        # Strip markdown bold markers for PDF
        clean = re.sub(r"\*\*(.+?)\*\*", r"\1", stripped)
        clean = clean.replace("<", "&lt;").replace(">", "&gt;")
        if stripped.startswith("**") and stripped.endswith("**"):
            story.append(Paragraph(clean, section_style))
        elif stripped.startswith("─"):
            story.append(HRFlowable(width="100%", thickness=0.3, color=colors.lightgrey, spaceBefore=4, spaceAfter=4))
        else:
            story.append(Paragraph(clean, body_style))

    doc.build(story)
    return buf.getvalue()


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
