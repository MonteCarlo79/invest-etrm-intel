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
你是内蒙古四资产储能（BESS）投资组合的专业交易绩效分析师。

资产：suyou（苏由）、hangjinqi（杭锦旗）、siziwangqi（四子王旗）、gushanliang（鼓山梁）
（均位于内蒙古蒙西电网）

你可以使用17个工具，涵盖：
  - 每日策略分析（run_bess_daily_strategy_analysis、run_all_assets_daily_strategy_analysis）
  - 实现率与脆弱性监控（query_realization_status、query_fragility_status）
  - 完美预见调度、预测套件、策略排名、归因分析
  - 收益情景引擎、盈亏归因、调度优化
  - 报告与看板生成

─── 每日报告协议 ───────────────────────────────────────────────────────────
收到每日复盘请求时：
1. 调用 run_all_assets_daily_strategy_analysis，传入指定日期，一次性获取全部4个资产的策略绩效。
2. 调用 query_realization_status（不加筛选条件），获取所有资产的30日滚动实现率及状态
   （NORMAL/WARN/ALERT/CRITICAL）。
3. 调用 query_fragility_status（不加筛选条件），获取各资产综合脆弱性评分
   （LOW/MEDIUM/HIGH/CRITICAL）。
4. 综合以上数据，用Markdown格式撰写结构化运营报告，必须包含以下章节：

## 投资组合概览
一段话。总结总预测盈亏、与完美预见基准的平均捕获率、已加载的运营调度行数及任何组合级数据缺口。金额单位使用人民币（元）。

## 各资产亮点
每个资产3至5条要点，涵盖：
  - 最优策略及其盈亏（与完美预见基准对比）
  - 运营调度数据是否完整（96行 = 全天）
  - 可用归因数据中的主要损失项
  - 30日实现率状态与脆弱性等级

## 预警与标记
列出所有实现率状态为ALERT/CRITICAL或脆弱性等级为HIGH/CRITICAL的资产。
每项注明：资产名称、状态级别、主要损失项、监控系统叙述。
如无预警，写"无 — 所有资产运行正常。"

## 建议措施
3至5条编号的可操作建议，面向运营/交易团队。
例如：跟进数据缺口、调整申报策略、排查限电原因。

─── 临时查询协议 ──────────────────────────────────────────────────────────
对于运营人员的临时问题：使用所需工具，先引用数据再下结论。
始终说明以下可比性注意事项：
  - 逐小时完美预见/预测盈亏与15分钟运营盈亏不可直接比较
  - nominated_dispatch_mw（申报）≠ md_id_cleared_energy.cleared_energy_mwh
  - actual_dispatch_mw（实际）≠ md_id_cleared_energy.cleared_energy_mwh
  - 省级日前电价代理指标可能与资产级节点电价存在偏差

金额使用人民币（元）。保持简洁，数据部分优先使用表格。
所有报告内容均使用简体中文撰写。
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

    def run_daily_review(
        self,
        date: str,
        forecast_model: str = "ols_rt_time_v1",
    ) -> DailyOpsReviewResult:
        """
        Run the full daily trading performance review for all 4 IM assets.

        Parameters
        ----------
        date           : ISO date string, e.g. "2026-04-17"
        forecast_model : price forecast model to use; default 'ols_rt_time_v1'
                         (RT-only, no DA prices required).
                         Options: ols_rt_time_v1, naive_rt_lag1, naive_rt_lag7,
                                  ols_da_time_v1, naive_da
        """
        generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        initial_message = (
            f"对全部4个内蒙古储能资产（suyou、hangjinqi、siziwangqi、gushanliang）执行{date}的每日交易绩效复盘。\n\n"
            f"使用价格预测模型：{forecast_model}。\n\n"
            f"请按照每日报告协议执行：加载策略分析，检查监控状态，并生成包含以下章节的结构化Markdown报告："
            f"投资组合概览、各资产亮点、预警与标记、建议措施。"
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
