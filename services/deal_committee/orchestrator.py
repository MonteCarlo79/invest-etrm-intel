# services/deal_committee/orchestrator.py
"""Committee pipeline — runs the 7 sections sequentially and assembles a CommitteeResult."""
from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass, field
from typing import Callable, Optional

from services.deal_committee.brief import DealBrief
from services.deal_committee.economics import (
    EconomicsResult, economics_section_markdown, run_economics,
)
from services.deal_committee.sections import SECTION_DEFS, SectionResult, build_question

QueryFn = Callable[[str, str, str], str]  # (agent_key, question, api_key) -> markdown


def default_query_fn(market: str, question: str, api_key: str) -> str:
    from services.hermes.market_agent_bridge import run_market_query  # lazy: hermes deps
    return run_market_query(market, question, api_key=api_key)


@dataclass
class CommitteeResult:
    brief: DealBrief
    sections: list[SectionResult]
    economics: Optional[EconomicsResult] = None
    synthesis: str = ""
    recommendation: str = ""


def run_risk_section(brief: DealBrief, engine=None) -> SectionResult:
    """Pull latest rm_* snapshots as the risk-benchmark table (no agent)."""
    from sqlalchemy import text
    from services.common.db_utils import get_engine
    engine = engine or get_engine()
    sql = text("""
        SELECT b.name AS book, p.snapshot_date,
               p.realized_cny, p.unrealized_mtm_cny,
               p.curtailment_rate_pct, p.equivalent_hours,
               v.var_1d_95_cny, v.var_10d_95_cny
        FROM marketdata.rm_pnl_snapshots p
        JOIN marketdata.rm_books b ON b.id = p.book_id
        LEFT JOIN marketdata.rm_var_snapshots v
               ON v.book_id = p.book_id AND v.snapshot_date = p.snapshot_date
              AND v.method = 'historical'
        WHERE p.snapshot_date >= CURRENT_DATE - INTERVAL '6 months'
        ORDER BY p.snapshot_date DESC, b.name
        LIMIT 60
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    if not rows:
        return SectionResult(key="risk", title="风险数据",
                             markdown="近 6 个月无风险台账(rm_*)快照数据。")
    lines = ["| 台账 | 日期 | 已实现P&L(¥M) | 未实现MtM(¥M) | 限电率 | VaR(1d,95%,¥M) |",
             "|---|---|---|---|---|---|"]
    for r in rows:
        lines.append(
            f"| {r[0]} | {r[1]} | {(r[2] or 0)/1e6:.2f} | {(r[3] or 0)/1e6:.2f} "
            f"| {('—' if r[4] is None else f'{float(r[4]):.1f}%')} "
            f"| {('—' if r[6] is None else f'{float(r[6])/1e6:.2f}')} |"
        )
    return SectionResult(key="risk", title="风险数据", markdown="\n".join(lines))


def _run_agent_with_timeout(query_fn: QueryFn, agent: str, question: str,
                            api_key: str, timeout_s: int) -> str:
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        fut = ex.submit(query_fn, agent, question, api_key)
        return fut.result(timeout=timeout_s)
    except concurrent.futures.TimeoutError:
        raise TimeoutError(f"章节超时(>{timeout_s}s)")
    finally:
        ex.shutdown(wait=False)  # leaked thread finishes in background; cannot kill threads


def run_single_section(key: str, brief: DealBrief, query_fn: QueryFn, api_key: str,
                       econ_fn=None, risk_fn=None, timeout_s: int = 180,
                       ) -> tuple[SectionResult, Optional[EconomicsResult]]:
    """Run one section. Returns (SectionResult, EconomicsResult|None for the economics key)."""
    title = next(s.title for s in SECTION_DEFS if s.key == key)
    try:
        if key == "economics":
            res = (econ_fn or run_economics)(brief)
            return SectionResult(key, title, economics_section_markdown(res, brief)), res
        if key == "risk":
            return (risk_fn or run_risk_section)(brief), None
        q = build_question(key, brief)
        agent = next(s.agent for s in SECTION_DEFS if s.key == key)
        md = _run_agent_with_timeout(query_fn, agent, q, api_key, timeout_s)
        return SectionResult(key, title, md), None
    except Exception as e:
        return SectionResult(key, title, status="failed", error=str(e)), None


def run_committee(brief: DealBrief, query_fn: QueryFn = default_query_fn, api_key: str = "",
                  econ_fn=None, risk_fn=None,
                  on_section_done: Optional[Callable[[SectionResult], None]] = None,
                  timeout_s: int = 180) -> CommitteeResult:
    result = CommitteeResult(brief=brief, sections=[])
    for sdef in SECTION_DEFS:
        sec, econ = run_single_section(sdef.key, brief, query_fn, api_key,
                                       econ_fn=econ_fn, risk_fn=risk_fn, timeout_s=timeout_s)
        if econ is not None:
            result.economics = econ
        result.sections.append(sec)
        if on_section_done:
            on_section_done(sec)
    return result
