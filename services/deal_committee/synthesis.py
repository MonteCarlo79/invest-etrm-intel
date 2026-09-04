"""Committee synthesis — one grounded LLM call producing summary, risks, recommendation."""
from __future__ import annotations

import re
from typing import Optional

from services.deal_committee.brief import DealBrief
from services.deal_committee.economics import EconomicsResult
from services.deal_committee.sections import SectionResult

SYNTHESIS_MODEL = "claude-sonnet-4-6"


def build_synthesis_prompt(brief: DealBrief, sections: list[SectionResult],
                           economics: Optional[EconomicsResult]) -> str:
    blocks = []
    for s in sections:
        body = s.markdown if s.status == "ok" else f"[本节生成失败: {s.error}]"
        blocks.append(f"### {s.title}\n{body}")
    if economics is not None:
        mc = economics.mc
        blocks.append(
            "### 经济性核心指标\n"
            f"收入 P10/P50/P90: ¥{mc.revenue_p10/1e6:.1f}M / ¥{mc.revenue_p50/1e6:.1f}M / ¥{mc.revenue_p90/1e6:.1f}M\n"
            f"股权 IRR P10/P50/P90: {mc.equity_irr_p10:.1%} / {mc.equity_irr_p50:.1%} / {mc.equity_irr_p90:.1%}\n"
            f"IRR 低于 8% 基准概率: {mc.irr_prob_below_hurdle:.0%}\n"
            f"NPV P50: ¥{mc.npv_p50/1e6:.1f}M"
        )
    return f"""你是投资决策委员会秘书。基于以下交易要素与各章节分析材料,撰写 DAF 的三个章节。

【交易要素】
{brief.model_dump_json(exclude={"field_confidence"})}

【章节材料】
{chr(10).join(blocks)}

【硬性要求】
1. 全程中文;技术术语可用英文(IRR、VaR、capture rate)。
2. 不得编造数字:所有数字必须来自上述材料;材料缺失或章节失败时,明确写出"数据缺失"及其对结论的影响。
3. 输出恰好三节,标题严格使用:
## 交易摘要
(项目、市场、经济性一句话概括,150 字内)
## 风险分析
(3-6 条编号风险,每条含:风险描述 / 可能性(高/中/低) / 影响 / 缓释措施)
## 投资建议
(首行写"结论:GO"或"结论:有条件 GO"或"结论:NO-GO";随后列出条件、风险缓释建议与核心假设)"""


def parse_recommendation(text: str) -> str:
    m = re.search(r"结论\s*[:：]\s*(?:谨慎推进[（(]?)?(有条件\s*GO|NO[-\s]?GO|GO)", text, re.I)
    if not m:
        m = re.search(r"(有条件\s*GO|NO[-\s]?GO)", text, re.I)
    if not m:
        return "GO" if re.search(r"\bGO\b", text) else ""
    label = m.group(1).upper().replace(" ", " ")
    label = re.sub(r"\s+", " ", label).replace("NO GO", "NO-GO")
    return "有条件 GO" if "有条件" in label else label


def run_synthesis(brief: DealBrief, sections: list[SectionResult],
                  economics: Optional[EconomicsResult], api_key: str,
                  client=None) -> tuple[str, str]:
    if client is None:
        from shared.anthropic_client import make_client
        client = make_client(api_key)
    resp = client.messages.create(
        model=SYNTHESIS_MODEL, max_tokens=6000,
        messages=[{"role": "user", "content": build_synthesis_prompt(brief, sections, economics)}],
    )
    text = resp.content[0].text.strip()
    return text, parse_recommendation(text)
