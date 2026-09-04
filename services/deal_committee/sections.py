# services/deal_committee/sections.py
"""Committee section definitions and per-section question builders."""
from __future__ import annotations

from dataclasses import dataclass

from services.deal_committee.brief import DealBrief


@dataclass(frozen=True)
class SectionDef:
    key: str
    title: str
    agent: str  # market_agent_bridge key; "" = non-agent section (economics / risk)


@dataclass
class SectionResult:
    key: str
    title: str
    markdown: str = ""
    status: str = "ok"  # "ok" | "failed"
    error: str = ""


SECTION_DEFS: tuple[SectionDef, ...] = (
    SectionDef("market_background", "市场背景", "spot"),
    SectionDef("policy", "政策与规则环境", "spot"),
    SectionDef("economics", "经济性测算", ""),
    SectionDef("ops_mengxi", "运营实证 · 蒙西储能", "mengxi"),
    SectionDef("ops_asset_risk", "运营实证 · 资产风险台账", "asset-risk"),
    SectionDef("ops_retail_risk", "运营实证 · 零售风险台账", "retail-risk"),
    SectionDef("risk", "风险数据", ""),
)


def _asset_desc(brief: DealBrief) -> str:
    parts = []
    if brief.asset_type in ("bess", "wind_bess", "solar_bess"):
        parts.append(f"{brief.capacity_mw:g}MW/{brief.capacity_mwh:g}MWh 储能")
    if brief.asset_type in ("wind", "wind_bess"):
        parts.append(f"{brief.installed_mw:g}MW 风电")
    if brief.asset_type in ("solar", "solar_bess"):
        parts.append(f"{brief.installed_mw:g}MW 光伏")
    return " + ".join(parts) or brief.asset_type


def build_question(key: str, brief: DealBrief) -> str:
    """Question sent to the headless agent for this section. KeyError for non-agent sections."""
    asset = _asset_desc(brief)
    node_clause = f",并网点/节点:{brief.node}" if brief.node else ""
    site = f"{brief.province}{node_clause}"
    questions = {
        "market_background": (
            f"作为电力市场分析师,评估{site}电力现货市场对新建{asset}项目的吸引力。"
            "请用数据回答:1) 近12个月日前/实时价格水平与走势;2) 价格波动率与峰谷价差;"
            "3) 省间送受电格局;4) 市场成熟度(结算试运行/正式运行)。中文回答。"
        ),
        "policy": (
            f"梳理{brief.province}电力市场关于{asset}的最新政策与交易规则:"
            "1) 现货市场结算规则要点;2) 独立储能/新能源参与现货与辅助服务的方式;"
            "3) 容量补偿/容量电价机制;4) 未来1-2年的政策风险点。"
            "请检索知识库文档并注明出处,中文回答。"
        ),
        "ops_mengxi": (
            "总结蒙西在运储能电站的实际运营表现:现货捕获率、日均循环次数、等效利用小时、"
            "结算均价水平、主要运营问题;并说明这些实证数据对评估新建"
            f"{asset}项目({brief.province})的参考意义。中文回答。"
        ),
        "ops_asset_risk": (
            f"汇总资产风险台账中与{brief.province}及同类({asset})资产相关的在运项目"
            "结算与 P&L 表现、最新 VaR 水平;如台账中无该省资产,请给出现有组合的基准数据并明确说明。"
            "中文回答。"
        ),
        "ops_retail_risk": (
            f"汇总零售风险台账中{brief.province}售电业务的批零价差、结算与保证金风险表现,"
            "评估该省市场流动性与零售侧价格信号;如该省无零售业务,请明确说明。中文回答。"
        ),
    }
    if key not in questions:
        raise KeyError(f"section {key!r} 不是 agent 章节(economics/risk 在本地生成)")
    return questions[key]
