from types import SimpleNamespace

from services.deal_committee.brief import DealBrief
from services.deal_committee.sections import SectionResult
from services.deal_committee.synthesis import (
    build_synthesis_prompt, parse_recommendation, run_synthesis,
)

BRIEF = DealBrief(deal_name="蒙西储能一期", asset_type="bess", province="蒙西",
                  capacity_mw=100, capacity_mwh=200, capex_total_yuan=1.2e9)
SECTIONS = [
    SectionResult("market_background", "市场背景", "近12个月均价 320 元/MWh"),
    SectionResult("policy", "政策与规则环境", "容量补偿政策已落地", status="ok"),
    SectionResult("ops_mengxi", "运营实证 · 蒙西储能", "捕获率 82%", status="failed",
                  error="agent exploded"),
]


class _FakeClient:
    def __init__(self, text): self._text = text
    @property
    def messages(self): return self
    def create(self, **kw): return SimpleNamespace(content=[SimpleNamespace(text=self._text)])


def test_prompt_contains_sections_kpis_and_grounding():
    p = build_synthesis_prompt(BRIEF, SECTIONS, None)
    assert "蒙西储能一期" in p
    assert "320 元/MWh" in p
    assert "容量补偿政策已落地" in p
    assert "agent exploded" in p          # failures are visible to the synthesizer
    assert "不得编造" in p                # grounding rule
    assert "GO" in p and "NO-GO" in p     # output contract


def test_parse_recommendation_variants():
    assert parse_recommendation("## 投资建议\n结论:GO\n理由…") == "GO"
    assert parse_recommendation("## 投资建议\n结论:有条件 GO,前提是…") == "有条件 GO"
    assert parse_recommendation("## 投资建议\n结论:NO-GO") == "NO-GO"
    assert parse_recommendation("结论:谨慎推进(有条件 GO)") == "有条件 GO"
    assert parse_recommendation("没有结论") == ""


def test_run_synthesis_returns_text_and_label():
    md = "## 交易摘要\n……\n## 风险分析\n……\n## 投资建议\n结论:有条件 GO\n……"
    text, rec = run_synthesis(BRIEF, SECTIONS, None, api_key="", client=_FakeClient(md))
    assert "交易摘要" in text
    assert rec == "有条件 GO"
