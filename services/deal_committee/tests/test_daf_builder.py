# services/deal_committee/tests/test_daf_builder.py
from services.deal_committee.brief import DealBrief
from services.deal_committee.daf_builder import build_daf, split_synthesis
from services.deal_committee.orchestrator import CommitteeResult
from services.deal_committee.sections import SectionResult
from services.deal_committee.tests.test_orchestrator import _fake_econ, BRIEF

SYNTH = ("## 交易摘要\n蒙西 100MW/200MWh 储能项目,经济性达标。\n"
         "## 风险分析\n1. 价格下行风险 / 可能性:中 / 影响:收入-15% / 缓释:签订保底\n"
         "## 投资建议\n结论:有条件 GO\n核心假设:均价 ≥ 300 元/MWh")


def test_split_synthesis_three_parts():
    parts = split_synthesis(SYNTH)
    assert "蒙西 100MW" in parts["交易摘要"]
    assert "价格下行风险" in parts["风险分析"]
    assert "有条件 GO" in parts["投资建议"]


def test_split_synthesis_missing_sections_default_empty():
    parts = split_synthesis("## 交易摘要\n只有摘要")
    assert parts["交易摘要"] and parts["风险分析"] == "" and parts["投资建议"] == ""


def test_split_synthesis_numbered_headings():
    synth = ("## 一、交易摘要\n编号标题下的摘要内容\n"
             "## 二、风险分析\n编号标题下的风险内容\n"
             "## 三、投资建议\n结论:有条件 GO\n编号标题下的建议内容")
    parts = split_synthesis(synth)
    assert "编号标题下的摘要内容" in parts["交易摘要"]
    assert "编号标题下的风险内容" in parts["风险分析"]
    assert "有条件 GO" in parts["投资建议"]
    assert "编号标题下的建议内容" in parts["投资建议"]


def test_build_daf_produces_valid_pdf():
    result = CommitteeResult(
        brief=BRIEF,
        sections=[
            SectionResult("market_background", "市场背景", "近12个月均价 320 元/MWh"),
            SectionResult("policy", "政策与规则环境", "容量补偿政策已落地"),
            SectionResult("economics", "经济性测算", "见下表"),
            SectionResult("ops_mengxi", "运营实证 · 蒙西储能", "捕获率 82%"),
            SectionResult("ops_asset_risk", "运营实证 · 资产风险台账", "组合 VaR 稳定"),
            SectionResult("ops_retail_risk", "运营实证 · 零售风险台账", "无该省零售业务",
                          status="failed", error="no data"),
            SectionResult("risk", "风险数据", "| 台账 | VaR |\n|---|---|\n| A | 1.0 |"),
        ],
        economics=_fake_econ(BRIEF),
        synthesis=SYNTH,
        recommendation="有条件 GO",
    )
    pdf = build_daf(result)
    assert pdf.startswith(b"%PDF")
    assert len(pdf) > 10_000


def test_build_daf_handles_failed_sections_and_no_economics():
    result = CommitteeResult(
        brief=DealBrief(deal_name="最小案例", province="山东"),
        sections=[SectionResult("market_background", "市场背景", "失败",
                                status="failed", error="timeout")],
        economics=None, synthesis="", recommendation="",
    )
    pdf = build_daf(result)
    assert pdf.startswith(b"%PDF")


def test_full_seam_chain_committee_result_to_pdf():
    """Locks the orchestrator → synthesis → DAF seam: all 7 sections + 3-part synthesis."""
    synthesis = ("## 交易摘要\n蒙西 100MW/200MWh 独立储能,全投资 IRR 达标。\n"
                 "## 风险分析\n"
                 "1. 现货价格下行 / 可能性:中 / 影响:收入-15% / 缓释:容量租赁托底\n"
                 "2. 政策退坡 / 可能性:低 / 影响:补偿减少 / 缓释:跟踪政策\n"
                 "## 投资建议\n"
                 "结论:有条件 GO\n"
                 "条件:1) 均价 ≥ 300 元/MWh;2) 负债率 ≤ 70%。")
    result = CommitteeResult(
        brief=BRIEF,
        sections=[
            SectionResult("market_background", "市场背景", "近12个月均价 320 元/MWh,价差 0.42 元/kWh"),
            SectionResult("policy", "政策与规则环境", "容量补偿 0.3 元/kWh 已落地"),
            SectionResult("economics", "经济性测算", "股权 IRR P50 9.0%,见 Monte Carlo 结果"),
            SectionResult("ops_mengxi", "运营实证 · 蒙西储能", "同类电站捕获率 82%,日均 1.1 循环"),
            SectionResult("ops_asset_risk", "运营实证 · 资产风险台账", "组合 VaR 稳定,无超限记录"),
            SectionResult("ops_retail_risk", "运营实证 · 零售风险台账", "该省零售敞口可控"),
            SectionResult("risk", "风险数据", "| 台账 | VaR |\n|---|---|\n| A | 1.0 |"),
        ],
        economics=_fake_econ(BRIEF),
        synthesis=synthesis,
        recommendation="有条件 GO",
    )
    pdf = build_daf(result)
    assert pdf.startswith(b"%PDF")
    assert len(pdf) > 10_000
