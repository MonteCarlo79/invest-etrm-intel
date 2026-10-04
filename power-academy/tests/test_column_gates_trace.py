from academy.column.gates import check_fact_trace, extract_tags, number_lines

DRAFT = """# 标题 2026年

山东现货10月3日实时均价0.42元/kWh，高于日前。[[E:e_sd]]

- 第一，机制不同。
- 第二，价格形成不同。

2026年10月，山西、山东等8省转入正式运行。[[E:ex_rule]]

首轮现货试点覆盖8省，基差0.05元。[[E:ex_rule]]

英国市场在1990年代启动池化交易。[[E:wf_gb]]
"""

def test_extract_tags():
    assert extract_tags(DRAFT) == {"e_sd", "ex_rule", "wf_gb"}


def test_number_lines_flags_quantities_not_dates_or_lists():
    lines = number_lines(DRAFT)
    texts = [t for _, t in lines]
    assert any("0.42元/kWh" in t for t in texts)
    assert any("8省" in t for t in texts)
    assert any("2026年10月，山西" in t for t in texts)   # date-led, but quantity after date
    assert not any("第一" in t for t in texts)
    assert not any("1990年代" in t for t in texts)
    assert not any("标题" in t for t in texts)           # heading without quantity


def test_number_lines_heading_and_decimal_lead_prose_flagged():
    texts = [t for _, t in number_lines(
        "## 山东均价0.42元创新高。[[E:e1]]\n1.5元/kWh的均价背后，是机制差异。[[E:e1]]\n1. 第一点。\n")]
    assert any("均价0.42元" in t for t in texts)
    assert any("1.5元/kWh" in t for t in texts)
    assert not any("第一点" in t for t in texts)


def test_fact_trace_unknown_and_uncovered():
    errs = check_fact_trace("均价0.42元/kWh。[[E:ghost]]\n基差扩大0.05元。", {"e_sd"})
    joined = " | ".join(errs)
    assert "unknown tag" in joined and "uncovered" in joined
    assert check_fact_trace("均价0.42元/kWh。[[E:e_sd]]", {"e_sd"}) == []
    # date-led line with an uncovered quantity must fail
    assert check_fact_trace("2026年10月，山东实时均价0.42元/kWh。", {"e_sd"})
