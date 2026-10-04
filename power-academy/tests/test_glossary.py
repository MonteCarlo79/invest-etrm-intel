from academy.glossary import check_pair, load_glossary, prompt_block

TERMS = [{"en": "spark spread", "zh": "火花价差"}, {"en": "swing option", "zh": "摆动期权"}]


def test_check_pair_flags_missing_zh_term():
    assert check_pair("The Spark Spread is wide.", "火花价差很宽。", TERMS) == []
    errs = check_pair("The spark spread is wide.", "发电利润价差很宽。", TERMS)
    assert errs == ["'spark spread' should appear as '火花价差'"]
    assert check_pair("nothing relevant", "无关", TERMS) == []


def test_seed_glossary_loads_and_prompt_block():
    terms = load_glossary("glossary/terms.yaml")
    assert len(terms) >= 15
    assert all(t["en"] and t["zh"] for t in terms)
    block = prompt_block(TERMS)
    assert "spark spread = 火花价差" in block
