"""Tests for news_screener batch scoring, keyword pre-filter, and scoring cap."""
import hashlib
import json
import re
from unittest.mock import MagicMock, patch

from services.hermes import news_screener as ns


def _article(title: str, body: str = "body text") -> dict:
    return {"title": title, "body": body, "url": "http://example.com/a"}


def _msg(text: str):
    m = MagicMock()
    m.content = [MagicMock(text=text)]
    return m


def _fake_client(responses: list) -> MagicMock:
    """Fake Anthropic client; consumes one response per messages.create call.
    A response that is an Exception instance is raised instead of returned."""
    client = MagicMock()
    client.messages.create.side_effect = [
        r if isinstance(r, Exception) else _msg(r) for r in responses
    ]
    return client


def _result(i: int) -> dict:
    return {
        "relevance": i,
        "region_bucket": "全国",
        "region_province": None,
        "category": "industry_news",
        "summary": f"摘要{i}",
    }


def _prompt_of(call) -> str:
    return call.kwargs["messages"][0]["content"]


def test_prefilter_no_keyword_no_llm_call():
    """Keyword-less title → scored 0 with NO client call."""
    arts = [_article("今日股市大涨创历史新高")]
    with patch("shared.anthropic_client.make_client") as mc:
        ns._apply_ai_scoring(arts, "test-key")
    mc.assert_not_called()
    assert arts[0]["ai_result"] == {
        "relevance": 0,
        "region_bucket": None,
        "region_province": None,
        "category": None,
        "summary": None,
    }


def test_batch_scoring_attaches_results_in_order():
    """Fenced JSON array for 3 articles → 3 results attached in order."""
    arts = [_article(f"储能行业新闻{i}") for i in range(3)]
    payload = [_result(i + 5) for i in range(3)]
    client = _fake_client(["```json\n" + json.dumps(payload) + "\n```"])
    with patch("shared.anthropic_client.make_client", return_value=client):
        ns._apply_ai_scoring(arts, "test-key")
    assert client.messages.create.call_count == 1
    assert [a["ai_result"]["relevance"] for a in arts] == [5, 6, 7]
    assert arts[0]["ai_result"]["summary"] == "摘要5"
    assert arts[2]["ai_result"]["region_bucket"] == "全国"


def test_batch_malformed_then_valid_retry():
    """First response garbage, retry returns valid → results used."""
    arts = [_article(f"电力市场快讯{i}") for i in range(2)]
    payload = [_result(6), _result(7)]
    client = _fake_client(["not json at all", json.dumps(payload)])
    with patch("shared.anthropic_client.make_client", return_value=client):
        ns._apply_ai_scoring(arts, "test-key")
    assert client.messages.create.call_count == 2
    assert [a["ai_result"]["relevance"] for a in arts] == [6, 7]


def test_batch_malformed_twice_falls_back_to_per_article():
    """Malformed twice → per-article _score_article fallback."""
    arts = [_article(f"新能源消纳{i}") for i in range(2)]
    client = _fake_client(["garbage1", "garbage2"])
    with patch("shared.anthropic_client.make_client", return_value=client), \
         patch.object(ns, "_score_article", side_effect=[_result(4), _result(5)]) as single:
        ns._apply_ai_scoring(arts, "test-key")
    assert client.messages.create.call_count == 2
    assert single.call_count == 2
    assert [a["ai_result"]["relevance"] for a in arts] == [4, 5]


def test_batch_claude_exception_returns_null_results():
    """Claude exception → null-result dicts (error-path shape), no retry, order preserved."""
    arts = [_article(f"储能电站投运{i}") for i in range(2)]
    client = _fake_client([RuntimeError("api down")])
    with patch("shared.anthropic_client.make_client", return_value=client):
        ns._apply_ai_scoring(arts, "test-key")
    assert client.messages.create.call_count == 1
    for a in arts:
        assert a["ai_result"] == {
            "relevance": None,
            "region_bucket": None,
            "region_province": None,
            "category": None,
            "summary": None,
        }


def test_cap_scores_only_120_articles():
    """125 keyword-matched articles → only 120 sent to Claude; the rest get null results."""
    arts = [_article(f"储能市场新闻 第{i}期") for i in range(125)]
    payload = json.dumps([_result(6)] * 10)
    client = _fake_client([payload] * 12)
    with patch("shared.anthropic_client.make_client", return_value=client):
        ns._apply_ai_scoring(arts, "test-key")
    assert client.messages.create.call_count == 12  # 120 articles / 10 per call
    for c in client.messages.create.call_args_list:
        assert len(re.findall(r"Article \d+\nTitle:", _prompt_of(c))) == 10
    scored = [a for a in arts if a["ai_result"]["relevance"] == 6]
    nulled = [a for a in arts if a["ai_result"]["relevance"] == 0]
    assert len(scored) == 120
    assert len(nulled) == 5
    # equal keyword hits → stable order → the tail of the list is dropped
    assert nulled == arts[120:]
    for a in nulled:
        assert a["ai_result"] == {
            "relevance": 0,
            "region_bucket": None,
            "region_province": None,
            "category": None,
            "summary": None,
        }


def test_batch_size_chunks_by_10():
    """25 articles → 3 Claude calls (10 + 10 + 5)."""
    arts = [_article(f"电力现货市场动态{i}") for i in range(25)]
    responses = [
        json.dumps([_result(6)] * 10),
        json.dumps([_result(6)] * 10),
        json.dumps([_result(6)] * 5),
    ]
    client = _fake_client(responses)
    with patch("shared.anthropic_client.make_client", return_value=client):
        ns._apply_ai_scoring(arts, "test-key")
    assert client.messages.create.call_count == 3
    sizes = [
        len(re.findall(r"Article \d+\nTitle:", _prompt_of(c)))
        for c in client.messages.create.call_args_list
    ]
    assert sizes == [10, 10, 5]
    assert all(a["ai_result"]["relevance"] == 6 for a in arts)


class TestSalvageBatchObjects:
    def test_salvages_complete_objects_from_truncated_array(self):
        from services.hermes.news_screener import _salvage_batch_objects
        text = ('[{"relevance": 8, "region_bucket": "全国", "region_province": null, '
                '"category": "industry_news", "summary": "a"}, '
                '{"relevance": 7, "region_bucket": "全国", "region_province": null, '
                '"category": "policy", "summary": "b"}, {"relevance": 9, "region_bu')
        result = _salvage_batch_objects(text, expected=3)
        assert result is not None and len(result) == 2
        assert result[0]["relevance"] == 8 and result[1]["relevance"] == 7

    def test_returns_none_when_nothing_complete(self):
        from services.hermes.news_screener import _salvage_batch_objects
        assert _salvage_batch_objects('[{"relevance": 8, "region_bu', expected=2) is None

    def test_returns_none_when_all_complete(self):
        # full-length array means it wasn't truncated — the main parser handles it
        from services.hermes.news_screener import _salvage_batch_objects
        text = '[{"relevance": 8}, {"relevance": 7}]'
        assert _salvage_batch_objects(text, expected=2) is None


# ── Macro-energy keyword coverage ─────────────────────────────────────────────
# Real titles from the 2026-09-04 digest that were all prefiltered to ★0 — the
# keyword list must cover macro energy vocabulary, not just market jargon.

_MACRO_ENERGY_TITLES = [
    "APEC中国年能源合作纵深观察:从新型电力系统建设看亚太能源合作新命题",
    "国家能源局:截至今年7月底,全国光伏发电装机达到12.86亿千瓦,首次超越煤电,光伏正式成为我国第一大电源",
    "国家能源局公开征求意见",
    "国家能源局关于印发《能源领域节能降碳行动计划(2026—2028年)》的通知",
    "供电企业信息公开迎重大修订!国家能源局新规明确配电网可开放容量按季度更新,分布式光伏用户迎来重大利好",
    "国家能源局就《供电企业信息公开实施办法》修订稿征求意见,要求按季度更新可开放容量",
    "刚刚,国家能源局宣布:“光伏”正式成为我国第一大电源",
    "国家能源局:明年全国风电光伏新增装机2亿千瓦左右",
]


def test_macro_energy_titles_pass_prefilter():
    """Each macro-energy title must hit ≥1 keyword (no silent ★0 without LLM call)."""
    for title in _MACRO_ENERGY_TITLES:
        assert ns._keyword_hits(title) >= 1, f"prefiltered to ★0: {title[:40]}"


# ── Pre-scoring dedup ─────────────────────────────────────────────────────────

def test_dedup_reuses_stored_metadata_no_llm():
    """Article whose body hash is already in the KB reuses stored AI metadata —
    no Claude call, stored relevance preserved for the digest card."""
    body = "国家能源局发布节能降碳行动计划全文，涉及煤电装机控制和新能源消纳。"
    arts = [_article("国家能源局关于印发《能源领域节能降碳行动计划》的通知", body)]
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    stored_row = (digest, 7, "已存摘要", "policy", "全国", None)

    cur = MagicMock()
    cur.fetchall.return_value = [stored_row]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur

    with patch.object(ns.psycopg2, "connect", return_value=conn) as conn_mock, \
         patch("shared.anthropic_client.make_client") as mc:
        ns._apply_ai_scoring(arts, "test-key", pg_url="postgres://x")

    conn_mock.assert_called_once_with("postgres://x")
    mc.assert_not_called()  # no LLM call for an already-ingested article
    assert arts[0]["ai_result"]["relevance"] == 7
    assert arts[0]["ai_result"]["summary"] == "已存摘要"
    assert arts[0]["ai_result"]["category"] == "policy"


def test_dedup_never_applies_to_empty_body():
    """Empty-body articles (Sogou CAPTCHA days) all share one hash — they must
    never be deduped, and no DB lookup should even happen for them."""
    arts = [_article("储能行业新闻", "")]
    client = _fake_client([json.dumps([_result(6)])])

    with patch.object(ns.psycopg2, "connect") as conn_mock, \
         patch("shared.anthropic_client.make_client", return_value=client):
        ns._apply_ai_scoring(arts, "test-key", pg_url="postgres://x")

    conn_mock.assert_not_called()
    assert arts[0]["ai_result"]["relevance"] == 6  # scored by LLM as usual


def test_dedup_db_failure_falls_back_to_scoring():
    """If the dedup lookup fails, scoring proceeds normally (no crash)."""
    body = "储能市场分析正文。"
    arts = [_article("储能行业新闻", body)]
    client = _fake_client([json.dumps([_result(6)])])

    with patch.object(ns.psycopg2, "connect", side_effect=Exception("db down")), \
         patch("shared.anthropic_client.make_client", return_value=client):
        ns._apply_ai_scoring(arts, "test-key", pg_url="postgres://x")

    assert arts[0]["ai_result"]["relevance"] == 6
