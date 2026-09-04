"""Tests for register_url WeChat anti-bot handling (knowledge_docs).

Root cause covered: since the NAT-EIP cutover, mp.weixin.qq.com serves an
"环境异常/完成验证后即可继续访问" challenge page to the bot-UA fetcher, and the
challenge text was being ingested as the document. register_url must (a) use
browser headers for WeChat URLs, (b) extract the js_content div, and
(c) refuse to ingest challenge pages with a clear user-facing error.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

from services.knowledge_pool import knowledge_docs as kd


_WECHAT_URL = "https://mp.weixin.qq.com/s/abc123"

_CHALLENGE_HTML = (
    "<html><head><title>环境异常</title></head><body>"
    "<div class='weui-msg__title'>环境异常</div>"
    "<p>当前环境异常，完成验证后即可继续访问。</p>"
    "</body></html>"
)

_ARTICLE_HTML = (
    "<html><head><title>fallback</title></head><body>"
    "<h1 id='activity-name'> 广东电力市场半年报告 </h1>"
    "<div id='js_content'>"
    "<p>2026年上半年广东电力现货市场均价走势与储能装机数据。</p>"
    "<p>第二段：新能源消纳与电网建设情况。</p>"
    "</div></body></html>"
)

_NO_CONTENT_HTML = (
    "<html><head><title>weixin</title></head><body>"
    "<div>something without the article div</div>"
    "</body></html>"
)


def _resp(html: str) -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    r.text = html
    r.content = html.encode("utf-8")
    r.url = _WECHAT_URL
    r.raise_for_status = MagicMock()
    return r


def _patch_db(fetchone_side_effect):
    """get_conn mock: `with get_conn() as conn` → conn; `with conn.cursor() as cur` → cur."""
    cur = MagicMock()
    cur.fetchone.side_effect = fetchone_side_effect
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cm = MagicMock()
    cm.__enter__.return_value = conn
    return cm, cur


def test_wechat_challenge_page_raises_not_ingested():
    with patch("requests.get", return_value=_resp(_CHALLENGE_HTML)), \
         patch.object(kd, "init_knowledge_tables") as init_mock, \
         patch.object(kd, "get_conn") as conn_mock:
        with pytest.raises(ValueError, match="验证|拦截|正文"):
            kd.register_url(_WECHAT_URL, api_key="k")
    init_mock.assert_not_called()   # must bail before touching the DB
    conn_mock.assert_not_called()


def test_wechat_fetch_uses_browser_headers_not_bot_ua():
    cm, _cur = _patch_db([None, (42,)])
    with patch("requests.get", return_value=_resp(_ARTICLE_HTML)) as get_mock, \
         patch.object(kd, "init_knowledge_tables"), \
         patch.object(kd, "get_conn", return_value=cm), \
         patch.object(kd, "auto_categorize", return_value="market_analytics"):
        kd.register_url(_WECHAT_URL, api_key="k")
    ua = get_mock.call_args.kwargs["headers"]["User-Agent"]
    assert "SpotMarketBot" not in ua
    assert "iPhone" in ua or "Safari" in ua


def test_wechat_article_ingests_js_content_text():
    cm, cur = _patch_db([None, (42,)])
    with patch("requests.get", return_value=_resp(_ARTICLE_HTML)), \
         patch.object(kd, "init_knowledge_tables"), \
         patch.object(kd, "get_conn", return_value=cm), \
         patch.object(kd, "auto_categorize", return_value="market_analytics"):
        doc_id, is_new, category = kd.register_url(_WECHAT_URL, api_key="k")
    assert (doc_id, is_new, category) == (42, True, "market_analytics")
    # chunks written from js_content, not from nav/chrome around it
    chunk_calls = cur.executemany.call_args_list
    assert chunk_calls, "expected chunk INSERTs"
    inserted = json.dumps(chunk_calls[0].args[1], ensure_ascii=False, default=str)
    assert "广东电力现货市场均价" in inserted
    assert "fallback" not in inserted


def test_wechat_page_without_js_content_raises():
    with patch("requests.get", return_value=_resp(_NO_CONTENT_HTML)), \
         patch.object(kd, "init_knowledge_tables"), \
         patch.object(kd, "get_conn"):
        with pytest.raises(ValueError, match="正文|拦截|验证"):
            kd.register_url(_WECHAT_URL, api_key="k")


def test_non_wechat_url_unchanged_bot_ua_and_generic_extract():
    """Regression guard: non-WeChat URLs keep the generic path."""
    url = "https://example.com/policy/123"
    html = ("<html><head><title>t</title></head><body>"
            "<h1>某省电力交易规则</h1><p>正文内容。</p></body></html>")
    resp = _resp(html)
    resp.url = url
    cm, _cur = _patch_db([None, (43,)])
    with patch("requests.get", return_value=resp) as get_mock, \
         patch.object(kd, "init_knowledge_tables"), \
         patch.object(kd, "get_conn", return_value=cm), \
         patch.object(kd, "auto_categorize", return_value="policy"):
        doc_id, is_new, _cat = kd.register_url(url, api_key="k")
    assert (doc_id, is_new) == (43, True)
    ua = get_mock.call_args.kwargs["headers"]["User-Agent"]
    assert "SpotMarketBot" in ua


def test_non_wechat_challenge_page_also_detected():
    url = "https://example.com/blocked"
    resp = _resp(_CHALLENGE_HTML)
    resp.url = url
    with patch("requests.get", return_value=resp), \
         patch.object(kd, "init_knowledge_tables"), \
         patch.object(kd, "get_conn"):
        with pytest.raises(ValueError, match="验证|拦截|正文"):
            kd.register_url(url, api_key="k")
