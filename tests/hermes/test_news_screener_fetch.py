"""Tests for news_screener._fetch_wechat_article anti-bot guards.

Covers the 2026-09-08 KB-poisoning fix: a WeChat 环境异常 challenge page must
raise SogouCaptchaError (caller skips the article) instead of being ingested
as the article body.
"""
from unittest.mock import MagicMock, patch

import pytest

from services.hermes import news_screener as ns


_URL = "https://mp.weixin.qq.com/s/abc123"

_CHALLENGE_HTML = (
    "<html><head><title>环境异常</title></head><body>"
    "<div class='weui-msg__title'>环境异常</div>"
    "<p>当前环境异常，完成验证后即可继续访问。</p>"
    "</body></html>"
)

_ARTICLE_HTML = (
    "<html><head><title>t</title></head><body>"
    "<h1 id='activity-name'> 广东电力市场半年报告 </h1>"
    "<div id='js_content'><p>现货均价与储能装机数据。</p></div>"
    "</body></html>"
)


def _resp(html: str) -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    r.text = html
    r.content = html.encode("utf-8")
    r.url = _URL
    r.raise_for_status = MagicMock()
    return r


def test_wechat_challenge_page_raises_not_ingested():
    with patch.object(ns, "proxy_fetch", return_value=_resp(_CHALLENGE_HTML)):
        with pytest.raises(ns.SogouCaptchaError, match="环境异常|challenge"):
            ns._fetch_wechat_article(_URL)


def test_wechat_real_article_returns_body_and_title():
    with patch.object(ns, "proxy_fetch", return_value=_resp(_ARTICLE_HTML)):
        body, title, final_url = ns._fetch_wechat_article(_URL)
    assert "现货均价与储能装机数据" in body
    assert title == "广东电力市场半年报告"
    assert final_url == _URL
