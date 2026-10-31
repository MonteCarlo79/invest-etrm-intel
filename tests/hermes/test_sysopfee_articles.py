"""Tests for services/hermes/sysopfee_articles.py"""
import sys
import types

import pytest

from services.hermes import sysopfee_articles as sa


_SAMPLE_HTML = """
<html><head>
<meta property="og:title" content="2026年7月各省市电网代理购电价格汇总" />
</head><body>
<div id="js_content">
<img data-src="https://mmbiz.qpic.cn/mmbiz_png/abc111/640?wx_fmt=jpeg" />
<img data-src="https://mmbiz.qpic.cn/mmbiz_png/def222/640?wx_fmt=png" />
<img src="https://mmbiz.qpic.cn/not-data-src-ignored/640" />
</div></body></html>
"""


def test_parse_article_month_and_images():
    month, urls = sa.parse_article(_SAMPLE_HTML)
    assert month == "2026-07-01"
    assert urls == [
        "https://mmbiz.qpic.cn/mmbiz_png/abc111/640?wx_fmt=jpeg",
        "https://mmbiz.qpic.cn/mmbiz_png/def222/640?wx_fmt=png",
    ]


def test_parse_article_no_month():
    month, urls = sa.parse_article("<html><body>no title here</body></html>")
    assert month is None
    assert urls == []


def test_fetch_article_challenge_detected(monkeypatch):
    class _Resp:
        text = "<html>环境异常，完成验证后即可继续访问</html>"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(sa.requests, "get", lambda *a, **k: _Resp())
    with pytest.raises(sa.ChallengePageError):
        sa.fetch_article("https://mp.weixin.qq.com/s/xxx")
