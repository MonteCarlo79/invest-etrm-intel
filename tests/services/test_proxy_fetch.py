"""Tests for services.common.proxy_fetch — WeChat anti-bot proxy routing.

Covers: env-var gating (WECHAT_PROXY_URL), host matching, and the direct-fetch
fallback when the proxy is unreachable or misconfigured (socks without pysocks).
"""
from unittest.mock import MagicMock, patch

import pytest
import requests

from services.common import proxy_fetch as pf


_WECHAT_URL = "https://mp.weixin.qq.com/s/abc123"
_SOGOU_URL = "https://weixin.sogou.com/weixin?type=2&query=test"
_OTHER_URL = "https://nea.gov.cn/policy/123"
_PROXY = "http://user:pass@cn-proxy.example:8080"


def _resp() -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    return r


def test_no_env_direct_fetch(monkeypatch):
    monkeypatch.delenv(pf.PROXY_ENV_VAR, raising=False)
    with patch("requests.get", return_value=_resp()) as get_mock:
        pf.fetch(_WECHAT_URL, headers={"User-Agent": "x"}, timeout=10)
    assert "proxies" not in get_mock.call_args.kwargs


def test_empty_env_treated_as_unset(monkeypatch):
    monkeypatch.setenv(pf.PROXY_ENV_VAR, "")
    with patch("requests.get", return_value=_resp()) as get_mock:
        pf.fetch(_WECHAT_URL, timeout=10)
    assert "proxies" not in get_mock.call_args.kwargs


@pytest.mark.parametrize("url", [_WECHAT_URL, _SOGOU_URL])
def test_env_set_proxied_hosts_use_proxy(monkeypatch, url):
    monkeypatch.setenv(pf.PROXY_ENV_VAR, _PROXY)
    with patch("requests.get", return_value=_resp()) as get_mock:
        pf.fetch(url, headers={"User-Agent": "x"}, timeout=10)
    assert get_mock.call_args.kwargs["proxies"] == {"http": _PROXY, "https": _PROXY}
    assert get_mock.call_args.kwargs["headers"] == {"User-Agent": "x"}


def test_env_set_non_wechat_url_stays_direct(monkeypatch):
    monkeypatch.setenv(pf.PROXY_ENV_VAR, _PROXY)
    with patch("requests.get", return_value=_resp()) as get_mock:
        pf.fetch(_OTHER_URL, timeout=10)
    assert "proxies" not in get_mock.call_args.kwargs


@pytest.mark.parametrize("exc", [
    requests.exceptions.ProxyError("dead proxy"),
    requests.exceptions.ConnectionError("conn refused"),
    requests.exceptions.ConnectTimeout("timeout"),
    requests.exceptions.InvalidSchema("Missing dependencies for SOCKS support"),
])
def test_proxy_failure_falls_back_to_direct(monkeypatch, exc):
    monkeypatch.setenv(pf.PROXY_ENV_VAR, _PROXY)
    with patch("requests.get", side_effect=[exc, _resp()]) as get_mock:
        resp = pf.fetch(_WECHAT_URL, timeout=10)
    assert resp.status_code == 200
    assert get_mock.call_count == 2
    assert "proxies" not in get_mock.call_args_list[1].kwargs


def test_kwargs_passthrough(monkeypatch):
    monkeypatch.setenv(pf.PROXY_ENV_VAR, _PROXY)
    with patch("requests.get", return_value=_resp()) as get_mock:
        pf.fetch(_WECHAT_URL, timeout=4, allow_redirects=True)
    assert get_mock.call_args.kwargs["allow_redirects"] is True
    assert get_mock.call_args.kwargs["timeout"] == 4
