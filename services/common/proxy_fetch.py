# -*- coding: utf-8 -*-
"""
Proxy-routed HTTP GET for anti-bot-sensitive hosts (WeChat ecosystem).

Since the 2026-08-30 NAT-EIP cutover, all platform egress goes through a single
AWS Singapore datacenter IP. mp.weixin.qq.com and weixin.sogou.com serve
anti-bot challenge pages ("环境异常/完成验证后即可继续访问") to that IP
profile, so WeChat article fetches fail loudly (pasted URLs) or silently skip
articles (routine news scan).

Set WECHAT_PROXY_URL to route only those hosts through a China-egress proxy:

    WECHAT_PROXY_URL=http://user:pass@proxy-host:port

socks5:// URLs require pysocks (requests[socks]); without it the fetch falls
back to direct with a logged warning. Unset/empty env → direct fetch
(previous behaviour). A dead proxy never breaks the pipeline: the fetch falls
back to direct and logs a warning, degrading to the old challenge-page error.
"""
from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)

PROXY_ENV_VAR = "WECHAT_PROXY_URL"

# Hosts that serve anti-bot challenges to the NAT datacenter IP.
_PROXY_HOSTS = ("mp.weixin.qq.com", "weixin.sogou.com")


def proxy_configured() -> str | None:
    """Return the configured proxy URL, or None when unset/empty."""
    return os.getenv(PROXY_ENV_VAR) or None


def needs_proxy(url: str) -> bool:
    return any(h in url for h in _PROXY_HOSTS)


def fetch(url: str, *, headers: dict | None = None, timeout: int = 30, **kwargs) -> requests.Response:
    """requests.get, routed via WECHAT_PROXY_URL for WeChat-ecosystem hosts.

    Direct fetch when: no proxy configured, URL is not a proxied host, or the
    proxy itself fails (logged warning — InvalidSchema covers socks5:// without
    pysocks installed).
    """
    proxy = proxy_configured() if needs_proxy(url) else None
    if proxy:
        try:
            return requests.get(
                url, headers=headers, timeout=timeout,
                proxies={"http": proxy, "https": proxy}, **kwargs,
            )
        except (requests.exceptions.ProxyError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.InvalidSchema) as exc:
            logger.warning(
                "proxy_fetch: proxy fetch failed for %s (%s); retrying direct", url, exc,
            )
    return requests.get(url, headers=headers, timeout=timeout, **kwargs)
