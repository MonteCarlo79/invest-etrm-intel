from __future__ import annotations
import logging
import time
from threading import Lock
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_TOKEN_TTL = 7000  # seconds (WeCom tokens last 7200s; refresh 200s early)


class WeComClient:
    def __init__(self, corp_id: str, agent_id: int, secret: str) -> None:
        self.corp_id = corp_id
        self.agent_id = agent_id
        self.secret = secret
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._lock = Lock()

    def _get_token(self) -> str:
        with self._lock:
            if self._token and time.time() < self._token_expires_at:
                return self._token
            resp = requests.get(
                "https://qyapi.weixin.qq.com/cgi-bin/gettoken",
                params={"corpid": self.corp_id, "corpsecret": self.secret},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("errcode", 0) != 0:
                raise RuntimeError(f"WeCom token error: {data}")
            self._token = data["access_token"]
            self._token_expires_at = time.time() + _TOKEN_TTL
            return self._token

    def send_text(self, user_id: str, text: str) -> None:
        token = self._get_token()
        payload = {
            "touser": user_id,
            "msgtype": "text",
            "agentid": self.agent_id,
            "text": {"content": text},
            "safe": 0,
        }
        resp = requests.post(
            "https://qyapi.weixin.qq.com/cgi-bin/message/send",
            params={"access_token": token},
            json=payload,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode", 0) not in (0, 48002):  # 48002 = app not authorised but msg sent
            logger.warning("WeCom send_text non-zero errcode: %s", data)
