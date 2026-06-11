from __future__ import annotations
import time
import httpx

WECOM_BASE = "https://qyapi.weixin.qq.com"


class WeComClient:
    def __init__(self, corp_id: str, agent_id: int, secret: str) -> None:
        self._corp_id = corp_id
        self._agent_id = agent_id
        self._secret = secret
        self._token: str | None = None
        self._token_expires_at: float = 0.0

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expires_at:
            return self._token
        resp = httpx.get(
            f"{WECOM_BASE}/cgi-bin/gettoken",
            params={"corpid": self._corp_id, "corpsecret": self._secret},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode", 0) != 0:
            raise RuntimeError(f"WeCom token error: {data}")
        self._token = data["access_token"]
        self._token_expires_at = time.time() + data["expires_in"] - 60  # 60s safety margin
        return self._token

    def send_text(self, user_id: str, text: str) -> None:
        token = self._get_token()
        resp = httpx.post(
            f"{WECOM_BASE}/cgi-bin/message/send",
            params={"access_token": token},
            json={
                "touser": user_id,
                "msgtype": "text",
                "agentid": self._agent_id,
                "text": {"content": text},
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode", 0) != 0:
            raise RuntimeError(f"WeCom send error: {data}")
