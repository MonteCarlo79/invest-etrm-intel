from __future__ import annotations
import logging
import time
from threading import Lock
from typing import Optional
from urllib.parse import urlparse, parse_qs

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

    def send_file(self, user_id: str, file_bytes: bytes, filename: str) -> None:
        """Upload a file via WeCom corp media API and send to a user as a file message."""
        token = self._get_token()
        # Step 1: upload media
        upload_resp = requests.post(
            "https://qyapi.weixin.qq.com/cgi-bin/media/upload",
            params={"access_token": token, "type": "file"},
            files={"media": (filename, file_bytes, "application/octet-stream")},
            timeout=60,
        )
        upload_resp.raise_for_status()
        upload_data = upload_resp.json()
        if upload_data.get("errcode", 0) != 0:
            raise RuntimeError(f"WeCom media upload failed: {upload_data}")
        media_id = upload_data["media_id"]
        # Step 2: send file message
        payload = {
            "touser": user_id,
            "msgtype": "file",
            "agentid": self.agent_id,
            "file": {"media_id": media_id},
        }
        send_resp = requests.post(
            "https://qyapi.weixin.qq.com/cgi-bin/message/send",
            params={"access_token": token},
            json=payload,
            timeout=10,
        )
        send_resp.raise_for_status()
        send_data = send_resp.json()
        if send_data.get("errcode", 0) not in (0, 48002):
            logger.warning("WeCom send_file non-zero errcode: %s", send_data)
        logger.info("WeCom send_file: sent %s to %s", filename, user_id)


def _extract_webhook_key(webhook_url: str) -> str:
    """Extract the `key` query param from a WeCom webhook URL."""
    qs = parse_qs(urlparse(webhook_url).query)
    keys = qs.get("key", [])
    if not keys:
        raise ValueError(f"WeCom webhook URL has no 'key' param: {webhook_url}")
    return keys[0]


def send_pdf_via_wecom_webhook(webhook_url: str, pdf_bytes: bytes, filename: str) -> None:
    """Upload a PDF and send it to a WeCom group bot webhook.

    WeCom webhook file flow:
      1. POST .../webhook/upload_media?key=KEY&type=file  → media_id
      2. POST .../webhook/send?key=KEY  with msgtype=file + media_id
    """
    key = _extract_webhook_key(webhook_url)
    upload_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={key}&type=file"
    send_url   = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={key}"

    # Step 1: upload
    upload_resp = requests.post(
        upload_url,
        files={"media": (filename, pdf_bytes, "application/pdf")},
        timeout=30,
    )
    upload_resp.raise_for_status()
    upload_data = upload_resp.json()
    if upload_data.get("errcode", 0) != 0:
        raise RuntimeError(f"WeCom webhook upload failed: {upload_data}")
    media_id = upload_data["media_id"]

    # Step 2: send
    send_resp = requests.post(
        send_url,
        json={"msgtype": "file", "file": {"media_id": media_id}},
        timeout=10,
    )
    send_resp.raise_for_status()
    send_data = send_resp.json()
    if send_data.get("errcode", 0) != 0:
        raise RuntimeError(f"WeCom webhook send failed: {send_data}")
    logger.info("WeCom webhook: sent %s (%d bytes)", filename, len(pdf_bytes))
