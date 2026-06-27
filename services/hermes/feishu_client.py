from __future__ import annotations
import json
import logging
import time
from threading import Lock
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_API = "https://open.feishu.cn/open-apis"
_TOKEN_TTL = 7000  # refresh 200 s before expiry


class FeishuClient:
    def __init__(self, app_id: str, app_secret: str) -> None:
        self.app_id = app_id
        self.app_secret = app_secret
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._lock = Lock()

    # ── Token ─────────────────────────────────────────────────────────────────

    def _get_token(self) -> str:
        with self._lock:
            if self._token and time.time() < self._token_expires_at:
                return self._token
            resp = requests.post(
                f"{_API}/auth/v3/tenant_access_token/internal",
                json={"app_id": self.app_id, "app_secret": self.app_secret},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("code", 0) != 0:
                raise RuntimeError(f"Feishu token error: {data}")
            self._token = data["tenant_access_token"]
            self._token_expires_at = time.time() + _TOKEN_TTL
            return self._token

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ── Messaging ─────────────────────────────────────────────────────────────

    def send_text(self, open_id: str, text: str) -> None:
        resp = requests.post(
            f"{_API}/im/v1/messages",
            headers=self._headers(),
            params={"receive_id_type": "open_id"},
            json={
                "receive_id": open_id,
                "msg_type": "text",
                "content": json.dumps({"text": text}),
            },
            timeout=10,
        )
        if not resp.ok:
            logger.error("Feishu send_text HTTP %s: %s", resp.status_code, resp.text)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code", 0) != 0:
            logger.warning("Feishu send_text non-zero code: %s", data)

    def send_card(self, open_id: str, card: dict) -> None:
        """Send a Feishu interactive card (msg_type=interactive)."""
        resp = requests.post(
            f"{_API}/im/v1/messages",
            headers=self._headers(),
            params={"receive_id_type": "open_id"},
            json={
                "receive_id": open_id,
                "msg_type": "interactive",
                "content": json.dumps(card),
            },
            timeout=10,
        )
        if not resp.ok:
            logger.error("Feishu send_card HTTP %s: %s", resp.status_code, resp.text[:300])
            return
        data = resp.json()
        if data.get("code", 0) != 0:
            logger.warning("Feishu send_card non-zero code: %s", data)

    def upload_file(self, file_bytes: bytes, filename: str, file_type: str = "pdf") -> str:
        """Upload a file to Feishu and return file_key."""
        resp = requests.post(
            f"{_API}/im/v1/files",
            headers=self._headers(),
            data={
                "file_type": file_type,
                "file_name": filename,
            },
            files={
                "file": (filename, file_bytes, "application/octet-stream"),
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code", 0) != 0:
            raise RuntimeError(f"Feishu upload_file error: {data}")
        return data["data"]["file_key"]

    def send_file(self, open_id: str, file_key: str) -> None:
        """Send a file message via file_key obtained from upload_file()."""
        resp = requests.post(
            f"{_API}/im/v1/messages",
            headers=self._headers(),
            params={"receive_id_type": "open_id"},
            json={
                "receive_id": open_id,
                "msg_type":   "file",
                "content":    json.dumps({"file_key": file_key}),
            },
            timeout=15,
        )
        if not resp.ok:
            logger.error("Feishu send_file HTTP %s: %s", resp.status_code, resp.text[:300])
        resp.raise_for_status()
        data = resp.json()
        if data.get("code", 0) != 0:
            logger.warning("Feishu send_file non-zero code: %s", data)

    def upload_image(self, image_bytes: bytes) -> str:
        """Upload a PNG/JPEG image to Feishu and return image_key."""
        resp = requests.post(
            f"{_API}/im/v1/images",
            headers=self._headers(),
            data={"image_type": "message"},
            files={"image": ("chart.png", image_bytes, "image/png")},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code", 0) != 0:
            raise RuntimeError(f"Feishu upload_image error: {data}")
        return data["data"]["image_key"]

    def send_image(self, open_id: str, image_key: str) -> None:
        """Send an image message using an image_key from upload_image()."""
        resp = requests.post(
            f"{_API}/im/v1/messages",
            headers=self._headers(),
            params={"receive_id_type": "open_id"},
            json={
                "receive_id": open_id,
                "msg_type":   "image",
                "content":    json.dumps({"image_key": image_key}),
            },
            timeout=15,
        )
        if not resp.ok:
            logger.error("Feishu send_image HTTP %s: %s", resp.status_code, resp.text[:300])
        resp.raise_for_status()
        data = resp.json()
        if data.get("code", 0) != 0:
            logger.warning("Feishu send_image non-zero code: %s", data)

    def download_resource(self, message_id: str, file_key: str, resource_type: str = "file") -> bytes:
        """Download a file or image attachment from a Feishu message."""
        resp = requests.get(
            f"{_API}/im/v1/messages/{message_id}/resources/{file_key}",
            headers=self._headers(),
            params={"type": resource_type},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.content

    # ── Reactions ─────────────────────────────────────────────────────────────

    def add_reaction(self, message_id: str, emoji_type: str) -> Optional[str]:
        """Add an emoji reaction to a message. Returns reaction_id or None on failure."""
        try:
            resp = requests.post(
                f"{_API}/im/v1/messages/{message_id}/reactions",
                headers=self._headers(),
                json={"reaction_type": {"emoji_type": emoji_type}},
                timeout=10,
            )
            if not resp.ok:
                logger.debug("Feishu add_reaction HTTP %s", resp.status_code)
                return None
            data = resp.json()
            if data.get("code", 0) != 0:
                logger.debug("Feishu add_reaction non-zero code: %s", data)
                return None
            return data.get("data", {}).get("reaction_id")
        except Exception as exc:
            logger.debug("Feishu add_reaction failed: %s", exc)
            return None

    def delete_reaction(self, message_id: str, reaction_id: str) -> bool:
        """Delete a reaction by its reaction_id. Returns True on success."""
        try:
            resp = requests.delete(
                f"{_API}/im/v1/messages/{message_id}/reactions/{reaction_id}",
                headers=self._headers(),
                timeout=10,
            )
            if not resp.ok:
                logger.debug("Feishu delete_reaction HTTP %s", resp.status_code)
                return False
            return resp.json().get("code", 0) == 0
        except Exception as exc:
            logger.debug("Feishu delete_reaction failed: %s", exc)
            return False
