from __future__ import annotations
import logging
import time
from threading import Lock
from typing import Callable, Optional

import requests

logger = logging.getLogger(__name__)

_GRAPH_BASE = "https://graph.microsoft.com/v1.0"
_TOKEN_ENDPOINT = "https://login.microsoftonline.com/consumers/oauth2/v2.0/token"
_SCOPES = "Mail.Read Mail.ReadWrite offline_access"
_TOKEN_TTL_MARGIN = 300


class OutlookClient:
    """Microsoft Graph mail client for personal Outlook/Hotmail accounts.

    Uses the same OAuth app (client_id / client_secret) as OneDriveClient
    but with a separate refresh token that carries Mail.Read scope.
    Obtain the token by running: py scripts/auth_microsoft_mail.py
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        on_token_rotated: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self._refresh_token = refresh_token
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = Lock()
        self._on_token_rotated = on_token_rotated

    # ── Token management ─────────────────────────────────────────────────────

    def _refresh(self) -> None:
        # Token was obtained via device code flow (public client — no client_secret).
        # Sending client_secret to /consumers/ for a public-client token returns HTTP 400.
        resp = requests.post(
            _TOKEN_ENDPOINT,
            data={
                "client_id": self.client_id,
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "scope": _SCOPES,
            },
            timeout=15,
        )
        if not resp.ok:
            logger.error("Outlook token refresh failed %s: %s", resp.status_code, resp.text)
            resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._expires_at = time.time() + data.get("expires_in", 3600) - _TOKEN_TTL_MARGIN
        if "refresh_token" in data:
            new_rt = data["refresh_token"]
            if new_rt != self._refresh_token:
                self._refresh_token = new_rt
                logger.info("Outlook refresh token rotated — persisting")
                if self._on_token_rotated:
                    try:
                        self._on_token_rotated(new_rt)
                    except Exception as exc:
                        logger.error("Failed to persist rotated Outlook token: %s", exc)

    def _token(self) -> str:
        with self._lock:
            if not self._access_token or time.time() >= self._expires_at:
                self._refresh()
            return self._access_token  # type: ignore[return-value]

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token()}"}

    # ── Mail operations ───────────────────────────────────────────────────────

    def list_messages(
        self,
        limit: int = 20,
        unread_only: bool = True,
        folder: str = "inbox",
    ) -> list[dict]:
        """Return recent messages from a mail folder, newest first."""
        params: dict = {
            "$top": limit,
            "$orderby": "receivedDateTime desc",
            "$select": (
                "id,subject,from,receivedDateTime,isRead,"
                "hasAttachments,bodyPreview,importance"
            ),
        }
        if unread_only:
            params["$filter"] = "isRead eq false"
        resp = requests.get(
            f"{_GRAPH_BASE}/me/mailFolders/{folder}/messages",
            headers=self._headers(),
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("value", [])

    def list_attachments(self, message_id: str) -> list[dict]:
        """Return attachment metadata (name, contentType, size) — no content."""
        resp = requests.get(
            f"{_GRAPH_BASE}/me/messages/{message_id}/attachments",
            headers=self._headers(),
            params={"$select": "id,name,contentType,size,isInline"},
            timeout=15,
        )
        resp.raise_for_status()
        return [
            a for a in resp.json().get("value", [])
            if not a.get("isInline", False)
        ]

    def download_attachment(self, message_id: str, attachment_id: str) -> bytes:
        """Download raw attachment bytes."""
        resp = requests.get(
            f"{_GRAPH_BASE}/me/messages/{message_id}/attachments/{attachment_id}/$value",
            headers=self._headers(),
            timeout=60,
        )
        resp.raise_for_status()
        return resp.content

    def mark_as_read(self, message_id: str) -> None:
        requests.patch(
            f"{_GRAPH_BASE}/me/messages/{message_id}",
            headers={**self._headers(), "Content-Type": "application/json"},
            json={"isRead": True},
            timeout=10,
        ).raise_for_status()
