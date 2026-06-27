"""Thin Telegram Bot API client (no extra dependencies — uses requests)."""
from __future__ import annotations

import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"
_MAX_TEXT = 4096  # Telegram hard limit per message


class TelegramClient:
    def __init__(self, token: str):
        self._token = token
        self._base = f"https://api.telegram.org/bot{token}"

    # ── Outbound ──────────────────────────────────────────────────────────────

    def send_text(self, chat_id: int | str, text: str) -> bool:
        """Send text, splitting into chunks if > 4096 chars."""
        chunks = [text[i : i + _MAX_TEXT] for i in range(0, len(text), _MAX_TEXT)]
        ok = True
        for chunk in chunks:
            try:
                resp = requests.post(
                    f"{self._base}/sendMessage",
                    json={"chat_id": chat_id, "text": chunk},
                    timeout=15,
                )
                if not resp.ok:
                    logger.error("Telegram sendMessage failed: %s", resp.text)
                    ok = False
            except Exception as exc:
                logger.error("Telegram sendMessage error: %s", exc)
                ok = False
        return ok

    def send_typing(self, chat_id: int | str) -> None:
        """Show typing indicator."""
        try:
            requests.post(
                f"{self._base}/sendChatAction",
                json={"chat_id": chat_id, "action": "typing"},
                timeout=5,
            )
        except Exception:
            pass

    def send_menu(
        self,
        chat_id: int | str,
        text: str,
        buttons: list[list[dict]],
        parse_mode: str = "HTML",
    ) -> bool:
        """Send a message with an inline keyboard.

        buttons format: [[{"text": "Label", "callback_data": "..."}], ...]
        Each inner list is a row; each dict is a button in that row.
        """
        try:
            payload: dict = {
                "chat_id": chat_id,
                "text": text,
                "reply_markup": {"inline_keyboard": buttons},
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode
            resp = requests.post(
                f"{self._base}/sendMessage", json=payload, timeout=15
            )
            if not resp.ok:
                logger.error("Telegram sendMenu failed: %s", resp.text)
                return False
            return True
        except Exception as exc:
            logger.error("Telegram sendMenu error: %s", exc)
            return False

    def answer_callback_query(self, callback_query_id: str, text: str = "") -> None:
        """Acknowledge a callback query to clear the button's loading state."""
        try:
            requests.post(
                f"{self._base}/answerCallbackQuery",
                json={"callback_query_id": callback_query_id, "text": text},
                timeout=5,
            )
        except Exception:
            pass

    # ── Inbound file download ─────────────────────────────────────────────────

    def get_file_bytes(self, file_id: str) -> Optional[bytes]:
        """Download a file by file_id. Returns bytes or None on error."""
        try:
            resp = requests.get(
                f"{self._base}/getFile",
                params={"file_id": file_id},
                timeout=10,
            )
            resp.raise_for_status()
            file_path = resp.json()["result"]["file_path"]
            dl = requests.get(
                f"https://api.telegram.org/file/bot{self._token}/{file_path}",
                timeout=60,
            )
            dl.raise_for_status()
            return dl.content
        except Exception as exc:
            logger.error("Telegram getFile error for %s: %s", file_id, exc)
            return None

    # ── Webhook registration ──────────────────────────────────────────────────

    def set_webhook(self, url: str, secret_token: str = "") -> bool:
        payload: dict = {"url": url}
        if secret_token:
            payload["secret_token"] = secret_token
        try:
            resp = requests.post(
                f"{self._base}/setWebhook", json=payload, timeout=10
            )
            ok = resp.ok and resp.json().get("ok")
            if ok:
                logger.info("Telegram webhook registered: %s", url)
            else:
                logger.error("Telegram setWebhook failed: %s", resp.text)
            return bool(ok)
        except Exception as exc:
            logger.error("Telegram setWebhook error: %s", exc)
            return False

    def delete_webhook(self) -> None:
        try:
            requests.post(f"{self._base}/deleteWebhook", timeout=10)
        except Exception:
            pass
