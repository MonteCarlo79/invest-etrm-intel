from __future__ import annotations
import logging
import requests

logger = logging.getLogger(__name__)


class WechatyBridgeClient:
    """HTTP client for the Wechaty bridge running on EC2."""

    def __init__(self, bridge_url: str) -> None:
        self.bridge_url = bridge_url.rstrip("/")

    def send(self, to: str, text: str) -> None:
        if not self.bridge_url:
            logger.debug("WechatyBridgeClient: bridge_url not set, skipping send")
            return
        try:
            resp = requests.post(
                f"{self.bridge_url}/send",
                json={"to": to, "text": text},
                timeout=5,
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("WechatyBridgeClient.send failed: %s", exc)
