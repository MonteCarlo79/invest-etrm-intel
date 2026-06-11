from __future__ import annotations
import httpx


class WechatyBridgeClient:
    def __init__(self, bridge_url: str) -> None:
        self._bridge_url = bridge_url.rstrip("/")

    def send(self, to: str, text: str) -> None:
        resp = httpx.post(
            f"{self._bridge_url}/send",
            json={"to": to, "text": text},
            timeout=10,
        )
        resp.raise_for_status()
