import respx
import httpx
from services.hermes.wechat_client import WechatyBridgeClient

BRIDGE_URL = "http://172.31.30.155:3000"


@respx.mock
def test_send_message():
    send_route = respx.post(f"{BRIDGE_URL}/send").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    client = WechatyBridgeClient(bridge_url=BRIDGE_URL)
    client.send(to="wxid_abc123", text="Hello from Hermes")
    assert send_route.called
    import json
    body = json.loads(send_route.calls[0].request.content)
    assert body["to"] == "wxid_abc123"
    assert body["text"] == "Hello from Hermes"


@respx.mock
def test_send_raises_on_error():
    respx.post(f"{BRIDGE_URL}/send").mock(
        return_value=httpx.Response(500, json={"error": "WeChat disconnected"})
    )
    client = WechatyBridgeClient(bridge_url=BRIDGE_URL)
    import pytest
    with pytest.raises(httpx.HTTPStatusError):
        client.send(to="wxid_abc123", text="Hello")
