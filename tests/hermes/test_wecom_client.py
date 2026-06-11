import time
import respx
import httpx
from services.hermes.wecom_client import WeComClient

CORP_ID = "ww123"
AGENT_ID = 1000001
SECRET = "secret123"
WECOM_BASE = "https://qyapi.weixin.qq.com"


@respx.mock
def test_get_access_token():
    respx.get(f"{WECOM_BASE}/cgi-bin/gettoken").mock(
        return_value=httpx.Response(
            200, json={"errcode": 0, "access_token": "token-xyz", "expires_in": 7200}
        )
    )
    client = WeComClient(corp_id=CORP_ID, agent_id=AGENT_ID, secret=SECRET)
    token = client._get_token()
    assert token == "token-xyz"


@respx.mock
def test_send_message():
    respx.get(f"{WECOM_BASE}/cgi-bin/gettoken").mock(
        return_value=httpx.Response(
            200, json={"errcode": 0, "access_token": "token-xyz", "expires_in": 7200}
        )
    )
    send_route = respx.post(f"{WECOM_BASE}/cgi-bin/message/send").mock(
        return_value=httpx.Response(200, json={"errcode": 0, "errmsg": "ok"})
    )
    client = WeComClient(corp_id=CORP_ID, agent_id=AGENT_ID, secret=SECRET)
    client.send_text(user_id="user1", text="Hello from Hermes")
    assert send_route.called
    payload = send_route.calls[0].request
    import json
    body = json.loads(payload.content)
    assert body["text"]["content"] == "Hello from Hermes"
    assert body["touser"] == "user1"


@respx.mock
def test_token_cached_until_expiry():
    call_count = 0

    def token_handler(request):
        nonlocal call_count
        call_count += 1
        return httpx.Response(
            200, json={"errcode": 0, "access_token": "token-xyz", "expires_in": 7200}
        )

    respx.get(f"{WECOM_BASE}/cgi-bin/gettoken").mock(side_effect=token_handler)
    respx.post(f"{WECOM_BASE}/cgi-bin/message/send").mock(
        return_value=httpx.Response(200, json={"errcode": 0, "errmsg": "ok"})
    )
    client = WeComClient(corp_id=CORP_ID, agent_id=AGENT_ID, secret=SECRET)
    client.send_text("u1", "msg1")
    client.send_text("u1", "msg2")
    assert call_count == 1  # token fetched once, reused
