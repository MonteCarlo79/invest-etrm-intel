import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch


@pytest.fixture
def app():
    with patch("services.hermes.app._make_clients") as mock_make:
        mock_make.return_value = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
        from services.hermes.app import create_app
        return create_app()


def test_health_check(app):
    client = TestClient(app)
    resp = client.get("/hermes/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_wecom_verification(app):
    client = TestClient(app)
    resp = client.get("/hermes/inbound/wecom?echostr=hello123")
    assert resp.status_code == 200
    assert resp.text == "hello123"


def test_inbound_wechat_returns_200(app):
    client = TestClient(app)
    payload = {
        "source": "wechat",
        "sender_id": "wxid_abc",
        "sender_name": "Alice",
        "text": "remind me to submit report",
        "timestamp": "2026-06-11T10:00:00Z",
    }
    resp = client.post("/hermes/inbound/wechat", json=payload)
    assert resp.status_code == 200


def test_inbound_wechat_rejects_bad_payload(app):
    client = TestClient(app)
    resp = client.post("/hermes/inbound/wechat", json={"bad": "data"})
    assert resp.status_code == 422
