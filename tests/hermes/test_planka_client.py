import pytest
import respx
import httpx
from services.hermes.planka_client import PlankaClient


PLANKA_URL = "https://todo.pjh-etrm.ai"


@pytest.fixture
def client():
    return PlankaClient(base_url=PLANKA_URL, email="admin@test.com", password="secret")


@respx.mock
def test_login_returns_token(client):
    respx.post(f"{PLANKA_URL}/api/access-tokens").mock(
        return_value=httpx.Response(200, json={"item": {"token": "tok-abc123"}})
    )
    token = client._login()
    assert token == "tok-abc123"


@respx.mock
def test_get_projects(client):
    respx.post(f"{PLANKA_URL}/api/access-tokens").mock(
        return_value=httpx.Response(200, json={"item": {"token": "tok-abc123"}})
    )
    respx.get(f"{PLANKA_URL}/api/projects").mock(
        return_value=httpx.Response(200, json={"items": [{"id": "proj-1", "name": "Personal"}]})
    )
    projects = client.get_projects()
    assert len(projects) == 1
    assert projects[0]["name"] == "Personal"


@respx.mock
def test_create_card(client):
    respx.post(f"{PLANKA_URL}/api/access-tokens").mock(
        return_value=httpx.Response(200, json={"item": {"token": "tok-abc123"}})
    )
    respx.post(f"{PLANKA_URL}/api/cards").mock(
        return_value=httpx.Response(200, json={"item": {"id": "card-99", "name": "Submit report"}})
    )
    card = client.create_card(
        list_id="list-1",
        title="Submit report",
        description="Due Friday",
        due_date="2026-06-14",
    )
    assert card["id"] == "card-99"


@respx.mock
def test_get_due_soon_cards(client):
    respx.post(f"{PLANKA_URL}/api/access-tokens").mock(
        return_value=httpx.Response(200, json={"item": {"token": "tok-abc123"}})
    )
    respx.get(f"{PLANKA_URL}/api/projects").mock(
        return_value=httpx.Response(200, json={"items": [{"id": "proj-1", "name": "Personal"}]})
    )
    respx.get(f"{PLANKA_URL}/api/projects/proj-1/boards").mock(
        return_value=httpx.Response(200, json={"items": [{"id": "board-1", "name": "Tasks"}]})
    )
    respx.get(f"{PLANKA_URL}/api/boards/board-1").mock(
        return_value=httpx.Response(200, json={
            "item": {"id": "board-1"},
            "included": {
                "cards": [
                    {"id": "c1", "name": "Submit report", "dueDate": "2026-06-12T09:00:00.000Z"},
                    {"id": "c2", "name": "Old task", "dueDate": "2026-01-01T09:00:00.000Z"},
                ]
            }
        })
    )
    from datetime import datetime, timezone
    cards = client.get_due_soon_cards(within_hours=24, now=datetime(2026, 6, 11, 10, 0, 0, tzinfo=timezone.utc))
    assert len(cards) == 1
    assert cards[0]["name"] == "Submit report"
