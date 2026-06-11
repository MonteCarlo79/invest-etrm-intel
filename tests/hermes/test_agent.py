import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from services.hermes.models import InboundMessage, AgentAction
from services.hermes.agent import HermesAgent


@pytest.fixture
def mock_planka():
    client = MagicMock()
    client.get_projects.return_value = [{"id": "proj-1", "name": "Personal"}]
    client.get_boards.return_value = [{"id": "board-1", "name": "Tasks"}]
    client.get_board.return_value = {
        "item": {"id": "board-1"},
        "included": {
            "lists": [{"id": "list-todo", "name": "To Do"}],
            "cards": [],
        }
    }
    client.create_card.return_value = {"id": "card-new", "name": "Submit report"}
    return client


@pytest.fixture
def agent(mock_planka):
    return HermesAgent(planka=mock_planka, anthropic_api_key="test-key")


def test_should_process_message_with_task_keyword(agent):
    msg = InboundMessage(
        source="wechat",
        sender_id="wxid_1",
        sender_name="Alice",
        text="please remind me to submit the report by Friday",
        timestamp=datetime(2026, 6, 11, 10, 0, tzinfo=timezone.utc),
    )
    assert agent._should_process(msg) is True


def test_should_skip_irrelevant_message(agent):
    msg = InboundMessage(
        source="wechat",
        sender_id="wxid_1",
        sender_name="Alice",
        text="haha 😂",
        timestamp=datetime(2026, 6, 11, 10, 0, tzinfo=timezone.utc),
    )
    assert agent._should_process(msg) is False


def test_process_returns_agent_action(agent):
    msg = InboundMessage(
        source="wechat",
        sender_id="wxid_1",
        sender_name="Alice",
        text="add task: review Q2 report due next Monday",
        timestamp=datetime(2026, 6, 11, 10, 0, tzinfo=timezone.utc),
    )

    mock_response = MagicMock()
    mock_response.content = [MagicMock(
        text='{"action": "create_task", "task": {"title": "Review Q2 report", "description": "", "due_date": "2026-06-16"}, "reply": "Added to your task list!"}'
    )]

    with patch("services.hermes.agent.anthropic.Anthropic") as mock_anthropic_cls:
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response
        mock_anthropic_cls.return_value = mock_client

        action = agent.process(msg)

    assert isinstance(action, AgentAction)
    assert action.action == "create_task"
    assert action.task.title == "Review Q2 report"
    assert action.reply == "Added to your task list!"
