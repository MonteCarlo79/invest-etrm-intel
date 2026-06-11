from datetime import datetime
from services.hermes.models import InboundMessage, AgentAction, TaskCreate


def test_inbound_message_from_wechat():
    msg = InboundMessage(
        source="wechat",
        sender_id="wxid_abc123",
        sender_name="John",
        text="remind me to submit the report by Friday",
        timestamp=datetime(2026, 6, 11, 10, 0, 0),
    )
    assert msg.source == "wechat"
    assert msg.text == "remind me to submit the report by Friday"
    assert msg.file_url is None


def test_inbound_message_with_file():
    msg = InboundMessage(
        source="wecom",
        sender_id="user@company.com",
        sender_name="Alice",
        text="see attached",
        timestamp=datetime(2026, 6, 11, 10, 0, 0),
        file_url="s3://bess-platform/hermes/files/report.pdf",
    )
    assert msg.file_url == "s3://bess-platform/hermes/files/report.pdf"


def test_agent_action_create_task():
    action = AgentAction(
        action="create_task",
        task=TaskCreate(
            title="Submit report",
            description="Attached: report.pdf",
            due_date="2026-06-14",
        ),
        reply="Got it! I've added 'Submit report' to your task list.",
    )
    assert action.action == "create_task"
    assert action.task.title == "Submit report"
    assert action.task.due_date == "2026-06-14"


def test_agent_action_ignore():
    action = AgentAction(action="ignore", reply=None, task=None)
    assert action.action == "ignore"
    assert action.task is None
