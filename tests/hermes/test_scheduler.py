from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from services.hermes.scheduler import send_due_reminders


def test_send_due_reminders_sends_for_each_due_card():
    mock_planka = MagicMock()
    mock_planka.get_due_soon_cards.return_value = [
        {"name": "Submit report", "dueDate": "2026-06-12T09:00:00.000Z"},
        {"name": "Review contract", "dueDate": "2026-06-12T14:00:00.000Z"},
    ]
    mock_wecom = MagicMock()
    mock_wechat = MagicMock()

    now = datetime(2026, 6, 11, 10, 0, 0, tzinfo=timezone.utc)
    send_due_reminders(
        planka=mock_planka,
        wecom=mock_wecom,
        wechat_bridge=mock_wechat,
        wecom_user_id="user1",
        wechat_id="wxid_abc",
        now=now,
    )

    assert mock_wecom.send_text.call_count == 2
    first_call_text = mock_wecom.send_text.call_args_list[0][1]["text"]
    assert "Submit report" in first_call_text
    assert "2026-06-12" in first_call_text


def test_send_due_reminders_noop_when_no_cards():
    mock_planka = MagicMock()
    mock_planka.get_due_soon_cards.return_value = []
    mock_wecom = MagicMock()
    mock_wechat = MagicMock()

    send_due_reminders(
        planka=mock_planka,
        wecom=mock_wecom,
        wechat_bridge=mock_wechat,
        wecom_user_id="user1",
        wechat_id="wxid_abc",
    )

    mock_wecom.send_text.assert_not_called()
    mock_wechat.send.assert_not_called()
