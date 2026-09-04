from unittest.mock import MagicMock

import services.hermes.scheduler as sched
import services.knowledge_pool.vault_writer as vw


def test_morning_briefing_writes_vault_note(monkeypatch):
    card_holder = {}
    feishu = MagicMock()
    feishu.send_card = lambda open_id, card: card_holder.update(card=card)
    tasks = MagicMock()
    tasks.list_open_cards.return_value = []
    monkeypatch.setattr(sched, "_retry_list_open_cards", lambda t: [])
    monkeypatch.setattr(sched, "_get_shanghai_weather", lambda: "多云 26°C")

    writes = []
    monkeypatch.setattr(vw, "write_briefing_note", lambda kind, content, note_date="": writes.append((kind, content)) or "ok")

    from datetime import datetime, timezone, timedelta
    now = datetime(2026, 8, 6, 7, 30, tzinfo=timezone(timedelta(hours=8)))
    sched.send_morning_briefing(tasks, feishu=feishu, feishu_owner_open_id="ou_x", now=now)

    assert writes and writes[0][0] == "morning"
    assert "每日提醒" in writes[0][1]


def test_card_to_markdown_extracts_text():
    card = {
        "header": {"title": {"content": "📋 每日提醒 — 2026-08-06"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": "🌤 多云"}},
            {"tag": "hr"},
            {"tag": "markdown", "content": "有什么需要帮忙的吗？"},
        ],
    }
    md = sched._card_to_markdown(card)
    assert "📋 每日提醒 — 2026-08-06" in md
    assert "🌤 多云" in md
    assert "---" in md
    assert "有什么需要帮忙的吗？" in md
