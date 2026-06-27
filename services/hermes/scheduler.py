from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING, Optional
from services.hermes.tasks_client import TasksClient
from services.hermes.wecom_client import WeComClient
from services.hermes.feishu_client import FeishuClient

if TYPE_CHECKING:
    from services.hermes.outlook_client import OutlookClient

logger = logging.getLogger(__name__)

_WEEKDAYS_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]


def send_due_reminders(
    tasks: TasksClient,
    wecom: Optional[WeComClient],
    wecom_user_id: str,
    feishu: Optional[FeishuClient] = None,
    feishu_owner_open_id: str = "",
    now: Optional[datetime] = None,
    within_hours: int = 24,
) -> None:
    if now is None:
        now = datetime.now(tz=timezone.utc)
    cards = tasks.get_due_soon_cards(within_hours=within_hours, now=now)
    for card in cards:
        due_date = card.get("due_date")
        due_str = str(due_date.date()) if due_date else ""
        text = f"Reminder: '{card['title']}' is due on {due_str}"
        if feishu and feishu_owner_open_id:
            try:
                feishu.send_text(open_id=feishu_owner_open_id, text=text)
            except Exception:
                pass
        if wecom and wecom_user_id:
            try:
                wecom.send_text(user_id=wecom_user_id, text=text)
            except Exception:
                pass


def send_morning_briefing(
    tasks: TasksClient,
    feishu: Optional[FeishuClient] = None,
    feishu_owner_open_id: str = "",
    now: Optional[datetime] = None,
) -> None:
    if not feishu or not feishu_owner_open_id:
        return
    if now is None:
        # Beijing time = UTC+8
        now = datetime.now(tz=timezone(timedelta(hours=8)))

    today = now.date()
    open_tasks = tasks.list_open_cards()

    overdue, due_today, due_week, later, no_date = [], [], [], [], []
    for t in open_tasks:
        d = t.get("due_date")
        if d is None:
            no_date.append(t)
        else:
            due = d.date() if hasattr(d, "date") else d
            delta = (due - today).days
            if delta < 0:
                overdue.append((t, due))
            elif delta == 0:
                due_today.append(t)
            elif delta <= 7:
                due_week.append((t, due))
            else:
                later.append((t, due))

    date_str = f"{now.year}年{now.month}月{now.day}日 {_WEEKDAYS_CN[now.weekday()]}"
    lines = [f"早上好！今天是 {date_str}\n"]

    if not open_tasks:
        lines.append("✅ 没有待办事项，今天轻松！")
    else:
        if overdue:
            lines.append(f"🔴 已逾期 ({len(overdue)}项)")
            for t, due in overdue:
                days = (today - due).days
                lines.append(f"  • {t['title']}（逾期 {days} 天）")

        if due_today:
            lines.append(f"\n🟡 今天到期 ({len(due_today)}项)")
            for t in due_today:
                lines.append(f"  • {t['title']}")

        if due_week:
            lines.append(f"\n📅 本周到期 ({len(due_week)}项)")
            for t, due in due_week:
                lines.append(f"  • {t['title']}（{_WEEKDAYS_CN[due.weekday()]}）")

        if no_date:
            lines.append(f"\n📋 无截止日期 ({len(no_date)}项)")
            for t in no_date:
                lines.append(f"  • {t['title']}")

        if later:
            lines.append(f"\n🗓 之后到期 ({len(later)}项)")
            for t, due in later:
                lines.append(f"  • {t['title']}（{due.month}月{due.day}日）")

    lines.append("\n有什么需要帮忙的吗？")
    try:
        feishu.send_text(open_id=feishu_owner_open_id, text="\n".join(lines))
    except Exception as exc:
        logger.error("Morning briefing failed: %s", exc)


def summarize_emails(messages: list[dict], api_key: str) -> str:
    """Use Claude Haiku to produce a concise digest of a list of email dicts."""
    from anthropic import Anthropic

    lines = []
    for i, m in enumerate(messages, 1):
        sender = m.get("from", {}).get("emailAddress", {})
        importance = " ⚠️" if m.get("importance") == "high" else ""
        attach = " 📎" if m.get("hasAttachments") else ""
        lines.append(
            f"{i}.{importance}{attach} From: {sender.get('name', sender.get('address', '?'))}\n"
            f"   Subject: {m.get('subject', '(no subject)')}\n"
            f"   Date: {m.get('receivedDateTime', '')[:10]}\n"
            f"   Preview: {m.get('bodyPreview', '')[:200]}"
        )

    client = Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=800,
        system=(
            "You are a personal assistant summarising unread emails for a busy professional. "
            "Be concise. Highlight urgent/action-required items first, then important updates, "
            "then FYIs. Note any attachments worth downloading. "
            "Match the language of the email subjects (Chinese if Chinese, English if English). "
            "Use bullet points."
        ),
        messages=[{
            "role": "user",
            "content": (
                f"Here are {len(messages)} unread emails:\n\n"
                + "\n\n".join(lines)
                + "\n\nProvide a brief highlight summary."
            ),
        }],
    )
    return resp.content[0].text.strip()


def send_email_digest(
    outlook: "OutlookClient",
    api_key: str,
    feishu: Optional[FeishuClient] = None,
    feishu_owner_open_id: str = "",
    limit: int = 25,
) -> None:
    """Fetch unread emails, summarise with Claude, send digest to Feishu."""
    if not feishu or not feishu_owner_open_id:
        return
    try:
        messages = outlook.list_messages(limit=limit, unread_only=True)
    except Exception as exc:
        logger.error("Email digest: failed to fetch messages: %s", exc)
        return

    if not messages:
        return  # Nothing to report — don't send noise

    try:
        summary = summarize_emails(messages, api_key)
    except Exception as exc:
        logger.error("Email digest: summarisation failed: %s", exc)
        summary = "\n".join(
            f"• {m.get('subject', '(no subject)')} — {m.get('from', {}).get('emailAddress', {}).get('name', '?')}"
            for m in messages
        )

    text = f"📧 邮件摘要（{len(messages)} 封未读）\n\n{summary}"
    try:
        feishu.send_text(open_id=feishu_owner_open_id, text=text)
    except Exception as exc:
        logger.error("Email digest: failed to send to Feishu: %s", exc)
