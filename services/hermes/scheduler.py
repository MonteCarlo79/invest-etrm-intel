from __future__ import annotations
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING, Optional
from services.hermes.tasks_client import TasksClient
from services.hermes.wecom_client import WeComClient
from services.hermes.feishu_client import FeishuClient

if TYPE_CHECKING:
    from services.hermes.outlook_client import OutlookClient

logger = logging.getLogger(__name__)


def _retry_list_open_cards(tasks: TasksClient, attempts: int = 3, delay: float = 30.0) -> list:
    """Call tasks.list_open_cards() with retries for transient DB connection failures."""
    import psycopg2
    last_exc: Exception = RuntimeError("no attempts")
    for attempt in range(1, attempts + 1):
        try:
            return tasks.list_open_cards()
        except psycopg2.OperationalError as exc:
            last_exc = exc
            if attempt < attempts:
                logger.warning(
                    "send_morning_briefing: DB connection failed (attempt %d/%d), "
                    "retrying in %.0fs: %s",
                    attempt, attempts, delay, exc,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "send_morning_briefing: DB unavailable after %d attempts: %s",
                    attempts, exc,
                )
    raise last_exc

_WEEKDAYS_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]


def _get_shanghai_weather() -> str:
    """Fetch today's Shanghai weather from wttr.in. Returns a short string or empty on failure."""
    try:
        import urllib.request, json as _json
        url = "https://wttr.in/Shanghai?format=j1&lang=zh"
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = _json.loads(resp.read())
        today = data["weather"][0]
        desc  = today["hourly"][4].get("lang_zh", [{}])[0].get("value", "") or today["hourly"][4].get("weatherDesc", [{}])[0].get("value", "")
        max_c = today["maxtempC"]
        min_c = today["mintempC"]
        feels = today["hourly"][4].get("FeelsLikeC", "")
        humidity = today["hourly"][4].get("humidity", "")
        parts = [f"上海今日天气：{desc}，{min_c}–{max_c}°C"]
        if humidity:
            parts.append(f"湿度{humidity}%")
        if feels:
            parts.append(f"体感{feels}°C")
        return "，".join(parts)
    except Exception as exc:
        logger.debug("Weather fetch failed: %s", exc)
        return ""


# WMO weather code → Chinese description (open-meteo codes)
_WMO_CN = {
    0: "晴", 1: "晴间多云", 2: "多云", 3: "阴",
    45: "雾", 48: "雾凇",
    51: "小毛毛雨", 53: "毛毛雨", 55: "大毛毛雨",
    61: "小雨", 63: "中雨", 65: "大雨",
    71: "小雪", 73: "中雪", 75: "大雪", 77: "冰晶",
    80: "阵雨", 81: "中阵雨", 82: "强阵雨",
    85: "阵雪", 86: "强阵雪",
    95: "雷雨", 96: "冰雹雷雨", 99: "强冰雹雷雨",
}


def _get_shanghai_weekly_forecast() -> str:
    """Fetch Mon–Sun 7-day forecast for Shanghai from Open-Meteo (no API key needed).
    Returns a markdown table string or empty string on failure.
    """
    try:
        import urllib.request, json as _json
        from datetime import date as _date
        url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=31.2304&longitude=121.4737"
            "&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum"
            "&forecast_days=7&timezone=Asia%2FShanghai"
        )
        with urllib.request.urlopen(url, timeout=8) as resp:
            data = _json.loads(resp.read())
        daily = data["daily"]
        rows = ["| 日期 | 天气 | 最高 | 最低 | 降水 |",
                "| ---- | ---- | ---- | ---- | ---- |"]
        for i, date_str in enumerate(daily["time"]):
            dt = _date.fromisoformat(date_str)
            wd = _WEEKDAYS_CN[dt.weekday()]
            day_label = f"{dt.month}/{dt.day} {wd}"
            code = int(daily["weather_code"][i])
            desc = _WMO_CN.get(code, f"代码{code}")
            tmax = f"{daily['temperature_2m_max'][i]:.0f}°C"
            tmin = f"{daily['temperature_2m_min'][i]:.0f}°C"
            rain = daily["precipitation_sum"][i]
            rain_str = f"{rain:.1f}mm" if rain and rain > 0 else "—"
            rows.append(f"| {day_label} | {desc} | {tmax} | {tmin} | {rain_str} |")
        return "\n".join(rows)
    except Exception as exc:
        logger.debug("Weekly forecast fetch failed: %s", exc)
        return ""


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


def _task_row(label: str, title: str, task_id: str) -> dict:
    """One div row with task label on left and a ✅ 完成 button on right.

    Uses the `extra` field pattern — the correct Feishu way to attach an inline
    button to a div. Nesting `action` inside a `column` is NOT supported and
    causes Feishu to degrade the entire card to plain text.
    """
    return {
        "tag": "div",
        "text": {"tag": "lark_md", "content": label},
        "extra": {
            "tag": "button",
            "text": {"tag": "plain_text", "content": "✅ 完成"},
            "type": "primary",
            "value": {"act": "done_task", "task_id": task_id, "title": title},
        },
    }


def build_task_card(
    open_tasks: list,
    now: datetime,
    weather_line: str = "",
    weekly_line: str = "",
) -> dict:
    """Build (but do not send) the morning briefing interactive card.

    Extracted so the done_task callback can rebuild the card in-place
    after marking a task complete (removes it from the list immediately).
    """
    today    = now.date()
    date_str = f"{now.year}年{now.month}月{now.day}日 {_WEEKDAYS_CN[now.weekday()]}"

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

    elements: list[dict] = []

    header_md = f"**早上好！今天是 {date_str}**"
    if weather_line:
        header_md += f"\n🌤 {weather_line}"
    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": header_md}})

    if weekly_line:
        elements.append({"tag": "hr"})
        elements.append({"tag": "div", "text": {"tag": "lark_md",
            "content": f"**🗓 本周上海天气预报（周一至周日）**\n{weekly_line}"}})

    elements.append({"tag": "hr"})

    if not open_tasks:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "✅ 没有待办事项，今天轻松！"}})
    else:
        def _add_section(section_label: str, items):
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**{section_label}**"}})
            for entry in items:
                t = entry[0] if isinstance(entry, tuple) else entry
                due = entry[1] if isinstance(entry, tuple) else None
                tid   = str(t.get("id", t.get("card_id", "")))
                title = t["title"]
                if due:
                    delta_d = (due - today).days
                    if delta_d < 0:
                        suffix = f"（逾期 {abs(delta_d)} 天）"
                    elif delta_d == 0:
                        suffix = "（今天）"
                    elif due.weekday() < 7:
                        suffix = f"（{_WEEKDAYS_CN[due.weekday()]}）"
                    else:
                        suffix = f"（{due.month}月{due.day}日）"
                    label = f"{title}{suffix}"
                else:
                    label = title
                elements.append(_task_row(label, title, tid))

        if overdue:
            _add_section(f"🔴 已逾期 ({len(overdue)}项)", overdue)
            elements.append({"tag": "hr"})
        if due_today:
            _add_section(f"🟡 今天到期 ({len(due_today)}项)", due_today)
            elements.append({"tag": "hr"})
        if due_week:
            _add_section(f"📅 本周到期 ({len(due_week)}项)", due_week)
            elements.append({"tag": "hr"})
        if no_date:
            _add_section(f"📋 无截止日期 ({len(no_date)}项)", no_date)
            elements.append({"tag": "hr"})
        if later:
            _add_section(f"🗓 之后到期 ({len(later)}项)", later)

    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "有什么需要帮忙的吗？"}})

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"content": f"📋 每日提醒 — {date_str}", "tag": "plain_text"},
        },
        "elements": elements,
    }


def _card_to_markdown(card: dict) -> str:
    """Flatten a Feishu card into plain markdown for vault persistence."""
    parts = []
    title = card.get("header", {}).get("title", {}).get("content", "")
    if title:
        parts.append(f"# {title}")
    for el in card.get("elements", []):
        if el.get("tag") == "hr":
            parts.append("---")
            continue
        text = el.get("text", {}).get("content") or el.get("content") or ""
        if text:
            parts.append(text)
    return "\n\n".join(parts)


def send_morning_briefing(
    tasks: TasksClient,
    feishu: Optional[FeishuClient] = None,
    feishu_owner_open_id: str = "",
    now: Optional[datetime] = None,
) -> None:
    if not feishu or not feishu_owner_open_id:
        return
    if now is None:
        now = datetime.now(tz=timezone(timedelta(hours=8)))

    try:
        open_tasks = _retry_list_open_cards(tasks)
    except Exception as exc:
        logger.error("send_morning_briefing: skipping — could not load tasks: %s", exc)
        try:
            feishu.send_text(
                open_id=feishu_owner_open_id,
                text=f"📋 早上好！今天是 {now.year}年{now.month}月{now.day}日 {_WEEKDAYS_CN[now.weekday()]}。\n⚠️ 无法加载待办事项（数据库暂时不可用）。",
            )
        except Exception:
            pass
        return

    weather   = _get_shanghai_weather()
    is_monday = (now.weekday() == 0)
    weekly    = _get_shanghai_weekly_forecast() if is_monday else ""

    card = build_task_card(open_tasks, now, weather_line=weather, weekly_line=weekly)

    try:
        feishu.send_card(open_id=feishu_owner_open_id, card=card)
        try:
            from services.knowledge_pool import vault_writer
            vault_writer.write_briefing_note("morning", _card_to_markdown(card))
        except Exception as exc2:
            logger.debug("Briefing vault note failed: %s", exc2)
    except Exception as exc:
        logger.error("Morning briefing card failed: %s", exc)
        date_str  = f"{now.year}年{now.month}月{now.day}日 {_WEEKDAYS_CN[now.weekday()]}"
        header_md = f"早上好！今天是 {date_str}"
        if weather:
            header_md += f"\n🌤 {weather}"
        lines = [header_md, "\n有什么需要帮忙的吗？"]
        try:
            feishu.send_text(open_id=feishu_owner_open_id, text="\n".join(lines))
        except Exception:
            pass


def summarize_emails(messages: list[dict], api_key: str) -> str:
    """Use Claude Haiku to produce a concise digest of a list of email dicts."""
    from shared.anthropic_client import make_client as _make_anthropic_client

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

    client = _make_anthropic_client(api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-6",  # haiku-4-5 requires use-case form; sonnet-4-6 text-only confirmed working
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
