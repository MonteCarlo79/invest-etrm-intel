from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from services.hermes.planka_client import PlankaClient
from services.hermes.wecom_client import WeComClient
from services.hermes.wechat_client import WechatyBridgeClient


def send_due_reminders(
    planka: PlankaClient,
    wecom: WeComClient,
    wechat_bridge: WechatyBridgeClient,
    wecom_user_id: str,
    wechat_id: str,
    now: Optional[datetime] = None,
    within_hours: int = 24,
) -> None:
    if now is None:
        now = datetime.now(tz=timezone.utc)
    cards = planka.get_due_soon_cards(within_hours=within_hours, now=now)
    for card in cards:
        due_str = card.get("dueDate", "")[:10]
        text = f"Reminder: '{card['name']}' is due on {due_str}"
        try:
            wecom.send_text(user_id=wecom_user_id, text=text)
        except Exception:
            pass
        try:
            wechat_bridge.send(to=wechat_id, text=text)
        except Exception:
            pass
