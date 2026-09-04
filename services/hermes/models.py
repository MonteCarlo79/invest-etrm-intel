from __future__ import annotations
from typing import Any
from pydantic import BaseModel


class InboundMessage(BaseModel):
    source: str  # "wecom"
    sender_id: str
    sender_name: str
    text: str
    timestamp: str


class Action(BaseModel):
    action: str  # CREATE | LIST | DONE | REPLY | ONEDRIVE_LIST | ONEDRIVE_SEARCH | ONEDRIVE_READ | ONEDRIVE_UPLOAD
    params: dict[str, Any] = {}
    reply: str = ""
