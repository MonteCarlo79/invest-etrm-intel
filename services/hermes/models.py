from __future__ import annotations
from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel


class InboundMessage(BaseModel):
    source: Literal["wechat", "wecom"]
    sender_id: str
    sender_name: str
    text: str
    timestamp: datetime
    file_url: Optional[str] = None


class TaskCreate(BaseModel):
    title: str
    description: str = ""
    due_date: Optional[str] = None  # ISO date string YYYY-MM-DD


class AgentAction(BaseModel):
    action: Literal["create_task", "list_tasks", "reply", "ignore"]
    task: Optional[TaskCreate] = None
    reply: Optional[str] = None


class WechatSendRequest(BaseModel):
    to: str          # WeChat ID or WeCom user ID
    text: str
    channel: Literal["wechat", "wecom"]
