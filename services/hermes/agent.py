from __future__ import annotations
import json
import re
from datetime import datetime, timezone
from typing import Optional
import anthropic
from services.hermes.models import InboundMessage, AgentAction, TaskCreate
from services.hermes.planka_client import PlankaClient

TASK_KEYWORDS = [
    "file", "report", "deadline", "by", "urgent", "action", "task",
    "remind", "due", "pls", "please", "doc", "请", "任务", "截止", "提醒",
    "add task", "schedule", "meeting", "submit",
]

SYSTEM_PROMPT = """You are Hermes, a personal assistant integrated with WeChat and a Kanban task board.

Your job is to help the user manage tasks. When you receive a message, decide:
1. If the message contains a task or action item → create a task
2. If the message is asking about their tasks → reply with task summary
3. If the message needs no action (casual chat, irrelevant) → ignore

Always respond with valid JSON in this exact format:
{
  "action": "create_task" | "reply" | "ignore",
  "task": {"title": "...", "description": "...", "due_date": "YYYY-MM-DD or null"},
  "reply": "... or null"
}

For due dates: interpret relative dates from the message timestamp context provided.
Keep replies concise and friendly. Reply in the same language as the user's message.
"""


class HermesAgent:
    def __init__(self, planka: PlankaClient, anthropic_api_key: str) -> None:
        self._planka = planka
        self._anthropic_api_key = anthropic_api_key

    def _should_process(self, msg: InboundMessage) -> bool:
        text_lower = msg.text.lower()
        return any(kw in text_lower for kw in TASK_KEYWORDS)

    def _get_board_context(self) -> str:
        try:
            projects = self._planka.get_projects()
            if not projects:
                return "No boards found."
            boards = self._planka.get_boards(projects[0]["id"])
            if not boards:
                return "No boards found."
            board = self._planka.get_board(boards[0]["id"])
            cards = board.get("included", {}).get("cards", [])
            if not cards:
                return "Task board is empty."
            summaries = [
                f"- {c['name']}" + (f" (due {c['dueDate'][:10]})" if c.get("dueDate") else "")
                for c in cards[:10]
            ]
            return "Current tasks:\n" + "\n".join(summaries)
        except Exception:
            return "Could not fetch task board."

    def _get_default_list_id(self) -> Optional[str]:
        try:
            projects = self._planka.get_projects()
            boards = self._planka.get_boards(projects[0]["id"])
            board = self._planka.get_board(boards[0]["id"])
            lists = board.get("included", {}).get("lists", [])
            todo_list = next(
                (l for l in lists if "to do" in l["name"].lower() or l["name"].lower() == "todo"),
                None,
            )
            return (todo_list or lists[0])["id"] if lists else None
        except Exception:
            return None

    def process(self, msg: InboundMessage) -> AgentAction:
        if not self._should_process(msg):
            return AgentAction(action="ignore")

        board_context = self._get_board_context()
        user_content = (
            f"Message from {msg.sender_name} via {msg.source} "
            f"at {msg.timestamp.isoformat()}:\n\n{msg.text}\n\n"
            f"Context:\n{board_context}"
        )

        client = anthropic.Anthropic(api_key=self._anthropic_api_key)
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_content}],
        )

        raw = response.content[0].text.strip()
        # Strip markdown code fences if present
        raw = re.sub(r"^```(?:json)?\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)

        data = json.loads(raw)
        task_data = data.get("task")
        task = TaskCreate(**task_data) if task_data else None
        return AgentAction(action=data["action"], task=task, reply=data.get("reply"))

    def execute(self, action: AgentAction) -> None:
        if action.action == "create_task" and action.task:
            list_id = self._get_default_list_id()
            if list_id:
                self._planka.create_card(
                    list_id=list_id,
                    title=action.task.title,
                    description=action.task.description,
                    due_date=action.task.due_date,
                )
