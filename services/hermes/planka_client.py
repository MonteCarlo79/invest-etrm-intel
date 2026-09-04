from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class PlankaClient:
    def __init__(self, base_url: str, email: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.email = email
        self.password = password
        self._token: Optional[str] = None
        self._board_id: Optional[str] = None
        self._inbox_list_id: Optional[str] = None

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _login(self) -> None:
        resp = requests.post(
            f"{self.base_url}/api/access-tokens",
            json={"emailOrUsername": self.email, "password": self.password},
            timeout=10,
        )
        resp.raise_for_status()
        self._token = resp.json()["item"]

    def _headers(self) -> dict[str, str]:
        if not self._token:
            self._login()
        return {"Authorization": f"Bearer {self._token}"}

    def _get(self, path: str) -> dict:
        resp = requests.get(f"{self.base_url}{path}", headers=self._headers(), timeout=10)
        if resp.status_code == 401:
            self._token = None
            self._login()
            resp = requests.get(f"{self.base_url}{path}", headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json: dict) -> dict:
        resp = requests.post(
            f"{self.base_url}{path}", headers=self._headers(), json=json, timeout=10
        )
        if resp.status_code == 401:
            self._token = None
            self._login()
            resp = requests.post(
                f"{self.base_url}{path}", headers=self._headers(), json=json, timeout=10
            )
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path: str, json: dict) -> dict:
        resp = requests.patch(
            f"{self.base_url}{path}", headers=self._headers(), json=json, timeout=10
        )
        if resp.status_code == 401:
            self._token = None
            self._login()
            resp = requests.patch(
                f"{self.base_url}{path}", headers=self._headers(), json=json, timeout=10
            )
        resp.raise_for_status()
        return resp.json()

    # ── Board / List discovery ────────────────────────────────────────────────

    def _ensure_board(self) -> None:
        if self._board_id:
            return
        data = self._get("/api/projects")
        projects = data.get("items", [])
        if not projects:
            raise RuntimeError("No Planka projects found")
        project_id = projects[0]["id"]
        boards_data = self._get(f"/api/projects/{project_id}")
        boards = boards_data.get("included", {}).get("boards", [])
        if not boards:
            raise RuntimeError("No boards found in project")
        self._board_id = boards[0]["id"]

    def _ensure_inbox_list(self) -> None:
        if self._inbox_list_id:
            return
        self._ensure_board()
        data = self._get(f"/api/boards/{self._board_id}")
        lists = data.get("included", {}).get("lists", [])
        if not lists:
            raise RuntimeError("No lists on board")
        # Use first list (typically Backlog / To Do / Inbox)
        self._inbox_list_id = lists[0]["id"]

    # ── Public API ────────────────────────────────────────────────────────────

    def create_card(self, title: str, due_date: Optional[str] = None, **_: object) -> dict:
        self._ensure_inbox_list()
        payload: dict = {"name": title, "position": 65535, "type": "project"}
        if due_date:
            payload["dueDate"] = due_date
        result = self._post(f"/api/lists/{self._inbox_list_id}/cards", payload)
        logger.info("Created Planka card: %s", title)
        return result.get("item", result)

    def complete_card(
        self,
        card_id: Optional[str] = None,
        title: Optional[str] = None,
        **_: object,
    ) -> dict:
        if not card_id and title:
            card_id = self._find_card_by_title(title)
        if not card_id:
            raise ValueError("card_id or title required to complete a card")
        result = self._patch(f"/api/cards/{card_id}", {"isCompleted": True})
        logger.info("Completed Planka card: %s", card_id)
        return result.get("item", result)

    def list_open_cards(self) -> list[dict]:
        self._ensure_board()
        data = self._get(f"/api/boards/{self._board_id}")
        cards = data.get("included", {}).get("cards", [])
        return [c for c in cards if not c.get("isCompleted")]

    def get_due_soon_cards(
        self,
        within_hours: int = 24,
        now: Optional[datetime] = None,
    ) -> list[dict]:
        if now is None:
            now = datetime.now(tz=timezone.utc)
        cutoff = now + timedelta(hours=within_hours)
        cards = self.list_open_cards()
        due_soon = []
        for card in cards:
            due_str = card.get("dueDate")
            if not due_str:
                continue
            try:
                due_dt = datetime.fromisoformat(due_str.replace("Z", "+00:00"))
                if now <= due_dt <= cutoff:
                    due_soon.append(card)
            except ValueError:
                pass
        return due_soon

    def _find_card_by_title(self, title: str) -> Optional[str]:
        cards = self.list_open_cards()
        title_lower = title.lower()
        for card in cards:
            if title_lower in card.get("name", "").lower():
                return card["id"]
        return None
