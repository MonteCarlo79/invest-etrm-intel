from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional
import httpx


class PlankaClient:
    def __init__(self, base_url: str, email: str, password: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._email = email
        self._password = password
        self._token: Optional[str] = None

    def _login(self) -> str:
        resp = httpx.post(
            f"{self._base_url}/api/access-tokens",
            json={"emailOrUsername": self._email, "password": self._password},
            timeout=10,
        )
        resp.raise_for_status()
        self._token = resp.json()["item"]["token"]
        return self._token

    def _headers(self) -> dict[str, str]:
        if not self._token:
            self._login()
        return {"Authorization": f"Bearer {self._token}"}

    def _get(self, path: str) -> dict:
        resp = httpx.get(f"{self._base_url}{path}", headers=self._headers(), timeout=10)
        if resp.status_code == 401:
            self._login()
            resp = httpx.get(f"{self._base_url}{path}", headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: dict) -> dict:
        resp = httpx.post(
            f"{self._base_url}{path}", json=body, headers=self._headers(), timeout=10
        )
        if resp.status_code == 401:
            self._login()
            resp = httpx.post(
                f"{self._base_url}{path}", json=body, headers=self._headers(), timeout=10
            )
        resp.raise_for_status()
        return resp.json()

    def get_projects(self) -> list[dict]:
        return self._get("/api/projects")["items"]

    def get_boards(self, project_id: str) -> list[dict]:
        return self._get(f"/api/projects/{project_id}/boards")["items"]

    def get_board(self, board_id: str) -> dict:
        return self._get(f"/api/boards/{board_id}")

    def create_card(
        self,
        list_id: str,
        title: str,
        description: str = "",
        due_date: Optional[str] = None,
    ) -> dict:
        body: dict = {"listId": list_id, "name": title, "description": description}
        if due_date:
            body["dueDate"] = f"{due_date}T09:00:00.000Z"
        return self._post("/api/cards", body)["item"]

    def get_due_soon_cards(
        self,
        within_hours: int = 24,
        now: Optional[datetime] = None,
    ) -> list[dict]:
        if now is None:
            now = datetime.now(tz=timezone.utc)
        cutoff = now + timedelta(hours=within_hours)
        due_cards = []
        for project in self.get_projects():
            for board in self.get_boards(project["id"]):
                board_data = self.get_board(board["id"])
                for card in board_data.get("included", {}).get("cards", []):
                    due_str = card.get("dueDate")
                    if not due_str:
                        continue
                    due_dt = datetime.fromisoformat(due_str.replace("Z", "+00:00"))
                    if now < due_dt <= cutoff:
                        due_cards.append(card)
        return due_cards
