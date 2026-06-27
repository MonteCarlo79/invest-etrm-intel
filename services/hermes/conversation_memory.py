"""Hermes conversation memory and long-term insight storage.

Tables (auto-created on first use):
  hermes.conversations  — rolling 20-turn chat history per chat_id
  hermes.insights       — auto-extracted facts with cosine similarity search via pgvector

Usage:
    from services.hermes.conversation_memory import HermesMemory
    mem = HermesMemory(pg_url, api_key)
    history = mem.load_history(chat_id)                    # list of {role, content}
    mem.save_turn(chat_id, "user", text)
    mem.save_turn(chat_id, "assistant", reply)
    mem.extract_and_save_insights(chat_id, user_text, assistant_reply)
    context = mem.get_relevant_insights(query)             # formatted string
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS hermes;

CREATE TABLE IF NOT EXISTS hermes.conversations (
    id          BIGSERIAL PRIMARY KEY,
    chat_id     TEXT NOT NULL,
    role        TEXT NOT NULL,          -- 'user' | 'assistant'
    content     TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS hermes_conversations_chat_id ON hermes.conversations(chat_id, created_at DESC);

CREATE TABLE IF NOT EXISTS hermes.insights (
    id          BIGSERIAL PRIMARY KEY,
    category    TEXT NOT NULL,          -- market_view | methodology | asset_note | user_preference | other
    subject     TEXT NOT NULL,
    content     TEXT NOT NULL,
    chat_id     TEXT,
    active      BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
"""

# How many turns to keep per chat_id (rolling window)
_MAX_TURNS = 20
# How many turns to inject into context
_CONTEXT_TURNS = 10
# How many insights to inject
_MAX_INSIGHTS = 5


class HermesMemory:
    def __init__(self, pg_url: str, api_key: str) -> None:
        self._pg_url = pg_url or os.environ.get("PGURL") or os.environ.get("DATABASE_URL", "")
        self._api_key = api_key
        self._conn = None
        self._ready = False

    def _get_conn(self):
        """Lazy-init connection + schema."""
        if self._conn is not None and not self._conn.closed:
            return self._conn
        try:
            import psycopg2
            self._conn = psycopg2.connect(self._pg_url, connect_timeout=5)
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute(_SCHEMA_DDL)
            self._ready = True
        except Exception as e:
            logger.warning("HermesMemory: DB unavailable (%s) — memory disabled", e)
            self._conn = None
        return self._conn

    # ── Chat history ──────────────────────────────────────────────────────────

    def load_history(self, chat_id: str) -> list[dict]:
        """Return up to _CONTEXT_TURNS recent turns as [{role, content}, ...]."""
        conn = self._get_conn()
        if not conn:
            return []
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT role, content FROM hermes.conversations "
                    "WHERE chat_id = %s ORDER BY created_at DESC LIMIT %s",
                    (chat_id, _CONTEXT_TURNS),
                )
                rows = cur.fetchall()
            # rows are newest-first; reverse to chronological order
            return [{"role": r[0], "content": r[1]} for r in reversed(rows)]
        except Exception as e:
            logger.warning("load_history failed: %s", e)
            return []

    def save_turn(self, chat_id: str, role: str, content: str) -> None:
        """Append a turn and prune to _MAX_TURNS."""
        conn = self._get_conn()
        if not conn:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO hermes.conversations (chat_id, role, content) VALUES (%s, %s, %s)",
                    (chat_id, role, content),
                )
                # Prune old turns beyond the rolling window
                cur.execute(
                    """
                    DELETE FROM hermes.conversations
                    WHERE chat_id = %s AND id NOT IN (
                        SELECT id FROM hermes.conversations
                        WHERE chat_id = %s ORDER BY created_at DESC LIMIT %s
                    )
                    """,
                    (chat_id, chat_id, _MAX_TURNS),
                )
        except Exception as e:
            logger.warning("save_turn failed: %s", e)

    # ── Insights ──────────────────────────────────────────────────────────────

    def search_insights(self, query: str, limit: int = _MAX_INSIGHTS) -> list[dict]:
        """Return relevant insights via full-text search."""
        conn = self._get_conn()
        if not conn:
            return []
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, category, subject, content FROM hermes.insights "
                    "WHERE active = TRUE "
                    "AND (subject ILIKE %s OR content ILIKE %s) "
                    "ORDER BY created_at DESC LIMIT %s",
                    (f"%{query[:100]}%", f"%{query[:100]}%", limit),
                )
                return [{"id": r[0], "category": r[1], "subject": r[2], "content": r[3]}
                        for r in cur.fetchall()]
        except Exception as e:
            logger.warning("search_insights failed: %s", e)
            return []

    def get_relevant_insights(self, query: str) -> str:
        """Return formatted insight block for injection into agent system prompt."""
        insights = self.search_insights(query)
        if not insights:
            return ""
        lines = [f"- [{i['category']}] {i['subject']}: {i['content']}" for i in insights]
        return "Remembered context from past conversations:\n" + "\n".join(lines)

    def save_insight(self, category: str, subject: str, content: str, chat_id: str = "") -> None:
        """Persist a single insight."""
        conn = self._get_conn()
        if not conn:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO hermes.insights (category, subject, content, chat_id) VALUES (%s, %s, %s, %s)",
                    (category, subject, content[:500], chat_id or None),
                )
        except Exception as e:
            logger.warning("save_insight failed: %s", e)

    def extract_and_save_insights(self, chat_id: str, user_msg: str, assistant_reply: str) -> int:
        """Use Claude Haiku to extract key facts and persist them. Returns count saved."""
        if not self._api_key:
            return 0
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self._api_key)
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=600,
                system=(
                    "Extract memorable facts, preferences, or decisions from this assistant conversation. "
                    "Return a JSON array. Each item: "
                    "{\"category\": one of [market_view, methodology, user_preference, asset_note, red_flag], "
                    "\"subject\": short title (≤60 chars), "
                    "\"content\": the key fact or view (≤200 chars)}. "
                    "Return [] if nothing reusable across future conversations. "
                    "Focus on: user preferences, analytical conclusions, recurring patterns."
                ),
                messages=[{"role": "user", "content":
                    f"User: {user_msg[:800]}\n\nAssistant: {assistant_reply[:1200]}\n\n"
                    "What is worth remembering from this exchange?"}],
            )
            raw = resp.content[0].text.strip()
            # Strip markdown fences
            if raw.startswith("```"):
                parts = raw.split("```")
                raw = parts[1] if len(parts) > 1 else raw
                if raw.startswith("json"):
                    raw = raw[4:]
            items = json.loads(raw)
            count = 0
            for item in items[:5]:
                if item.get("subject") and item.get("content"):
                    self.save_insight(
                        category=item.get("category", "other"),
                        subject=item["subject"],
                        content=item["content"],
                        chat_id=chat_id,
                    )
                    count += 1
            return count
        except Exception as e:
            logger.debug("extract_and_save_insights failed: %s", e)
            return 0


def migrate(pg_url: str) -> None:
    """Run schema DDL. Safe to call repeatedly (CREATE IF NOT EXISTS)."""
    import psycopg2
    conn = psycopg2.connect(pg_url, connect_timeout=10)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_DDL)
    conn.close()
    print("hermes schema migration complete.")
