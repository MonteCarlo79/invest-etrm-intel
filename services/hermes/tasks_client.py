from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from threading import Lock
from typing import Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS hermes_tasks (
    id          SERIAL PRIMARY KEY,
    title       TEXT NOT NULL,
    due_date    TIMESTAMPTZ,
    status      TEXT NOT NULL DEFAULT 'open',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hermes_settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hermes_file_rules (
    id              SERIAL PRIMARY KEY,
    pattern         TEXT NOT NULL,
    folder_template TEXT NOT NULL,
    auto_kb         BOOLEAN NOT NULL DEFAULT FALSE,
    auto_digest     BOOLEAN NOT NULL DEFAULT FALSE,
    auto_etl        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hermes_seen_messages (
    message_id  TEXT PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class TasksClient:
    def __init__(self, db_url: str) -> None:
        self._db_url = db_url
        self._lock = Lock()
        self._ensure_table()

    def _conn(self):
        return psycopg2.connect(self._db_url)

    def _ensure_table(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_CREATE_TABLE)
                # Migrate existing tables to add new columns (idempotent)
                cur.execute("""
                    ALTER TABLE hermes_file_rules
                        ADD COLUMN IF NOT EXISTS auto_kb     BOOLEAN NOT NULL DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS auto_digest BOOLEAN NOT NULL DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS auto_etl    BOOLEAN NOT NULL DEFAULT FALSE
                """)
            conn.commit()

    # ── Public API (matches PlankaClient interface) ───────────────────────────

    def create_card(self, title: str, due_date: Optional[str] = None, **_: object) -> dict:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "INSERT INTO hermes_tasks (title, due_date) VALUES (%s, %s) RETURNING *",
                    (title, due_date),
                )
                row = dict(cur.fetchone())
            conn.commit()
        logger.info("Created task: %s", title)
        return row

    def complete_card(
        self,
        card_id: Optional[str] = None,
        title: Optional[str] = None,
        **_: object,
    ) -> dict:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if card_id:
                    cur.execute(
                        "UPDATE hermes_tasks SET status='done' WHERE id=%s RETURNING *",
                        (int(card_id),),
                    )
                elif title:
                    cur.execute(
                        "UPDATE hermes_tasks SET status='done' WHERE status='open' AND title ILIKE %s RETURNING *",
                        (f"%{title}%",),
                    )
                else:
                    raise ValueError("card_id or title required")
                row = cur.fetchone()
                if row is None:
                    raise ValueError(f"Task not found: {card_id or title}")
                row = dict(row)
            conn.commit()
        logger.info("Completed task: %s", row.get("title"))
        return row

    def list_open_cards(self) -> list[dict]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM hermes_tasks WHERE status='open' ORDER BY due_date NULLS LAST, created_at"
                )
                return [dict(r) for r in cur.fetchall()]

    def get_setting(self, key: str) -> Optional[str]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM hermes_settings WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else None

    def set_setting(self, key: str, value: str) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO hermes_settings (key, value, updated_at) VALUES (%s, %s, NOW())
                       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()""",
                    (key, value),
                )
            conn.commit()

    def claim_message(self, message_id: str) -> bool:
        """Atomically claim a message ID. Returns True if first claim (process it), False if duplicate."""
        with self._conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        "INSERT INTO hermes_seen_messages (message_id) VALUES (%s)",
                        (message_id,),
                    )
                    conn.commit()
                    # Also purge old entries (keep last 24h) to avoid unbounded growth
                    cur.execute("DELETE FROM hermes_seen_messages WHERE created_at < NOW() - INTERVAL '24 hours'")
                    conn.commit()
                    return True
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()
                    return False

    def get_file_rules(self) -> list[dict]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM hermes_file_rules ORDER BY id")
                return [dict(r) for r in cur.fetchall()]

    def add_file_rule(
        self,
        pattern: str,
        folder_template: str,
        auto_kb: bool = False,
        auto_digest: bool = False,
        auto_etl: bool = False,
    ) -> dict:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "INSERT INTO hermes_file_rules (pattern, folder_template, auto_kb, auto_digest, auto_etl) VALUES (%s, %s, %s, %s, %s) RETURNING *",
                    (pattern, folder_template, auto_kb, auto_digest, auto_etl),
                )
                row = dict(cur.fetchone())
            conn.commit()
        logger.info("Added file rule: pattern=%s folder=%s auto_kb=%s auto_digest=%s auto_etl=%s",
                    pattern, folder_template, auto_kb, auto_digest, auto_etl)
        return row

    def delete_file_rule(self, rule_id: int) -> bool:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM hermes_file_rules WHERE id=%s", (rule_id,))
                deleted = cur.rowcount > 0
            conn.commit()
        return deleted

    def get_due_soon_cards(
        self,
        within_hours: int = 24,
        now: Optional[datetime] = None,
    ) -> list[dict]:
        if now is None:
            now = datetime.now(tz=timezone.utc)
        cutoff = now + timedelta(hours=within_hours)
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM hermes_tasks WHERE status='open' AND due_date IS NOT NULL"
                    " AND due_date >= %s AND due_date <= %s",
                    (now, cutoff),
                )
                return [dict(r) for r in cur.fetchall()]
# build: Tue Jun 16 14:45:41     2026
