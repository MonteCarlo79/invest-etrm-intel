"""Per-user UI preferences persisted in the existing key/value table `hermes_settings`.

Pure DB helpers (no Streamlit). Keys are namespaced: "<app>.<pref>:<user>".
Read failures return the default — a missing table or DB hiccup must never break a page.
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import text

log = logging.getLogger(__name__)
TABLE = "hermes_settings"


def pref_key(app: str, pref: str, user: str | None) -> str:
    return f"{app}.{pref}:{(user or 'anonymous').strip().lower()}"


def load_pref(engine, key: str, default=None):
    try:
        with engine.connect() as c:
            row = c.execute(text(f"SELECT value FROM {TABLE} WHERE key = :k"), {"k": key}).fetchone()
        return json.loads(row[0]) if row and row[0] is not None else default
    except Exception as e:  # noqa: BLE001 — preferences are best-effort
        log.warning("load_pref(%s) failed: %s", key, e)
        return default


def save_pref(engine, key: str, value) -> bool:
    try:
        with engine.begin() as c:
            c.execute(text(f"INSERT INTO {TABLE} (key, value, updated_at) VALUES (:k, :v, CURRENT_TIMESTAMP) "
                           "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP"),
                      {"k": key, "v": json.dumps(value, ensure_ascii=False)})
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("save_pref(%s) failed: %s", key, e)
        return False
