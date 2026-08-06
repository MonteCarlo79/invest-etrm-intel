"""Write Hermes-authored markdown notes into the knowledge vault via OneDrive.

Briefings land directly in hermes/briefings/ (operational record).
Insights land in hermes/inbox/ with review_status: pending for human
review/promotion in Obsidian (Stage 4 review loop).

All functions return None on any failure — never break the chat/scheduler loop.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

VAULT_ROOT = "etrm/bess-platform/knowledge"
BRIEFINGS_AREA = "hermes/briefings"
INBOX_AREA = "hermes/inbox"

_CST = timezone(timedelta(hours=8))


def _client():
    """Shared OneDriveClient or None. Isolated for tests to monkeypatch."""
    from services.hermes.onedrive_client import get_shared_onedrive_client
    return get_shared_onedrive_client()


def _slug(text: str, max_len: int = 24) -> str:
    """Filename-safe slug: keep CJK + alnum, collapse everything else to '-'."""
    text = unicodedata.normalize("NFKC", text).strip()
    out = []
    for ch in text:
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("-")
    slug = re.sub(r"-{2,}", "-", "".join(out)).strip("-")
    return slug[:max_len].strip("-") or "note"


def _frontmatter(**fields) -> str:
    lines = ["---"]
    for k, v in fields.items():
        if v is None or v == "":
            continue
        lines.append(f"{k}: {v}")
    lines.append("---")
    return "\n".join(lines)


def _upload(area: str, filename: str, text: str) -> Optional[str]:
    client = _client()
    if client is None:
        logger.debug("vault write skipped (%s/%s) — OneDrive not configured", area, filename)
        return None
    folder = f"{VAULT_ROOT}/{area}"
    try:
        client.upload_file(folder, filename, text.encode("utf-8"), conflict_behavior="replace")
    except Exception as exc:
        logger.warning("vault note upload failed (%s/%s): %s", area, filename, exc)
        return None
    return f"{area}/{filename}"


def write_briefing_note(kind: str, content: str, note_date: str = "") -> Optional[str]:
    """Write a scheduled-output note (morning briefing / daily report)."""
    if kind not in ("morning", "daily_report"):
        raise ValueError(f"unknown briefing kind: {kind}")
    now = datetime.now(tz=_CST)
    date_str = note_date or now.strftime("%Y-%m-%d")
    fm = _frontmatter(
        note_type="briefing",
        kind=kind,
        date=date_str,
        source="hermes",
        created=now.isoformat(timespec="seconds"),
    )
    return _upload(BRIEFINGS_AREA, f"{date_str}-{kind}.md", fm + "\n\n" + content.strip() + "\n")


def write_insight_note(
    category: str,
    content: str,
    source_app: str,
    province: str = "",
    confidence: str = "medium",
) -> Optional[str]:
    """Write an auto-extracted insight to the review inbox."""
    now = datetime.now(tz=_CST)
    date_str = now.strftime("%Y-%m-%d")
    slug = _slug(province + "-" + content if province else content)
    fm = _frontmatter(
        note_type="insight",
        category=category,
        province=province,
        confidence=confidence,
        source_app=source_app,
        review_status="pending",
        created=now.isoformat(timespec="seconds"),
    )
    body = fm + f"\n\n# {category}: {_slug(content, 48)}\n\n{content.strip()}\n"
    return _upload(INBOX_AREA, f"{date_str}-{slug}.md", body)
