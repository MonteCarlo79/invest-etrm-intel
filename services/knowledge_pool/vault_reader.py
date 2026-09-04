"""Read markdown knowledge from the vault via OneDrive at query time.

Matching order: date mention → daily report + briefing notes; CN province →
province note; theme keyword → concept note; else OneDrive full-text search.
Read scope (spec §2): spot_market vault + hermes/briefings. hermes/inbox is
deliberately excluded (unreviewed).

All functions return empty results on any failure — knowledge I/O is optional.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional

logger = logging.getLogger(__name__)

VAULT_ROOT = "etrm/bess-platform/knowledge"
AREAS = {
    "daily": "spot_market/01_daily_reports",
    "provinces": "spot_market/02_provinces",
    "concepts": "spot_market/03_concepts",
    "briefings": "hermes/briefings",
}

_LIST_CACHE: dict[str, tuple[float, list[str]]] = {}
_LIST_TTL = 900  # 15 min

_DATE_RE = re.compile(r"(20\d{2})[-/年.](\d{1,2})[-/月.](\d{1,2})")


def _client():
    from services.hermes.onedrive_client import get_shared_onedrive_client
    return get_shared_onedrive_client()


def _list_area(area: str) -> list[str]:
    """Filenames in an area, cached 15 min. [] on any error."""
    now = time.time()
    cached = _LIST_CACHE.get(area)
    if cached and now - cached[0] < _LIST_TTL:
        return cached[1]
    client = _client()
    if client is None:
        return []
    try:
        items = client.list_items(f"{VAULT_ROOT}/{AREAS[area]}")
        names = [i["name"] for i in items if "file" in i or i.get("name", "").endswith(".md")]
    except Exception as exc:
        logger.debug("vault list failed (%s): %s", area, exc)
        return []
    _LIST_CACHE[area] = (now, names)
    return names


def _normalize_date(query: str) -> Optional[str]:
    m = _DATE_RE.search(query)
    if not m:
        return None
    y, mo, d = m.groups()
    return f"{y}-{int(mo):02d}-{int(d):02d}"


def search_notes(query: str = "", province: str = "", date: str = "", limit: int = 3) -> list[dict]:
    """Find up to `limit` vault notes relevant to the query."""
    hits: list[dict] = []

    def _add(area: str, name: str):
        entry = {"path": f"{AREAS[area]}/{name}", "name": name, "area": area}
        if entry not in hits:
            hits.append(entry)

    # 1. date mention → daily report + briefings
    date_str = date or _normalize_date(query)
    if date_str:
        for area in ("daily", "briefings"):
            for name in _list_area(area):
                if name.startswith(date_str):
                    _add(area, name)

    # 2. province name → province note
    prov = province
    if not prov:
        for name in _list_area("provinces"):
            if name.endswith(".md") and name[:-3] in query:
                prov = name[:-3]
                break
    if prov:
        for name in _list_area("provinces"):
            if name == f"{prov}.md":
                _add("provinces", name)

    # 3. theme keyword → concept note
    for name in _list_area("concepts"):
        stem = name[:-3] if name.endswith(".md") else name
        if stem and stem in query:
            _add("concepts", name)

    # 4. free-text fallback → OneDrive search, restricted to in-scope areas
    if not hits and query:
        client = _client()
        if client is not None:
            try:
                for item in client.search(query)[:10]:
                    parent = item.get("parentReference", {}).get("path", "")
                    name = item.get("name", "")
                    if not name.endswith(".md"):
                        continue
                    for area, rel in AREAS.items():
                        if parent.endswith(rel):
                            _add(area, name)
                            break
            except Exception as exc:
                logger.debug("vault OneDrive search failed: %s", exc)

    return hits[:limit]


def read_note(path: str, max_chars: int = 2000) -> str:
    """Read a vault-relative note. '' on failure."""
    if ".." in path or path.startswith("/") or "inbox" in path:
        return ""
    client = _client()
    if client is None:
        return ""
    try:
        text = client.read_file_by_path(f"{VAULT_ROOT}/{path}").decode("utf-8", errors="replace")
    except Exception as exc:
        logger.debug("vault read failed (%s): %s", path, exc)
        return ""
    if len(text) > max_chars:
        text = text[:max_chars].rstrip() + "\n[…truncated]"
    return text


def retrieve_vault_context(query: str, max_notes: int = 3, max_chars_per_note: int = 2000) -> str:
    """Formatted context block for prompt injection. '' when nothing found."""
    try:
        hits = search_notes(query=query, limit=max_notes)
    except Exception as exc:
        logger.debug("vault search error: %s", exc)
        return ""
    if not hits:
        return ""
    parts = ["## Vault knowledge (from markdown notes)"]
    for h in hits:
        body = read_note(h["path"], max_chars=max_chars_per_note)
        if body:
            parts.append(f"### {h['name']} ({h['path']})\n{body}")
    return "\n\n".join(parts) if len(parts) > 1 else ""
