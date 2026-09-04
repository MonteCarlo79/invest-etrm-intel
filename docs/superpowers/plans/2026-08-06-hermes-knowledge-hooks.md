# Hermes Knowledge Read/Write Hooks — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect Hermes (and its bess_map/mengxi headless sub-agents) to the `knowledge/` Obsidian vault — read notes into prompts at query time, write briefings + insight notes back via OneDrive.

**Architecture:** Two new leaf modules in `services/knowledge_pool/`: `vault_reader.py` (OneDrive-direct reads with filename/frontmatter-aware matching) and `vault_writer.py` (markdown notes with YAML frontmatter uploaded via the existing `OneDriveClient`). A shared lazy `OneDriveClient` factory lives in `services/hermes/onedrive_client.py`. Injection points mirror the existing `retrieve_for_agent` / `_retrieve_kb_context` patterns. All knowledge I/O is fail-silent (returns `""`/`None`) so chat loops never break.

**Tech Stack:** Python 3.13, requests (OneDrive Graph API), psycopg2 (settings table), pytest, unittest.mock.

**Spec:** `docs/superpowers/specs/2026-08-06-hermes-knowledge-hooks-design.md`

## Global Constraints

- OneDrive path convention (proven in production by `thinking_agent.py:19`): root-relative paths are lowercase `etrm/bess-platform/...` — vault root constant is exactly `"etrm/bess-platform/knowledge"`.
- `OneDriveClient` signature (onedrive_client.py:26): `OneDriveClient(client_id, client_secret, refresh_token, tenant="consumers", on_token_rotated=None)`.
- `upload_file(folder_path, filename, content_bytes, conflict_behavior="replace")` — exact call pattern from thinking_agent.py:324-329.
- Read scope: `knowledge/spot_market/` + `knowledge/hermes/briefings/` ONLY. Never read `hermes/inbox/` (unreviewed content).
- Write targets: briefings → `knowledge/hermes/briefings/`; insights → `knowledge/hermes/inbox/`.
- Injection caps: max 3 notes × 2,000 chars per query.
- Every new public function returns empty/`None` on ANY exception (log at debug/warning) — knowledge I/O must never break a chat loop.
- No DB migrations. No Terraform changes. No changes to `agent.py`'s action enum/dispatch.
- Commits: after each task, explicit `git add <paths>` (never `git add -A`/`git add .`).

---

### Task 1: Shared OneDrive client factory

**Files:**
- Modify: `services/hermes/onedrive_client.py` (append at end of file)
- Test: `services/hermes/tests/test_onedrive_factory.py`

**Interfaces:**
- Produces: `get_shared_onedrive_client(pg_url: str = "") -> Optional[OneDriveClient]` — used by vault_reader (Task 3) and vault_writer (Task 2). Reads `ONEDRIVE_CLIENT_ID` / `ONEDRIVE_CLIENT_SECRET` from env; refresh token from `hermes_settings.onedrive_refresh_token` (DB) with `ONEDRIVE_REFRESH_TOKEN` env fallback; persists rotated tokens back to `hermes_settings`. Returns `None` when unconfigured or on any error. Module-level singleton under a lock.

- [ ] **Step 1: Write the failing test**

```python
import os
from unittest.mock import MagicMock, patch

import services.hermes.onedrive_client as odc


def _reset():
    odc._SHARED_CLIENT = None


def test_returns_none_when_unconfigured(monkeypatch):
    _reset()
    monkeypatch.delenv("ONEDRIVE_CLIENT_ID", raising=False)
    monkeypatch.delenv("ONEDRIVE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("ONEDRIVE_REFRESH_TOKEN", raising=False)
    monkeypatch.setattr(odc, "_load_setting", lambda pg_url, key: "")
    assert odc.get_shared_onedrive_client() is None


def test_builds_client_from_env(monkeypatch):
    _reset()
    monkeypatch.setenv("ONEDRIVE_CLIENT_ID", "cid")
    monkeypatch.setenv("ONEDRIVE_CLIENT_SECRET", "csec")
    monkeypatch.setenv("ONEDRIVE_REFRESH_TOKEN", "rt0")
    monkeypatch.setattr(odc, "_load_setting", lambda pg_url, key: "")
    client = odc.get_shared_onedrive_client()
    assert client is not None
    assert client.client_id == "cid"
    # singleton: second call returns same object
    assert odc.get_shared_onedrive_client() is client
    _reset()


def test_db_token_wins_over_env(monkeypatch):
    _reset()
    monkeypatch.setenv("ONEDRIVE_CLIENT_ID", "cid")
    monkeypatch.setenv("ONEDRIVE_CLIENT_SECRET", "csec")
    monkeypatch.setenv("ONEDRIVE_REFRESH_TOKEN", "rt-env")
    monkeypatch.setattr(odc, "_load_setting", lambda pg_url, key: "rt-db")
    client = odc.get_shared_onedrive_client(pg_url="postgres://x")
    assert client._refresh_token == "rt-db"
    _reset()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/hermes/tests/test_onedrive_factory.py -v`
Expected: FAIL — `AttributeError: module ... has no attribute 'get_shared_onedrive_client'`

- [ ] **Step 3: Implement**

Append to `services/hermes/onedrive_client.py`:

```python
# ── Shared client factory (used by knowledge_pool vault_reader/vault_writer) ──

import os

_SHARED_CLIENT: Optional["OneDriveClient"] = None
_SHARED_LOCK = Lock()


def _load_setting(pg_url: str, key: str) -> str:
    """Read a value from hermes_settings. Returns '' on any error."""
    url = pg_url or os.environ.get("PGURL", "")
    if not url:
        return ""
    try:
        import psycopg2
        with psycopg2.connect(url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM hermes_settings WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else ""
    except Exception as exc:
        logger.debug("hermes_settings read failed (%s): %s", key, exc)
        return ""


def _save_setting(pg_url: str, key: str, value: str) -> None:
    url = pg_url or os.environ.get("PGURL", "")
    if not url:
        return
    try:
        import psycopg2
        with psycopg2.connect(url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO hermes_settings (key, value, updated_at) VALUES (%s, %s, NOW())
                       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()""",
                    (key, value),
                )
            conn.commit()
    except Exception as exc:
        logger.warning("hermes_settings write failed (%s): %s", key, exc)


def get_shared_onedrive_client(pg_url: str = "") -> Optional["OneDriveClient"]:
    """Lazily build a process-wide OneDriveClient from env + hermes_settings.

    Returns None if OneDrive is not configured (or on any error) — callers
    must treat knowledge I/O as optional.
    """
    global _SHARED_CLIENT
    with _SHARED_LOCK:
        if _SHARED_CLIENT is not None:
            return _SHARED_CLIENT
        client_id = os.environ.get("ONEDRIVE_CLIENT_ID", "")
        client_secret = os.environ.get("ONEDRIVE_CLIENT_SECRET", "")
        if not client_id or not client_secret:
            return None
        refresh_token = _load_setting(pg_url, "onedrive_refresh_token") or os.environ.get(
            "ONEDRIVE_REFRESH_TOKEN", ""
        )
        if not refresh_token:
            return None

        def _rotated(new_token: str) -> None:
            _save_setting(pg_url, "onedrive_refresh_token", new_token)

        try:
            _SHARED_CLIENT = OneDriveClient(
                client_id=client_id,
                client_secret=client_secret,
                refresh_token=refresh_token,
                on_token_rotated=_rotated,
            )
        except Exception as exc:
            logger.warning("Shared OneDriveClient init failed: %s", exc)
            return None
        return _SHARED_CLIENT
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/hermes/tests/test_onedrive_factory.py -v`
Expected: 3 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/hermes/onedrive_client.py services/hermes/tests/test_onedrive_factory.py
git commit -m "Add shared OneDrive client factory for knowledge vault access"
```

---

### Task 2: `vault_writer.py` — write briefing + insight notes

**Files:**
- Create: `services/knowledge_pool/vault_writer.py`
- Test: `services/knowledge_pool/tests/test_vault_writer.py`

**Interfaces:**
- Consumes: `get_shared_onedrive_client(pg_url="") -> Optional[OneDriveClient]` (Task 1)
- Produces:
  - `write_briefing_note(kind: str, content: str, note_date: str = "") -> Optional[str]` — kind `"morning"`/`"daily_report"`; returns vault-relative path written, or `None` on failure
  - `write_insight_note(category: str, content: str, source_app: str, province: str = "", confidence: str = "medium") -> Optional[str]]`
  - Both upload under vault root `"etrm/bess-platform/knowledge"`; briefings → `hermes/briefings/YYYY-MM-DD-<kind>.md`; insights → `hermes/inbox/YYYY-MM-DD-<slug>.md`

- [ ] **Step 1: Write the failing test**

```python
from services.knowledge_pool import vault_writer


class FakeOneDrive:
    def __init__(self):
        self.uploads = []

    def upload_file(self, folder_path, filename, content, conflict_behavior="replace"):
        self.uploads.append((folder_path, filename, content.decode("utf-8")))


def test_briefing_note_path_and_frontmatter(monkeypatch):
    fake = FakeOneDrive()
    monkeypatch.setattr(vault_writer, "_client", lambda: fake)
    path = vault_writer.write_briefing_note("morning", "# 早报\n今天多云", note_date="2026-08-06")
    assert path == "hermes/briefings/2026-08-06-morning.md"
    folder, filename, text = fake.uploads[0]
    assert folder == "etrm/bess-platform/knowledge/hermes/briefings"
    assert filename == "2026-08-06-morning.md"
    assert "note_type: briefing" in text
    assert "kind: morning" in text
    assert "date: 2026-08-06" in text
    assert "source: hermes" in text
    assert "# 早报" in text


def test_insight_note_goes_to_inbox_with_pending_review(monkeypatch):
    fake = FakeOneDrive()
    monkeypatch.setattr(vault_writer, "_client", lambda: fake)
    path = vault_writer.write_insight_note(
        category="market_view",
        content="山东现货午后低价与光伏出力强相关。",
        source_app="bess_map",
        province="山东",
        confidence="high",
    )
    folder, filename, text = fake.uploads[0]
    assert folder == "etrm/bess-platform/knowledge/hermes/inbox"
    assert filename.startswith("2026-") and filename.endswith(".md")
    assert "山东" in filename
    assert "note_type: insight" in text
    assert "review_status: pending" in text
    assert "category: market_view" in text
    assert "confidence: high" in text
    assert path is not None


def test_write_returns_none_when_no_client(monkeypatch):
    monkeypatch.setattr(vault_writer, "_client", lambda: None)
    assert vault_writer.write_briefing_note("morning", "x") is None
    assert vault_writer.write_insight_note("t", "c", "app") is None


def test_write_returns_none_on_upload_error(monkeypatch):
    class Boom:
        def upload_file(self, *a, **k):
            raise RuntimeError("network down")
    monkeypatch.setattr(vault_writer, "_client", lambda: Boom())
    assert vault_writer.write_briefing_note("morning", "x") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/knowledge_pool/tests/test_vault_writer.py -v`
Expected: FAIL — `ModuleNotFoundError: services.knowledge_pool.vault_writer`

- [ ] **Step 3: Implement**

Create `services/knowledge_pool/vault_writer.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/knowledge_pool/tests/test_vault_writer.py -v`
Expected: 4 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/knowledge_pool/vault_writer.py services/knowledge_pool/tests/test_vault_writer.py
git commit -m "Add vault_writer: OneDrive briefing + insight note uploads"
```

---

### Task 3: `vault_reader.py` — read/inject vault knowledge

**Files:**
- Create: `services/knowledge_pool/vault_reader.py`
- Test: `services/knowledge_pool/tests/test_vault_reader.py`

**Interfaces:**
- Consumes: `get_shared_onedrive_client()` (Task 1)
- Produces:
  - `search_notes(query: str = "", province: str = "", date: str = "", limit: int = 3) -> list[dict]` — each dict `{path, name, area}`; path is vault-root-relative (e.g. `spot_market/01_daily_reports/2026-08-05.md`)
  - `read_note(path: str, max_chars: int = 2000) -> str`
  - `retrieve_vault_context(query: str, max_notes: int = 3, max_chars_per_note: int = 2000) -> str` — formatted `## Vault knowledge (from markdown notes)` block or `""`
- Area map (read scope, per spec): `daily → spot_market/01_daily_reports`, `provinces → spot_market/02_provinces`, `concepts → spot_market/03_concepts`, `briefings → hermes/briefings`

- [ ] **Step 1: Write the failing test**

```python
from services.knowledge_pool import vault_reader


class FakeOneDrive:
    def __init__(self, listing=None, files=None, search_results=None):
        self._listing = listing or {}
        self._files = files or {}
        self._search = search_results or []

    def list_items(self, folder_path="/"):
        return self._listing.get(folder_path, [])

    def read_file_by_path(self, file_path):
        return self._files[file_path].encode("utf-8")

    def search(self, query):
        return self._search


def _fake_client():
    listing = {
        "etrm/bess-platform/knowledge/spot_market/01_daily_reports": [
            {"name": "2026-08-04.md"}, {"name": "2026-08-05.md"},
        ],
        "etrm/bess-platform/knowledge/spot_market/02_provinces": [
            {"name": "山东.md"}, {"name": "山西.md"},
        ],
        "etrm/bess-platform/knowledge/spot_market/03_concepts": [
            {"name": "新能源出力下降.md"}, {"name": "检修.md"},
        ],
        "etrm/bess-platform/knowledge/hermes/briefings": [
            {"name": "2026-08-05-morning.md"},
        ],
    }
    files = {
        "etrm/bess-platform/knowledge/spot_market/01_daily_reports/2026-08-05.md": "8月5日 山东均价0.32",
        "etrm/bess-platform/knowledge/spot_market/02_provinces/山东.md": "# 山东\n光伏大省",
        "etrm/bess-platform/knowledge/spot_market/03_concepts/新能源出力下降.md": "# 新能源出力下降",
    }
    return FakeOneDrive(listing, files)


def test_date_mention_finds_daily_note(monkeypatch):
    monkeypatch.setattr(vault_reader, "_client", lambda: _fake_client())
    hits = vault_reader.search_notes(query="8月5日价格如何 2026-08-05")
    assert hits[0]["path"] == "spot_market/01_daily_reports/2026-08-05.md"
    assert hits[0]["area"] == "daily"


def test_province_name_finds_province_note(monkeypatch):
    monkeypatch.setattr(vault_reader, "_client", lambda: _fake_client())
    hits = vault_reader.search_notes(query="山东的市场结构")
    assert any(h["path"] == "spot_market/02_provinces/山东.md" for h in hits)


def test_read_note_truncates(monkeypatch):
    monkeypatch.setattr(vault_reader, "_client", lambda: _fake_client())
    text = vault_reader.read_note("spot_market/02_provinces/山东.md", max_chars=5)
    assert text.startswith("# 山东")
    assert "[…truncated]" in text


def test_retrieve_context_formats_block(monkeypatch):
    monkeypatch.setattr(vault_reader, "_client", lambda: _fake_client())
    block = vault_reader.retrieve_vault_context("山东 2026-08-05 价格")
    assert "## Vault knowledge (from markdown notes)" in block
    assert "山东均价0.32" in block
    assert "光伏大省" in block


def test_empty_when_unconfigured(monkeypatch):
    monkeypatch.setattr(vault_reader, "_client", lambda: None)
    assert vault_reader.search_notes(query="x") == []
    assert vault_reader.retrieve_vault_context("x") == ""


def test_search_falls_back_to_onedrive_search(monkeypatch):
    fake = _fake_client()
    fake._search = [{
        "name": "2026-08-01.md",
        "parentReference": {"path": "/drive/root:/etrm/bess-platform/knowledge/spot_market/01_daily_reports"},
    }]
    monkeypatch.setattr(vault_reader, "_client", lambda: fake)
    hits = vault_reader.search_notes(query="完全无关的查询词zzz")
    assert hits[0]["area"] == "daily"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/knowledge_pool/tests/test_vault_reader.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement**

Create `services/knowledge_pool/vault_reader.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/knowledge_pool/tests/test_vault_reader.py -v`
Expected: 6 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/knowledge_pool/vault_reader.py services/knowledge_pool/tests/test_vault_reader.py
git commit -m "Add vault_reader: OneDrive-direct vault search + context injection"
```

---

### Task 4: Mirror expert insights to vault inbox

**Files:**
- Modify: `services/knowledge_pool/expert_memory.py:305-361` (`extract_spot_insights` — add `source_app` param + mirror block)
- Modify: `services/bess_map/headless_agent.py:464` (call site — pass `source_app="bess_map"`)
- Modify: `services/mengxi_trading/headless_agent.py:288` (call site — pass `source_app="mengxi_trader"`)
- Test: `services/knowledge_pool/tests/test_expert_memory_mirror.py`

**Interfaces:**
- Consumes: `vault_writer.write_insight_note(category, content, source_app, province, confidence)` (Task 2)
- Produces: `extract_spot_insights(user_msg, agent_reply, api_key, source_app="spot_market")` — new optional param (bridge call site at market_agent_bridge.py:294 keeps the default); side effect = one inbox note per stored insight

- [ ] **Step 1: Write the failing test**

```python
from unittest.mock import MagicMock, patch

import services.knowledge_pool.expert_memory as em


def test_stored_insights_mirrored_to_vault(monkeypatch):
    # fake LLM extraction returning one insight
    fake_resp = MagicMock()
    fake_resp.content = [MagicMock(text='{"insights": [{"insight": "山东午后光伏压价", "type": "market_view", "province": "山东", "confidence": "high"}]}')]
    fake_client = MagicMock()
    fake_client.messages.create.return_value = fake_resp
    monkeypatch.setattr(em, "_make_anthropic_client", lambda api_key: fake_client)

    # fake DB connection
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.__enter__.return_value = conn
    monkeypatch.setattr(em, "get_conn", lambda: conn)

    calls = []
    import services.knowledge_pool.vault_writer as vw
    monkeypatch.setattr(vw, "write_insight_note", lambda **kw: calls.append(kw) or "ok")

    stored = em.extract_spot_insights(user_msg="q", agent_reply="a", api_key="k")
    assert stored == 1
    assert calls == [{
        "category": "market_view",
        "content": "山东午后光伏压价",
        "source_app": "spot_market",
        "province": "山东",
        "confidence": "high",
    }]


def test_source_app_param_flows_to_note(monkeypatch):
    fake_resp = MagicMock()
    fake_resp.content = [MagicMock(text='{"insights": [{"insight": "蒙西调度偏差大", "type": "ops_note", "confidence": "medium"}]}')]
    fake_client = MagicMock()
    fake_client.messages.create.return_value = fake_resp
    monkeypatch.setattr(em, "_make_anthropic_client", lambda api_key: fake_client)
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.__enter__.return_value = conn
    monkeypatch.setattr(em, "get_conn", lambda: conn)
    calls = []
    import services.knowledge_pool.vault_writer as vw
    monkeypatch.setattr(vw, "write_insight_note", lambda **kw: calls.append(kw) or "ok")

    em.extract_spot_insights(user_msg="q", agent_reply="a", api_key="k", source_app="mengxi_trader")
    assert calls[0]["source_app"] == "mengxi_trader"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/knowledge_pool/tests/test_expert_memory_mirror.py -v`
Expected: FAIL — `calls == []` (mirror not invoked); second test FAILs on unexpected kwarg

- [ ] **Step 3: Implement**

In `services/knowledge_pool/expert_memory.py`, change the signature (line 305) to:

```python
def extract_spot_insights(user_msg: str, agent_reply: str, api_key: str, source_app: str = "spot_market") -> int:
```

and replace the `stored` loop + commit block (currently lines 336-361) with:

```python
    stored = 0
    stored_items: list[dict] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            for item in insights:
                try:
                    cur.execute(
                        """
                        INSERT INTO staging.kp_expert_insights
                            (insight_text, insight_type, province, confidence,
                             source_session, validated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            item.get("insight", "")[:1000],
                            item.get("type", "other"),
                            item.get("province") or None,
                            item.get("confidence", "medium"),
                            date.today().isoformat(),
                        ),
                    )
                    stored += 1
                    stored_items.append(item)
                except Exception as exc:
                    logger.debug("Failed to store insight: %s", exc)
        conn.commit()

    # Mirror to vault inbox (Stage 4 review loop) — never raises
    if stored_items:
        try:
            from services.knowledge_pool import vault_writer
            for item in stored_items:
                vault_writer.write_insight_note(
                    category=item.get("type", "other"),
                    content=item.get("insight", "")[:1000],
                    source_app=source_app,
                    province=item.get("province") or "",
                    confidence=item.get("confidence", "medium"),
                )
        except Exception as exc:
            logger.debug("Vault insight mirror failed: %s", exc)

    return stored
```

In `services/bess_map/headless_agent.py:464`, change the call to:

```python
                extract_spot_insights(user_msg=question, agent_reply=answer, api_key=api_key, source_app="bess_map")
```

In `services/mengxi_trading/headless_agent.py:288`, change the call to:

```python
                extract_spot_insights(user_msg=question, agent_reply=answer, api_key=api_key, source_app="mengxi_trader")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/knowledge_pool/tests/test_expert_memory_mirror.py -v`
Expected: 2 PASSED. Also run the full existing knowledge_pool suite: `~/.venvs/bess-platform/bin/python -m pytest services/knowledge_pool/tests/ -q` — no regressions.

- [ ] **Step 5: Commit**

```bash
git add services/knowledge_pool/expert_memory.py services/knowledge_pool/tests/test_expert_memory_mirror.py services/bess_map/headless_agent.py services/mengxi_trading/headless_agent.py
git commit -m "Mirror extracted expert insights to vault inbox as markdown notes"
```

---

### Task 5: Inject vault context into bess_map + mengxi headless agents

**Files:**
- Modify: `services/bess_map/headless_agent.py:434-441` (after the `retrieve_for_agent` append)
- Modify: `services/mengxi_trading/headless_agent.py:262-271` (after the expert-memory append)
- Test: `services/knowledge_pool/tests/test_headless_vault_injection.py`

**Interfaces:**
- Consumes: `vault_reader.retrieve_vault_context(query) -> str` (Task 3)
- Produces: no signature changes; `system` prompt gains a vault block when notes match

- [ ] **Step 1: Write the failing test**

```python
from unittest.mock import MagicMock, patch

import services.knowledge_pool.vault_reader as vr


def _fake_llm_client():
    resp = MagicMock()
    resp.stop_reason = "end_turn"
    block = MagicMock()
    block.text = "答案"
    resp.content = [block]
    client = MagicMock()
    client.messages.create.return_value = resp
    return client


def test_bess_map_system_contains_vault_block(monkeypatch):
    import services.bess_map.headless_agent as ba
    monkeypatch.setattr(vr, "retrieve_vault_context", lambda q: "## Vault knowledge\nMARKER_BESS")
    monkeypatch.setattr(ba, "_make_engine", lambda pg_url="": MagicMock(dispose=lambda: None))
    monkeypatch.setattr("shared.anthropic_client.make_client", lambda api_key: _fake_llm_client())
    client_holder = {}

    import shared.anthropic_client as ac
    fake = _fake_llm_client()
    monkeypatch.setattr(ac, "make_client", lambda api_key: fake)
    ba.run_bess_map_query("山东价格", api_key="k", pg_url="")
    system = fake.messages.create.call_args.kwargs["system"]
    assert "MARKER_BESS" in system


def test_mengxi_system_contains_vault_block(monkeypatch):
    import services.mengxi_trading.headless_agent as ma
    monkeypatch.setattr(vr, "retrieve_vault_context", lambda q: "## Vault knowledge\nMARKER_MX")
    monkeypatch.setattr(ma, "_make_engine", lambda pg_url="": MagicMock())
    import shared.anthropic_client as ac
    fake = _fake_llm_client()
    monkeypatch.setattr(ac, "make_client", lambda api_key: fake)
    ma.run_mengxi_query("蒙西收益", api_key="k", pg_url="")
    system = fake.messages.create.call_args.kwargs["system"]
    assert "MARKER_MX" in system
```

Note: if the headless modules' import-time DB/engine wiring makes direct import impractical in tests, patch at the module attribute level as above; the key assertion is that the vault block reaches the `system` kwarg.

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/knowledge_pool/tests/test_headless_vault_injection.py -v`
Expected: FAIL — markers absent from system prompt

- [ ] **Step 3: Implement**

In `services/bess_map/headless_agent.py`, immediately after the existing block (lines 434-441):

```python
    try:
        from services.knowledge_pool.advanced_retrieval import retrieve_for_agent
        kb_ctx = retrieve_for_agent(query=question, api_key=api_key, app="strategist", top_k=4)
        if kb_ctx and "No relevant" not in kb_ctx:
            system += f"\n\nKNOWLEDGE BASE CONTEXT:\n{kb_ctx}"
    except Exception:
        pass
```

append:

```python
    try:
        from services.knowledge_pool import vault_reader
        vault_ctx = vault_reader.retrieve_vault_context(question)
        if vault_ctx:
            system += f"\n\n{vault_ctx}"
    except Exception:
        pass
```

In `services/mengxi_trading/headless_agent.py`, immediately after the expert-memory block (lines 264-271), append the identical vault block.

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/knowledge_pool/tests/test_headless_vault_injection.py -v`
Expected: 2 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/bess_map/headless_agent.py services/mengxi_trading/headless_agent.py services/knowledge_pool/tests/test_headless_vault_injection.py
git commit -m "Inject vault knowledge context into bess_map and mengxi headless agents"
```

---

### Task 6: Inject vault context into Hermes main agent

**Files:**
- Modify: `services/hermes/agent.py:470-481` (the `kb_ctx` injection in `process()`)

**Interfaces:**
- Consumes: `vault_reader.retrieve_vault_context(query) -> str` (Task 3)
- Produces: no signature changes

The `process()` method drives the full Azure→DeepSeek→Claude chain and is impractical to unit-test in isolation; verification is compile + the Task 9 smoke test.

- [ ] **Step 1: Implement**

Replace (agent.py:470-481):

```python
        # Inject KB context when available
        kb_ctx = self._retrieve_kb_context(msg.text)
        if kb_ctx:
```

with:

```python
        # Inject KB context when available
        kb_ctx = self._retrieve_kb_context(msg.text)
        try:
            from services.knowledge_pool import vault_reader
            vault_ctx = vault_reader.retrieve_vault_context(msg.text)
            if vault_ctx:
                kb_ctx = (kb_ctx + "\n\n" + vault_ctx) if kb_ctx else vault_ctx
        except Exception:
            pass
        if kb_ctx:
```

- [ ] **Step 2: Verify compile**

Run: `~/.venvs/bess-platform/bin/python -m py_compile services/hermes/agent.py && echo COMPILE_OK`
Expected: COMPILE_OK

- [ ] **Step 3: Commit**

```bash
git add services/hermes/agent.py
git commit -m "Inject vault knowledge context into Hermes main agent"
```

---

### Task 7: Persist morning briefing as vault note

**Files:**
- Modify: `services/hermes/scheduler.py:288-300` (after the Feishu card send in `send_morning_briefing`)
- Test: `services/hermes/tests/test_briefing_note.py`

**Interfaces:**
- Consumes: `vault_writer.write_briefing_note(kind, content, note_date="")` (Task 2)
- Produces: `_card_to_markdown(card: dict) -> str` — local helper in scheduler.py extracting text from a Feishu card (`header.title.content` + each element's `text.content`/`content` strings, `hr` → `---`)

- [ ] **Step 1: Write the failing test**

```python
from unittest.mock import MagicMock

import services.hermes.scheduler as sched
import services.knowledge_pool.vault_writer as vw


def test_morning_briefing_writes_vault_note(monkeypatch):
    card_holder = {}
    feishu = MagicMock()
    feishu.send_card = lambda open_id, card: card_holder.update(card=card)
    tasks = MagicMock()
    tasks.list_open_cards.return_value = []
    monkeypatch.setattr(sched, "_retry_list_open_cards", lambda t: [])
    monkeypatch.setattr(sched, "_get_shanghai_weather", lambda: "多云 26°C")

    writes = []
    monkeypatch.setattr(vw, "write_briefing_note", lambda kind, content, note_date="": writes.append((kind, content)) or "ok")

    from datetime import datetime, timezone, timedelta
    now = datetime(2026, 8, 6, 7, 30, tzinfo=timezone(timedelta(hours=8)))
    sched.send_morning_briefing(tasks, feishu=feishu, feishu_owner_open_id="ou_x", now=now)

    assert writes and writes[0][0] == "morning"
    assert "每日提醒" in writes[0][1]


def test_card_to_markdown_extracts_text():
    card = {
        "header": {"title": {"content": "📋 每日提醒 — 2026-08-06"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": "🌤 多云"}},
            {"tag": "hr"},
            {"tag": "markdown", "content": "有什么需要帮忙的吗？"},
        ],
    }
    md = sched._card_to_markdown(card)
    assert "📋 每日提醒 — 2026-08-06" in md
    assert "🌤 多云" in md
    assert "---" in md
    assert "有什么需要帮忙的吗？" in md
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/hermes/tests/test_briefing_note.py -v`
Expected: FAIL — `writes` empty / no `_card_to_markdown`

- [ ] **Step 3: Implement**

In `services/hermes/scheduler.py`, add the helper (module level, near `build_task_card`):

```python
def _card_to_markdown(card: dict) -> str:
    """Flatten a Feishu card into plain markdown for vault persistence."""
    parts = []
    title = card.get("header", {}).get("title", {}).get("content", "")
    if title:
        parts.append(f"# {title}")
    for el in card.get("elements", []):
        if el.get("tag") == "hr":
            parts.append("---")
            continue
        text = el.get("text", {}).get("content") or el.get("content") or ""
        if text:
            parts.append(text)
    return "\n\n".join(parts)
```

In `send_morning_briefing`, replace the send block (lines 288-291):

```python
    try:
        feishu.send_card(open_id=feishu_owner_open_id, card=card)
    except Exception as exc:
```

with:

```python
    try:
        feishu.send_card(open_id=feishu_owner_open_id, card=card)
        try:
            from services.knowledge_pool import vault_writer
            vault_writer.write_briefing_note("morning", _card_to_markdown(card))
        except Exception as exc2:
            logger.debug("Briefing vault note failed: %s", exc2)
    except Exception as exc:
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/hermes/tests/test_briefing_note.py -v`
Expected: 2 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/hermes/scheduler.py services/hermes/tests/test_briefing_note.py
git commit -m "Persist morning briefing to vault as markdown note"
```

---

### Task 8: Persist daily market report as vault note

**Files:**
- Modify: `services/hermes/market_report.py:1066-1067` (after `feishu.send_file(...)` in `send_daily_report`)
- Test: `services/hermes/tests/test_daily_report_note.py`

**Interfaces:**
- Consumes: `vault_writer.write_briefing_note(kind, content, note_date="")` (Task 2)
- Produces: `_report_to_markdown(report: dict, period_str: str) -> str` — local helper; report schema (market_report.py:300-334): `{executive_summary: str, sections: [{title, content, items: [{title, content, source, date}]}]}`

- [ ] **Step 1: Write the failing test**

```python
import services.hermes.market_report as mr


def test_report_to_markdown_renders_sections():
    report = {
        "executive_summary": "今日市场震荡。",
        "sections": [
            {
                "title": "山东现货",
                "content": "午后低价频现。",
                "items": [{"title": "光伏出力新高", "content": "14时出力达32GW", "source": "山东省调", "date": "2026-08-05"}],
            },
            {"title": "山西现货", "content": "价格平稳。"},
        ],
    }
    md = mr._report_to_markdown(report, "2026年08月06日")
    assert md.startswith("# 电力市场日报 — 2026年08月06日")
    assert "今日市场震荡。" in md
    assert "## 山东现货" in md
    assert "午后低价频现。" in md
    assert "- **光伏出力新高**（山东省调, 2026-08-05）：14时出力达32GW" in md
    assert "## 山西现货" in md
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/hermes/tests/test_daily_report_note.py -v`
Expected: FAIL — `AttributeError: no attribute '_report_to_markdown'`

- [ ] **Step 3: Implement**

In `services/hermes/market_report.py`, add module-level helper:

```python
def _report_to_markdown(report: dict, period_str: str) -> str:
    """Render the structured daily-report dict as a markdown vault note body."""
    lines = [f"# 电力市场日报 — {period_str}", "", report.get("executive_summary", "")]
    for section in report.get("sections", []):
        lines += ["", f"## {section.get('title', '')}", "", section.get("content", "")]
        for item in section.get("items", []) or []:
            src = f"（{item.get('source')}, {item.get('date')}）" if item.get("source") else ""
            lines.append(f"- **{item.get('title', '')}**{src}：{item.get('content', '')}")
    return "\n".join(lines).strip() + "\n"
```

In `send_daily_report`, replace (lines 1066-1067):

```python
        feishu.send_file(owner_open_id, file_key)
        logger.info("Daily report sent: %s (%d bytes)", filename, len(pdf_bytes))
```

with:

```python
        feishu.send_file(owner_open_id, file_key)
        logger.info("Daily report sent: %s (%d bytes)", filename, len(pdf_bytes))
        try:
            from services.knowledge_pool import vault_writer
            vault_writer.write_briefing_note(
                "daily_report", _report_to_markdown(report, period_str)
            )
        except Exception as exc2:
            logger.debug("Daily report vault note failed: %s", exc2)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/hermes/tests/test_daily_report_note.py -v`
Expected: 1 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/hermes/market_report.py services/hermes/tests/test_daily_report_note.py
git commit -m "Persist daily market report to vault as markdown note"
```

---

### Task 9: Full suite + live smoke test

**Files:**
- Create: `scripts/smoke_vault_hooks.py`

**Interfaces:**
- Consumes: everything above (live OneDrive)

- [ ] **Step 1: Run the full test suites for touched packages**

Run:
```bash
~/.venvs/bess-platform/bin/python -m pytest services/knowledge_pool/tests/ services/hermes/tests/ -q
```
Expected: all PASSED (including pre-existing tests — no regressions)

- [ ] **Step 2: Write the smoke script**

Create `scripts/smoke_vault_hooks.py`:

```python
"""Live smoke test for vault hooks. Run: python scripts/smoke_vault_hooks.py
Requires config/.env (PGURL, ONEDRIVE_*). Writes one scratch note to the vault
inbox and reads back one existing note."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / "config" / ".env")

from services.knowledge_pool import vault_reader, vault_writer

print("[1] read vault index …")
idx = vault_reader.read_note("spot_market/04_indices/index.md", max_chars=400)
print("    OK" if idx else "    FAIL — empty")

print("[2] search 山东 …")
hits = vault_reader.search_notes(query="山东现货市场")
print("   ", [h["path"] for h in hits] or "FAIL — no hits")

print("[3] write scratch note …")
path = vault_writer.write_insight_note(
    category="smoke_test", content="vault hooks smoke test — safe to delete",
    source_app="manual", confidence="low",
)
print("   ", path or "FAIL — write returned None")

print("[4] read scratch back …")
if path:
    back = vault_reader._client().read_file_by_path(
        f"{vault_writer.VAULT_ROOT}/{path}").decode("utf-8")
    print("    OK" if "smoke test" in back else "    FAIL — content mismatch")
print("DONE")
```

Note: read-back uses the private `_client()` because `read_note` deliberately refuses `inbox/` paths (read-scope rule) — the smoke test verifies the write landed via the raw client instead.

- [ ] **Step 3: Run it live**

Run: `cd /Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform && ~/.venvs/bess-platform/bin/python scripts/smoke_vault_hooks.py`
Expected: `[1] OK`, `[2]` non-empty paths, `[3]` a path, `[4] OK`. Then verify in OneDrive/Obsidian that `knowledge/hermes/inbox/<date>-vault-hooks-smoke-test….md` exists; delete it from OneDrive web.

- [ ] **Step 4: Commit**

```bash
git add scripts/smoke_vault_hooks.py
git commit -m "Add live smoke test for vault read/write hooks"
```

---

## Self-Review Notes

- **Spec coverage:** read path (Tasks 3, 5, 6) ✓; write briefings (7, 8) ✓; write insights (4) ✓; OneDrive-direct (1) ✓; inbox exclusion enforced in `read_note` ✓; caps ✓; fail-silent ✓; no enum/dispatch changes ✓.
- **Deploy is NOT in this plan** — Hermes image rebuild + ECS redeploy requires separate explicit confirmation (CLAUDE.md deploy protocol).
- `agent.py` (Task 6) is compile-verified + smoke-covered, not unit-tested (full LLM chain impractical to mock) — accepted trade-off, stated openly.
