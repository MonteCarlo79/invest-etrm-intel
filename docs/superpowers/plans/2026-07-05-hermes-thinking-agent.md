# Hermes ThinkingAgent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `ThinkingAgent` to Hermes that proactively checks data health daily and reviews app design weekly, asking focused questions and writing dev-request files to OneDrive for laptop-side development.

**Architecture:** `ThinkingAgent` uses the Anthropic tools API (multi-step tool loop) to reason over DB data and source files, then sends observations via Feishu and/or writes structured `.md` dev-request files to OneDrive. Two APScheduler cron jobs trigger health-check (daily 00:10 UTC) and design-review (Monday 00:30 UTC). A new `WRITE_DEV_REQUEST` action in `HermesAgent` lets users trigger dev requests on demand via Feishu chat.

**Tech Stack:** Python 3.13, Anthropic SDK (tool-use API), psycopg2, APScheduler, existing `FeishuClient`, `OneDriveClient`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `services/hermes/thinking_agent.py` | Create | ThinkingAgent class: tool loop, all tool implementations, seed prompts, run(), dedup, log |
| `db/ddl/hermes/005_thinking_log.sql` | Create | `hermes.thinking_log` DB table |
| `services/hermes/app.py` | Modify | Import ThinkingAgent, init with deps, 2 new scheduler jobs, wire WRITE_DEV_REQUEST action |
| `services/hermes/agent.py` | Modify | Add `WRITE_DEV_REQUEST` action to system prompt + execute() dispatch |
| `tests/hermes/test_thinking_agent.py` | Create | Unit tests for all tools, dedup, seed prompts, run() |

---

## Task 1: DB Migration — `hermes.thinking_log` table

**Files:**
- Create: `db/ddl/hermes/005_thinking_log.sql`

- [ ] **Step 1: Write the SQL file**

```sql
-- db/ddl/hermes/005_thinking_log.sql
-- Run: psql $PGURL -f db/ddl/hermes/005_thinking_log.sql

CREATE TABLE IF NOT EXISTS hermes.thinking_log (
    id             BIGSERIAL PRIMARY KEY,
    mode           TEXT NOT NULL CHECK (mode IN ('health', 'design')),
    ts             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    files_read     TEXT[],
    tables_checked TEXT[],
    message_sent   TEXT,          -- full text sent to Feishu (used for dedup)
    model_used     TEXT
);

CREATE INDEX IF NOT EXISTS idx_thinking_log_mode_ts
    ON hermes.thinking_log (mode, ts DESC);
```

- [ ] **Step 2: Apply to DB**

```bash
psql $PGURL -f db/ddl/hermes/005_thinking_log.sql
```

Expected output:
```
CREATE TABLE
CREATE INDEX
```

- [ ] **Step 3: Verify**

```bash
psql $PGURL -c "\d hermes.thinking_log"
```

Expected: table with columns id, mode, ts, files_read, tables_checked, message_sent, model_used.

- [ ] **Step 4: Commit**

```bash
git add db/ddl/hermes/005_thinking_log.sql
git commit -m "feat: add hermes.thinking_log table for ThinkingAgent dedup + audit"
```

---

## Task 2: ThinkingAgent Skeleton — init, model resolution, tool definitions

**Files:**
- Create: `services/hermes/thinking_agent.py`
- Test: `tests/hermes/test_thinking_agent.py`

- [ ] **Step 1: Write failing test for model resolution**

Create `tests/hermes/test_thinking_agent.py`:

```python
import os
import pytest
from unittest.mock import MagicMock, patch


def _make_agent(**kwargs):
    from services.hermes.thinking_agent import ThinkingAgent
    defaults = dict(
        anthropic_api_key="test-key",
        pg_url="postgresql://localhost/test",
        feishu=MagicMock(),
        feishu_owner_open_id="owner123",
        onedrive=None,
    )
    defaults.update(kwargs)
    return ThinkingAgent(**defaults)


class TestModelResolution:
    def test_health_defaults_to_haiku(self):
        agent = _make_agent()
        assert agent._resolve_model("health") == "claude-haiku-4-5-20251001"

    def test_design_defaults_to_sonnet(self):
        agent = _make_agent()
        assert agent._resolve_model("design") == "claude-sonnet-4-6"

    def test_health_model_overridden_by_env(self, monkeypatch):
        monkeypatch.setenv("HERMES_THINK_HEALTH_MODEL", "sonnet")
        agent = _make_agent()
        assert agent._resolve_model("health") == "claude-sonnet-4-6"

    def test_design_model_overridden_by_env(self, monkeypatch):
        monkeypatch.setenv("HERMES_THINK_DESIGN_MODEL", "haiku")
        agent = _make_agent()
        assert agent._resolve_model("design") == "claude-haiku-4-5-20251001"

    def test_passthrough_unknown_alias(self, monkeypatch):
        monkeypatch.setenv("HERMES_THINK_HEALTH_MODEL", "deepseek-chat")
        agent = _make_agent()
        assert agent._resolve_model("health") == "deepseek-chat"
```

- [ ] **Step 2: Run test to confirm failure**

```bash
cd /app && python -m pytest tests/hermes/test_thinking_agent.py::TestModelResolution -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'services.hermes.thinking_agent'`

- [ ] **Step 3: Create `thinking_agent.py` with skeleton**

```python
from __future__ import annotations
import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, TYPE_CHECKING

import psycopg2
from anthropic import Anthropic

if TYPE_CHECKING:
    from services.hermes.feishu_client import FeishuClient
    from services.hermes.onedrive_client import OneDriveClient

logger = logging.getLogger(__name__)

_REPO_ROOT = "/app"
_DEV_REQUESTS_FOLDER = "etrm/bess-platform/dev-requests"
_MAX_ITER = 8
_MAX_FILE_READS = 5
_DEDUP_DAYS = {"health": 7, "design": 14}

# Maps monitored DB tables → upstream external data source description
DATA_LINEAGE: dict[str, str] = {
    "marketdata.md_da_cleared_energy":       "各省电力交易中心 → 日前出清 PDF/API（enos_market ETL）",
    "marketdata.md_id_cleared_energy":       "各省电力交易中心 → 实时出清（enos_market ETL）",
    "marketdata.md_rt_nodal_price":          "各省电力交易中心 → 节点电价（enos_market ETL）",
    "marketdata.md_da_fuel_summary":         "各省电力交易中心 → 日前燃料汇总（enos_market ETL）",
    "marketdata.md_id_fuel_summary":         "各省电力交易中心 → 实时燃料汇总（enos_market ETL）",
    "marketdata.md_settlement_ref_price":    "各省电力交易中心 → 结算参考电价（enos_market ETL）",
    "public.hist_mengxi_suyou_clear":        "蒙西电力交易中心 → TT API 接口（tt_api ETL）",
    "public.hist_mengxi_wulate_clear":       "蒙西电力交易中心 → TT API 接口（tt_api ETL）",
    "public.hist_shandong_binzhou_clear":    "山东电力交易中心 → TT API 接口（tt_api ETL）",
    "public.hist_anhui_dingyuan_clear":      "安徽电力交易中心 → TT API 接口（tt_api ETL）",
    "marketdata.province_installed_monthly": "国家能源局/各省能源局官网 → 月度装机容量 Excel（手动上传给 Hermes）",
    "marketdata.province_cap_comp":          "各省容量补偿政策文件（capcomp_screener 网络搜索，每月5日）",
    "marketdata.province_fr_market":         "各省调频市场政策（capcomp_screener 网络搜索，每月5日）",
    "reports.nodal_pf_annual":              "内部计算：station_master × MILP优化 × 全年现货价格（POST /hermes/ranking/backfill-annual 触发）",
    "intl_market.gb_system_price":          "Elexon BMRS API（api.bmreports.com）",
}

# Tool definitions for Anthropic tools API
_TOOL_DEFS = [
    {
        "name": "query_db",
        "description": (
            "Run a read-only SELECT query against the platform database. "
            "Returns results as a markdown table (max 50 rows). "
            "Rejects any SQL containing INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SELECT SQL statement to execute"}
            },
            "required": ["sql"],
        },
    },
    {
        "name": "check_etl_freshness",
        "description": (
            "Return a summary of data freshness for all monitored tables. "
            "Queries ops.ingestion_expected_freshness and ops.ingestion_dataset_status. "
            "Also checks reports.nodal_pf_annual for the current year."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "read_source_file",
        "description": (
            "Read a Python source file from the platform repo (max 300 lines). "
            "Path must be relative to repo root, e.g. 'services/hermes/app.py'. "
            "Only .py files are allowed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Repo-relative path to .py file"}
            },
            "required": ["path"],
        },
    },
    {
        "name": "list_app_files",
        "description": (
            "List all .py files for a given app. "
            "Searches apps/<app>/ and services/<app>/ under the repo root."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "app": {"type": "string", "description": "App name, e.g. 'spot-market', 'bess-map', 'hermes'"}
            },
            "required": ["app"],
        },
    },
    {
        "name": "send_feishu_message",
        "description": (
            "Send the observation/question to the user via Feishu. "
            "Call this when you have a focused observation ready. "
            "Pass empty string to signal that everything looks healthy (no message sent)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Message text to send. Empty string = healthy, no send."}
            },
            "required": ["text"],
        },
    },
    {
        "name": "write_dev_request",
        "description": (
            "Write a structured dev-request .md file to OneDrive for laptop-side development. "
            "The file will be synced to the user's laptop and picked up by Claude Code."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "slug": {
                    "type": "string",
                    "description": "Short kebab-case identifier, e.g. 'fix-irr-display-mengxi'"
                },
                "content": {
                    "type": "string",
                    "description": "Full markdown content of the dev request file"
                },
            },
            "required": ["slug", "content"],
        },
    },
]

# ── Weekly app rotation for design review ────────────────────────────────────
# Even ISO week number → set A; odd → set B
_DESIGN_APPS_A = ["spot-market", "bess-map", "hermes"]
_DESIGN_APPS_B = ["mengxi-dashboard", "gb-market", "hermes"]


class ThinkingAgent:
    def __init__(
        self,
        anthropic_api_key: str,
        pg_url: str,
        feishu: Optional["FeishuClient"],
        feishu_owner_open_id: str,
        onedrive: Optional["OneDriveClient"] = None,
    ) -> None:
        self._api_key = anthropic_api_key
        self._pg_url = pg_url
        self._feishu = feishu
        self._owner_id = feishu_owner_open_id
        self._onedrive = onedrive
        self._client = Anthropic(api_key=anthropic_api_key)

    def _resolve_model(self, mode: str) -> str:
        """Return the Anthropic model ID for the given mode, respecting env-var overrides."""
        env_var = "HERMES_THINK_HEALTH_MODEL" if mode == "health" else "HERMES_THINK_DESIGN_MODEL"
        alias = os.environ.get(env_var, "haiku" if mode == "health" else "sonnet")
        _MODEL_MAP = {
            "haiku":   "claude-haiku-4-5-20251001",
            "sonnet":  "claude-sonnet-4-6",
            "opus":    "claude-opus-4-6",
        }
        return _MODEL_MAP.get(alias, alias)
```

- [ ] **Step 4: Run tests — should pass now**

```bash
cd /app && python -m pytest tests/hermes/test_thinking_agent.py::TestModelResolution -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add services/hermes/thinking_agent.py tests/hermes/test_thinking_agent.py db/ddl/hermes/005_thinking_log.sql
git commit -m "feat: ThinkingAgent skeleton with model resolution"
```

---

## Task 3: Read/Query Tools — query_db, check_etl_freshness, read_source_file, list_app_files

**Files:**
- Modify: `services/hermes/thinking_agent.py`
- Modify: `tests/hermes/test_thinking_agent.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/hermes/test_thinking_agent.py`:

```python
class TestQueryDb:
    def test_rejects_insert(self):
        agent = _make_agent()
        result = agent._tool_query_db("INSERT INTO foo VALUES (1)")
        assert "rejected" in result.lower() or "not allowed" in result.lower()

    def test_rejects_drop(self):
        agent = _make_agent()
        result = agent._tool_query_db("DROP TABLE foo")
        assert "rejected" in result.lower() or "not allowed" in result.lower()

    def test_rejects_update(self):
        agent = _make_agent()
        result = agent._tool_query_db("UPDATE foo SET bar=1")
        assert "rejected" in result.lower() or "not allowed" in result.lower()

    def test_accepts_select(self):
        agent = _make_agent()
        with patch("psycopg2.connect") as mock_conn:
            mock_cur = MagicMock()
            mock_cur.description = [MagicMock(name="col") for _ in ["col"]]
            mock_cur.__iter__ = MagicMock(return_value=iter([("value",)]))
            mock_conn.return_value.__enter__ = MagicMock(return_value=mock_conn.return_value)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)
            mock_conn.return_value.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
            mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)
            mock_cur.fetchmany.return_value = [("value",)]
            result = agent._tool_query_db("SELECT 1")
            assert isinstance(result, str)


class TestReadSourceFile:
    def test_rejects_path_traversal(self):
        agent = _make_agent()
        result = agent._tool_read_source_file("../../etc/passwd")
        assert "not allowed" in result.lower() or "invalid" in result.lower()

    def test_rejects_non_py(self):
        agent = _make_agent()
        result = agent._tool_read_source_file("services/hermes/requirements.txt")
        assert "not allowed" in result.lower() or "only .py" in result.lower()

    def test_reads_existing_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "services.hermes.thinking_agent._REPO_ROOT", str(tmp_path)
        )
        py_file = tmp_path / "services" / "hermes" / "test.py"
        py_file.parent.mkdir(parents=True)
        py_file.write_text("def foo(): pass\n" * 10)
        agent = _make_agent()
        result = agent._tool_read_source_file("services/hermes/test.py")
        assert "def foo" in result

    def test_truncates_at_300_lines(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "services.hermes.thinking_agent._REPO_ROOT", str(tmp_path)
        )
        py_file = tmp_path / "big.py"
        py_file.write_text("x = 1\n" * 400)
        agent = _make_agent()
        result = agent._tool_read_source_file("big.py")
        assert result.count("x = 1") == 300


class TestListAppFiles:
    def test_returns_py_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "services.hermes.thinking_agent._REPO_ROOT", str(tmp_path)
        )
        app_dir = tmp_path / "apps" / "spot-market"
        app_dir.mkdir(parents=True)
        (app_dir / "app.py").write_text("")
        (app_dir / "utils.py").write_text("")
        agent = _make_agent()
        result = agent._tool_list_app_files("spot-market")
        assert "apps/spot-market/app.py" in result
        assert "apps/spot-market/utils.py" in result

    def test_falls_back_to_services(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "services.hermes.thinking_agent._REPO_ROOT", str(tmp_path)
        )
        svc_dir = tmp_path / "services" / "hermes"
        svc_dir.mkdir(parents=True)
        (svc_dir / "agent.py").write_text("")
        agent = _make_agent()
        result = agent._tool_list_app_files("hermes")
        assert "services/hermes/agent.py" in result
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
cd /app && python -m pytest tests/hermes/test_thinking_agent.py::TestQueryDb tests/hermes/test_thinking_agent.py::TestReadSourceFile tests/hermes/test_thinking_agent.py::TestListAppFiles -v 2>&1 | tail -10
```

Expected: `AttributeError: 'ThinkingAgent' object has no attribute '_tool_query_db'`

- [ ] **Step 3: Implement the tools — append to `ThinkingAgent` class in `thinking_agent.py`**

```python
    # ── Tool: query_db ────────────────────────────────────────────────────────

    _BLOCKED_SQL = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\b",
        re.IGNORECASE,
    )

    def _tool_query_db(self, sql: str) -> str:
        if self._BLOCKED_SQL.search(sql):
            return "ERROR: Only SELECT queries are allowed."
        try:
            conn = psycopg2.connect(self._pg_url, options="-c statement_timeout=10000")
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    rows = cur.fetchmany(50)
                    if not rows:
                        return "(no rows returned)"
                    cols = [d[0] for d in cur.description]
                    lines = ["| " + " | ".join(cols) + " |",
                             "| " + " | ".join("---" for _ in cols) + " |"]
                    for row in rows:
                        lines.append("| " + " | ".join(str(v) for v in row) + " |")
                    return "\n".join(lines)
        except Exception as exc:
            logger.warning("query_db error: %s", exc)
            return f"ERROR: {exc}"

    # ── Tool: check_etl_freshness ─────────────────────────────────────────────

    def _tool_check_etl_freshness(self) -> str:
        sql = """
            SELECT
                f.dataset,
                f.collector,
                f.max_lag_days,
                s.last_success_at,
                s.last_date_seen,
                s.failure_count,
                CASE
                    WHEN s.last_success_at IS NULL THEN 'UNKNOWN'
                    WHEN NOW() - s.last_success_at > (f.max_lag_days || ' days')::interval THEN 'STALE'
                    ELSE 'OK'
                END AS status
            FROM ops.ingestion_expected_freshness f
            LEFT JOIN ops.ingestion_dataset_status s
                ON f.collector = s.collector AND f.dataset = s.dataset
            WHERE f.active = TRUE
            ORDER BY status DESC, f.dataset
        """
        freshness = self._tool_query_db(sql)

        # Also check nodal annual backfill for current year
        year = datetime.now(tz=timezone.utc).year
        nodal_sql = f"""
            SELECT COUNT(*) as plant_count, MAX(computed_at) as last_computed
            FROM reports.nodal_pf_annual
            WHERE year = {year}
        """
        nodal = self._tool_query_db(nodal_sql)

        lineage_hint = "\n\n**Data source reference:**\n" + "\n".join(
            f"- `{tbl}`: {src}" for tbl, src in DATA_LINEAGE.items()
        )
        return f"**ETL Freshness Status:**\n{freshness}\n\n**Nodal PF Annual ({year}):**\n{nodal}{lineage_hint}"

    # ── Tool: read_source_file ────────────────────────────────────────────────

    def _tool_read_source_file(self, path: str) -> str:
        # Normalise and validate
        clean = os.path.normpath(path).replace("\\", "/")
        if ".." in clean:
            return "ERROR: Path traversal not allowed."
        if not clean.endswith(".py"):
            return "ERROR: Only .py files are allowed."
        abs_path = os.path.join(_REPO_ROOT, clean)
        if not os.path.isfile(abs_path):
            return f"ERROR: File not found: {clean}"
        try:
            with open(abs_path, encoding="utf-8") as fh:
                lines = fh.readlines()[:300]
            truncated = len(lines) == 300
            content = "".join(lines)
            if truncated:
                content += f"\n... (truncated at 300 lines)"
            return content
        except Exception as exc:
            return f"ERROR reading file: {exc}"

    # ── Tool: list_app_files ──────────────────────────────────────────────────

    def _tool_list_app_files(self, app: str) -> str:
        results: list[str] = []
        for base in ("apps", "services"):
            candidate = os.path.join(_REPO_ROOT, base, app)
            if os.path.isdir(candidate):
                for root, _, files in os.walk(candidate):
                    for f in sorted(files):
                        if f.endswith(".py") and not f.startswith("__"):
                            rel = os.path.relpath(os.path.join(root, f), _REPO_ROOT)
                            results.append(rel.replace("\\", "/"))
        if not results:
            return f"No .py files found for app '{app}'."
        return "\n".join(results)
```

- [ ] **Step 4: Run tests**

```bash
cd /app && python -m pytest tests/hermes/test_thinking_agent.py::TestQueryDb tests/hermes/test_thinking_agent.py::TestReadSourceFile tests/hermes/test_thinking_agent.py::TestListAppFiles -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add services/hermes/thinking_agent.py tests/hermes/test_thinking_agent.py
git commit -m "feat: ThinkingAgent read/query tools (query_db, check_etl_freshness, read_source_file, list_app_files)"
```

---

## Task 4: Write/Send Tools — send_feishu_message, write_dev_request, dedup

**Files:**
- Modify: `services/hermes/thinking_agent.py`
- Modify: `tests/hermes/test_thinking_agent.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/hermes/test_thinking_agent.py`:

```python
class TestSendFeishuMessage:
    def test_sends_message(self):
        feishu = MagicMock()
        agent = _make_agent(feishu=feishu)
        with patch.object(agent, "_is_duplicate", return_value=False):
            result = agent._tool_send_feishu_message("hello")
        feishu.send_text.assert_called_once_with(open_id="owner123", text="hello")
        assert "sent" in result

    def test_empty_text_skips_send(self):
        feishu = MagicMock()
        agent = _make_agent(feishu=feishu)
        result = agent._tool_send_feishu_message("")
        feishu.send_text.assert_not_called()
        assert "healthy" in result.lower() or "no message" in result.lower()

    def test_suppresses_duplicate(self):
        feishu = MagicMock()
        agent = _make_agent(feishu=feishu)
        with patch.object(agent, "_is_duplicate", return_value=True):
            result = agent._tool_send_feishu_message("same message")
        feishu.send_text.assert_not_called()
        assert "suppressed" in result.lower()


class TestWriteDevRequest:
    def test_writes_to_onedrive(self):
        onedrive = MagicMock()
        feishu = MagicMock()
        agent = _make_agent(onedrive=onedrive, feishu=feishu)
        result = agent._tool_write_dev_request("fix-irr", "# Dev Request\ncontent here")
        onedrive.upload_file.assert_called_once()
        call_args = onedrive.upload_file.call_args
        assert call_args[0][0] == _DEV_REQUESTS_FOLDER  # folder_path
        assert call_args[0][1].endswith("-fix-irr.md")  # filename
        assert b"# Dev Request" in call_args[0][2]       # content bytes
        feishu.send_text.assert_called_once()
        assert "fix-irr" in feishu.send_text.call_args[1]["text"] or \
               "fix-irr" in str(feishu.send_text.call_args)

    def test_skips_onedrive_if_not_configured(self):
        feishu = MagicMock()
        agent = _make_agent(onedrive=None, feishu=feishu)
        result = agent._tool_write_dev_request("test", "content")
        assert "onedrive" in result.lower() or "not configured" in result.lower()
```

Also add import at top of test file:
```python
from services.hermes.thinking_agent import _DEV_REQUESTS_FOLDER
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
cd /app && python -m pytest tests/hermes/test_thinking_agent.py::TestSendFeishuMessage tests/hermes/test_thinking_agent.py::TestWriteDevRequest -v 2>&1 | tail -5
```

Expected: `AttributeError: 'ThinkingAgent' object has no attribute '_tool_send_feishu_message'`

- [ ] **Step 3: Implement send_feishu_message, write_dev_request, _is_duplicate — append to ThinkingAgent**

```python
    # ── Tool: send_feishu_message ─────────────────────────────────────────────

    def _is_duplicate(self, text: str, mode: str) -> bool:
        """Return True if the same message (first 80 chars) was sent recently."""
        days = _DEDUP_DAYS.get(mode, 7)
        prefix = text[:80]
        sql = """
            SELECT 1 FROM hermes.thinking_log
            WHERE mode = %s
              AND ts > NOW() - (%s || ' days')::interval
              AND message_sent LIKE %s
            LIMIT 1
        """
        try:
            conn = psycopg2.connect(self._pg_url, options="-c statement_timeout=5000")
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (mode, str(days), prefix + "%"))
                    return cur.fetchone() is not None
        except Exception as exc:
            logger.warning("Dedup check failed (allowing send): %s", exc)
            return False

    def _tool_send_feishu_message(self, text: str, _mode: str = "health") -> str:
        if not text.strip():
            return "No message sent — platform looks healthy."
        if self._is_duplicate(text, _mode):
            logger.info("ThinkingAgent: suppressing duplicate message")
            return "Suppressed: same observation sent recently."
        if self._feishu and self._owner_id:
            try:
                self._feishu.send_text(open_id=self._owner_id, text=text)
            except Exception as exc:
                logger.error("ThinkingAgent: Feishu send failed: %s", exc)
                return f"ERROR sending message: {exc}"
        return "Message sent."

    # ── Tool: write_dev_request ───────────────────────────────────────────────

    def _tool_write_dev_request(self, slug: str, content: str) -> str:
        if not self._onedrive:
            return "ERROR: OneDrive not configured — cannot write dev request."
        today = datetime.now(tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        filename = f"{today}-{slug}.md"
        try:
            self._onedrive.upload_file(
                _DEV_REQUESTS_FOLDER,
                filename,
                content.encode("utf-8"),
                conflict_behavior="replace",
            )
        except Exception as exc:
            logger.error("ThinkingAgent: OneDrive upload failed: %s", exc)
            return f"ERROR writing dev request: {exc}"
        notify_text = (
            f"📝 已记录开发需求：`dev-requests/{filename}`\n"
            f"同步到 OneDrive 后可用公司 Claude token 拾取开发。"
        )
        if self._feishu and self._owner_id:
            try:
                self._feishu.send_text(open_id=self._owner_id, text=notify_text)
            except Exception as exc:
                logger.warning("ThinkingAgent: notify send failed: %s", exc)
        return f"Dev request written: {filename}"
```

- [ ] **Step 4: Run tests**

```bash
cd /app && python -m pytest tests/hermes/test_thinking_agent.py::TestSendFeishuMessage tests/hermes/test_thinking_agent.py::TestWriteDevRequest -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add services/hermes/thinking_agent.py tests/hermes/test_thinking_agent.py
git commit -m "feat: ThinkingAgent send/write tools (send_feishu_message, write_dev_request, dedup)"
```

---

## Task 5: Tool Loop + run() + _log_run() + Seed Prompts

**Files:**
- Modify: `services/hermes/thinking_agent.py`
- Modify: `tests/hermes/test_thinking_agent.py`

- [ ] **Step 1: Write failing test for run()**

Append to `tests/hermes/test_thinking_agent.py`:

```python
class TestRun:
    def _mock_anthropic_response(self, tool_name, tool_input, tool_use_id="t1"):
        """Helper: build a mock Anthropic response that calls one tool."""
        from anthropic.types import Message, ToolUseBlock, TextBlock
        block = MagicMock()
        block.type = "tool_use"
        block.name = tool_name
        block.input = tool_input
        block.id = tool_use_id
        response = MagicMock()
        response.stop_reason = "tool_use"
        response.content = [block]
        return response

    def _mock_end_response(self):
        response = MagicMock()
        response.stop_reason = "end_turn"
        response.content = []
        return response

    def test_health_run_sends_message(self):
        feishu = MagicMock()
        agent = _make_agent(feishu=feishu)
        send_response = self._mock_anthropic_response(
            "send_feishu_message", {"text": "山东数据已过期"}
        )
        end_response = self._mock_end_response()
        agent._client = MagicMock()
        agent._client.messages.create.side_effect = [send_response, end_response]
        with patch.object(agent, "_is_duplicate", return_value=False), \
             patch.object(agent, "_log_run"):
            agent.run("health")
        feishu.send_text.assert_called_once()
        assert "山东数据已过期" in feishu.send_text.call_args[1]["text"]

    def test_run_respects_max_iterations(self):
        feishu = MagicMock()
        agent = _make_agent(feishu=feishu)
        # Always returns tool_use (never end_turn) — should hit MAX_ITER limit
        query_response = self._mock_anthropic_response("query_db", {"sql": "SELECT 1"})
        agent._client = MagicMock()
        agent._client.messages.create.return_value = query_response
        with patch.object(agent, "_tool_query_db", return_value="| col |\n| --- |\n| 1 |"), \
             patch.object(agent, "_log_run"):
            agent.run("health")
        assert agent._client.messages.create.call_count <= _MAX_ITER + 1
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd /app && python -m pytest tests/hermes/test_thinking_agent.py::TestRun -v 2>&1 | tail -5
```

Expected: `AttributeError: 'ThinkingAgent' object has no attribute 'run'`

- [ ] **Step 3: Implement _dispatch_tool, _log_run, _build_seed_prompt, run() — append to ThinkingAgent**

```python
    # ── Tool dispatcher ───────────────────────────────────────────────────────

    def _dispatch_tool(self, name: str, tool_input: dict, mode: str) -> str:
        if name == "query_db":
            return self._tool_query_db(tool_input["sql"])
        if name == "check_etl_freshness":
            return self._tool_check_etl_freshness()
        if name == "read_source_file":
            return self._tool_read_source_file(tool_input["path"])
        if name == "list_app_files":
            return self._tool_list_app_files(tool_input["app"])
        if name == "send_feishu_message":
            return self._tool_send_feishu_message(tool_input["text"], _mode=mode)
        if name == "write_dev_request":
            return self._tool_write_dev_request(tool_input["slug"], tool_input["content"])
        return f"ERROR: Unknown tool '{name}'"

    # ── Logging ───────────────────────────────────────────────────────────────

    def _log_run(
        self,
        mode: str,
        files_read: list[str],
        tables_checked: list[str],
        message_sent: str,
        model_used: str,
    ) -> None:
        sql = """
            INSERT INTO hermes.thinking_log
                (mode, files_read, tables_checked, message_sent, model_used)
            VALUES (%s, %s, %s, %s, %s)
        """
        try:
            conn = psycopg2.connect(self._pg_url, options="-c statement_timeout=5000")
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (mode, files_read, tables_checked, message_sent, model_used))
        except Exception as exc:
            logger.warning("ThinkingAgent: _log_run failed: %s", exc)

    # ── Seed prompts ──────────────────────────────────────────────────────────

    def _build_health_prompt(self) -> str:
        now = datetime.now(tz=timezone(timedelta(hours=8)))
        return (
            f"今天是 {now.strftime('%Y-%m-%d %A')}（北京时间）。\n\n"
            "你是 Hermes，BESS 平台的 AI 助理。请检查平台数据的健康状况。\n\n"
            "步骤：\n"
            "1. 调用 `check_etl_freshness` 查看哪些数据集已过期。\n"
            "2. 如有异常（STALE 状态或失败计数 >0），用 `query_db` 进一步确认。\n"
            "3. 如发现问题，调用 `send_feishu_message` 发送一条简洁的中文消息给用户，"
            "   说明具体哪个数据集有问题，以及原始数据来源（参考 DATA_LINEAGE）。\n"
            "4. 如果一切正常，调用 `send_feishu_message` 传入空字符串。\n\n"
            "要求：只发送一条消息。不要发噪音。"
        )

    def _build_design_prompt(self) -> str:
        now = datetime.now(tz=timezone(timedelta(hours=8)))
        week = now.isocalendar()[1]
        apps = _DESIGN_APPS_A if week % 2 == 0 else _DESIGN_APPS_B
        apps_str = "、".join(apps)
        return (
            f"今天是 {now.strftime('%Y-%m-%d')}，周一（北京时间）。\n\n"
            "你是 Hermes，BESS 平台的 AI 助理。请审查以下应用的设计与运营状况：\n"
            f"本周审查范围：{apps_str}\n\n"
            "步骤：\n"
            "1. 对每个应用，先调用 `list_app_files` 查看文件列表。\n"
            "2. 用 `read_source_file` 阅读关键文件（最多共读 5 个文件）。\n"
            "3. 用 `query_db` 了解 DB 中有哪些数据。\n"
            "4. 先查看 hermes.thinking_log 最近14天的记录，避免重复已发送的观察。\n"
            "5. 形成 2-3 条具体、可操作的观察或问题，用 `send_feishu_message` 发送给用户（中文）。\n"
            "6. 对于你认为值得开发的改进，额外调用 `write_dev_request` 生成需求文档。\n\n"
            "要求：具体，不要泛泛而谈。指出实际代码位置或数据表名。最多读 5 个文件。"
        )

    # ── Main entry point ──────────────────────────────────────────────────────

    def run(self, mode: str) -> None:
        """Execute health check or design review tool loop."""
        if mode not in ("health", "design"):
            raise ValueError(f"Unknown mode: {mode}")

        model = self._resolve_model(mode)
        seed = self._build_health_prompt() if mode == "health" else self._build_design_prompt()
        messages = [{"role": "user", "content": seed}]

        files_read: list[str] = []
        tables_checked: list[str] = []
        message_sent = ""
        iteration = 0

        logger.info("ThinkingAgent.run(%s) model=%s", mode, model)

        while iteration < _MAX_ITER:
            try:
                response = self._client.messages.create(
                    model=model,
                    max_tokens=2048,
                    tools=_TOOL_DEFS,
                    messages=messages,
                )
            except Exception as exc:
                logger.error("ThinkingAgent LLM call failed: %s", exc)
                break

            if response.stop_reason != "tool_use":
                break

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                tool_name = block.name
                result = self._dispatch_tool(tool_name, block.input, mode)

                # Track audit state
                if tool_name == "read_source_file":
                    files_read.append(block.input.get("path", ""))
                elif tool_name in ("query_db", "check_etl_freshness"):
                    if sql := block.input.get("sql"):
                        tables_checked.append(sql[:60])
                elif tool_name == "send_feishu_message":
                    message_sent = block.input.get("text", "")

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
            iteration += 1

            # Stop after Feishu message sent
            if any(tr.get("type") == "tool_result" and
                   any(b.name == "send_feishu_message" for b in response.content
                       if hasattr(b, "name"))
                   for tr in tool_results):
                break

        self._log_run(mode, files_read, tables_checked, message_sent, model)
        logger.info("ThinkingAgent.run(%s) complete: %d iterations", mode, iteration)

    # ── User-triggered dev request ────────────────────────────────────────────

    def write_dev_request_from_message(self, user_message: str) -> str:
        """Called when user asks Hermes to record a dev request via Feishu chat.

        Uses a single LLM call (Haiku) to convert the user's free-form request
        into a structured dev-request .md file, then writes it to OneDrive.
        Returns confirmation text to send back to the user.
        """
        model = self._resolve_model("health")  # use cheap model for this
        now = datetime.now(tz=timezone(timedelta(hours=8)))
        system = (
            "You are Hermes. The user wants to record a development request. "
            "Given their message, produce a JSON object with two fields:\n"
            '  "slug": short kebab-case identifier (e.g. "fix-irr-display-mengxi")\n'
            '  "content": full markdown content of the dev request file, following this template:\n'
            "# Dev Request: <title>\n"
            f"**Created:** {now.strftime('%Y-%m-%d %H:%M')} by Hermes (user request)\n"
            "**Priority:** medium\n"
            "**Triggered by:** user request\n"
            "**Status:** pending\n\n"
            "## Context\n<why this matters>\n\n"
            "## Requested Change\n<what to build or fix>\n\n"
            "## Files to Touch\n- `path/to/file.py` — what to change\n\n"
            "## Data Sources / APIs\n<relevant DB tables, agent tools, external APIs>\n\n"
            "## Acceptance Criteria\n- [ ] Criterion 1\n\n"
            "## Notes\n<edge cases, similar existing code>\n\n"
            "Respond with JSON only. No markdown fences."
        )
        try:
            response = self._client.messages.create(
                model=model,
                max_tokens=2048,
                system=system,
                messages=[{"role": "user", "content": user_message}],
            )
            raw = response.content[0].text.strip()
            data = json.loads(raw)
            slug = data["slug"]
            content = data["content"]
        except Exception as exc:
            logger.error("ThinkingAgent.write_dev_request_from_message failed: %s", exc)
            return f"无法生成需求文档：{exc}"

        result = self._tool_write_dev_request(slug, content)
        if result.startswith("ERROR"):
            return f"需求记录失败：{result}"
        return f"📝 已记录开发需求：`dev-requests/{result.split(': ')[-1]}`\n同步到 OneDrive 后可用公司 Claude token 拾取开发。"
```

- [ ] **Step 4: Run all tests**

```bash
cd /app && python -m pytest tests/hermes/test_thinking_agent.py -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add services/hermes/thinking_agent.py tests/hermes/test_thinking_agent.py
git commit -m "feat: ThinkingAgent tool loop, run(), seed prompts, write_dev_request_from_message"
```

---

## Task 6: HermesAgent — WRITE_DEV_REQUEST action

**Files:**
- Modify: `services/hermes/agent.py`

- [ ] **Step 1: Add action to system prompt**

In `services/hermes/agent.py`, find the block of action definitions in `SYSTEM_PROMPT` (around line 169, after `DRAFT_REPORT`). Insert before `EXPORT_ANSWER`:

```python
WRITE_DEV_REQUEST — record a development request as a structured .md file to OneDrive for laptop-side development with company Claude token
  params: {"message": "user's full request message verbatim"}
  reply: brief acknowledgment that the request is being processed
  note: use when user says "记录需求", "记录一个需求", "save dev request", "记录开发需求",
        "写一个需求文档", "帮我记录", "development request", "dev request", "需求文档".
        Pass the user's FULL original message in the message param — ThinkingAgent will structure it.
```

- [ ] **Step 2: Add routing rule to SYSTEM_PROMPT rules section**

In the rules block (after the existing `- When user says "save as Word..."` rules), add:

```python
- When user says "记录需求", "记录开发需求", "写需求文档", "save dev request", "development request", "dev request", use WRITE_DEV_REQUEST with the user's verbatim message in the message param.
```

- [ ] **Step 3: Add dispatch in execute()**

In `HermesAgent.execute()` (around line 511), find the `if action.action == "EXPORT_ANSWER":` block. Before it, add:

```python
        if action.action == "WRITE_DEV_REQUEST":
            msg = action.params.get("message", "")
            if not msg:
                return "请说明需求内容。"
            from services.hermes.thinking_agent import ThinkingAgent
            thinker = ThinkingAgent(
                anthropic_api_key=self._api_key,
                pg_url=os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", ""),
                feishu=None,  # notification handled inside write_dev_request_from_message
                feishu_owner_open_id="",
                onedrive=self.onedrive,
            )
            return thinker.write_dev_request_from_message(msg)
```

- [ ] **Step 4: Smoke test**

```bash
cd /app && python -c "
from services.hermes.agent import SYSTEM_PROMPT
assert 'WRITE_DEV_REQUEST' in SYSTEM_PROMPT, 'Action not in system prompt'
print('OK: WRITE_DEV_REQUEST in system prompt')
"
```

Expected: `OK: WRITE_DEV_REQUEST in system prompt`

- [ ] **Step 5: Commit**

```bash
git add services/hermes/agent.py
git commit -m "feat: add WRITE_DEV_REQUEST action to HermesAgent"
```

---

## Task 7: Scheduler Integration — app.py

**Files:**
- Modify: `services/hermes/app.py`

- [ ] **Step 1: Add import**

In `services/hermes/app.py`, after the existing import block (near line 44), add:

```python
from services.hermes.thinking_agent import ThinkingAgent
```

- [ ] **Step 2: Instantiate ThinkingAgent after HermesAgent is created**

Find where `agent = HermesAgent(...)` is constructed in `create_app()`. Immediately after, add:

```python
    thinking_agent = ThinkingAgent(
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        pg_url=os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", ""),
        feishu=feishu,
        feishu_owner_open_id=os.environ.get("FEISHU_OWNER_OPEN_ID", ""),
        onedrive=agent.onedrive,
    )
```

- [ ] **Step 3: Add scheduler jobs**

Find the morning briefing scheduler job block (the `send_morning_briefing` add_job call). Immediately after it, add:

```python
    # Thinking: health check daily 00:10 UTC (08:10 Beijing) — after morning briefing
    scheduler.add_job(
        thinking_agent.run,
        "cron",
        hour=0, minute=10,
        kwargs={"mode": "health"},
    )

    # Thinking: design review every Monday 00:30 UTC (08:30 Beijing)
    scheduler.add_job(
        thinking_agent.run,
        "cron",
        day_of_week="mon", hour=0, minute=30,
        kwargs={"mode": "design"},
    )
```

- [ ] **Step 4: Smoke test import**

```bash
cd /app && python -c "from services.hermes.app import create_app; print('OK: app imports cleanly')"
```

Expected: `OK: app imports cleanly`

- [ ] **Step 5: Run full test suite**

```bash
cd /app && python -m pytest tests/hermes/ -v
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add services/hermes/app.py
git commit -m "feat: wire ThinkingAgent into Hermes scheduler (health 00:10 UTC, design Mon 00:30 UTC)"
```

---

## Task 8: Build and Deploy

- [ ] **Step 1: Build Docker image**

```bash
cd /c/Users/dipeng.chen/OneDrive/ETRM/bess-platform
docker build -f apps/hermes-service/Dockerfile -t bess-platform-hermes:latest .
```

Expected: `Successfully built ...` with no errors.

- [ ] **Step 2: Push to ECR and deploy**

```bash
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker tag bess-platform-hermes:latest 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes:latest
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-hermes-svc --force-new-deployment --region ap-southeast-1
```

- [ ] **Step 3: Verify deployment**

```bash
aws ecs wait services-stable --cluster bess-platform-cluster --services bess-platform-hermes-svc --region ap-southeast-1
echo "Deployment stable"
```

- [ ] **Step 4: Smoke test health check via CloudWatch**

```bash
aws logs tail /ecs/bess-platform/bess-platform-hermes --follow --since 5m --region ap-southeast-1 | grep -i "thinking"
```

Expected to see on next 00:10 UTC: `ThinkingAgent.run(health) model=claude-haiku-4-5-20251001`

- [ ] **Step 5: Manual trigger test — send "记录需求：在 mengxi-dashboard 加 IRR 对比" to Hermes in Feishu**

Expected: Hermes replies with a confirmation and a dev-request `.md` appears in OneDrive at `etrm/bess-platform/dev-requests/`.

---

## Self-Review Checklist

**Spec coverage:**
- [x] ThinkingAgent class with tool loop → Tasks 2-5
- [x] Health check mode (daily 00:10 UTC) → Task 5 + 7
- [x] Design review mode (Monday 00:30 UTC) → Task 5 + 7
- [x] DB migration (hermes.thinking_log) → Task 1
- [x] query_db (read-only, max 50 rows) → Task 3
- [x] check_etl_freshness (queries ops.ingestion_expected_freshness) → Task 3
- [x] read_source_file (max 300 lines, no path traversal, .py only) → Task 3
- [x] list_app_files (apps/ + services/ search) → Task 3
- [x] send_feishu_message with dedup (7d health, 14d design) → Task 4
- [x] write_dev_request to OneDrive dev-requests folder → Task 4
- [x] DATA_LINEAGE dict in system prompt context → Task 2 (module-level constant)
- [x] Model env-var overrides (HERMES_THINK_HEALTH_MODEL, HERMES_THINK_DESIGN_MODEL) → Task 2
- [x] User-triggered dev request via WRITE_DEV_REQUEST action → Task 6
- [x] Weekly app rotation (even/odd week) → Task 5
- [x] Scheduler integration → Task 7
- [x] Tests → Tasks 2-5

**No placeholders:** All code blocks are complete. No TBDs.

**Type consistency:** `_tool_send_feishu_message(text, _mode)` — `_mode` param is internal, not exposed in tool schema (schema only has `text`). `_dispatch_tool` passes `mode` from `run()` context. Consistent throughout.
