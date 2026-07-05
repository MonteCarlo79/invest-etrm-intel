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

    # ── Tool: query_db ────────────────────────────────────────────────────────

    _BLOCKED_SQL = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\b",
        re.IGNORECASE,
    )

    def _tool_query_db(self, sql: str) -> str:
        if self._BLOCKED_SQL.search(sql):
            return "ERROR: Query not allowed — only SELECT statements are permitted."
        try:
            conn = psycopg2.connect(self._pg_url, options="-c statement_timeout=10000")
            try:
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
            finally:
                conn.close()
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
        # Resolve to absolute path within repo root
        abs_path = os.path.normpath(os.path.join(_REPO_ROOT, path))
        if not abs_path.startswith(_REPO_ROOT + os.sep):
            return "ERROR: Path traversal not allowed."
        if not abs_path.endswith(".py"):
            return "ERROR: Only .py files are allowed."
        if not os.path.isfile(abs_path):
            return f"ERROR: File not found: {abs_path}"
        try:
            with open(abs_path, encoding="utf-8") as fh:
                all_lines = fh.readlines()
            lines = all_lines[:300]
            truncated = len(all_lines) > 300
            content = "".join(lines)
            if truncated:
                content += "\n... (truncated at 300 lines)"
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
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, (mode, str(days), prefix + "%"))
                        return cur.fetchone() is not None
            finally:
                conn.close()
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
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, (mode, files_read, tables_checked, message_sent, model_used))
            finally:
                conn.close()
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
            feishu_sent = False
            for block in response.content:
                if block.type != "tool_use":
                    continue
                tool_name = block.name
                result = self._dispatch_tool(tool_name, block.input, mode)

                if tool_name == "read_source_file":
                    files_read.append(block.input.get("path", ""))
                elif tool_name in ("query_db", "check_etl_freshness"):
                    if sql := block.input.get("sql"):
                        tables_checked.append(sql[:60])
                elif tool_name == "send_feishu_message":
                    message_sent = block.input.get("text", "")
                    feishu_sent = True

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
            iteration += 1

            if feishu_sent:
                break

        self._log_run(mode, files_read, tables_checked, message_sent, model)
        logger.info("ThinkingAgent.run(%s) complete: %d iterations", mode, iteration)

    # ── User-triggered dev request ────────────────────────────────────────────

    def write_dev_request_from_message(self, user_message: str) -> str:
        """Convert user's free-form request into a structured dev-request .md on OneDrive."""
        model = self._resolve_model("health")
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
        filename = result.split(": ")[-1]
        return f"📝 已记录开发需求：`dev-requests/{filename}`\n同步到 OneDrive 后可用公司 Claude token 拾取开发。"
