from __future__ import annotations
import asyncio
import collections
import hashlib
import hmac as _hmac
import json as _json
import os
import logging
import threading
import time
from typing import Optional

logging.basicConfig(level=logging.INFO)

# sender_id → (folder_path, remaining_count)
# remaining_count: N = save next N files to this folder; -1 = unlimited until cleared
_pending_folders: dict[str, tuple[str, int]] = {}
# sender_id → user hint for AI-based market-fundamentals classification
_pending_classify: dict[str, str] = {}
# sender_id → (category, hint) for knowledge base ingestion
_pending_kb_ingest: dict[str, tuple[str, str]] = {}
# message_id → {sender_id, filename, file_key, resource_type, current_folder}
# allows the post-upload routing card to re-route files to a different folder
_pending_reroute: dict[str, dict] = {}
# sender_id → (folder_path, region_label) — survey/research-report mode
_pending_survey: dict[str, tuple[str, str]] = {}
from fastapi import FastAPI, BackgroundTasks, Query, Request, Response
from apscheduler.schedulers.background import BackgroundScheduler
from services.hermes.models import InboundMessage
from services.hermes.agent import HermesAgent
from services.hermes.tasks_client import TasksClient
from services.hermes.wecom_client import WeComClient
from services.hermes.feishu_client import FeishuClient
from services.hermes.telegram_client import TelegramClient
from services.hermes.onedrive_client import OneDriveClient
from services.hermes.outlook_client import OutlookClient
from services.hermes.scheduler import send_due_reminders, send_morning_briefing, send_email_digest, summarize_emails
from services.hermes.mengxi_ranking_report import send_daily_ranking as _send_mengxi_ranking
from services.hermes.mengxi_bess_screener import screen_new_bess as _screen_new_bess
from services.hermes.news_screener import screen_news_sources as _screen_news_sources
from services.hermes.spot_ingest_bridge import is_spot_pdf, ingest_pdf_bytes
from services.hermes.market_classifier import classify_to_market_fundamentals, is_document_file
from services.hermes.capacity_etl import upsert_capacity, is_capacity_file
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Hermes category menu ───────────────────────────────────────────────────────
_MENU_TRIGGER_WORDS = {"/start", "/menu", "/help", "start", "menu", "help",
                       "菜单", "帮助菜单", "/菜单", "menu mengxi", "/menu mengxi"}

# Telegram inline keyboard menu (HTML parse mode)
# callback_data must be ≤ 64 bytes — use short keys mapped to full questions below
_MENU_TEXT_HTML = (
    "🤖 <b>Hermes</b> — AI Assistant\n\n"
    "Select a category or just type your question:\n"
    "<i>选择工作领域，或直接发送问题</i>"
)
_MENU_BUTTONS = [
    [
        {"text": "📅 Calendar & Tasks",  "callback_data": "cat:calendar"},
        {"text": "⚡ China Markets",      "callback_data": "cat:china"},
    ],
    [
        {"text": "🌐 Intl Markets",       "callback_data": "cat:intl"},
        {"text": "📊 Trading Ops",        "callback_data": "cat:trading"},
    ],
    [
        {"text": "🔢 Quant Models",       "callback_data": "cat:quant"},
        {"text": "🏗 Structuring",        "callback_data": "cat:struct"},
    ],
    [
        {"text": "📋 Meeting Prep",       "callback_data": "cat:meeting"},
        {"text": "📁 Reports & KB",       "callback_data": "cat:kb"},
    ],
    [
        {"text": "⚙️ App Status",         "callback_data": "cat:status"},
    ],
]
# Maps short callback_data keys → full agent questions
_CALLBACK_MAP: dict[str, str] = {
    "cat:calendar": "你在日历、任务管理和提醒方面能做什么？",
    "cat:china":    "介绍你在中国现货电力市场和BESS省份分析方面的功能",
    "cat:intl":     "你能分析哪些国际电力市场？分别能提供哪些数据？",
    "cat:trading":  "你在BESS交易运营和内蒙古资产管理方面能做什么？",
    "cat:quant":    "你能做哪些量化分析？IRR计算、NPV和调度策略对比怎么用？",
    "cat:struct":   "在项目结构化、尽调分析和条款解读方面你能帮什么忙？",
    "cat:meeting":  "帮我准备一个会议简报，你需要我提供哪些信息？",
    "cat:kb":       "怎么上传报告到知识库？支持哪些文件格式和分类？",
    "cat:status":   "app status",
}

# Feishu interactive card menu (msg_type=interactive)
_FEISHU_MENU_CARD: dict = {
    "config": {"wide_screen_mode": True},
    "header": {
        "template": "blue",
        "title": {"content": "🤖 Hermes — AI Assistant", "tag": "plain_text"},
    },
    "elements": [
        {"tag": "div", "text": {"tag": "lark_md", "content": "**选择工作领域**，或直接发送问题："}},
        {
            "tag": "action",
            "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "📅 日历与任务"},   "type": "primary", "value": {"cat": "calendar"}},
                {"tag": "button", "text": {"tag": "plain_text", "content": "⚡ 中国电力市场"}, "type": "primary", "value": {"cat": "china"}},
            ],
        },
        {
            "tag": "action",
            "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "🌐 国际市场"},   "type": "default", "value": {"cat": "intl"}},
                {"tag": "button", "text": {"tag": "plain_text", "content": "📊 交易运营"},   "type": "default", "value": {"cat": "trading"}},
            ],
        },
        {
            "tag": "action",
            "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "🔢 量化模型"},   "type": "default", "value": {"cat": "quant"}},
                {"tag": "button", "text": {"tag": "plain_text", "content": "🏗 结构化分析"}, "type": "default", "value": {"cat": "struct"}},
            ],
        },
        {
            "tag": "action",
            "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "📋 会议准备"},     "type": "default", "value": {"cat": "meeting"}},
                {"tag": "button", "text": {"tag": "plain_text", "content": "📁 报告与知识库"}, "type": "default", "value": {"cat": "kb"}},
            ],
        },
        {
            "tag": "action",
            "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "⚙️ 应用状态"}, "type": "danger", "value": {"cat": "status"}},
            ],
        },
    ],
}

_BASE_CN_CARD        = "etrm/bess-platform/data/market-fundamentals"
_BASE_INTL_CARD      = "etrm/bess-platform/data/intl-markets"
_SURVEY_REPORT_BASE  = "etrm/bess-platform/data/market-fundamentals/调研报告"
_SURVEY_ASSET_BASE   = "etrm/bess-platform/assets/调研"


def _build_route_card(filename: str, current_folder: str, message_id: str) -> dict:
    """Post-upload routing card — lets the user re-route a file with one tap."""

    def _btn(label: str, folder: str, btn_type: str = "default") -> dict:
        return {
            "tag": "button",
            "text": {"tag": "plain_text", "content": label},
            "type": btn_type,
            "value": {"act": "route", "mid": message_id, "to": folder},
        }

    short = current_folder.replace("etrm/bess-platform/data/", "…/")
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "green",
            "title": {"content": f"📁 {filename}", "tag": "plain_text"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"已归档至 `{short}`\n路径有误？点击下方按钮重新存档："}},
            {"tag": "hr"},
            # Row 1 — confirm + cross-province specials
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "✓ 路径正确"},
                 "type": "primary", "value": {"act": "confirm", "mid": message_id}},
                _btn("🌐 全国政策",  f"{_BASE_CN_CARD}/【0全国】/1-政策"),
                _btn("📊 各省装机",  f"{_BASE_CN_CARD}/【各省份装机数据】"),
                _btn("📋 政策月报",  f"{_BASE_CN_CARD}/【政策研究月报】"),
            ]},
            # Row 2 — top provincial markets
            {"tag": "action", "actions": [
                _btn("山东", f"{_BASE_CN_CARD}/【15.山东】/1-信息披露"),
                _btn("广东", f"{_BASE_CN_CARD}/【19.广东】/1-信息披露"),
                _btn("蒙西", f"{_BASE_CN_CARD}/【5.1蒙西】/1-信息披露"),
                _btn("山西", f"{_BASE_CN_CARD}/【4.山西】/1-信息披露"),
                _btn("江苏", f"{_BASE_CN_CARD}/【10.江苏】/1-信息披露"),
            ]},
            # Row 3 — international markets
            {"tag": "action", "actions": [
                _btn("🇬🇧 GB",    f"{_BASE_INTL_CARD}/gb/reports"),
                _btn("🇦🇺 AU",    f"{_BASE_INTL_CARD}/au/reports"),
                _btn("ERCOT",   f"{_BASE_INTL_CARD}/ercot/reports"),
                _btn("CAISO",   f"{_BASE_INTL_CARD}/caiso/reports"),
                _btn("PJM",     f"{_BASE_INTL_CARD}/pjm/reports"),
            ]},
        ],
    }


def _build_save_picker_card() -> dict:
    """Standalone /save picker — tap a button to set the folder for the next upload."""

    def _btn(label: str, folder: str) -> dict:
        return {
            "tag": "button",
            "text": {"tag": "plain_text", "content": label},
            "type": "default",
            "value": {"act": "set_folder", "to": folder},
        }

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"content": "📁 选择下一个文件的存档位置", "tag": "plain_text"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": "点击目标文件夹，然后发送文件："}},
            {"tag": "hr"},
            {"tag": "action", "actions": [
                _btn("🌐 全国政策",  f"{_BASE_CN_CARD}/【0全国】/1-政策"),
                _btn("🌐 全国信披",  f"{_BASE_CN_CARD}/【0全国】/1-信息披露"),
                _btn("📊 各省装机",  f"{_BASE_CN_CARD}/【各省份装机数据】"),
                _btn("📋 政策月报",  f"{_BASE_CN_CARD}/【政策研究月报】"),
            ]},
            {"tag": "action", "actions": [
                _btn("山东", f"{_BASE_CN_CARD}/【15.山东】/1-信息披露"),
                _btn("广东", f"{_BASE_CN_CARD}/【19.广东】/1-信息披露"),
                _btn("蒙西", f"{_BASE_CN_CARD}/【5.1蒙西】/1-信息披露"),
                _btn("山西", f"{_BASE_CN_CARD}/【4.山西】/1-信息披露"),
                _btn("江苏", f"{_BASE_CN_CARD}/【10.江苏】/1-信息披露"),
            ]},
            {"tag": "action", "actions": [
                _btn("湖南", f"{_BASE_CN_CARD}/【18.湖南】/1-信息披露"),
                _btn("浙江", f"{_BASE_CN_CARD}/【11.浙江】/1-信息披露"),
                _btn("安徽", f"{_BASE_CN_CARD}/【12.安徽】/1-信息披露"),
                _btn("湖北", f"{_BASE_CN_CARD}/【17.湖北】/1-信息披露"),
                _btn("河南", f"{_BASE_CN_CARD}/【16.河南】/1-信息披露"),
            ]},
            {"tag": "action", "actions": [
                _btn("🇬🇧 GB data",    f"{_BASE_INTL_CARD}/gb/data"),
                _btn("🇬🇧 GB reports", f"{_BASE_INTL_CARD}/gb/reports"),
                _btn("🇦🇺 AU",         f"{_BASE_INTL_CARD}/au/reports"),
                _btn("ERCOT",        f"{_BASE_INTL_CARD}/ercot/reports"),
                _btn("CAISO",        f"{_BASE_INTL_CARD}/caiso/reports"),
            ]},
            {"tag": "action", "actions": [
                _btn("📤 Hermes Uploads", "Hermes Uploads"),
            ]},
        ],
    }


def _build_survey_card(mode: str) -> dict:
    """Region picker for 调研报告 (mode='report') or 资产调研 (mode='asset')."""
    def _btn(label: str, region: str) -> dict:
        return {
            "tag": "button",
            "text": {"tag": "plain_text", "content": label},
            "type": "default",
            "value": {"act": "survey_region", "mode": mode, "region": region},
        }

    if mode == "report":
        header_color = "orange"
        title = "📋 调研报告 — 选择地区"
        hint = "选择地区后，发送文字或文件，内容将保存到调研报告目录并录入知识库。发送「取消」退出。"
    else:
        header_color = "purple"
        title = "🔍 资产调研 — 选择地区"
        hint = "选择地区后，输入资产名称，再发送文字或文件，内容将保存到资产调研目录并录入知识库。发送「取消」退出。"

    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": header_color, "title": {"content": title, "tag": "plain_text"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": hint}},
            {"tag": "hr"},
            {"tag": "action", "actions": [
                _btn("🇨🇳 全国", "全国"),
                _btn("山东", "山东"), _btn("广东", "广东"), _btn("蒙西", "蒙西"),
            ]},
            {"tag": "action", "actions": [
                _btn("蒙东", "蒙东"), _btn("山西", "山西"), _btn("江苏", "江苏"),
                _btn("浙江", "浙江"), _btn("湖南", "湖南"),
            ]},
            {"tag": "action", "actions": [
                _btn("安徽", "安徽"), _btn("湖北", "湖北"), _btn("河南", "河南"),
                _btn("四川", "四川"), _btn("福建", "福建"),
            ]},
            {"tag": "action", "actions": [
                _btn("🇬🇧 GB", "GB"), _btn("🇦🇺 AU", "AU"), _btn("ERCOT", "ERCOT"),
                _btn("CAISO", "CAISO"), _btn("PJM", "PJM"),
            ]},
        ],
    }


# Feishu / plain-text menu (fallback if send_card fails)
_MENU_TEXT_PLAIN = """\
🤖 Hermes — AI Assistant

📅 Calendar & Tasks / 日历与任务
  任务管理、设置提醒、邮件摘要

⚡ China Markets / 中国电力市场
  现货价差 (spot) · 各省BESS收益 (bess-map)

🌐 International Markets
  GB · AU · ERCOT · CAISO · PJM · PH · PO

📊 Trading Ops / 交易运营
  内蒙古BESS资产 · 蒙西交易P&L

🔢 Quant Models / 量化模型
  BESS IRR/NPV计算 · 调度策略对比

🏗 Structuring / 结构化分析
  项目经济测算 · 条款解读 · 市场准入分析

📋 Meeting Prep / 会议准备
  示例: "帮我准备一个关于[主题]的会议简报"

📁 Reports & KB / 报告与知识库
  发文件 → 存OneDrive / 添加知识库 / 自动归类

⚙️ App Control / 应用控制
  "app status" · "开启 bess-map"

发送 /menu 可随时查看此菜单"""


def _make_clients():
    tasks = TasksClient(db_url=os.environ["HERMES_DB_URL"])

    wecom: Optional[WeComClient] = None
    _corp_id = os.environ.get("WECOM_CORP_ID", "")
    _agent_id = os.environ.get("WECOM_AGENT_ID", "")
    if _corp_id and _agent_id.isdigit():
        wecom = WeComClient(
            corp_id=_corp_id,
            agent_id=int(_agent_id),
            secret=os.environ.get("WECOM_SECRET", ""),
        )

    feishu: Optional[FeishuClient] = None
    _feishu_app_id = os.environ.get("FEISHU_APP_ID", "")
    _feishu_app_secret = os.environ.get("FEISHU_APP_SECRET", "")
    if _feishu_app_id and _feishu_app_secret:
        feishu = FeishuClient(app_id=_feishu_app_id, app_secret=_feishu_app_secret)

    onedrive: Optional[OneDriveClient] = None
    _od_client_id = os.environ.get("ONEDRIVE_CLIENT_ID", "")
    _od_refresh_token = (
        tasks.get_setting("onedrive_refresh_token")
        or os.environ.get("ONEDRIVE_REFRESH_TOKEN", "")
    )
    if _od_client_id and _od_refresh_token:
        def _on_token_rotated(new_token: str) -> None:
            tasks.set_setting("onedrive_refresh_token", new_token)

        onedrive = OneDriveClient(
            client_id=_od_client_id,
            client_secret=os.environ.get("ONEDRIVE_CLIENT_SECRET", ""),
            refresh_token=_od_refresh_token,
            on_token_rotated=_on_token_rotated,
        )

    outlook: Optional[OutlookClient] = None
    _outlook_rt = (
        tasks.get_setting("outlook_refresh_token")
        or os.environ.get("OUTLOOK_REFRESH_TOKEN", "")
    )
    if _od_client_id and _outlook_rt:
        def _on_outlook_token_rotated(new_token: str) -> None:
            tasks.set_setting("outlook_refresh_token", new_token)

        outlook = OutlookClient(
            client_id=_od_client_id,
            client_secret=os.environ.get("ONEDRIVE_CLIENT_SECRET", ""),
            refresh_token=_outlook_rt,
            on_token_rotated=_on_outlook_token_rotated,
        )

    telegram: Optional[TelegramClient] = None
    _tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if _tg_token:
        telegram = TelegramClient(token=_tg_token)

    agent = HermesAgent(
        tasks=tasks,
        anthropic_api_key=os.environ["ANTHROPIC_API_KEY"],
        onedrive=onedrive,
    )
    return tasks, wecom, feishu, telegram, agent, outlook


def create_app() -> FastAPI:
    tasks, wecom, feishu, telegram, agent, outlook = _make_clients()

    scheduler = BackgroundScheduler()
    # Due reminders: once daily at 8:05 AM Beijing (00:05 UTC) — not every 15 min
    scheduler.add_job(
        send_due_reminders,
        "cron",
        hour=0,
        minute=5,
        kwargs={
            "tasks": tasks,
            "wecom": wecom,
            "wecom_user_id": os.environ.get("WECOM_USER_ID", "@all"),
            "feishu": feishu,
            "feishu_owner_open_id": os.environ.get("FEISHU_OWNER_OPEN_ID", ""),
        },
    )
    # Morning briefing: 8:00 AM Beijing time (UTC+8 = 00:00 UTC)
    scheduler.add_job(
        send_morning_briefing,
        "cron",
        hour=0,
        minute=0,
        kwargs={
            "tasks": tasks,
            "feishu": feishu,
            "feishu_owner_open_id": os.environ.get("FEISHU_OWNER_OPEN_ID", ""),
        },
    )
    # Mengxi BESS ranking report: 7:00 AM Beijing (23:00 UTC previous day)
    _mengxi_pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
    if _mengxi_pg_url:
        scheduler.add_job(
            _send_mengxi_ranking,
            "cron",
            hour=23, minute=0,
            kwargs={
                "feishu":            feishu,
                "owner_open_id":     os.environ.get("FEISHU_OWNER_OPEN_ID", ""),
                "pg_url":            _mengxi_pg_url,
                "onedrive_client":   agent.onedrive,
            },
        )
        # New-BESS screener: 06:30 UTC (14:30 Beijing) — after market data typically arrives
        scheduler.add_job(
            _screen_new_bess,
            "cron",
            hour=6, minute=30,
            kwargs={
                "pg_url":          _mengxi_pg_url,
                "onedrive_client": agent.onedrive,
                "feishu":          feishu,
                "owner_open_id":   os.environ.get("FEISHU_OWNER_OPEN_ID", ""),
            },
        )

        # News screener: 06:00 UTC (14:00 Beijing) — scrape + score + ingest + send digest
        scheduler.add_job(
            _screen_news_sources,
            "cron",
            hour=6, minute=0,
            kwargs={
                "pg_url":          _mengxi_pg_url,
                "api_key":         os.environ.get("ANTHROPIC_API_KEY", ""),
                "feishu":          feishu,
                "owner_open_id":   os.environ.get("FEISHU_OWNER_OPEN_ID", ""),
            },
        )

    # Email digest: 9:00 AM Beijing (01:00 UTC) — only if Outlook is configured
    if outlook:
        scheduler.add_job(
            send_email_digest,
            "cron",
            hour=1,
            minute=3,
            kwargs={
                "outlook": outlook,
                "api_key": os.environ.get("ANTHROPIC_API_KEY", ""),
                "feishu": feishu,
                "feishu_owner_open_id": os.environ.get("FEISHU_OWNER_OPEN_ID", ""),
            },
        )
    scheduler.start()

    app = FastAPI()

    @app.get("/hermes/health")
    def health():
        return {"status": "ok"}

    @app.post("/hermes/admin/trigger-report")
    async def trigger_report(request: Request, background: BackgroundTasks):
        """Manually trigger a scheduled report. Requires X-Admin-Token header."""
        admin_token = os.environ.get("HERMES_ADMIN_TOKEN", "")
        if not admin_token or request.headers.get("X-Admin-Token") != admin_token:
            return Response(content="Unauthorized", status_code=401)
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        report = body.get("report", "mengxi_ranking")
        if report == "mengxi_ranking":
            pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
            owner_open_id = os.environ.get("FEISHU_OWNER_OPEN_ID", "")
            background.add_task(_send_mengxi_ranking, feishu=feishu,
                                 owner_open_id=owner_open_id, pg_url=pg_url,
                                 onedrive_client=agent.onedrive)
            return {"status": "triggered", "report": report}
        return {"status": "unknown_report", "report": report}

    @app.post("/hermes/news-screener/run")
    async def run_news_screener(background: BackgroundTasks):
        """Trigger a manual news-screener run. Returns immediately; runs in background."""
        _pg = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
        if not _pg:
            return Response(content="DB not configured", status_code=503)
        background.add_task(
            _screen_news_sources,
            pg_url=_pg,
            api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
            feishu=feishu,
            owner_open_id=os.environ.get("FEISHU_OWNER_OPEN_ID", ""),
        )
        return {"status": "started"}

    @app.get("/hermes/inbound/wecom")
    def wecom_verify(echostr: Optional[str] = Query(default=None)):
        return Response(content=echostr or "", media_type="text/plain")

    @app.post("/hermes/inbound/wecom")
    async def wecom_inbound(msg: InboundMessage, background: BackgroundTasks):
        background.add_task(_handle_message, msg, agent, wecom, feishu, outlook, telegram)
        return {"status": "accepted"}

    @app.post("/hermes/inbound/feishu")
    async def feishu_inbound(request: Request, background: BackgroundTasks):
        body = await request.body()

        # Timing-safe signature verification when FEISHU_ENCRYPT_KEY is configured.
        # Algorithm: SHA256(timestamp + nonce + encrypt_key + body_utf8)
        encrypt_key = os.environ.get("FEISHU_ENCRYPT_KEY", "")
        if encrypt_key:
            ts = request.headers.get("x-lark-request-timestamp", "")
            nonce = request.headers.get("x-lark-request-nonce", "")
            sig = request.headers.get("x-lark-signature", "")
            if ts and nonce and sig:
                content = f"{ts}{nonce}{encrypt_key}{body.decode('utf-8', errors='replace')}"
                computed = hashlib.sha256(content.encode()).hexdigest()
                if not _hmac.compare_digest(computed, sig):
                    logger.warning("Feishu webhook: invalid signature from %s", request.client)
                    return Response(status_code=401, content="Invalid signature")

        try:
            payload = _json.loads(body)
        except Exception:
            return {"status": "parse_error"}

        # Decrypt if Feishu sent an AES-encrypted payload (FEISHU_ENCRYPT_KEY is set)
        if "encrypt" in payload and encrypt_key:
            try:
                import base64
                key = hashlib.sha256(encrypt_key.encode()).digest()
                raw = base64.b64decode(payload["encrypt"])
                iv, content = raw[:16], raw[16:]
                try:
                    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
                    from cryptography.hazmat.backends import default_backend
                    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
                    dec = cipher.decryptor()
                    plain = dec.update(content) + dec.finalize()
                except ImportError:
                    from Crypto.Cipher import AES as _AES
                    plain = _AES.new(key, _AES.MODE_CBC, iv).decrypt(content)
                pad = plain[-1]
                payload = _json.loads(plain[:-pad].decode("utf-8"))
            except Exception as exc:
                logger.warning("feishu webhook decrypt failed: %s", exc)
                return {"status": "decrypt_error"}

        if payload.get("type") == "url_verification":
            return {"challenge": payload.get("challenge", "")}

        event = payload.get("event", {})
        sender = event.get("sender", {}).get("sender_id", {})
        open_id = sender.get("open_id", "")
        message = event.get("message", {})
        msg_type = message.get("message_type", "")

        if msg_type != "text" or not open_id:
            logger.debug("Feishu webhook ignored: msg_type=%s open_id=%s", msg_type, open_id)
            return {"status": "ignored"}

        try:
            text = _json.loads(message.get("content", "{}")).get("text", "").strip()
        except Exception:
            return {"status": "parse_error"}

        if not text:
            return {"status": "empty"}

        # Menu command — serve directly
        if text.lower().lstrip("/") in _MENU_TRIGGER_WORDS or text.lower() in _MENU_TRIGGER_WORDS:
            if feishu:
                try:
                    feishu.send_card(open_id=open_id, card=_FEISHU_MENU_CARD)
                except Exception:
                    feishu.send_text(open_id=open_id, text=_MENU_TEXT_PLAIN)
            return {"status": "accepted"}

        logger.info("Feishu HTTP webhook text from open_id=%s: %s", open_id, text)
        msg = InboundMessage(
            source="feishu",
            sender_id=open_id,
            sender_name=sender.get("user_id", open_id),
            text=text,
            timestamp=message.get("create_time", ""),
        )
        background.add_task(_handle_message, msg, agent, wecom, feishu, outlook)
        return {"status": "accepted"}

    # ── Telegram webhook ──────────────────────────────────────────────────────
    _tg_secret = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")

    @app.post("/hermes/inbound/telegram")
    async def telegram_inbound(request: Request, background: BackgroundTasks):
        # Optional secret-token verification
        if _tg_secret:
            incoming = request.headers.get("x-telegram-bot-api-secret-token", "")
            if not _hmac.compare_digest(incoming, _tg_secret):
                return Response(status_code=401, content="Invalid secret")

        try:
            payload = _json.loads(await request.body())
        except Exception:
            return {"status": "parse_error"}

        update_id = str(payload.get("update_id", ""))

        # ── Inline keyboard callback query ────────────────────────────────────
        cq = payload.get("callback_query")
        if cq:
            cq_id = cq.get("id", "")
            from_user = cq.get("from", {})
            cq_chat_id = str(
                cq.get("message", {}).get("chat", {}).get("id", "")
                or from_user.get("id", "")
            )
            cq_text = _CALLBACK_MAP.get((cq.get("data") or "").strip(),
                                       (cq.get("data") or "").strip())
            cq_sender = from_user.get("username") or from_user.get("first_name", cq_chat_id)
            if telegram:
                telegram.answer_callback_query(cq_id)
            if cq_text and cq_chat_id:
                if not tasks.claim_message(f"tg_cq_{cq_id}"):
                    return {"status": "duplicate"}
                inbound = InboundMessage(
                    source="telegram",
                    sender_id=cq_chat_id,
                    sender_name=cq_sender,
                    text=cq_text,
                    timestamp=update_id,
                )
                background.add_task(_handle_message, inbound, agent, wecom, feishu, outlook, telegram)
            return {"status": "accepted"}

        message = payload.get("message") or payload.get("edited_message")
        if not message:
            return {"status": "no_message"}

        chat_id = str(message.get("chat", {}).get("id", ""))
        from_user = message.get("from", {})
        sender_name = from_user.get("username") or from_user.get("first_name", chat_id)

        # Deduplication via update_id
        if update_id and not tasks.claim_message(f"tg_{update_id}"):
            return {"status": "duplicate"}

        # Text message
        text = (message.get("text") or message.get("caption") or "").strip()
        if text:
            # ── Menu command — serve directly, no agent call ───────────────
            if text.lower().lstrip("/") in _MENU_TRIGGER_WORDS or text.lower() in _MENU_TRIGGER_WORDS:
                if telegram:
                    telegram.send_menu(chat_id, _MENU_TEXT_HTML, _MENU_BUTTONS)
                return {"status": "accepted"}

            inbound = InboundMessage(
                source="telegram",
                sender_id=chat_id,
                sender_name=sender_name,
                text=text,
                timestamp=str(message.get("date", "")),
            )
            background.add_task(_handle_message, inbound, agent, wecom, feishu, outlook, telegram)
            return {"status": "accepted"}

        # Photo
        photos = message.get("photo")
        if photos and telegram and agent.onedrive:
            file_id = photos[-1]["file_id"]  # largest resolution
            filename = f"photo_{message.get('message_id', update_id)}.jpg"
            background.add_task(
                _handle_telegram_file, chat_id, file_id, filename, agent, telegram
            )
            return {"status": "accepted"}

        # Document
        doc = message.get("document")
        if doc and telegram and agent.onedrive:
            file_id = doc["file_id"]
            filename = doc.get("file_name", f"doc_{update_id}")
            background.add_task(
                _handle_telegram_file, chat_id, file_id, filename, agent, telegram
            )
            return {"status": "accepted"}

        return {"status": "ignored"}

    # ── Feishu card action callback (HTTP) ───────────────────────────────────
    @app.post("/hermes/inbound/feishu-card")
    async def feishu_card_inbound(request: Request, background: BackgroundTasks):
        """Receives card button tap callbacks from Feishu.
        Configure this URL in the Feishu developer console:
        Developer Console → App → Features → Bot → Card Action Request URL
        """
        try:
            body = await request.body()
            payload = _json.loads(body)
        except Exception:
            return {"status": "parse_error"}

        # Decrypt if Feishu sent an AES-encrypted payload
        _enc_key = os.environ.get("FEISHU_ENCRYPT_KEY", "")
        if "encrypt" in payload and _enc_key:
            try:
                import base64
                key = hashlib.sha256(_enc_key.encode()).digest()
                raw = base64.b64decode(payload["encrypt"])
                iv, content = raw[:16], raw[16:]
                try:
                    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
                    from cryptography.hazmat.backends import default_backend
                    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
                    dec = cipher.decryptor()
                    plain = dec.update(content) + dec.finalize()
                except ImportError:
                    from Crypto.Cipher import AES as _AES
                    plain = _AES.new(key, _AES.MODE_CBC, iv).decrypt(content)
                pad = plain[-1]
                payload = _json.loads(plain[:-pad].decode("utf-8"))
            except Exception as exc:
                logger.warning("feishu-card decrypt failed: %s", exc)
                return {"status": "decrypt_error"}

        # URL verification handshake
        if "challenge" in payload:
            return {"challenge": payload["challenge"]}

        open_id = payload.get("open_id", "")
        action  = payload.get("action", {})
        value   = action.get("value") or {}
        act     = value.get("act", "")

        # ── Routing-card actions ──────────────────────────────────────────────
        if act == "confirm":
            # User confirmed the auto-detected folder — just clean up
            mid = value.get("mid", "")
            _pending_reroute.pop(mid, None)
            if open_id and feishu:
                feishu.send_text(open_id=open_id, text="✅ 路径确认。")
            return {}

        if act == "route":
            # User tapped a re-route button — re-download and upload to new folder
            mid       = value.get("mid", "")
            new_folder = value.get("to", "")
            info = _pending_reroute.pop(mid, None)
            if not info or not new_folder:
                if open_id and feishu:
                    feishu.send_text(open_id=open_id, text="⚠️ 无法重新归档（记录已过期，请重新发送文件）。")
                return {}

            def _reroute():
                try:
                    fb = feishu.download_resource(
                        info["filename"],  # message_id equivalent not needed — use file_key
                        info["file_key"],
                        info["resource_type"],
                    )
                    result = agent.onedrive.upload_file(
                        folder_path=new_folder,
                        filename=info["filename"],
                        content=fb,
                    )
                    feishu.send_text(
                        open_id=info["sender_id"],
                        text=f"✅ 已重新归档《{result.get('name')}》到 OneDrive/{new_folder.strip('/')}",
                    )
                except Exception as exc:
                    logger.error("Re-route failed: %s", exc)
                    if feishu:
                        feishu.send_text(open_id=info["sender_id"], text=f"重新归档失败：{exc}")

            background.add_task(_reroute)
            return {}

        if act == "set_folder":
            # /save picker — store as pending folder; -1 = unlimited until user sends text
            folder_choice = value.get("to", "")
            if open_id and folder_choice:
                _pending_folders[open_id] = (folder_choice, -1)
                short = folder_choice.replace("etrm/bess-platform/data/", "…/")
                if feishu:
                    feishu.send_text(open_id=open_id, text=f"📁 已设置存档位置：{short}\n请依次发送文件，发送文字消息可取消。")
            return {}

        if act == "survey_region":
            survey_mode = value.get("mode", "report")
            region = value.get("region", "")
            if not open_id or not region:
                return {}
            if survey_mode == "report":
                folder = f"{_SURVEY_REPORT_BASE}/{region}"
                _pending_survey[open_id] = (folder, region, "report", True)
                if feishu:
                    feishu.send_text(open_id=open_id, text=(
                        f"📋 调研报告模式已开启 — {region}\n"
                        f"请发送文字或文件，内容将保存到「调研报告/{region}」并录入知识库。\n"
                        "发送「取消」退出。"
                    ))
            else:  # asset
                # Store region, wait for asset name as next text message
                _pending_survey[open_id] = ("", region, "asset_need_name", True)
                if feishu:
                    feishu.send_text(open_id=open_id, text=(
                        f"🔍 资产调研 — {region}\n"
                        "请输入资产名称（例如：宏海科技光储），我将创建对应目录。"
                    ))
            return {}

        # ── Legacy category menu buttons ─────────────────────────────────────
        cat      = value.get("cat", "")
        question = _CALLBACK_MAP.get(f"cat:{cat}", "")

        if question and open_id:
            inbound = InboundMessage(
                source="feishu",
                sender_id=open_id,
                sender_name=open_id,
                text=question,
                timestamp="",
            )
            background.add_task(_handle_message, inbound, agent, wecom, feishu, outlook)
        return {}

    # ── Feishu WebSocket startup ───────────────────────────────────────────────
    if feishu:
        app_id = os.environ.get("FEISHU_APP_ID", "")
        app_secret = os.environ.get("FEISHU_APP_SECRET", "")

        @app.on_event("startup")
        async def _feishu_ws_startup():
            # Run the blocking lark WS client in a daemon thread so it never
            # blocks the asyncio event loop (health checks / HTTP endpoints).
            t = threading.Thread(
                target=_feishu_ws_thread,
                args=(app_id, app_secret, agent, feishu, outlook),
                daemon=True,
            )
            t.start()
            logger.info("Feishu WebSocket thread started")

    # ── Telegram webhook registration on startup ──────────────────────────────
    if telegram:
        _tg_webhook_url = os.environ.get("TELEGRAM_WEBHOOK_URL", "")

        @app.on_event("startup")
        async def _telegram_webhook_startup():
            if _tg_webhook_url:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None, telegram.set_webhook, _tg_webhook_url, _tg_secret
                )
            else:
                logger.warning("TELEGRAM_WEBHOOK_URL not set — webhook not registered")

    return app


_CHAT_LOCK_MAX = 256  # LRU cap for per-chat threading.Locks


def _feishu_ws_thread(
    app_id: str,
    app_secret: str,
    agent: HermesAgent,
    feishu: FeishuClient,
    outlook: Optional[OutlookClient] = None,
) -> None:
    """Blocking lark WS loop, runs in a daemon thread.

    Running in a thread (not as an asyncio task) is critical: the lark WS SDK
    uses synchronous I/O internally and would block the uvicorn event loop if
    awaited, causing HTTP health-checks and all inbound endpoints to time out.

    Per-chat serial queue: each chat_id gets a threading.Lock (LRU-bounded at
    _CHAT_LOCK_MAX). Messages in the same chat are processed one at a time.
    Reactions: Typing while processing → removed on success, CrossMark on fail.
    """
    import lark_oapi as lark
    from lark_oapi.api.im.v1 import P2ImMessageReceiveV1

    # ── Per-chat LRU lock table (threading.Lock, not asyncio.Lock) ───────────
    _chat_locks: collections.OrderedDict[str, threading.Lock] = collections.OrderedDict()
    _registry_lock = threading.Lock()

    def _get_chat_lock(chat_id: str) -> threading.Lock:
        with _registry_lock:
            lock = _chat_locks.get(chat_id)
            if lock is not None:
                _chat_locks.move_to_end(chat_id)
                return lock
            if len(_chat_locks) >= _CHAT_LOCK_MAX:
                for key in list(_chat_locks):
                    if not _chat_locks[key].locked():
                        _chat_locks.pop(key)
                        break
                else:
                    _chat_locks.pop(next(iter(_chat_locks)))
            lock = threading.Lock()
            _chat_locks[chat_id] = lock
            return lock

    # ── Serialised + reaction-wrapped message dispatch ────────────────────────
    def _enqueue(chat_id: str, message_id: str, fn, *args) -> None:
        """Acquire per-chat lock, add Typing reaction, run fn, clean up."""
        with _get_chat_lock(chat_id):
            reaction_id: Optional[str] = None
            if message_id:
                try:
                    reaction_id = feishu.add_reaction(message_id, "Typing")
                except Exception:
                    pass
            try:
                ok = fn(*args)
            except Exception as exc:
                logger.error("Feishu WS handler error: %s", exc, exc_info=True)
                ok = False
            if message_id:
                if reaction_id:
                    try:
                        feishu.delete_reaction(message_id, reaction_id)
                    except Exception:
                        pass
                if not ok:
                    try:
                        feishu.add_reaction(message_id, "CrossMark")
                    except Exception:
                        pass

    # ── Lark SDK callback (called from lark's internal thread) ───────────────
    def handle(data: P2ImMessageReceiveV1) -> None:
        try:
            event = data.event
            message = event.message
            sender = event.sender.sender_id
            mid = message.message_id or ""
            chat_id = message.chat_id or sender.open_id or ""

            if mid and not agent.tasks.claim_message(mid):
                logger.info("Feishu WS duplicate message_id=%s, skipping", mid)
                return

            content_obj = _json.loads(message.content or "{}")

            if message.message_type == "text":
                text = content_obj.get("text", "").strip()
                if not text:
                    return
                if text.lower().lstrip("/") in _MENU_TRIGGER_WORDS or text.lower() in _MENU_TRIGGER_WORDS:
                    try:
                        feishu.send_card(open_id=sender.open_id or "", card=_FEISHU_MENU_CARD)
                    except Exception:
                        try:
                            feishu.send_text(open_id=sender.open_id or "", text=_MENU_TEXT_PLAIN)
                        except Exception as _me:
                            logger.error("Menu send failed: %s", _me)
                    return
                logger.info("Feishu WS text from %s: %s", sender.open_id, text)
                inbound = InboundMessage(
                    source="feishu",
                    sender_id=sender.open_id or "",
                    sender_name=sender.user_id or sender.open_id or "",
                    text=text,
                    timestamp=message.create_time or "",
                )
                threading.Thread(
                    target=_enqueue,
                    args=(chat_id, mid, _handle_message, inbound, agent, None, feishu, outlook, None),
                    daemon=True,
                ).start()

            elif message.message_type == "file":
                file_key = content_obj.get("file_key", "")
                filename = content_obj.get("file_name", file_key)
                logger.info("Feishu WS file from %s: %s", sender.open_id, filename)
                threading.Thread(
                    target=_enqueue,
                    args=(chat_id, mid, _handle_file_message,
                          sender.open_id or "", mid, file_key, filename, "file", agent, feishu),
                    daemon=True,
                ).start()

            elif message.message_type == "image":
                file_key = content_obj.get("image_key", "")
                ext = content_obj.get("image_type", "jpg")
                filename = f"{file_key}.{ext}"
                logger.info("Feishu WS image from %s: %s", sender.open_id, filename)
                threading.Thread(
                    target=_enqueue,
                    args=(chat_id, mid, _handle_file_message,
                          sender.open_id or "", mid, file_key, filename, "image", agent, feishu),
                    daemon=True,
                ).start()
        except Exception as exc:
            logger.error("Feishu WS handle error: %s", exc, exc_info=True)

    dispatcher = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(handle)
        .build()
    )
    cli = lark.ws.Client(
        app_id,
        app_secret,
        event_handler=dispatcher,
        log_level=lark.LogLevel.INFO,
    )

    while True:
        try:
            logger.info("Feishu WS connecting...")
            cli.start()  # blocking — safe to call from a thread
        except Exception as exc:
            logger.error("Feishu WS connection lost: %s", exc)
            time.sleep(15)


def _handle_file_message(
    sender_id: str,
    message_id: str,
    file_key: str,
    filename: str,
    resource_type: str,
    agent: HermesAgent,
    feishu: Optional[FeishuClient],
) -> bool:
    if not feishu or not agent.onedrive:
        if feishu:
            feishu.send_text(open_id=sender_id, text="OneDrive 未配置，无法保存文件。")
        return False
    # Priority: 0) survey mode, 1) explicit pending folder, 2) explicit AI classify request,
    #           3) DB auto-route rule, 4) auto-classify all documents, 5) Hermes Uploads
    _survey_file_mode = None
    if sender_id in _pending_survey:
        _sv_f, _sv_l, _sv_m, _sv_kb = _pending_survey[sender_id]
        if _sv_m in ("report", "asset_ready"):
            _survey_file_mode = (_sv_f, _sv_l, _sv_kb)

    folder = None
    if _survey_file_mode:
        folder = _survey_file_mode[0]

    _folder_entry = _pending_folders.get(sender_id)
    if _folder_entry:
        folder, _remaining = _folder_entry
        if _remaining == 1:
            _pending_folders.pop(sender_id)       # last file in the batch
        elif _remaining > 1:
            _pending_folders[sender_id] = (folder, _remaining - 1)
        # _remaining == -1 → unlimited, keep as-is
    if folder is None and sender_id in _pending_classify:
        hint = _pending_classify.pop(sender_id)
        try:
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            folder = classify_to_market_fundamentals(filename, hint, api_key)
            logger.info("AI-classified (explicit) '%s' to '%s'", filename, folder)
        except Exception as exc:
            logger.error("AI classification failed: %s", exc)
    matched_rule = None
    if folder is None:
        rules = agent.tasks.get_file_rules()
        for rule in rules:
            if rule["pattern"].lower() in filename.lower():
                year = datetime.now().year
                folder = rule["folder_template"].replace("{year}", str(year))
                matched_rule = rule
                logger.info("Auto-routing '%s' to '%s' via rule %s", filename, folder, rule["id"])
                break
    # Auto-classify all document files (xlsx, pdf, docx, etc.) if no folder yet
    if folder is None and is_document_file(filename):
        try:
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            classified = classify_to_market_fundamentals(filename, "", api_key)
            if classified:  # None means NOT_MARKET
                folder = classified
                logger.info("Auto-classified '%s' → '%s'", filename, folder)
        except Exception as exc:
            logger.error("Auto-classify failed: %s", exc)
    if folder is None:
        folder = "Hermes Uploads"
    try:
        file_bytes = feishu.download_resource(message_id, file_key, resource_type)
        result = agent.onedrive.upload_file(folder_path=folder, filename=filename, content=file_bytes)
        reply = f"✅ 已保存《{result.get('name')}》到 OneDrive/{folder.strip('/')}"
    except Exception as exc:
        logger.error("File upload failed: %s", exc)
        reply = f"上传失败：{exc}"
        feishu.send_text(open_id=sender_id, text=reply)
        return False

    # Show remaining count hint if a multi-file batch is active
    _remaining_hint = ""
    _cur_entry = _pending_folders.get(sender_id)
    if _cur_entry:
        _, _rem = _cur_entry
        if _rem > 0:
            _remaining_hint = f"（还需发送 {_rem} 个文件）"
        elif _rem == -1:
            _remaining_hint = "（发送文字消息可取消批量存档）"

    # Send interactive routing card for all uploaded files so the user can re-route with one tap.
    # Fall back to plain text if card send fails.
    _sent_card = False
    if True:  # always show card for any uploaded file
        try:
            _pending_reroute[message_id] = {
                "sender_id": sender_id,
                "filename": filename,
                "file_key": file_key,
                "resource_type": resource_type,
                "current_folder": folder,
            }
            feishu.send_card(
                open_id=sender_id,
                card=_build_route_card(filename, folder, message_id),
            )
            _sent_card = True
        except Exception as _ce:
            logger.warning("Route card send failed, falling back to text: %s", _ce)
            _pending_reroute.pop(message_id, None)

    if not _sent_card:
        feishu.send_text(open_id=sender_id, text=reply)

    # Knowledge base ingestion (explicit user request)
    if sender_id in _pending_kb_ingest:
        category, hint = _pending_kb_ingest.pop(sender_id)
        kb_reply = agent.ingest_file_to_kb(filename, file_bytes, category=category)
        feishu.send_text(open_id=sender_id, text=kb_reply)
    # Auto KB ingest via file rule
    elif matched_rule and matched_rule.get("auto_kb"):
        try:
            kb_reply = agent.ingest_file_to_kb(filename, file_bytes, category="research_report")
            feishu.send_text(open_id=sender_id, text=kb_reply)
        except Exception as exc:
            logger.error("Auto KB ingest failed: %s", exc)

    # Survey mode KB ingest
    elif _survey_file_mode and _survey_file_mode[2]:  # kb_ingest flag
        try:
            kb_reply = agent.ingest_file_to_kb(filename, file_bytes, category="research_report")
            feishu.send_text(open_id=sender_id, text=kb_reply)
        except Exception as exc:
            logger.error("Survey KB ingest failed: %s", exc)

    # Auto digest via file rule
    if matched_rule and matched_rule.get("auto_digest"):
        try:
            digest = agent.generate_file_digest(filename, file_bytes)
            feishu.send_text(open_id=sender_id, text=digest)
        except Exception as exc:
            logger.error("Auto digest failed: %s", exc)

    # Auto ETL: upsert capacity data into province_installed_monthly
    _should_etl = (matched_rule and matched_rule.get("auto_etl")) or is_capacity_file(filename)
    if _should_etl:
        try:
            pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            result = upsert_capacity(file_bytes, filename, pg_url, api_key)
            if result["upserted"] > 0:
                prov_list = "、".join(result["provinces"][:8])
                if len(result["provinces"]) > 8:
                    prov_list += f"等{len(result['provinces'])}省"
                etl_msg = (
                    f"📊 装机数据已入库（{result['year_month']}）\n"
                    f"更新 {result['upserted']} 个省份：{prov_list}\n"
                    f"bess-map 储能需求Tab已自动更新。"
                )
            else:
                errs = "; ".join(result["errors"][:2])
                etl_msg = f"⚠️ 装机数据入库失败：{errs}"
            feishu.send_text(open_id=sender_id, text=etl_msg)
        except Exception as exc:
            logger.error("Capacity ETL failed: %s", exc, exc_info=True)
            feishu.send_text(open_id=sender_id, text=f"⚠️ 装机数据入库失败：{exc}")

    # Spot market PDF: trigger ingestion pipeline
    if is_spot_pdf(filename) and resource_type == "file":
        try:
            logger.info("spot_ingest: triggering pipeline for %s", filename)
            summary = ingest_pdf_bytes(filename, file_bytes)
            if summary["errors"]:
                feishu.send_text(open_id=sender_id, text=f"⚠️ 数据入库部分失败：{summary['errors'][0]}")
            elif summary["dates"]:
                date_str = ", ".join(summary["dates"])
                feishu.send_text(
                    open_id=sender_id,
                    text=f"📊 现货数据已入库：{date_str}，{summary['provinces']} 省，共 {summary['upserted']} 行",
                )
        except Exception as exc:
            logger.error("spot_ingest: pipeline error: %s", exc, exc_info=True)
            feishu.send_text(open_id=sender_id, text=f"⚠️ 现货数据入库失败：{exc}")

    return True


def _handle_telegram_file(
    chat_id: str,
    file_id: str,
    filename: str,
    agent: HermesAgent,
    telegram: TelegramClient,
) -> bool:
    """Download a Telegram file and upload to OneDrive, with spot ingest if applicable."""
    if not agent.onedrive:
        telegram.send_text(chat_id, "OneDrive 未配置，无法保存文件。")
        return False
    telegram.send_typing(chat_id)
    folder = _pending_folders.pop(chat_id, None)
    matched_rule = None
    if folder is None:
        rules = agent.tasks.get_file_rules()
        for rule in rules:
            if rule["pattern"].lower() in filename.lower():
                year = datetime.now().year
                folder = rule["folder_template"].replace("{year}", str(year))
                matched_rule = rule
                break
    if folder is None and is_document_file(filename):
        try:
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            classified = classify_to_market_fundamentals(filename, "", api_key)
            if classified:
                folder = classified
                logger.info("Auto-classified (Telegram) '%s' → '%s'", filename, folder)
        except Exception as exc:
            logger.error("Auto-classify (Telegram) failed: %s", exc)
    if folder is None:
        folder = "Hermes Uploads"
    try:
        file_bytes = telegram.get_file_bytes(file_id)
        if not file_bytes:
            telegram.send_text(chat_id, "文件下载失败。")
            return False
        result = agent.onedrive.upload_file(folder_path=folder, filename=filename, content=file_bytes)
        reply = f"✅ 已保存《{result.get('name')}》到 OneDrive/{folder.strip('/')}"
    except Exception as exc:
        logger.error("Telegram file upload failed: %s", exc)
        telegram.send_text(chat_id, f"上传失败：{exc}")
        return False

    telegram.send_text(chat_id, reply)

    # Knowledge base ingestion
    if chat_id in _pending_kb_ingest:
        category, hint = _pending_kb_ingest.pop(chat_id)
        kb_reply = agent.ingest_file_to_kb(filename, file_bytes, category=category)
        telegram.send_text(chat_id, kb_reply)
    elif matched_rule and matched_rule.get("auto_kb"):
        try:
            kb_reply = agent.ingest_file_to_kb(filename, file_bytes, category="research_report")
            telegram.send_text(chat_id, kb_reply)
        except Exception as exc:
            logger.error("Auto KB ingest (Telegram) failed: %s", exc)

    if matched_rule and matched_rule.get("auto_digest"):
        try:
            digest = agent.generate_file_digest(filename, file_bytes)
            telegram.send_text(chat_id, digest)
        except Exception as exc:
            logger.error("Auto digest (Telegram) failed: %s", exc)

    # Auto ETL: upsert capacity data
    _should_etl_tg = (matched_rule and matched_rule.get("auto_etl")) or is_capacity_file(filename)
    if _should_etl_tg:
        try:
            pg_url = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            result = upsert_capacity(file_bytes, filename, pg_url, api_key)
            if result["upserted"] > 0:
                prov_list = "、".join(result["provinces"][:8])
                etl_msg = (
                    f"📊 装机数据已入库（{result['year_month']}）\n"
                    f"更新 {result['upserted']} 个省份：{prov_list}\n"
                    f"bess-map 储能需求Tab已自动更新。"
                )
            else:
                errs = "; ".join(result["errors"][:2])
                etl_msg = f"⚠️ 装机数据入库失败：{errs}"
            telegram.send_text(chat_id, etl_msg)
        except Exception as exc:
            logger.error("Capacity ETL (Telegram) failed: %s", exc, exc_info=True)

    # Spot market PDF ingest
    if is_spot_pdf(filename):
        try:
            summary = ingest_pdf_bytes(filename, file_bytes)
            if summary["errors"]:
                telegram.send_text(chat_id, f"⚠️ 数据入库部分失败：{summary['errors'][0]}")
            elif summary["dates"]:
                date_str = ", ".join(summary["dates"])
                telegram.send_text(
                    chat_id,
                    f"📊 现货数据已入库：{date_str}，{summary['provinces']} 省，共 {summary['upserted']} 行",
                )
        except Exception as exc:
            logger.error("Telegram spot_ingest error: %s", exc)
            telegram.send_text(chat_id, f"⚠️ 现货数据入库失败：{exc}")

    return True


def _handle_message(
    msg: InboundMessage,
    agent: HermesAgent,
    wecom: Optional[WeComClient],
    feishu: Optional[FeishuClient] = None,
    outlook: Optional[OutlookClient] = None,
    telegram: Optional[TelegramClient] = None,
) -> bool:
    import re as _re
    chat_id = msg.sender_id or ""

    # ── Clear unlimited pending folder on any text message ───────────────────
    # (unlimited batches set via /save card use count=-1; a new text message ends the batch)
    _fe = _pending_folders.get(chat_id)
    if _fe and _fe[1] == -1:
        _pending_folders.pop(chat_id, None)

    # ── LingFeng password update (intercept before agent routing) ────────────
    # Accepts: "lingfeng password: NEW_PW"  /  "/lingfeng_password NEW_PW"
    #          "lingfeng密码: NEW_PW"        /  "lingfeng pw NEW_PW"
    _lf_pw: str | None = None
    _lf_m = _re.search(
        r'(?:lingfeng|灵峰|凌峰)[\s\S]{0,20}?(?:password|密码|pw)\s*[:\s：]+\s*(\S+)',
        msg.text, _re.I,
    )
    if not _lf_m:
        _lf_m = _re.match(r'/?(?:lingfeng_password|lf_pw)\s+(\S+)', msg.text.strip(), _re.I)
    if _lf_m:
        _lf_pw = _lf_m.group(1).strip()

    if _lf_pw:
        try:
            agent.tasks.set_setting("lingfeng_new_password", _lf_pw)
            logger.info("LingFeng new password stored via Hermes from sender=%s", chat_id)
        except Exception as _e:
            logger.error("Failed to store lingfeng_new_password: %s", _e)
        _lf_reply = (
            "✅ LingFeng新密码已保存。\n"
            "下次定时运行（凌晨4点）将自动恢复采集并补填缺失数据，完成后通知您。\n\n"
            "✅ LingFeng new password saved.\n"
            "Scraping resumes at the next scheduled run (04:00) and will backfill missing data automatically."
        )
        try:
            if msg.source == "feishu" and feishu:
                feishu.send_text(open_id=msg.sender_id, text=_lf_reply)
            elif msg.source == "telegram" and telegram:
                telegram.send_text(chat_id=msg.sender_id, text=_lf_reply)
            elif msg.source == "wecom" and wecom:
                wecom.send_text(user_id=msg.sender_id, text=_lf_reply)
        except Exception as _e:
            logger.error("Failed to reply lingfeng password ack: %s", _e)
        return True

    # ── LingFeng manual backfill trigger ──────────────────────────────────────
    # Accepts: "lingfeng run"  /  "lingfeng backfill"  /  "lingfeng补填"
    #          "lingfeng backfill 2026-01-01"  /  "lingfeng backfill 2026-01-01 to 2026-01-31"
    #          "/lf_run"  /  "/lf_run 2026-01-01"  /  "/lf_run 2026-01-01:2026-01-31"
    _lf_run_m = _re.search(
        r'(?:lingfeng|灵峰|凌峰)[\s\S]{0,10}?(?:run|backfill|补填|补数|跑一下|执行)',
        msg.text, _re.I,
    )
    if not _lf_run_m:
        _lf_run_m = _re.match(r'/?lf_run\b', msg.text.strip(), _re.I)
    if _lf_run_m:
        # Extract optional date range from anywhere in the message
        _dates = _re.findall(r'\d{4}-\d{2}-\d{2}', msg.text)
        if len(_dates) >= 2:
            _trigger_val = f"{_dates[0]}:{_dates[1]}"
            _date_desc = f"{_dates[0]} → {_dates[1]}"
        elif len(_dates) == 1:
            _trigger_val = _dates[0]
            _date_desc = f"{_dates[0]} → yesterday"
        else:
            _trigger_val = "auto"
            _date_desc = "last 7 days"
        try:
            agent.tasks.set_setting("lingfeng_trigger_run", _trigger_val)
            logger.info("LingFeng run triggered via Hermes: %s from sender=%s", _trigger_val, chat_id)
        except Exception as _e:
            logger.error("Failed to store lingfeng_trigger_run: %s", _e)
        _run_reply = (
            f"⏳ LingFeng补填已触发（{_date_desc}）。\n"
            f"将在15分钟内开始运行，完成后通知您。\n\n"
            f"⏳ LingFeng backfill triggered ({_date_desc}).\n"
            f"Will start within 15 minutes. You'll be notified on completion."
        )
        try:
            if msg.source == "feishu" and feishu:
                feishu.send_text(open_id=msg.sender_id, text=_run_reply)
            elif msg.source == "telegram" and telegram:
                telegram.send_text(chat_id=msg.sender_id, text=_run_reply)
            elif msg.source == "wecom" and wecom:
                wecom.send_text(user_id=msg.sender_id, text=_run_reply)
        except Exception as _e:
            logger.error("Failed to reply lingfeng run ack: %s", _e)
        return True
    # ── /news command — manually trigger news screener ───────────────────────
    if _re.match(r'^/?(?:news|新闻|资讯|news.screener)$', msg.text.strip(), _re.I):
        def _news_reply(text: str) -> None:
            try:
                if msg.source == "feishu" and feishu:
                    feishu.send_text(open_id=msg.sender_id, text=text)
                elif msg.source == "telegram" and telegram:
                    telegram.send_text(chat_id=msg.sender_id, text=text)
                elif msg.source == "wecom" and wecom:
                    wecom.send_text(user_id=msg.sender_id, text=text)
            except Exception as _e:
                logger.error("news reply failed: %s", _e)

        _pg = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
        if not _pg:
            _news_reply("⚠️ 数据库未配置，无法运行新闻筛查。")
            return True
        _news_reply("⏳ 正在运行新闻筛查，稍候…")

        def _run_news():
            try:
                _screen_news_sources(
                    pg_url=_pg,
                    api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
                    feishu=feishu,
                    owner_open_id=os.environ.get("FEISHU_OWNER_OPEN_ID", msg.sender_id),
                )
            except Exception as _e:
                logger.error("Manual news screener failed: %s", _e)
                _news_reply(f"⚠️ 新闻筛查失败：{_e}")

        import threading as _threading
        _threading.Thread(target=_run_news, daemon=True).start()
        return True

    # ── /report command — manually trigger a scheduled report ───────────────
    # Accepts: "/report mengxi"  /  "/报告 蒙西"  /  "resend mengxi report"  /  "蒙西储能日报"
    _report_m = _re.match(r'^/?report(?:\s+(mengxi|蒙西|ranking))?$', msg.text.strip(), _re.I)
    if not _report_m:
        _report_m = _re.search(r'resend.{0,10}(mengxi|蒙西|ranking).{0,10}report', msg.text.strip(), _re.I)
    if not _report_m:
        _report_m = _re.match(r'^蒙西储能日报$', msg.text.strip()) or (msg.text.strip() == "蒙西储能日报")
    if _report_m:
        def _send_reply(text: str) -> None:
            try:
                if msg.source == "feishu" and feishu:
                    feishu.send_text(open_id=msg.sender_id, text=text)
                elif msg.source == "telegram" and telegram:
                    telegram.send_text(chat_id=msg.sender_id, text=text)
                elif msg.source == "wecom" and wecom:
                    wecom.send_text(user_id=msg.sender_id, text=text)
            except Exception as _e:
                logger.error("report reply send failed: %s", _e)

        _pg = os.environ.get("PGURL") or os.environ.get("HERMES_DB_URL", "")
        _oid = os.environ.get("FEISHU_OWNER_OPEN_ID", "")
        if not _pg:
            _send_reply("⚠️ 数据库未配置，无法生成报告。")
            return True
        _send_reply("⏳ 正在生成蒙西BESS排名日报，稍候…")

        def _run_mengxi_report():
            try:
                _send_mengxi_ranking(feishu=feishu, owner_open_id=_oid, pg_url=_pg,
                                     onedrive_client=agent.onedrive)
            except Exception as _e:
                logger.error("Manual mengxi report failed: %s", _e)
                _send_reply(f"⚠️ 报告生成失败：{_e}")

        import threading
        threading.Thread(target=_run_mengxi_report, daemon=True).start()
        return True

    # ── Survey mode — 调研报告 / 资产调研 ────────────────────────────────────
    _txt = msg.text.strip()

    # Show region picker cards
    if _txt in ("调研报告", "市场调研") and msg.source == "feishu" and feishu:
        try:
            feishu.send_card(open_id=msg.sender_id, card=_build_survey_card("report"))
        except Exception as _se:
            logger.error("survey report card failed: %s", _se)
        return True

    if _txt in ("资产调研",) and msg.source == "feishu" and feishu:
        try:
            feishu.send_card(open_id=msg.sender_id, card=_build_survey_card("asset"))
        except Exception as _se:
            logger.error("survey asset card failed: %s", _se)
        return True

    # Cancel survey mode
    if _re.match(r'^/?取消$|^/?cancel$', _txt, _re.I) and chat_id in _pending_survey:
        _pending_survey.pop(chat_id, None)
        if msg.source == "feishu" and feishu:
            feishu.send_text(open_id=msg.sender_id, text="✅ 已退出调研记录模式。")
        return True

    # Handle text when survey mode is active
    if chat_id in _pending_survey:
        _sv_folder, _sv_label, _sv_mode, _sv_kb = _pending_survey[chat_id]

        def _survey_reply(text: str) -> None:
            try:
                if msg.source == "feishu" and feishu:
                    feishu.send_text(open_id=msg.sender_id, text=text)
                elif msg.source == "telegram" and telegram:
                    telegram.send_text(chat_id=msg.sender_id, text=text)
            except Exception as _e:
                logger.error("survey reply failed: %s", _e)

        if _sv_mode == "asset_need_name":
            # Text is the asset name — set up the folder and advance state
            asset_name = _txt[:60]  # cap length for folder name safety
            folder = f"{_SURVEY_ASSET_BASE}/{_sv_label}/{asset_name}"
            _pending_survey[chat_id] = (folder, f"{_sv_label}/{asset_name}", "asset_ready", True)
            _survey_reply(
                f"🔍 资产调研模式已开启 — {_sv_label} / {asset_name}\n"
                f"请发送文字调研笔记或相关文件，内容将保存到「assets/调研/{_sv_label}/{asset_name}」并录入知识库。\n"
                "发送「取消」退出。"
            )
            return True

        if _sv_mode in ("report", "asset_ready"):
            # Save text as a markdown note file to OneDrive
            from datetime import datetime as _dt2, timezone as _tz2, timedelta as _td2
            _ts = _dt2.now(tz=_tz2(_td2(hours=8))).strftime("%Y%m%d_%H%M%S")
            _note_filename = f"{_ts}_调研笔记.md"
            _note_content = f"# 调研笔记 — {_sv_label}\n\n{_txt}\n".encode("utf-8")
            try:
                if not agent.onedrive:
                    raise RuntimeError("OneDrive 未配置")
                result = agent.onedrive.upload_file(
                    folder_path=_sv_folder,
                    filename=_note_filename,
                    content=_note_content,
                )
                _kb_hint = ""
                if _sv_kb:
                    try:
                        _kb_msg = agent.ingest_file_to_kb(_note_filename, _note_content, category="research_report")
                        _kb_hint = f"\n{_kb_msg}"
                    except Exception as _kbe:
                        logger.warning("Survey KB ingest failed: %s", _kbe)
                _survey_reply(
                    f"✅ 已保存笔记《{result.get('name')}》到 OneDrive/{_sv_folder.strip('/')}"
                    f"{_kb_hint}"
                )
            except Exception as exc:
                logger.error("Survey note upload failed: %s", exc)
                _survey_reply(f"⚠️ 保存失败：{exc}")
            return True

    # ── /save command — show folder picker card for next upload ─────────────
    if _re.match(r'^/?save$', msg.text.strip(), _re.I) and msg.source == "feishu" and feishu:
        try:
            feishu.send_card(open_id=msg.sender_id, card=_build_save_picker_card())
        except Exception as _se:
            logger.error("/save card failed: %s", _se)
            feishu.send_text(open_id=msg.sender_id, text="发送文件夹选择卡片失败，请使用文字指定路径（如：存到山东）。")
        return True

    # ── /model command — switch LLM without consuming agent tokens ───────────
    _model_m = _re.match(r'^/?model(?:\s+(\S+))?$', msg.text.strip(), _re.I)
    if _model_m:
        arg = (_model_m.group(1) or "").strip().lower()
        avail = agent.available_models()
        _model_icons = {"gpt": "🟦", "deepseek": "🟩", "claude": "🟧"}

        def _send_model_reply(text: str) -> None:
            try:
                if msg.source == "feishu" and feishu:
                    feishu.send_text(open_id=msg.sender_id, text=text)
                elif msg.source == "telegram" and telegram:
                    telegram.send_text(chat_id=msg.sender_id, text=text)
                elif msg.source == "wecom" and wecom:
                    wecom.send_text(user_id=msg.sender_id, text=text)
            except Exception as _e:
                logger.error("model reply send failed: %s", _e)

        if not arg or arg == "status":
            # Show current model and available options
            current = agent.get_model_pref(chat_id)
            lines = [f"🤖 Current model: {agent._MODEL_LABELS.get(current, current)}", ""]
            lines.append("Available models:")
            for m in avail:
                icon = _model_icons.get(m, "⬜")
                lines.append(f"  {icon} /model {m}  —  {agent._MODEL_LABELS.get(m, m)}")
            lines.append("  🔄 /model auto  —  Auto (priority chain)")
            _send_model_reply("\n".join(lines))
        else:
            try:
                canon = agent.set_model_pref(chat_id, arg)
                if canon != "auto" and canon not in avail:
                    _send_model_reply(
                        f"⚠️ Model '{agent._MODEL_LABELS.get(canon, canon)}' is not configured "
                        f"(missing API key). Set as preference anyway — will fall back to next available."
                    )
                else:
                    label = agent._MODEL_LABELS.get(canon, canon)
                    _send_model_reply(f"✅ Model switched to: {label}")
            except ValueError as ve:
                avail_str = ", ".join(f"/model {m}" for m in [*avail, "auto"])
                _send_model_reply(f"❌ {ve}\nAvailable: {avail_str}")
        return True
    # ─────────────────────────────────────────────────────────────────────────

    try:
        action = agent.process(msg, chat_id=chat_id)
        if action.action == "SAVE_NEXT_FILE":
            folder = action.params.get("folder_path", "Hermes Uploads")
            count  = int(action.params.get("count", 1))
            _pending_folders[msg.sender_id] = (folder, count)
            reply = action.reply or f"好的，把文件发给我，我帮你存到 OneDrive/{folder.strip('/')}"
        elif action.action == "CLASSIFY_NEXT_FILE":
            hint = action.params.get("hint", "")
            _pending_classify[msg.sender_id] = hint
            reply = action.reply or "好的，把文件发给我，我会自动归类到市场基础信息的对应目录。"
        elif action.action == "INGEST_NEXT_FILE":
            category = action.params.get("category", "")
            hint = action.params.get("hint", "")
            _pending_kb_ingest[msg.sender_id] = (category, hint)
            reply = action.reply or "好的，把文件发给我，我帮你添加到知识库。"
        elif action.action == "EMAIL_SUMMARY":
            if not outlook:
                reply = "邮件未配置。请先运行 py scripts/auth_microsoft_mail.py 并设置 OUTLOOK_REFRESH_TOKEN。"
            else:
                try:
                    limit = int(action.params.get("limit", 20))
                    messages = outlook.list_messages(limit=limit, unread_only=True)
                    if not messages:
                        reply = "✅ 收件箱没有未读邮件。"
                    else:
                        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
                        summary = summarize_emails(messages, api_key)
                        reply = f"📧 邮件摘要（{len(messages)} 封未读）\n\n{summary}"
                except Exception as exc:
                    logger.error("EMAIL_SUMMARY failed: %s", exc)
                    reply = f"读取邮件失败：{exc}"
        else:
            extra = agent.execute(action)
            reply = extra if extra else action.reply
        logger.info("Action=%s reply_len=%s", action.action, len(reply) if reply else 0)
        if not reply:
            logger.warning("Empty reply for action=%s params=%s", action.action, action.params)
        if reply:
            # Append Beijing time to every Hermes reply
            from datetime import datetime as _dt, timezone as _tz, timedelta as _td
            _bj_now = _dt.now(tz=_tz(_td(hours=8)))
            reply = f"{reply}\n─\n[{_bj_now.strftime('%Y-%m-%d %H:%M')} 北京时间]"
            if msg.source == "feishu" and feishu:
                feishu.send_text(open_id=msg.sender_id, text=reply)
            elif msg.source == "wecom" and wecom:
                wecom.send_text(user_id=msg.sender_id, text=reply)
            elif msg.source == "telegram" and telegram:
                telegram.send_text(chat_id=msg.sender_id, text=reply)
            # process() already saved raw JSON to assistant history.
            # Only run post-processing here (insight extraction for MARKET_AGENT).
            try:
                mem = agent._get_memory()
                if mem and chat_id and action.action == "MARKET_AGENT":
                    mem.extract_and_save_insights(chat_id, msg.text, reply)
            except Exception as _mem_err:
                logger.debug("Memory insight extraction failed: %s", _mem_err)

        # Send chart image if GENERATE_CHART produced one
        _chart_bytes = getattr(agent, "_pending_chart_bytes", None)
        if _chart_bytes:
            agent._pending_chart_bytes = None
            try:
                if msg.source == "feishu" and feishu:
                    image_key = feishu.upload_image(_chart_bytes)
                    feishu.send_image(open_id=msg.sender_id, image_key=image_key)
                    logger.info("Chart sent to %s (%d KB)", msg.sender_id, len(_chart_bytes) // 1024)
                elif msg.source == "telegram" and telegram:
                    telegram.send_photo(chat_id=msg.sender_id, photo_bytes=_chart_bytes)
            except Exception as _ce:
                logger.error("Chart image send failed: %s", _ce)
                try:
                    _err_msg = f"⚠️ 图表已生成但发送失败：{_ce}"
                    if msg.source == "feishu" and feishu:
                        feishu.send_text(open_id=msg.sender_id, text=_err_msg)
                except Exception:
                    pass

        return True
    except Exception as e:
        logger.error("Error handling message: %s", e)
        return False


if os.environ.get("HERMES_DB_URL"):
    app = create_app()
