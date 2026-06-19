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

# sender_id → folder path for the next file upload
_pending_folders: dict[str, str] = {}
# sender_id → user hint for AI-based market-fundamentals classification
_pending_classify: dict[str, str] = {}
# sender_id → (category, hint) for knowledge base ingestion
_pending_kb_ingest: dict[str, tuple[str, str]] = {}
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
from services.hermes.spot_ingest_bridge import is_spot_pdf, ingest_pdf_bytes
from services.hermes.market_classifier import classify_to_market_fundamentals
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Hermes category menu ───────────────────────────────────────────────────────
_MENU_TRIGGER_WORDS = {"/start", "/menu", "/help", "start", "menu", "help",
                       "菜单", "帮助菜单", "/菜单"}

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
    scheduler.add_job(
        send_due_reminders,
        "interval",
        minutes=15,
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
        action = payload.get("action", {})
        cat = (action.get("value") or {}).get("cat", "")
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
    # Priority: 1) explicit pending folder, 2) AI classify, 3) DB auto-route rule, 4) default
    folder = _pending_folders.pop(sender_id, None)
    if folder is None and sender_id in _pending_classify:
        hint = _pending_classify.pop(sender_id)
        try:
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            folder = classify_to_market_fundamentals(filename, hint, api_key)
            logger.info("AI-classified '%s' to '%s'", filename, folder)
        except Exception as exc:
            logger.error("AI classification failed: %s", exc)
    if folder is None:
        rules = agent.tasks.get_file_rules()
        for rule in rules:
            if rule["pattern"].lower() in filename.lower():
                year = datetime.now().year
                folder = rule["folder_template"].replace("{year}", str(year))
                logger.info("Auto-routing '%s' to '%s' via rule %s", filename, folder, rule["id"])
                break
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

    feishu.send_text(open_id=sender_id, text=reply)

    # Knowledge base ingestion (explicit user request)
    if sender_id in _pending_kb_ingest:
        category, hint = _pending_kb_ingest.pop(sender_id)
        kb_reply = agent.ingest_file_to_kb(filename, file_bytes, category=category)
        feishu.send_text(open_id=sender_id, text=kb_reply)

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
    if folder is None:
        rules = agent.tasks.get_file_rules()
        for rule in rules:
            if rule["pattern"].lower() in filename.lower():
                year = datetime.now().year
                folder = rule["folder_template"].replace("{year}", str(year))
                break
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
    # ─────────────────────────────────────────────────────────────────────────

    try:
        action = agent.process(msg, chat_id=chat_id)
        if action.action == "SAVE_NEXT_FILE":
            folder = action.params.get("folder_path", "Hermes Uploads")
            _pending_folders[msg.sender_id] = folder
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
        if reply:
            if msg.source == "feishu" and feishu:
                feishu.send_text(open_id=msg.sender_id, text=reply)
            elif msg.source == "wecom" and wecom:
                wecom.send_text(user_id=msg.sender_id, text=reply)
            elif msg.source == "telegram" and telegram:
                telegram.send_text(chat_id=msg.sender_id, text=reply)
            # Save assistant reply to conversation memory
            try:
                mem = agent._get_memory()
                if mem and chat_id:
                    mem.save_turn(chat_id, "assistant", reply)
                    # Auto-extract insights from MARKET_AGENT answers
                    if action.action == "MARKET_AGENT":
                        mem.extract_and_save_insights(chat_id, msg.text, reply)
            except Exception as _mem_err:
                logger.debug("Memory save failed: %s", _mem_err)
        return True
    except Exception as e:
        logger.error("Error handling message: %s", e)
        return False


if os.environ.get("HERMES_DB_URL"):
    app = create_app()
