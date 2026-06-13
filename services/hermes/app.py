from __future__ import annotations
import os
import logging
from typing import Optional
from fastapi import FastAPI, BackgroundTasks, Query, Response
from apscheduler.schedulers.background import BackgroundScheduler
from services.hermes.models import InboundMessage
from services.hermes.agent import HermesAgent
from services.hermes.planka_client import PlankaClient
from services.hermes.wecom_client import WeComClient
from services.hermes.wechat_client import WechatyBridgeClient
from services.hermes.scheduler import send_due_reminders

logger = logging.getLogger(__name__)


def _make_clients():
    planka = PlankaClient(
        base_url=os.environ["PLANKA_BASE_URL"],
        email=os.environ["PLANKA_EMAIL"],
        password=os.environ["PLANKA_PASSWORD"],
    )
    wecom = None
    if os.environ.get("WECOM_CORP_ID"):
        wecom = WeComClient(
            corp_id=os.environ["WECOM_CORP_ID"],
            agent_id=int(os.environ["WECOM_AGENT_ID"]),
            secret=os.environ["WECOM_SECRET"],
        )
    wechat_bridge = WechatyBridgeClient(bridge_url=os.environ["WECHATY_BRIDGE_URL"])
    agent = HermesAgent(planka=planka, anthropic_api_key=os.environ["ANTHROPIC_API_KEY"])
    return planka, wecom, wechat_bridge, agent


def create_app() -> FastAPI:
    planka, wecom, wechat_bridge, agent = _make_clients()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        send_due_reminders,
        "interval",
        minutes=15,
        kwargs={
            "planka": planka,
            "wecom": wecom,
            "wechat_bridge": wechat_bridge,
            "wecom_user_id": os.environ.get("WECOM_USER_ID", "@all"),
            "wechat_id": os.environ.get("WECHAT_OWNER_ID", ""),
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
        background.add_task(_handle_message, msg, agent, wecom, wechat_bridge)
        return {"status": "accepted"}

    @app.post("/hermes/inbound/wechat")
    async def wechat_inbound(msg: InboundMessage, background: BackgroundTasks):
        background.add_task(_handle_message, msg, agent, wecom, wechat_bridge)
        return {"status": "accepted"}

    return app


def _handle_message(
    msg: InboundMessage,
    agent: HermesAgent,
    wecom: Optional[WeComClient],
    wechat_bridge: WechatyBridgeClient,
) -> None:
    try:
        action = agent.process(msg)
        agent.execute(action)
        if action.reply:
            if msg.source == "wecom" and wecom:
                wecom.send_text(user_id=msg.sender_id, text=action.reply)
            else:
                wechat_bridge.send(to=msg.sender_id, text=action.reply)
    except Exception as e:
        logger.error("Error handling message: %s", e)


# Module-level app for uvicorn; only created when env vars are present.
# Tests call create_app() directly after patching _make_clients.
if os.environ.get("PLANKA_BASE_URL"):
    app = create_app()
