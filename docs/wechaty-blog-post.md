---
slug: hermes-ai-wechat-trading-assistant
title: "Hermes: Building an AI-Powered WeChat Personal Assistant for Energy Trading Operations"
author: MonteCarlo79
author_title: Energy Trading Developer
author_url: https://github.com/MonteCarlo79
author_image_url: https://avatars.githubusercontent.com/MonteCarlo79
tags: [wechaty, padlocal, claude, chatbot, python, fastapi, energy-trading]
---

> Hermes is an AI-powered personal assistant that listens to WeChat messages, understands natural language requests, and manages a Kanban task board — purpose-built for a battery energy storage (BESS) trading desk.

[![Powered by Wechaty](https://img.shields.io/badge/Powered%20By-Wechaty-brightgreen.svg)](https://wechaty.js.org)
[![Wechaty Contributor Program](https://img.shields.io/badge/Wechaty-Contributor%20Program-green.svg)](https://wechaty.js.org/docs/contributing/)

<!--truncate-->

## Background

Running an energy trading desk generates a constant stream of operational tasks: market data anomalies to investigate, settlement figures to verify, reports to send, model parameters to update. These tasks arrive via WeChat messages at all hours.

The existing workflow was: read message → manually create a Kanban card → remember to follow up. It worked, but the friction meant small tasks got lost.

**Hermes** solves this by making WeChat itself the task interface. Send a message like *"remind me to check the Inner Mongolia settlement tomorrow morning"* and Hermes creates the card, sets the due date, and sends a reminder when it comes due — all without leaving WeChat.

## Architecture

```
WeChat (personal) ──► Wechaty Bridge (Node.js / EC2)
                              │
                              ▼ HTTP POST
                        Hermes Service (Python / FastAPI / ECS Fargate)
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
             Claude (Anthropic)    Planka Kanban API
             (intent parsing)      (task CRUD)
```

The system has two parts:

1. **Wechaty Bridge** — a lightweight Node.js process on EC2 that maintains the WeChat session via PadLocal and forwards inbound messages to the Hermes FastAPI service over HTTP.
2. **Hermes Service** — a Python/FastAPI application on AWS ECS Fargate. It uses Claude to parse intent and executes the appropriate Planka operation.

A background scheduler (APScheduler) runs every 15 minutes to check for cards due within 24 hours and send reminders back via WeChat.

## The Wechaty Bridge

The bridge (`apps/hermes-wechaty/index.js`) is intentionally minimal — it handles the WeChat session and acts as a dumb HTTP relay:

```javascript
const { WechatyBuilder } = require('wechaty');
const express = require('express');
const axios = require('axios');
const QRCode = require('qrcode');

const HERMES_INBOUND_URL = process.env.HERMES_INBOUND_URL;
const BRIDGE_PORT = parseInt(process.env.BRIDGE_PORT || '3000', 10);
const QR_PORT = parseInt(process.env.QR_PORT || '3001', 10);

let currentQrData = null;

const bot = WechatyBuilder.build({
  name: 'hermes-wechat',
  puppet: 'wechaty-puppet-padlocal',
  puppetOptions: {
    token: process.env.PADLOCAL_TOKEN,
  },
});

bot.on('scan', async (qrcode, status) => {
  console.log(`QR scan required (status=${status})`);
  currentQrData = qrcode;
});

bot.on('login', (user) => {
  console.log(`Logged in as: ${user}`);
  currentQrData = null;
});

bot.on('message', async (message) => {
  if (message.self() || message.room()) return;
  if (!message.text()) return;

  const contact = message.talker();
  const payload = {
    source: 'wechat',
    sender_id: contact.id,
    sender_name: contact.name(),
    text: message.text(),
    timestamp: message.date().toISOString(),
  };

  await axios.post(HERMES_INBOUND_URL, payload, { timeout: 5000 });
});

bot.start();
```

A small Express server on port 3001 serves the QR code as an HTML page during initial setup — useful when running headlessly on a remote server.

## The AI Agent (Python / Claude)

The core of Hermes is a Claude-powered agent that maps natural language to structured Planka operations:

```python
# services/hermes/agent.py
SYSTEM_PROMPT = """
You are Hermes, an assistant for a trading desk. You help manage tasks on a Kanban board.

Given a message, decide what to do:
- CREATE: create a new card (extract title, due date if mentioned)
- LIST: list open cards
- DONE: mark a card as complete
- REPLY: reply with information (no board action needed)

Respond in JSON: {"action": "CREATE|LIST|DONE|REPLY", "params": {...}, "reply": "..."}
"""

class HermesAgent:
    def __init__(self, planka: PlankaClient, anthropic_api_key: str):
        self.planka = planka
        self.client = Anthropic(api_key=anthropic_api_key)

    def process(self, msg: InboundMessage) -> Action:
        response = self.client.messages.create(
            model="claude-opus-4-6",
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": msg.text}],
        )
        return Action.model_validate_json(response.content[0].text)

    def execute(self, action: Action) -> None:
        if action.action == "CREATE":
            self.planka.create_card(**action.params)
        elif action.action == "DONE":
            self.planka.complete_card(**action.params)
```

## Due-Date Reminders

A scheduler checks Planka for cards due within the next 24 hours and sends reminders back to WeChat:

```python
# services/hermes/scheduler.py
def send_due_reminders(planka, wechat_bridge, wechat_id, within_hours=24):
    cards = planka.get_due_soon_cards(within_hours=within_hours)
    for card in cards:
        due_str = card.get("dueDate", "")[:10]
        text = f"Reminder: '{card['name']}' is due on {due_str}"
        wechat_bridge.send(to=wechat_id, text=text)
```

The FastAPI app wires everything together with a 15-minute background job:

```python
# services/hermes/app.py
scheduler = BackgroundScheduler()
scheduler.add_job(send_due_reminders, "interval", minutes=15, kwargs={...})
scheduler.start()

@app.post("/hermes/inbound/wechat")
async def wechat_inbound(msg: InboundMessage, background: BackgroundTasks):
    background.add_task(_handle_message, msg, agent, wechat_bridge)
    return {"status": "accepted"}
```

## Deployment

The bridge runs on a `t3.micro` EC2 instance to maintain a persistent WeChat session, managed by PM2. The Hermes FastAPI service runs on AWS ECS Fargate.

```
EC2 (t3.micro, PM2)               ECS Fargate
┌─────────────────┐               ┌──────────────────────┐
│ hermes-wechaty  │──────────────►│ Hermes FastAPI        │
│ (Node.js +      │  HTTP POST    │ /hermes/inbound/wechat│
│  PadLocal)      │               └──────────┬───────────┘
└─────────────────┘                          │
                                  ┌──────────┴──────────┐
                                  ▼                     ▼
                             Planka (Kanban)      Anthropic (Claude)
```

## Key Takeaways

**Wechaty makes the session layer invisible.** Without it, maintaining a personal WeChat connection from a Linux server would require deep protocol work. With PadLocal, I write zero WeChat protocol code — just handle `message` events.

**The AI intent layer is thin.** The `process()` method is ~15 lines. Claude handles natural language ambiguity (relative dates, implicit card references) far better than any rule-based parser.

**Decoupling pays off.** The Node.js bridge has no business logic — it just relays messages. This lets me redeploy and update the Python service (new Claude prompts, new Planka actions) without touching the WeChat session.

## Example Interactions

| Message | What Hermes does |
|---------|-----------------|
| `Add task: check Inner Mongolia settlement, due Friday` | Creates Planka card with due date |
| `What's on my list?` | Replies with open cards |
| `Mark the settlement check as done` | Completes the matching card |
| (daily at 08:00) | Sends reminders for cards due today |

---

*Hermes is built on [Wechaty](https://wechaty.js.org) with the [PadLocal](https://github.com/padlocal/puppet-padlocal) puppet. It serves a BESS trading platform processing dispatch data across Inner Mongolia, China.*
