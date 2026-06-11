# Hermes Agent — AI Personal Assistant with WeChat Integration

**Date:** 2026-06-11
**Status:** Approved for implementation
**Depends on:** Planka deployed and accessible (REST API at `https://planka.{domain}`)

---

## Overview

Hermes is a Claude-powered personal assistant that bridges WeChat (personal + WeCom) with Planka. It:
- **Passively monitors** WeChat conversations for action items, files, and important info
- **Responds to direct commands** ("add task X", "what's due today?", "remind me at 9am")
- **Sends scheduled reminders** to your WeChat for Planka tasks with due dates

---

## Architecture

```
[Personal WeChat] ←────────────────────────────────────────────────┐
       ↕ WeChat protocol                                            │
[Wechaty Bridge — EC2 i-078297b9e83f03dc1 (172.31.30.155)]        │
       ↓ POST /inbound (private VPC)                               │
[Hermes FastAPI — ECS Fargate] ←── ALB /hermes/* ←── WeCom webhook┘
       ↓
[Claude API]  ←── message understanding, task extraction, reply drafting
       ↓
[Planka REST API]  ←── create/read/update/complete tasks
       ↓
[WeCom API]  ←── send reminders/replies to WeCom
[Wechaty bridge (EC2)]  ←── send replies to personal WeChat
```

**Data flow summary:**
1. Message arrives (WeChat personal via Wechaty, or WeCom via webhook)
2. Hermes sends message to Claude with context (recent conversation, current Planka tasks)
3. Claude decides: extract task / answer query / do nothing
4. Hermes executes the decision (creates Planka card, sends reply)
5. Scheduler runs every 15 minutes, checks Planka for upcoming due dates, sends reminders

---

## Sub-components

### 1. Hermes FastAPI Service (`services/hermes/`)

Python 3.11 FastAPI service. Runs on ECS Fargate.

**Endpoints:**
- `POST /hermes/inbound/wechat` — receives messages from Wechaty bridge (internal only, not behind ALB auth)
- `POST /hermes/inbound/wecom` — WeCom webhook (message receipt + verification token)
- `GET /hermes/inbound/wecom` — WeCom webhook URL verification (GET with `echostr`)
- `GET /hermes/health` — ALB health check

**Modules:**
```
services/hermes/
├── app.py                  # FastAPI app, routes
├── agent.py                # Claude agent logic (message → action)
├── planka_client.py        # Planka REST API wrapper (CRUD tasks)
├── wecom_client.py         # WeCom API: send message, verify webhook
├── wechat_client.py        # HTTP client to call Wechaty bridge on EC2
├── scheduler.py            # APScheduler: reminder job every 15 min
├── models.py               # Pydantic models for messages and tasks
└── requirements.txt
```

**Key logic in `agent.py`:**
- System prompt defines Hermes persona and capabilities
- On each inbound message: sends last 10 messages + current Planka board state to Claude
- Claude responds with structured JSON: `{ "action": "create_task|reply|ignore", "task": {...}, "reply": "..." }`
- Hermes executes the action, sends reply back via the originating channel

**Passive monitoring rules (applied before calling Claude):**
- Skip group chats unless directly @mentioned
- Skip messages from contacts in a configurable `ignored_senders` list
- Apply keyword filter: if message contains none of `["file", "report", "deadline", "by", "urgent", "action", "task", "remind", "due", "pls", "please", "doc", "请", "任务", "截止", "提醒"]` → skip Claude call (save tokens)
- Files/attachments: download to S3 (`bess-platform` bucket, `hermes/files/` prefix), store S3 URL in Planka card description

**Scheduler (`scheduler.py`):**
- Runs at startup inside the FastAPI process (APScheduler background scheduler)
- Every 15 minutes: query Planka for cards with due dates in next 24 hours
- For each: send a WeChat reminder with card title, list name, due time
- Reminder sent via WeCom (primary) and optionally personal WeChat

---

### 2. Wechaty Bridge (`apps/hermes-wechaty/`)

Node.js 20 service. Runs **directly on EC2** (not in ECS). Deployed via SSH + PM2.

**Why EC2, not ECS:** WeChat personal login requires a persistent QR scan session. Wechaty maintains a local login state on disk. ECS containers are ephemeral — if the task restarts, you'd need to re-scan QR every time. EC2 gives persistence.

**Stack:** Node.js + Wechaty + `wechaty-puppet-wechat4u` (web WeChat protocol). If web WeChat is unavailable for the account, escalate to `wechaty-puppet-padlocal` (paid, ~$10/month).

**What it does:**
- Maintains WeChat personal session (QR scan once, session persists in `~/.wechaty/`)
- On message received: POST to `http://{hermes_ecs_private_ip}/hermes/inbound/wechat`
- Exposes `POST /send` on port `3000` (private only) so Hermes can push outbound messages
- Exposes `GET /qr` on port `3001` (temporary, for initial setup) — renders QR code as HTML page for browser-based scanning via SSM port-forward tunnel

```
apps/hermes-wechaty/
├── index.js           # Wechaty bot: listen, forward to Hermes, expose /send + /qr
├── package.json
└── ecosystem.config.js  # PM2 process config
```

**Networking:** EC2 security group must allow outbound to Hermes ECS private IP on port 8000. Hermes ECS security group must allow inbound from EC2 private IP (`172.31.30.155`) on port 8000.

---

### 3. WeCom Integration

**Setup (manual, once):** Create a WeCom "Custom Application" (自定义应用) in the WeCom admin console. This gives:
- `WECOM_CORP_ID` — your company ID
- `WECOM_AGENT_ID` — the custom app agent ID
- `WECOM_SECRET` — the app secret

Set the webhook URL to `https://{domain}/hermes/inbound/wecom` in the WeCom app settings.

**Hermes sends to WeCom via:** `POST https://qyapi.weixin.qq.com/cgi-bin/message/send` with bearer token.

---

## AWS Deployment

### ECS Task Definition (`hermes`)
- Image: built from `services/hermes/Dockerfile`, pushed to new ECR repo `hermes`
- Port: `8000`
- CPU: `512`, Memory: `1024` (Claude API calls can be slow)
- Environment variables:
  - `ANTHROPIC_API_KEY`
  - `PLANKA_BASE_URL` — `https://planka.{domain}`
  - `PLANKA_EMAIL` / `PLANKA_PASSWORD` — Planka admin credentials
  - `WECOM_CORP_ID` / `WECOM_AGENT_ID` / `WECOM_SECRET`
  - `WECHATY_BRIDGE_URL` — `http://172.31.30.155:3000` (EC2 private IP)
  - `S3_BUCKET` — existing bess-platform S3 bucket
  - `AWS_DEFAULT_REGION` — `ap-southeast-1`
  - `HERMES_WECOM_TOKEN` — shared secret for WeCom webhook verification

### ALB Rules
- **Priority 46:** `POST /hermes/inbound/wecom` + `GET /hermes/inbound/wecom` — forward only (no Cognito; WeCom calls this endpoint unauthenticated)
- **Priority 47:** `/hermes/*` — Cognito auth, then forward (for future admin UI)
- **Priority 48:** `/hermes/health` — forward, no auth (health check)

### ECR Repository (`hermes`)
New ECR repo + lifecycle policy (keep last 5 images), consistent with other services.

### Security Group Rules (new)
- EC2 (`172.31.30.155`) → Hermes ECS security group: allow TCP 8000
- Hermes ECS → EC2: allow TCP 3000 (outbound to Wechaty bridge)
- Hermes ECS → `0.0.0.0/0` TCP 443 (outbound to Anthropic API, WeCom API, Planka)

---

## EC2 Access & Setup (one-time)

The EC2 (`i-078297b9e83f03dc1`) currently has **no IAM instance profile** and no SSH key on record. Access is via **AWS Systems Manager Session Manager** after attaching a role.

**Step 1 — Attach SSM IAM role (Terraform):**
```hcl
resource "aws_iam_role" "hermes_ec2" { ... }  # AmazonSSMManagedInstanceCore policy
resource "aws_iam_instance_profile" "hermes_ec2" { ... }
resource "aws_iam_instance_profile_association" "hermes_ec2" {
  instance_id          = "i-078297b9e83f03dc1"
  iam_instance_profile = aws_iam_instance_profile.hermes_ec2.name
}
```

**Step 2 — Connect via SSM (no SSH key needed):**
```bash
aws ssm start-session --target i-078297b9e83f03dc1 --region ap-southeast-1
```

**Step 3 — Install and deploy:**
```bash
# Inside SSM session
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
sudo npm install -g pm2
cd ~ && git clone {repo} && cd bess-platform/apps/hermes-wechaty
npm install
```

**Step 4 — QR code scan (no interactive terminal needed):**
Wechaty bridge exposes a `/qr` HTTP endpoint on port `3001` (temporary, internal only) that renders the QR code as an HTML page. To scan:
1. Open an SSM port-forward tunnel: `aws ssm start-session --target i-... --document-name AWS-StartPortForwardingSession --parameters '{"portNumber":["3001"],"localPortNumber":["3001"]}'`
2. Open `http://localhost:3001/qr` in browser, scan with WeChat mobile

After scan, session persists in `~/.wechaty/`. Close the port-forward tunnel.

**Step 5 — Start with PM2:**
```bash
pm2 start ecosystem.config.js
pm2 save
pm2 startup  # follow the printed sudo command
```

Session state persists in `~/.wechaty/`. If the session expires (WeChat requires re-login), repeat Step 4. Expected session lifetime: weeks to months.

---

## Files Created / Changed

| File | Change |
|------|--------|
| `services/hermes/app.py` | New: FastAPI app |
| `services/hermes/agent.py` | New: Claude agent |
| `services/hermes/planka_client.py` | New: Planka API client |
| `services/hermes/wecom_client.py` | New: WeCom API client |
| `services/hermes/wechat_client.py` | New: Wechaty bridge client |
| `services/hermes/scheduler.py` | New: APScheduler reminder job |
| `services/hermes/models.py` | New: Pydantic models |
| `services/hermes/requirements.txt` | New |
| `services/hermes/Dockerfile` | New |
| `apps/hermes-wechaty/index.js` | New: Wechaty Node.js bridge |
| `apps/hermes-wechaty/package.json` | New |
| `apps/hermes-wechaty/ecosystem.config.js` | New: PM2 config |
| `infra/terraform/main.tf` | Add: ECR repo, ECS task/service, target groups, ALB rules, SG rules, SSM IAM role + instance profile for EC2 |
| `infra/terraform/variables.tf` | Add: Hermes variables |
| `infra/terraform/terraform.tfvars` | Add: values |

---

## Constraints & Risks

| Risk | Mitigation |
|------|-----------|
| Web WeChat blocked for this account | Fall back to PadLocal puppet; ~$10/month |
| WeChat session expires | PM2 restarts the process; re-scan QR manually |
| Claude API latency (1–3s per message) | Keyword pre-filter skips most messages; async processing |
| Planka REST API is unofficial (no public docs) | Reverse-engineer from browser DevTools or use Planka's internal API |
| WeCom token rotation (every 2 hours) | `wecom_client.py` caches token with TTL |
| Files from WeChat can be large | Stream to S3 directly from Wechaty bridge; don't buffer in Hermes |

---

## Out of Scope (this version)

- Web UI for Hermes configuration
- Multi-user support
- WhatsApp / Telegram / other channels
- Voice message transcription
- Proactive web research (Hermes only acts on WeChat inputs)
