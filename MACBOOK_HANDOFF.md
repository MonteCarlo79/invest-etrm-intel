# MacBook Claude Code Handoff Note
*Generated 2026-06-28 — for a new MacBook joining the bess-platform project*

---

## Context

This Windows machine (`dipeng.chen`) runs the primary bess-platform development environment. The new MacBook is a second collaborating machine. Both share the same OneDrive sync, so the project codebase and `.claude/` project config are already available — you just need to configure the global Claude Code setup.

---

## Step 1 — Install Claude Code

```bash
npm install -g @anthropic-ai/claude-code
```

---

## Step 2 — Create `~/.claude/settings.json`

Replace `<YOUR_TOKEN>` and `<PATH_TO_BESS_PLATFORM>` (e.g. `~/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform` or wherever OneDrive syncs on Mac).

```json
{
  "env": {
    "ANTHROPIC_AUTH_TOKEN": "<YOUR_TOKEN>",
    "ANTHROPIC_BASE_URL": "https://llm-gateway.envisioncn.com",
    "CLAUDE_CODE_MAX_OUTPUT_TOKENS": "32000",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
    "DISABLE_PROMPT_CACHING": "1",
    "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS": "1",
    "ANTHROPIC_MODEL": "claude-sonnet-4-6",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "claude-haiku-4-5"
  },
  "permissions": {
    "defaultMode": "auto",
    "additionalDirectories": ["<PATH_TO_BESS_PLATFORM>"]
  },
  "hooks": {
    "SessionStart": [{
      "hooks": [{
        "type": "command",
        "command": "echo '{\"hookSpecificOutput\": {\"hookEventName\": \"SessionStart\", \"additionalContext\": \"The primary working directory for this user is <PATH_TO_BESS_PLATFORM>. Unless instructed otherwise, assume the user is working in this directory.\"}}'",
        "shell": "bash"
      }]
    }]
  },
  "autoUpdatesChannel": "latest",
  "skipAutoPermissionPrompt": true
}
```

---

## Step 3 — Install Plugins

Run inside Claude Code:

```
/install-plugin superpowers
/install-plugin frontend-design
/install-plugin skill-creator
/install-plugin github
/install-plugin claude-code-setup
```

---

## Step 4 — Copy Memory Files

The Windows memory path is:
```
C:\Users\dipeng.chen\.claude\projects\C--Users-dipeng-chen--local-bin\memory\
```

On Mac, the equivalent global memory path will be something like:
```
~/.claude/projects/<encoded-path>--local-bin/memory/
```

The encoded path segment is derived from your Mac's home directory. To find the exact folder name Claude Code will use, start a session first and save one memory — Claude will create the folder. Then copy the `*.md` files from the Windows memory folder into that Mac memory folder (via OneDrive or direct transfer).

Alternatively, copy the entire memory folder now using a shared location:

```bash
# On Mac, after locating the correct memory dir:
cp /Volumes/OneDrive/ETRM/bess-platform/MEMORY_BACKUP/*.md \
   ~/.claude/projects/<your-encoded-path>/memory/
```

A backup of all memory files is kept at:
```
bess-platform/knowledge/memory_backup/   ← (create this if transferring manually)
```

---

## What's Already Synced via OneDrive (no action needed)

| Path | Contents |
|------|----------|
| `bess-platform/.claude/agents/` | 4 persona subagents + code-reviewer + test-runner |
| `bess-platform/.claude/commands/` | Slash commands |
| `bess-platform/.claude/hooks/` | Project-level hooks |
| `bess-platform/CLAUDE.md` | Project instructions for Claude |
| All source code | `apps/`, `services/`, `libs/`, `infra/` |

---

## Active Projects Summary (as of 2026-06-28)

| Project | Status | Key Location |
|---------|--------|--------------|
| Spot Market App | v32 live, v33 committed (80a2218) not deployed | `apps/spot-market/` |
| GB Market App | v71 deployed 2026-06-04 | `apps/gb-market/` |
| AU Market App | v6 deployed 2026-06-04 | `apps/au-market/` |
| PH + PO Market Apps | PH v15, PO v12 deployed 2026-06-10 | `apps/ph-market/`, `apps/po-market/` |
| Hermes Service | v14/td:58 deployed 2026-06-19 | `services/hermes/` |
| BESS Map App | v47/td:66 deployed 2026-06-17 | `apps/bess-map/` |
| Crystal-Ball Fortune App | v33/td:38 deployed 2026-06-06 | `apps/fortune-teller/` |
| Mengxi Trading Ops App | v8 deployed; rebuild needed for latest fixes | `apps/mengxi-trading-ops/` |
| Inner Mongolia Ops Ingestion | Live, 109 tests passing | `services/ops_ingestion/inner_mongolia/` |
| BESS Daily Strategy Report | 5-tab Streamlit, 5-strategy P&L in DB | `libs/decision_models/` |
| LingFeng Data Pipeline | 29-province daily at 04:00 | `services/lingfeng/` |
| Fengxing Nodal Prices | v10 deployed, backfills running | `services/fengxing/` |
| DeepTutor | v1 deployed at tutor.pjh-etrm.ai | `apps/deeptutor/` |

---

## Collaboration Workflow (Windows + MacBook)

1. **Primary deployment machine**: Windows (has AWS credentials, Terraform, ECR push access). Do final `docker build / push / task update` from Windows unless you set up AWS CLI on Mac too.
2. **Development**: Both machines can edit code freely — OneDrive syncs automatically.
3. **Avoid simultaneous edits** to the same file — OneDrive will create conflict copies.
4. **Memory**: Each machine maintains its own `~/.claude/projects/.../memory/` — they are NOT synced. If you make important project decisions on Mac, paste them back to Windows Claude so it can update its memory, and vice versa.
5. **Git**: Both machines can commit/push. The repo remote is the source of truth; sync via `git pull` before starting work.

---

## AWS / Infrastructure Notes

- ECR registry and ECS task definitions are managed from Windows.
- If you need to deploy from Mac, install AWS CLI and run `aws configure` with the same credentials.
- Terraform state is in S3 — no local state files to worry about.
- The `infra/terraform/terraform.tfvars` file is **never committed** (contains secrets).

---

## One-Time Mac Environment Setup (Python / Docker)

```bash
# Python — use pyenv or conda, match Python 3.11
pyenv install 3.11
pyenv global 3.11

# Install project deps (from bess-platform root)
pip install -r requirements.txt   # or per-service requirements

# Docker Desktop for Mac — needed for local builds
# https://www.docker.com/products/docker-desktop/
```

---

## Questions / Handoff Contact

Both Claude instances share this file via OneDrive. Leave notes or TODOs in `bess-platform/knowledge/handoff_notes.md` for cross-machine communication.
