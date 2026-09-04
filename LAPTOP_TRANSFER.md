# Laptop Transfer Plan — bess-platform

> **Scenario:** Current laptop is lost or replaced. This document covers how to resume
> the project on a new machine, including a limited-access scenario where only
> Claude.ai web is available (no Claude Code CLI).

---

## What You Do NOT Lose

Because the working directory is on OneDrive and code is on GitHub, you lose nothing except local tooling:

| Asset | Where it lives | Status after laptop loss |
|-------|---------------|--------------------------|
| All source code | `OneDrive\ETRM\bess-platform` + GitHub `invest-etrm-intel` | **Safe — OneDrive syncs automatically** |
| All secrets & env vars | `config/.env` + `infra/terraform/terraform.tfvars` | **Safe — OneDrive** |
| AWS infrastructure | ECS, RDS, ECR, ALB — all in AWS | **Running — unaffected** |
| Production apps | `https://www.pjh-etrm.ai` | **Live — unaffected** |
| Database | RDS `bess-platform-pg` (ap-southeast-1) | **Live — unaffected** |
| Container images | ECR (all image versions) | **Safe — AWS ECR** |
| CLAUDE.md / MEMORY.md / ERRORS.md | OneDrive working directory | **Safe** |
| Claude Code memory | `C:\Users\<name>\.claude\projects\...\memory\` | **Lost** — recreate from CLAUDE.md + MEMORY.md |

---

## Scenario A — Full Recovery (Claude Code CLI available)

### Step 1 — Install core tooling (in order)

```powershell
# 1. Git — https://git-scm.com/download/win
# 2. Python 3.11+ — https://www.python.org/downloads/
#    During install: tick "Add Python to PATH"
# 3. Docker Desktop — https://www.docker.com/products/docker-desktop/
# 4. AWS CLI v2 — https://aws.amazon.com/cli/
# 5. Terraform — https://developer.hashicorp.com/terraform/install
#    (or: winget install HashiCorp.Terraform)
# 6. Node.js LTS — https://nodejs.org/ (needed for Claude Code)
# 7. Claude Code CLI:
npm install -g @anthropic-ai/claude-code
```

### Step 2 — AWS credentials

Two options; pick one:

**Option A — IAM user keys (current method):**
```powershell
aws configure
# AWS Access Key ID:     <from AWS Console → IAM → Your user → Security credentials>
# AWS Secret Access Key: <same>
# Default region:        ap-southeast-1
# Default output:        json
```

**Option B — SSO / AWS Identity Center (recommended for new setup):**
```powershell
aws configure sso
```

**Verify:**
```powershell
aws sts get-caller-identity
aws ecr describe-repositories --region ap-southeast-1 --query "repositories[*].repositoryName"
```

### Step 3 — ECR login (needed for docker push/pull)

```powershell
$pass = aws ecr get-login-password --region ap-southeast-1
docker login --username AWS --password $pass 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
```

Token expires ~12 hours — re-run when it does.

### Step 4 — Verify OneDrive sync

```powershell
# Wait for OneDrive to finish syncing, then:
ls "C:\Users\<you>\OneDrive\ETRM\bess-platform"
# Should see: CLAUDE.md, apps/, services/, infra/, config/, etc.
```

If OneDrive is slow, clone from GitHub as a fallback:
```powershell
git clone https://github.com/MonteCarlo79/invest-etrm-intel.git bess-platform
git checkout cost-optimisation
# Then copy config/.env from a backup or re-create from terraform.tfvars
```

### Step 5 — Install Python dependencies (for local dev/pipeline runs)

```powershell
# From project root — install per-app or use a single consolidated env:
pip install streamlit pandas sqlalchemy psycopg2-binary anthropic boto3 scipy numpy statsmodels python-pptx python-docx openpyxl xlrd pdfplumber
```

### Step 6 — Initialise Terraform

```powershell
cd infra/terraform
terraform init
terraform plan    # should show "No changes" if AWS is in sync
```

### Step 7 — Restore Claude Code memory

Claude Code memory for this project is stored in:
```
C:\Users\<you>\.claude\projects\C--Users-<you>-OneDrive-ETRM-bess-platform\memory\
```
This folder is NOT on OneDrive. After reinstalling Claude Code, it starts blank. Bootstrap it by:

1. Open Claude Code in the project directory.
2. Paste this prompt:
   > "Read CLAUDE.md and MEMORY.md in this directory. Then read ERRORS.md.
   > Rebuild your project memory index from these files. Save key entries to your
   > auto-memory system."
3. Claude will repopulate memory from the committed markdown files.

### Step 8 — Smoke test

```powershell
# Load env
Get-Content config\.env | ForEach-Object {
  if ($_ -match '^([^#][^=]+)=(.+)$') {
    [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim())
  }
}

# Run BESS Map locally
$env:AUTH_MODE="dev"
streamlit run apps/bess-map/app.py --server.port 8503
# Browse http://localhost:8503 — Province Ranking should load data from RDS
```

---

## Scenario B — Claude Web Only (claude.ai, no CLI)

You still have full access to: OneDrive files, GitHub, AWS Console, the running production system, and the RDS database. You lose the terminal-integrated AI workflow but can replicate it with copy-paste.

### What changes

| Capability | Claude Code CLI | Claude Web |
|-----------|----------------|------------|
| Read/edit files automatically | Yes | No — paste code manually |
| Run bash commands | Yes | No — you run them yourself |
| Multi-file context | Automatic | Paste relevant sections |
| Memory across sessions | Persistent `.claude/` folder | None — start each session by pasting CLAUDE.md |
| Subagents / parallel tools | Yes | No |

### How to work effectively with Claude Web

**Start every session with this paste:**
```
<context>
[paste full contents of CLAUDE.md]
</context>

Today's date: YYYY-MM-DD
Current branch: cost-optimisation
What I'm working on: [describe task]
```

**For code changes:**
1. Open the file in VS Code (or any editor) on the new laptop.
2. Paste the relevant function/section into Claude Web with: "Here is `services/bess_map/forecast_engine.py`, lines 320–400. Change X to Y."
3. Claude returns the updated code — paste it back into the file.
4. Run any commands (git, python, terraform) yourself in PowerShell.

**For multi-file tasks:** Use the "Projects" feature in Claude.ai (if available on your plan).
Create a Project named `bess-platform` and upload the key files as project knowledge:
- `CLAUDE.md`
- `MEMORY.md`
- The specific app file you're editing

**For running the capture pipeline or other scripts:**
Claude Web gives you the exact command; you run it in PowerShell yourself:
```powershell
# Example — run capture pipeline
python services/bess_map/run_capture_pipeline.py --province shandong --model ols_rt_time_v1 --force
```

**For Terraform deploys:**
Claude Web can generate the exact `terraform apply` or `aws ecs update-service` commands.
You copy-paste and run them in your own terminal.

**Tooling still needed on the new laptop** (same as Scenario A, Steps 1–6):
Git, Python, Docker Desktop, AWS CLI, Terraform. Claude Code CLI is optional.

---

## Key Credentials — Where to Find Them

All secrets are in OneDrive (do not need to be memorised):

| Secret | File | Key name |
|--------|------|----------|
| RDS password | `config/.env` | `PGPASSWORD` |
| Full DB DSN | `config/.env` | `PGURL` |
| Anthropic API key | `config/.env` | `ANTHROPIC_API_KEY` |
| All above + TT API keys | `infra/terraform/terraform.tfvars` | — |
| AWS credentials | AWS Console → IAM | Must be regenerated (not stored in repo) |

> **If `config/.env` is missing** (OneDrive hasn't synced yet): all values are also in
> `infra/terraform/terraform.tfvars` which is on OneDrive. Copy from there.

---

## AWS Console Access (no CLI needed)

If you can't install the AWS CLI immediately, the AWS Console at
`https://console.aws.amazon.com` gives you:

- **ECS** → Clusters → `bess-platform-cluster` → view running services, force redeployment
- **ECR** → view/delete image repositories
- **RDS** → `bess-platform-pg` → connect details, reboot if needed
- **CloudWatch** → Logs → `/ecs/bess-platform-*` → live service logs
- **Route 53** → `pjh-etrm.ai` DNS records

---

## Quick Priority Order After Laptop Loss

1. **Verify production is still running** — open `https://www.pjh-etrm.ai` in a browser.
   If it loads, nothing is broken. AWS runs independently.
2. **Wait for OneDrive to sync** on the new machine (or pull from GitHub).
3. **Install Git + Python + AWS CLI** — minimum needed for pipeline runs and deploys.
4. **Regenerate AWS credentials** if the old ones were compromised.
5. **Re-login to Docker + ECR** only when you need to build/push a new image.
6. **Install Claude Code** when available — rebuild memory from CLAUDE.md.
7. **Terraform init** only when you need to apply infra changes.

---

## GitHub Repository

`https://github.com/MonteCarlo79/invest-etrm-intel`
Active branch: `cost-optimisation`

```powershell
git clone https://github.com/MonteCarlo79/invest-etrm-intel.git
cd invest-etrm-intel
git checkout cost-optimisation
```
