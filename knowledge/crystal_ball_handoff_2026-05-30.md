# Crystal-Ball Fortune Teller — Session Handoff (2026-05-30)

## What was built

Standalone BaZi + 紫微斗数 fortune-telling app, deployed at **https://www.pjh-etrm.ai/crystal-ball**.

### Files
| Path | Purpose |
|------|---------|
| `apps/fortune-teller/app.py` | Main Streamlit app (~1750 lines, 8 tabs) |
| `apps/fortune-teller/bazi_engine.py` | BaZi four-pillars engine (sxtwl) |
| `apps/fortune-teller/ziwei_engine.py` | 紫微斗数 engine (14 stars, 12 palaces) |
| `apps/fortune-teller/fortune_report.py` | Daily PDF + SMTP + WeCom |
| `apps/fortune-teller/requirements.txt` | Python deps |
| `apps/fortune-teller/Dockerfile` | Image build |
| `services/fortune_knowledge/` | KB CRUD (ingest_text, search_knowledge, sessions) |
| `db/ddl/fortune_schema.sql` | PostgreSQL schema |

### AWS deployment
- **ECR**: `319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/crystal-ball-fortune:v6`
- **ECS cluster**: `bess-platform-cluster`
- **ECS service**: `bess-platform-crystal-ball-svc`
- **Task definition**: `bess-platform-crystal-ball:6` (latest)
- **Port**: 8520, Streamlit base path `/crystal-ball`
- **DB**: PostgreSQL schema `fortune` on `bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata`
- **Region**: `ap-southeast-1`

### 8 Tabs
1. **命主管理** — Add/manage persons  
2. **命盘** — BaZi 4 pillars + 五行 + 大运 + **0-80岁人生事件** (AI, cached in `fortune.life_events`, also in KB)  
3. **流年运势** — Annual fortune (auto-loads current year) + **重点留意月份** (12-month grid + key periods, in `annual_readings.key_months`)  
4. **今日运势** — Daily 刑冲合化 + AI commentary  
5. **AI命理师** — Claude Sonnet 4.6 agent + 5 tools + KB search  
6. **知识库** — KB management (also receives auto-saved life events + ziwei analyses)  
7. **报告管理** — Email/WeCom daily PDF report  
8. **紫微斗数** — 12-palace grid + 大限 timeline + AI analysis (cached in `fortune.ziwei_charts.ai_analysis`)

### DB tables (fortune schema)
```sql
fortune.persons              -- person profiles
fortune.bazi_charts          -- computed BaZi (UNIQUE person_id)
fortune.annual_readings      -- 6 areas + key_months JSONB (UNIQUE person_id, year)
fortune.daily_readings       -- daily (UNIQUE person_id, reading_date)
fortune.life_events          -- 0-80 events JSON (UNIQUE person_id)
fortune.ziwei_charts         -- ZiWei + ai_analysis TEXT (UNIQUE person_id)
fortune.knowledge_docs       -- KB (life events + ziwei analyses auto-saved here)
fortune.expert_insights      -- extracted insights
fortune.agent_sessions       -- chat sessions
fortune.report_webhooks      -- WeCom webhooks
fortune.report_log           -- delivery log
```

Schema migrations run idempotently at startup via `_migrate_schema()` in `app.py`.

---

## Critical technical facts

### sxtwl API (Python Chinese calendar library)
The correct API for sxtwl `Day` objects — **do not use deprecated names**:
```python
d = sxtwl.fromSolar(year, month, day)
d.hasJieQi()        # bool — has solar term today?
d.getJieQi()        # int index — 1=小寒,2=大寒,3=立春,4=雨水… odd=节,even=气
d.getMonthGZ()      # GZ object with .tg (stem 0-9) and .dz (branch 0-11)
d.getDayGZ()        # GZ object
d.getYearGZ(True)   # GZ object (True = 立春 boundary)
d.after(n)          # Day — n days forward
d.before(n)         # Day — n days back
d.getLunarYear()    # int
d.getLunarMonth()   # int (negative = leap month)
d.getLunarDay()     # int
```
**DO NOT use**: `.m60`, `.d60`, `.isJie`, `.next()` — these do not exist.

60-jiazi index formula: `(6 * gz.tg - 5 * gz.dz) % 60`

### Streamlit HTML in st.markdown
**Critical**: Never use multi-line f-strings with conditional expressions inside `st.markdown(..., unsafe_allow_html=True)`. A blank line (from an empty conditional `{"<div>" if x else ""}`) causes the markdown parser to exit HTML mode and render subsequent `<div>` tags as literal text.

**Fix pattern** — build HTML as concatenated single-line string:
```python
flag_inner = f"<div style='...'>{flag_str}</div>" if flag_str else "<div>&nbsp;</div>"
html = (
    f"<div style='...'>"
    f"<div style='...'>{content}</div>"
    f"{flag_inner}"
    f"</div>"
)
st.markdown(html, unsafe_allow_html=True)
```

### Docker build
```bash
cd C:\Users\dipeng.chen\OneDrive\Crystal-Ball
docker build -f apps/fortune-teller/Dockerfile -t crystal-ball-fortune:vN .
docker tag crystal-ball-fortune:vN 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/crystal-ball-fortune:vN
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/crystal-ball-fortune:vN
```

### ECS deploy (register new task def + update service)
```bash
# Register — copy container-definitions from revision 6, only change image tag
aws ecs register-task-definition --region ap-southeast-1 \
  --family bess-platform-crystal-ball \
  --task-role-arn "arn:aws:iam::319383842493:role/bess-platform-task-role" \
  --execution-role-arn "arn:aws:iam::319383842493:role/bess-platform-task-exec" \
  --network-mode awsvpc --requires-compatibilities FARGATE \
  --cpu 512 --memory 1024 \
  --container-definitions '[{ "name":"crystal-ball","image":"...vN",...}]'

# Update service
aws ecs update-service \
  --cluster bess-platform-cluster \
  --service bess-platform-crystal-ball-svc \
  --task-definition bess-platform-crystal-ball:N \
  --force-new-deployment --region ap-southeast-1
```

Also update `infra/terraform/terraform.tfvars` → `image_crystal_ball = "...vN"`.

### Logs
```bash
# MSYS_NO_PATHCONV=1 is required in Git Bash to prevent path conversion
MSYS_NO_PATHCONV=1 aws logs get-log-events \
  --log-group-name "/ecs/bess-platform" \
  --log-stream-name "crystal-ball/crystal-ball/<TASK_ID>" \
  --region ap-southeast-1 --limit 30
```

---

## Pending actions (TO DO in next session)

### 1. Push bess-platform to GitHub (blocked by secret scanning)
Branch `cost-optimisation` is **12 commits ahead** of origin (11 old + 1 new crystal-ball commit).  
Push is blocked because `infra/terraform/terraform.tfvars` contains the Anthropic API key at line 42 (already in all prior commits).

**To unblock**: Visit https://github.com/MonteCarlo79/invest-etrm-intel/security/secret-scanning/unblock-secret/3EP237e1hg1ucuCALJeuH4c9fI4  
Then re-run: `cd bess-platform && git push origin cost-optimisation`

### 2. Push Crystal-Ball to GitHub (new repo needed)
Crystal-Ball git repo is **initialized locally** at `C:\Users\dipeng.chen\OneDrive\Crystal-Ball` with one commit (`bf80000`). No remote yet.

**To push**:
```bash
# 1. Create a new GitHub repo (e.g. MonteCarlo79/Crystal-Ball) — private recommended
# 2. Add remote and push:
cd "C:\Users\dipeng.chen\OneDrive\Crystal-Ball"
git remote add origin https://github.com/MonteCarlo79/Crystal-Ball.git
git branch -M main
git push -u origin main
```

### 3. 紫微斗数 algorithm validation
The engine uses the **南派 classical algorithm**. The 14-star placement relative to 紫微 (z) and 天府 (f=z+6):
- Verify with a known chart (e.g., input a birth date and cross-check against a trusted 紫微斗数 software)
- Key formula to validate: `calc_ming_gong`, `calc_wuxing_ju`, `calc_ziwei_branch`
- The `_NAYIN_JU` lookup table covers all 30 pairs correctly

### 4. Potential improvements identified
- **流年运势 重点月份**: Auto-generate on page load (like annual reading) — currently requires button click
- **Daily report PDF**: Does not yet include 紫微斗数 or life events sections
- **紫微斗数 辅星**: Only 14 main stars placed; auxiliary stars (文昌, 文曲, 左辅, 右弼, etc.) not implemented
- **Palace layout**: Currently 3×4 linear grid; traditional 紫微斗数 uses a specific 4×3 counter-clockwise layout starting from 命宫

---

## Context for new session

This app lives in two repos:
1. **Crystal-Ball** (standalone app): `C:\Users\dipeng.chen\OneDrive\Crystal-Ball\`
2. **bess-platform** (infra): `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\` — branch `cost-optimisation`

The two share the same AWS account (319383842493), RDS instance, ALB, and ECS cluster. The Crystal-Ball app is deployed as a separate service within the bess-platform ECS cluster, behind Cognito auth at `/crystal-ball` path.

Current deployment is **healthy** — v6 running (1/1 tasks, 172.31.x.x:8520, Streamlit started as of last check).
