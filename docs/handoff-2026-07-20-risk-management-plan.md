# Handoff: Risk Management — Implementation Plan Session

**Date:** 2026-07-20  
**Status:** Ready for plan writing → execution  
**Branch:** `cost-optimisation`

---

## What was completed in prior sessions

1. **Design specs (Approved, committed):**
   - `docs/superpowers/specs/2026-07-16-asset-risk-design.md` — App 1 (Asset Risk)
   - `docs/superpowers/specs/2026-07-16-retail-risk-design.md` — App 2 (Retail Risk)

2. **Handoff doc (committed):**
   - `docs/handoff-2026-07-16-risk-management.md` — Full design context, migration sources, column mappings, schema decisions

3. **No implementation code written yet.** The plan writing session explored the codebase but did not produce the plan file or any implementation.

---

## What needs to happen next

**Write the implementation plan for App 1 (Asset Risk Management), then execute it.**

App 2 (Retail Risk) builds after `libs/risk/` and `libs/settlement/` from App 1 are stable.

---

## Prompt for new session

```
Read these 3 docs in order:
1. docs/handoff-2026-07-16-risk-management.md (full design context)
2. docs/superpowers/specs/2026-07-16-asset-risk-design.md (App 1 spec, Approved)
3. docs/handoff-2026-07-20-risk-management-plan.md (this file — status + codebase notes)

Then invoke the writing-plans skill to create the implementation plan for App 1
(Asset Risk Management) and save it to:
  docs/superpowers/plans/2026-07-16-asset-risk-management.md

After the plan is written and approved, execute it using subagent-driven-development.
```

---

## Codebase context (gathered during exploration)

Key patterns the plan writer needs to know:

### Directory structure
```
apps/           — Streamlit apps (one folder per app)
libs/           — Shared Python libraries (options/ exists; risk/ and settlement/ do NOT exist yet)
services/       — Backend services (lingfeng/ exists with Playwright collector)
db/ddl/marketdata/ — SQL DDL files (raw .sql, no Alembic)
shared/agents/  — Shared agent utilities (db.py, logging_utils.py)
auth/           — rbac.py (Cognito OIDC + boto3)
config/         — .env files
tests/          — pytest tests (minimal existing coverage)
infra/docker/   — postgres-init
```

### DB connection pattern
```python
# shared/agents/db.py — simple psycopg2 wrapper
import os, psycopg2, pandas as pd
from contextlib import contextmanager

def get_dsn() -> str:
    return os.getenv("DB_DSN") or os.getenv("PGURL")

@contextmanager
def get_conn():
    conn = psycopg2.connect(get_dsn(), connect_timeout=10, options="-c statement_timeout=30000")
    try:
        yield conn
    finally:
        conn.close()

def run_query(sql, params=None) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=params)

def execute_sql(sql, params=None) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
```

### App Streamlit pattern (from mengxi-dashboard/app.py)
```python
import os, sys
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _repo_root)
from dotenv import load_dotenv
load_dotenv(os.path.join(_repo_root, "config", ".env"), override=False)

# DB: PGURL env var → psycopg2 or SQLAlchemy
@st.cache_resource
def _get_sqlalchemy_engine():
    from sqlalchemy import create_engine
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    return create_engine(url, pool_pre_ping=True)

# Tabs
tab1, tab2, tab3 = st.tabs(["Tab A", "Tab B", "Tab C"])
```

### Auth pattern (from auth/rbac.py)
```python
from auth.rbac import require_role, get_user
role = require_role(["Admin", "Trader", "Quant"])
user = get_user()
```

### Agent pattern (from strategy-agent/app.py)
```python
from shared.agents.logging_utils import ensure_agent_log_table, log_agent_request
# Uses anthropic SDK: os.environ.get("ANTHROPIC_API_KEY")
# Chat with st.chat_input() + st.session_state for message history
```

### Dockerfile pattern (from mengxi-dashboard/Dockerfile)
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY apps/mengxi-dashboard/requirements.txt ./apps/mengxi-dashboard/requirements.txt
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --trusted-host pypi.tuna.tsinghua.edu.cn --timeout 300 --retries 5 \
    -r apps/mengxi-dashboard/requirements.txt
COPY libs/ ./libs/
COPY services/ ./services/
COPY apps/mengxi-dashboard/ ./apps/mengxi-dashboard/
ENV PYTHONPATH=/app
EXPOSE 8505
CMD ["streamlit", "run", "apps/mengxi-dashboard/app.py", \
     "--server.port=8505", "--server.address=0.0.0.0", \
     "--server.baseUrlPath=mengxi-dashboard", \
     "--server.fileWatcherType=none", \
     "--server.enableCORS=false", "--server.enableXsrfProtection=false"]
```

### DDL pattern (from db/ddl/marketdata/ops_bess_dispatch.sql)
- Raw SQL files, `CREATE TABLE IF NOT EXISTS marketdata.table_name (...)`
- Extensive COMMENT ON TABLE/COLUMN
- Indexes with `CREATE INDEX IF NOT EXISTS`
- Schema: `marketdata`

### Existing requirements (mengxi-dashboard)
```
streamlit>=1.30, plotly>=5.18, pandas>=2.0, psycopg2-binary, sqlalchemy>=2.0,
openpyxl>=3.0, scipy>=1.10, python-dotenv, anthropic>=0.40, boto3>=1.34,
streamlit-autorefresh>=1.0, numpy>=1.26, numpy-financial>=1.0
```

### Existing LingFeng service
`services/lingfeng/collector.py` — Playwright scraper that downloads Excel from LingFeng SaaS.
```python
from services.lingfeng.collector import collect
path = collect(username, password, market, indicator, start_date, end_date, download_dir)
```

### Tab file pattern (from mengxi-dashboard/wind_farm_tab.py)
- Uses `from sqlalchemy import text`
- Queries with `pd.read_sql(text("SELECT ..."), conn, params={...})`
- Returns DataFrames, renders with plotly + st.dataframe

---

## App 1 build sequence (from spec §7)

1. DB migrations — all `rm_` tables (rm_assets, rm_books, rm_positions, rm_position_volumes, rm_dispatch_plan, rm_dispatch_daily, rm_forward_curves, rm_settlements, rm_settlement_items, rm_pnl_snapshots, rm_var_snapshots)
2. `libs/settlement/parser.py` — multi-format ingestion (PDF, Excel, wind farm migration)
3. `libs/settlement/categorizer.py` — category rules + Mengxi wind settlement rule
4. `libs/risk/mtm.py` + `libs/risk/pnl.py`
5. `libs/risk/var.py` + `libs/risk/greeks.py`
6. `services/forward_curve/` — LingFeng pull + manual upload
7. `services/operating_assets/` — WeCom receiver + folder watcher + ingestion pipeline
8. App tabs 1–5
9. Tab 6 agent
10. Docker + ECS deploy

### App 1 details
- **Path:** `apps/asset-risk/` | **Port:** 8512 | **ECR:** `bess-asset-risk`
- **6 tabs:** Asset Config, Settlement, Realised P&L, Positions & MtM, VaR & Greeks, Agent
- **New shared libs:** `libs/risk/` (mtm, var, pnl, greeks), `libs/settlement/` (parser, categorizer)
- **New services:** `services/forward_curve/`, `services/operating_assets/`

---

## Important file paths

| What | Path |
|---|---|
| Asset risk spec | `docs/superpowers/specs/2026-07-16-asset-risk-design.md` |
| Retail risk spec | `docs/superpowers/specs/2026-07-16-retail-risk-design.md` |
| Original handoff | `docs/handoff-2026-07-16-risk-management.md` |
| This handoff | `docs/handoff-2026-07-20-risk-management-plan.md` |
| Existing libs/options | `libs/options/` (black_scholes.py, smile.py, structures.py) |
| Existing LingFeng | `services/lingfeng/collector.py` |
| BESS dispatch DDL | `db/ddl/marketdata/ops_bess_dispatch.sql` |
| Mengxi dashboard (reference app) | `apps/mengxi-dashboard/` |
| Shared DB utility | `shared/agents/db.py` |
| Auth module | `auth/rbac.py` |
