import os
import sys
import urllib.parse
from pathlib import Path

import boto3
import streamlit as st
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

# --------------------------------------------------
# PATH SETUP
# --------------------------------------------------

sys.path.append(str(Path(__file__).resolve().parents[2]))

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------

st.set_page_config(page_title="BESS Intelligence Platform", layout="wide")

# --------------------------------------------------
# PROJECT IMPORTS
# --------------------------------------------------

from shared.agents.registry import get_visible_apps, get_visible_by_category
from auth.rbac import get_user, get_groups, get_email

# --------------------------------------------------
# AWS DEBUG
# --------------------------------------------------

if os.getenv("SHOW_AWS_DEBUG", "false").lower() == "true":
    import requests

    st.write("=== AWS DEBUG START ===")

    safe_env = {
        k: v
        for k, v in os.environ.items()
        if not any(x in k for x in ["KEY", "SECRET", "PASSWORD"])
    }
    st.write("ENV:", safe_env)

    try:
        metadata_url = "http://169.254.170.2/v2/credentials/"
        resp = requests.get(metadata_url, timeout=2)
        st.write("Metadata status:", resp.status_code)
        st.write("Metadata body (truncated):", resp.text[:200])
    except Exception as e:
        st.write("Metadata error:", str(e))

    try:
        session = boto3.Session()
        creds = session.get_credentials()
        st.write("Boto3 creds object:", creds)

        if creds:
            frozen = creds.get_frozen_credentials()
            st.write("Access key (partial):", frozen.access_key[:6] + "****")
        else:
            st.write("Boto3 creds: None ❌")

    except Exception as e:
        st.write("Boto3 error:", str(e))

    st.write("=== AWS DEBUG END ===")

# --------------------------------------------------
# AUTH
# --------------------------------------------------

ROLE_GROUPS = ["Admin", "Quant", "Trader", "Analyst", "Viewer"]


def resolve_role() -> str | None:
    email = (get_email() or "").strip().lower()
    groups = [g.strip().lower() for g in (get_groups() or [])]

    group_role_map = {
        "admin": "Admin",
        "quant": "Quant",
        "trader": "Trader",
        "analyst": "Analyst",
        "viewer": "Viewer",
    }

    for g in groups:
        if g in group_role_map:
            return group_role_map[g]

    raw_map = os.getenv("EMAIL_ROLE_MAP", "")
    mapping = {}

    for item in raw_map.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        k, v = item.split("=", 1)
        mapping[k.strip().lower()] = v.strip()

    if email in mapping and mapping[email] in ROLE_GROUPS:
        return mapping[email]

    if email == "chen_dpeng@hotmail.com":
        return "Admin"

    return "Viewer"


allowed_roles = ROLE_GROUPS

user = get_user()
if not user:
    st.warning("Please log in via SSO.")
    st.stop()

# --------------------------------------------------
# AWS + DB
# --------------------------------------------------

PGURL = os.getenv("DB_DSN")

engine = None
if PGURL:
    try:
        engine = create_engine(PGURL)
    except Exception:
        engine = None

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
USER_POOL_ID = os.getenv("COGNITO_USER_POOL_ID", "")

ecs = boto3.client("ecs", region_name=AWS_REGION)
cognito = boto3.client("cognito-idp", region_name=AWS_REGION)

cluster = os.getenv("ECS_CLUSTER")
private_subnets = [x.strip() for x in os.getenv("PRIVATE_SUBNETS", "").split(",") if x.strip()]
task_security_group = os.getenv("TASK_SECURITY_GROUPS")

# --------------------------------------------------
# COGNITO HELPERS
# --------------------------------------------------


def get_live_cognito_role() -> str | None:
    if not USER_POOL_ID or not user:
        return None

    username = (
        user.get("cognito:username")
        or user.get("username")
        or user.get("sub")
        or get_email()
    )
    if not username:
        return None

    try:
        resp = cognito.admin_list_groups_for_user(
            UserPoolId=USER_POOL_ID,
            Username=username,
        )
        groups = [g["GroupName"] for g in resp.get("Groups", [])]

        role_priority = ["Admin", "Trader", "Quant", "Analyst", "Viewer"]
        for r in role_priority:
            if r in groups:
                return r

        return None
    except Exception:
        return None


def list_cognito_users():
    if not USER_POOL_ID:
        return []

    users = []
    pagination_token = None

    while True:
        kwargs = {"UserPoolId": USER_POOL_ID, "Limit": 60}
        if pagination_token:
            kwargs["PaginationToken"] = pagination_token

        resp = cognito.list_users(**kwargs)

        for u in resp.get("Users", []):
            attrs = {a["Name"]: a["Value"] for a in u.get("Attributes", [])}
            users.append(
                {
                    "username": u.get("Username"),
                    "email": attrs.get("email", ""),
                    "status": u.get("UserStatus", ""),
                    "enabled": u.get("Enabled", True),
                }
            )

        pagination_token = resp.get("PaginationToken")
        if not pagination_token:
            break

    users.sort(key=lambda x: ((x["email"] or "").lower(), x["username"].lower()))
    return users


def get_user_groups(username: str):
    if not USER_POOL_ID:
        return []

    resp = cognito.admin_list_groups_for_user(
        UserPoolId=USER_POOL_ID,
        Username=username,
    )
    return [g["GroupName"] for g in resp.get("Groups", [])]


def set_user_role(username: str, new_role: str):
    current_groups = get_user_groups(username)

    for group in ROLE_GROUPS:
        if group in current_groups and group != new_role:
            cognito.admin_remove_user_from_group(
                UserPoolId=USER_POOL_ID,
                Username=username,
                GroupName=group,
            )

    if new_role not in current_groups:
        cognito.admin_add_user_to_group(
            UserPoolId=USER_POOL_ID,
            Username=username,
            GroupName=new_role,
        )

# --------------------------------------------------
# ROLE / PERMISSIONS
# --------------------------------------------------

role = get_live_cognito_role() or resolve_role()

if not role:
    st.error(f"Access denied. No valid role found. Email: {get_email()}")
    st.stop()

if role not in allowed_roles:
    st.error(f"Access denied. Your role: {role}. Allowed roles: {allowed_roles}")
    st.stop()

user_email = user.get("email", "unknown") if user else "unknown"

IS_VIEWER = role == "Viewer"
CAN_OPEN_APPS = role in ["Admin", "Trader", "Quant", "Analyst"]
CAN_QUICK_ASK = role in ["Admin", "Trader", "Quant", "Analyst"]
CAN_MANAGE_USERS = role == "Admin"

COGNITO_DOMAIN = os.getenv(
    "COGNITO_DOMAIN",
    "https://bess-platform-auth.auth.ap-southeast-1.amazoncognito.com",
)
COGNITO_CLIENT_ID = os.getenv("COGNITO_CLIENT_ID", "")
LOGOUT_REDIRECT_URI = os.getenv(
    "LOGOUT_REDIRECT_URI",
    "https://www.pjh-etrm.ai/signed-out",
)

logout_url = (
    f"{COGNITO_DOMAIN}/logout"
    f"?client_id={COGNITO_CLIENT_ID}"
    f"&logout_uri={urllib.parse.quote(LOGOUT_REDIRECT_URI, safe='')}"
)

# --------------------------------------------------
# QUICK ASK — Anthropic one-shot helper
# --------------------------------------------------

_QUICK_ASK_SYSTEM = {
    "strategist": (
        "You are the Strategist — China spot electricity market analyst. "
        "You give concise, expert answers on: spot market prices, inter-provincial flows, "
        "market fundamentals (load, new energy, system tightness), and market rules. "
        "Keep answers under 150 words. No tool calls — answer from your domain knowledge."
    ),
    "quant": (
        "You are the Quant — BESS investment economics specialist. "
        "You give concise, expert answers on: province-level BESS economics, LP dispatch, "
        "IRR modelling, capture rates, and investment screening. "
        "Keep answers under 150 words. No tool calls — answer from your domain knowledge."
    ),
    "trader": (
        "You are the Trader — Inner Mongolia BESS trading operations analyst. "
        "You give concise, expert answers on: asset P&L attribution, dispatch quality, "
        "execution gaps, RT price dynamics for the 4 IM BESS assets "
        "(SuYou, HangJinQi, SiZiWangQi, GuShanLiang). "
        "Keep answers under 150 words. No tool calls — answer from your domain knowledge."
    ),
    "deal_structurer": (
        "You are the Deal Structurer — investment committee analyst. "
        "You give concise, expert answers on: BESS investment deal structuring, "
        "market attractiveness assessment, IRR hurdle rates, equity/debt structure, "
        "and investment memorandum framing for China renewable assets. "
        "Keep answers under 150 words. No tool calls — answer from your domain knowledge."
    ),
    "gb_analyst": (
        "You are the GB Analyst — Great Britain BESS market intelligence specialist. "
        "You give concise, expert answers on: GB BESS leaderboard performance, EPEX DA "
        "prices and arbitrage spreads, Balancing Mechanism (BM) revenues, ancillary "
        "markets (FFR, DCL, DCH, reserve), system price and NIV dynamics, asset "
        "owner/operator benchmarking, and BESS options valuation and dispatch modelling. "
        "Keep answers under 150 words. No tool calls — answer from your domain knowledge."
    ),
}


def _quick_ask(agent_key: str, question: str) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return "ANTHROPIC_API_KEY not configured."
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            system=_QUICK_ASK_SYSTEM[agent_key],
            messages=[{"role": "user", "content": question}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        return f"Error: {e}"


# --------------------------------------------------
# APP URL RESOLVER
# --------------------------------------------------

_DEV_PORTS = {
    "spot-markets":     "8505",
    "bess-map":         "8503",
    "mengxi-dashboard": "8511",
    "gb-market":        "8508",
}


def _app_url(path_slug: str) -> str:
    """Resolve app URL — APP_URL_MAP overrides first, then dev-mode localhost, then ALB path."""
    raw = os.getenv("APP_URL_MAP", "")
    for item in raw.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        slug, url = item.split("=", 1)
        if slug.strip() == path_slug:
            return url.strip()
    if os.getenv("AUTH_MODE", "alb_oidc").lower() == "dev" and path_slug in _DEV_PORTS:
        return f"http://localhost:{_DEV_PORTS[path_slug]}"
    return f"/{path_slug}/"


# --------------------------------------------------
# AGENT SECTION RENDERER
# --------------------------------------------------

def _render_agent_section(
    icon: str,
    name: str,
    subtitle: str,
    description: str,
    capabilities: list[str],
    app_slug: str | None,
    agent_key: str,
    available: bool = True,
):
    with st.container(border=True):
        # Header row: icon + name + Open App button
        hcol1, hcol2 = st.columns([7, 2])
        with hcol1:
            st.markdown(
                f"<h2 style='margin:0; padding:0'>{icon} {name}</h2>"
                f"<p style='color:#888; margin:0; font-size:0.9rem'>{subtitle}</p>",
                unsafe_allow_html=True,
            )
        with hcol2:
            st.markdown("<br>", unsafe_allow_html=True)
            if available and CAN_OPEN_APPS and app_slug:
                st.link_button(
                    "Open App →",
                    _app_url(app_slug),
                    use_container_width=True,
                    type="primary",
                )
            else:
                st.button(
                    "Coming Soon",
                    disabled=True,
                    key=f"open_{agent_key}",
                    use_container_width=True,
                )

        st.markdown(f"*{description}*")

        # Capabilities
        cap_cols = st.columns(2)
        half = len(capabilities) // 2 + len(capabilities) % 2
        for i, cap in enumerate(capabilities):
            cap_cols[i // half].markdown(f"- {cap}")

        st.markdown("---")

        # Quick Ask
        if not IS_VIEWER and CAN_QUICK_ASK:
            with st.expander("Quick Ask", expanded=False):
                q_key = f"qa_input_{agent_key}"
                r_key = f"qa_reply_{agent_key}"

                if r_key not in st.session_state:
                    st.session_state[r_key] = ""

                q = st.text_input(
                    f"Ask {name} a quick question",
                    key=q_key,
                    placeholder=f"e.g. What provinces have the best BESS economics right now?",
                    label_visibility="collapsed",
                )
                c1, c2 = st.columns([2, 8])
                with c1:
                    if st.button("Ask", key=f"qa_btn_{agent_key}", type="primary"):
                        if q.strip():
                            with st.spinner("Thinking…"):
                                st.session_state[r_key] = _quick_ask(agent_key, q.strip())
                with c2:
                    if st.button("Clear", key=f"qa_clear_{agent_key}"):
                        st.session_state[r_key] = ""

                if st.session_state[r_key]:
                    st.info(st.session_state[r_key])
                    st.caption(
                        f"Quick answer — no live data access. Open the app for full {name} analysis."
                    )


# --------------------------------------------------
# HEADER
# --------------------------------------------------

header_left, header_right = st.columns([6, 1])

with header_left:
    st.title("BESS Investment-Trading-Asset Intelligence")
    st.caption(f"User: {user_email} | Role: {role}")

with header_right:
    st.markdown("<br>", unsafe_allow_html=True)
    if os.getenv("AUTH_MODE", "alb_oidc").lower() == "dev":
        st.caption("Dev Mode")
    else:
        st.markdown(
            f'<a href="{logout_url}" target="_self">'
            f'<button style="padding:0.5rem 1rem; border-radius:0.5rem; border:1px solid #ccc; background:white; cursor:pointer;">Logout</button>'
            f"</a>",
            unsafe_allow_html=True,
        )

if IS_VIEWER:
    st.info("You are signed in as Viewer. This role has read-only access to the portal.")

# --------------------------------------------------
# DATA OPERATIONS STATUS
# --------------------------------------------------

st.subheader("Data Operations Status")

try:
    import sqlalchemy as _sa
    from shared.data_ops.status import get_recent_ops, get_pipeline_jobs

    _pgurl = os.environ.get("PGURL") or os.environ.get("DB_DSN", "")
    _ops_engine = _sa.create_engine(_pgurl) if _pgurl else None

    if _ops_engine is None:
        st.info("DB not configured (PGURL missing).")
    else:
        _ops  = get_recent_ops(_ops_engine, hours=48)
        _jobs = get_pipeline_jobs(_ops_engine)

        # ── Summary tiles: last run per op type ─────────────────────────
        if not _ops.empty:
            _last = _ops.groupby("op_name").first().reset_index()
            _status_icon = {"success": "✅", "running": "⏳", "failed": "❌"}
            cols = st.columns(max(len(_last), 1))
            for i, (_, row) in enumerate(_last.iterrows()):
                icon = _status_icon.get(row["status"], "❓")
                cols[i].metric(
                    label=row["op_name"],
                    value=f"{icon} {row['status']}",
                    delta=row.get("market") or "",
                )
        else:
            st.info("No data operations recorded in the last 48 hours.")

        # ── Running pipeline jobs ────────────────────────────────────────
        if not _jobs.empty:
            running = _jobs[_jobs["status"] == "running"]
            if not running.empty:
                names = ", ".join(running["job_name"].tolist())
                st.warning(f"{len(running)} pipeline job(s) currently running: {names}")

        # ── Recent ops table (collapsible) ───────────────────────────────
        if not _ops.empty:
            with st.expander("Recent operations (last 48 h)", expanded=False):
                st.dataframe(
                    _ops[["op_name", "market", "date_range", "status", "message",
                           "started_at", "duration_s"]],
                    use_container_width=True,
                    hide_index=True,
                )
except Exception as _e:
    st.info(f"Data operations status unavailable: {_e}")

st.divider()

# --------------------------------------------------
# ADMIN USER MANAGEMENT
# --------------------------------------------------

if CAN_MANAGE_USERS:
    st.subheader("User Access Management")

    try:
        if not USER_POOL_ID:
            st.warning("COGNITO_USER_POOL_ID is not configured.")
        else:
            users = list_cognito_users()

            if not users:
                st.info("No Cognito users found.")
            else:
                user_options = {
                    f"{u['email'] or '-'} | {u['username']}": u
                    for u in users
                }

                selected_label = st.selectbox(
                    "Select user",
                    options=list(user_options.keys()),
                    key="admin_select_user",
                )

                selected_user = user_options[selected_label]
                current_groups = get_user_groups(selected_user["username"])
                current_role = next((g for g in ROLE_GROUPS if g in current_groups), "Viewer")

                c1, c2, c3 = st.columns([2, 2, 1])
                with c1:
                    st.write(f"**Username:** {selected_user['username']}")
                    st.write(f"**Email:** {selected_user['email'] or '-'}")
                with c2:
                    st.write(f"**Status:** {selected_user['status']}")
                    st.write(f"**Current role:** {current_role}")
                with c3:
                    new_role = st.selectbox(
                        "New role",
                        options=ROLE_GROUPS,
                        index=ROLE_GROUPS.index(current_role)
                        if current_role in ROLE_GROUPS
                        else ROLE_GROUPS.index("Viewer"),
                        key=f"new_role_{selected_user['username']}",
                    )

                if st.button("Apply role", key=f"apply_role_{selected_user['username']}"):
                    try:
                        set_user_role(selected_user["username"], new_role)
                        st.success(
                            f"Updated {selected_user['email'] or selected_user['username']} to role {new_role}."
                        )
                        st.rerun()
                    except ClientError as e:
                        st.error(f"Failed to update role: {e}")
                    except Exception as e:
                        st.error(f"Failed to update role: {e}")

    except ClientError as e:
        st.error(f"Cognito admin panel failed: {e}")
    except Exception as e:
        st.error(f"Cognito admin panel failed: {e}")

    st.divider()

# --------------------------------------------------
# 4 AGENT SECTIONS
# --------------------------------------------------

st.subheader("Your Intelligence Team")
st.caption(
    "Five specialist agents covering the full investment lifecycle. "
    "Open an app for deep analysis, or Quick Ask for instant answers."
)

st.markdown("<br>", unsafe_allow_html=True)

# ── Row 1: Strategist + Quant ──────────────────────────────────────────────
col_strategist, col_quant = st.columns(2)

with col_strategist:
    _render_agent_section(
        icon="📊",
        name="Strategist",
        subtitle="China Spot Market Intelligence · Pillar 1",
        description=(
            "Analyses China's provincial spot electricity markets — price spreads, "
            "inter-provincial flows, market fundamentals, and system tightness. "
            "Trained on market rules, exchange annual reports, and policy documents "
            "via the Knowledge Pool."
        ),
        capabilities=[
            "Daily DA/RT price spread & volatility by province",
            "Inter-provincial flow analysis (省间现货交易)",
            "Market fundamentals: load, new energy, thermal capacity",
            "System tightness & congestion signals",
            "Knowledge base: market rules, policy docs, annual reports",
            "Conversation memory across sessions",
        ],
        app_slug="spot-markets",
        agent_key="strategist",
        available=True,
    )

with col_quant:
    _render_agent_section(
        icon="📐",
        name="Quant",
        subtitle="BESS Investment Economics · Pillar 2",
        description=(
            "Screens provinces for BESS investment attractiveness using LP perfect-foresight "
            "dispatch. Computes theoretical revenue, capture rate, and equity IRR under "
            "configurable CapEx/O&M/RTE scenarios. Province ranking updated daily."
        ),
        capabilities=[
            "Province ranking: annual revenue/MWh/day (2h and 4h)",
            "LP-optimal dispatch detail by province and date",
            "IRR, NPV, and payback under custom assumptions",
            "Capture rate vs perfect-foresight benchmark",
            "Realised vs theoretical revenue comparison",
            "Conversation memory across sessions",
        ],
        app_slug="bess-map",
        agent_key="quant",
        available=True,
    )

st.markdown("<br>", unsafe_allow_html=True)

# ── Row 2: Trader + Deal Structurer ──────────────────────────────────────
col_trader, col_deal = st.columns(2)

with col_trader:
    _render_agent_section(
        icon="⚡",
        name="Trader",
        subtitle="Mengxi BESS Trading Operations · Pillar 3",
        description=(
            "Operations and trading analyst for the 4 Inner Mongolia BESS assets. "
            "Tracks daily P&L attribution across the full 5-step waterfall — from "
            "perfect-foresight upper bound down to actual cleared dispatch — and "
            "identifies execution gaps."
        ),
        capabilities=[
            "Daily P&L waterfall for SuYou, HangJinQi, SiZiWangQi, GuShanLiang",
            "Dispatch quality: charge/discharge curves, SoC profile",
            "Execution gap attribution (grid restriction, forecast error, nomination)",
            "RT clearing price dynamics and market context",
            "Strategy comparison: multi-strategy simulation",
            "Auto-save memory: ops observations persist across sessions",
        ],
        app_slug="mengxi-dashboard",
        agent_key="trader",
        available=True,
    )

with col_deal:
    _render_agent_section(
        icon="🏦",
        name="Deal Structurer",
        subtitle="Investment Committee · Pillar 5",
        description=(
            "Orchestrates the investment committee process — aggregating market signals "
            "from the Strategist, economics from the Quant, and ops benchmarks from the "
            "Trader into a structured investment recommendation. Quick Ask available now; "
            "full app coming in the next build."
        ),
        capabilities=[
            "Market screen: province attractiveness assessment",
            "Economics case: IRR vs hurdle rate (target: >12% equity IRR)",
            "Ops benchmark: realisation rate vs IM asset portfolio",
            "Risk factors: regulatory, curtailment, counterparty, grid access",
            "Deal structure: equity/debt split, duration, subsidy, exit horizon",
            "Investment memorandum drafting (full app TBD)",
        ],
        app_slug=None,
        agent_key="deal_structurer",
        available=False,
    )

st.markdown("<br>", unsafe_allow_html=True)

# ── Row 3: GB Analyst (full width) ───────────────────────────────────────────
col_gb, _ = st.columns(2)

with col_gb:
    _render_agent_section(
        icon='<img src="https://flagcdn.com/w40/gb.png" style="height:0.9em;vertical-align:middle;border-radius:2px;margin-right:2px;">',
        name="GB Analyst",
        subtitle="Great Britain BESS Market Intelligence · GB Market",
        description=(
            "Live intelligence platform for the GB battery storage market. "
            "Tracks daily asset performance across the full GB BESS fleet, covering "
            "wholesale arbitrage, Balancing Mechanism, ancillary services (FFR, DC), "
            "and reserve markets. Includes AI-generated market commentary, pricing "
            "models, and automated daily reports delivered by email and WeCom."
        ),
        capabilities=[
            "Daily BESS leaderboard: revenue by asset, owner, operator",
            "EPEX DA prices: baseload, peak/off-peak, arbitrage spreads",
            "Balancing Mechanism & system price / NIV analysis",
            "Ancillary markets: FFR, DCL, DCH, reserve clearing prices",
            "Pricing models: BESS options value, PF dispatch, OLS forecast",
            "Automated daily PDF report via email & WeCom",
        ],
        app_slug="gb-market",
        agent_key="gb_analyst",
        available=True,
    )

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.divider()
st.caption(
    "Investment-Trading-Asset Intelligence and Decisions System · "
    "BESS Platform · Powered by Claude"
)
