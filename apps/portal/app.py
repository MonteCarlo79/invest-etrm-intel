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

st.set_page_config(page_title="BESS Platform", layout="wide")

# --------------------------------------------------
# PROJECT IMPORTS
# --------------------------------------------------

from shared.agents.registry import get_visible_apps, get_visible_by_category
from auth.rbac import get_user, get_groups, get_email
from shared.metrics.portfolio import get_portfolio_metrics
from shared.metrics.agents import get_agent_status
from shared.metrics.dispatch import get_dispatch_preview
from shared.metrics.market import get_price_series

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
CAN_RUN_AGENTS = role in ["Admin", "Trader", "Quant", "Analyst"]
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

visible_apps = [] if role == "Viewer" else get_visible_apps(role)
app_items = [] if role == "Viewer" else get_visible_by_category(role, "Applications")
agent_items = [] if role == "Viewer" else get_visible_by_category(role, "Agents")

# --------------------------------------------------
# ECS RUNNER
# --------------------------------------------------


def run_ecs_task(task_def: str, display_name: str):
    try:
        if not task_def:
            st.error(f"No task_definition configured for {display_name}")
            return

        if not cluster:
            st.error("ECS_CLUSTER is not configured.")
            return

        if not private_subnets:
            st.error("PRIVATE_SUBNETS is not configured.")
            return

        if not task_security_group:
            st.error("TASK_SECURITY_GROUPS is not configured.")
            return

        response = ecs.run_task(
            cluster=cluster,
            taskDefinition=task_def,
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": private_subnets,
                    "securityGroups": [task_security_group],
                    "assignPublicIp": "DISABLED",
                }
            },
        )

        failures = response.get("failures", [])
        tasks = response.get("tasks", [])

        if failures:
            st.error(f"Failed to start {display_name}: {failures}")
        elif tasks:
            st.success(f"{display_name} started.")
        else:
            st.warning(f"No task started for {display_name}.")
    except Exception as e:
        st.error(f"Failed to start {display_name}: {e}")

# --------------------------------------------------
# UI HELPERS
# --------------------------------------------------


def render_application_cards(items: list[dict]):
    if not items:
        st.info("No application modules are visible for your role.")
        return

    cols = st.columns(3)
    for i, item in enumerate(items):
        with cols[i % 3]:
            with st.container(border=True):
                st.markdown(f"### {item['name']}")
                st.write(item.get("description", "-"))
                path = item.get("path", "")

                if CAN_OPEN_APPS and path:
                    st.link_button("Open", path, use_container_width=True)
                else:
                    st.button(
                        "Open",
                        disabled=True,
                        key=f"disabled_open_{item['name']}",
                        use_container_width=True,
                    )


def render_agent_cards(items: list[dict]):
    if not items:
        st.info("No agent modules are visible for your role.")
        return

    cols = st.columns(2)
    for i, item in enumerate(items):
        with cols[i % 2]:
            with st.container(border=True):
                st.markdown(f"### {item['name']}")
                st.write(item.get("description", "-"))

                task_def = item.get("task_definition")
                path = item.get("path", "")

                btn_cols = st.columns(2)

                with btn_cols[0]:
                    if CAN_OPEN_APPS and path:
                        st.link_button(
                            "Open UI",
                            path,
                            use_container_width=True,
                        )
                    else:
                        st.button(
                            "Open UI",
                            disabled=True,
                            key=f"disabled_open_agent_{item['name']}",
                            use_container_width=True,
                        )

                with btn_cols[1]:
                    if CAN_RUN_AGENTS and task_def:
                        if st.button(
                            "Run Task",
                            key=f"run_{item['name']}",
                            use_container_width=True,
                        ):
                            run_ecs_task(task_def, item["name"])
                    else:
                        st.button(
                            "Run Task",
                            disabled=True,
                            key=f"disabled_run_{item['name']}",
                            use_container_width=True,
                        )

                if task_def:
                    st.caption(f"ECS task: {task_def}")

# --------------------------------------------------
# HEADER
# --------------------------------------------------

header_left, header_right = st.columns([6, 1])

with header_left:
    st.title("⚡ BESS Energy Investment & Trading Platform")
    st.caption(f"User: {user_email} | Role: {role}")

with header_right:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        f'<a href="{logout_url}" target="_self">'
        f'<button style="padding:0.5rem 1rem; border-radius:0.5rem; border:1px solid #ccc; background:white; cursor:pointer;">Logout</button>'
        f"</a>",
        unsafe_allow_html=True,
    )

if IS_VIEWER:
    st.info("You are signed in as Viewer. This role has read-only access to the portal.")

# --------------------------------------------------
# PORTFOLIO SNAPSHOT
# --------------------------------------------------

st.subheader("Portfolio Snapshot")

try:
    metrics = get_portfolio_metrics()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total PnL", f"{metrics['total_pnl']:,.0f} ¥")
    m2.metric("Today PnL", f"{metrics['today_pnl']:,.0f} ¥")
    m3.metric("Active Assets", metrics["assets"])
    m4.metric("Running Agents", len(agent_items))
except Exception:
    st.info("Portfolio metrics not available yet.")

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
# PLATFORM METRICS
# --------------------------------------------------

k1, k2, k3, k4 = st.columns(4)
k1.metric("Role", role)
k2.metric("Visible Modules", len(visible_apps))
k3.metric("Agent Modules", len(agent_items))
k4.metric("Status", "Operational")

# --------------------------------------------------
# APPLICATIONS
# --------------------------------------------------

st.subheader("Applications")
render_application_cards(app_items)

# --------------------------------------------------
# AI AGENTS
# --------------------------------------------------

st.divider()
st.subheader("AI Agents")
render_agent_cards(agent_items)

# --------------------------------------------------
# AGENT STATUS
# --------------------------------------------------

st.divider()
st.subheader("Agent Status")

try:
    df_agents = get_agent_status()
    st.dataframe(df_agents, use_container_width=True)
except Exception:
    st.info("Agent status table not available.")

# --------------------------------------------------
# DISPATCH PREVIEW
# --------------------------------------------------

st.divider()
st.subheader("Dispatch Preview (Next 24h)")

try:
    df_dispatch = get_dispatch_preview()
    st.dataframe(df_dispatch, use_container_width=True)
except Exception:
    st.info("Execution plan not available yet.")

# --------------------------------------------------
# MARKET PRICES
# --------------------------------------------------

st.divider()
st.subheader("Market Prices")

try:
    df_prices = get_price_series()
    st.line_chart(df_prices.set_index("timestamp"))
except Exception as e:
    st.info(f"Market prices not available yet: {e}")

# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.divider()
st.info(
    """
    **Platform Control Tower**

    Applications provide operational tools.

    AI Agents automate:

    • Strategy generation  
    • Portfolio optimization  
    • Execution planning  
    • Development workflows
    """
)