import base64
import json
import os
from typing import Any, Dict, List, Optional

import boto3
import streamlit as st


OIDC_DATA_HEADER = "x-amzn-oidc-data"
OIDC_IDENTITY_HEADER = "x-amzn-oidc-identity"


def _get_headers() -> Dict[str, str]:
    try:
        headers = getattr(st.context, "headers", {})
        return {str(k).lower(): v for k, v in headers.items()}
    except Exception:
        return {}


def _get_header(headers: Dict[str, str], name: str) -> Optional[str]:
    return headers.get(name.lower())


def _pad_b64(value: str) -> str:
    return value + "=" * (-len(value) % 4)


def _decode_jwt_payload(jwt_token: str) -> Dict[str, Any]:
    try:
        parts = jwt_token.split(".")
        if len(parts) < 2:
            return {}
        payload = parts[1]
        decoded = base64.urlsafe_b64decode(_pad_b64(payload))
        return json.loads(decoded.decode("utf-8"))
    except Exception:
        return {}


def _get_user_from_oidc() -> Optional[Dict[str, Any]]:
    headers = _get_headers()

    token = _get_header(headers, OIDC_DATA_HEADER)
    if not token:
        token = os.environ.get("HTTP_X_AMZN_OIDC_DATA")

    if not token:
        return None

    payload = _decode_jwt_payload(token)
    if not payload:
        return None

    identity = _get_header(headers, OIDC_IDENTITY_HEADER) or os.environ.get("HTTP_X_AMZN_OIDC_IDENTITY")
    if identity and "email" not in payload:
        payload["email"] = identity

    payload["_auth_source"] = "oidc"
    return payload


def _password_login() -> Optional[Dict[str, Any]]:
    if "user" in st.session_state:
        return st.session_state["user"]

    st.sidebar.markdown("### Login")
    password = st.sidebar.text_input("Password", type="password")

    if not password:
        return None

    admin_pw = os.getenv("ADMIN_PASSWORD")
    internal_pw = os.getenv("INTERNAL_PASSWORD")
    investor_pw = os.getenv("INVESTOR_PASSWORD")

    if password == admin_pw:
        user = {"email": "admin", "role": "Admin"}
    elif password == internal_pw:
        user = {"email": "internal", "role": "Trader"}
    elif password == investor_pw:
        user = {"email": "investor", "role": "Analyst"}
    else:
        st.sidebar.error("Invalid password")
        return None

    user["_auth_source"] = "password"
    st.session_state["user"] = user
    return user


def get_user() -> Optional[Dict[str, Any]]:
    user = _get_user_from_oidc()
    if user:
        return user

    if os.getenv("ENABLE_PASSWORD_LOGIN", "false").lower() == "true":
        return _password_login()

    return None


def get_email() -> Optional[str]:
    user = get_user()
    if not user:
        return None

    return (
        user.get("email")
        or user.get("preferred_username")
        or user.get("cognito:username")
        or user.get("username")
        or user.get("sub")
    )


def _email_role_map() -> Dict[str, str]:
    raw_map = os.getenv("EMAIL_ROLE_MAP", "")
    mapping: Dict[str, str] = {}

    for item in raw_map.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        k, v = item.split("=", 1)
        mapping[k.strip().lower()] = v.strip()

    return mapping


def _normalize_role_name(value: str) -> str:
    v = (value or "").strip().lower()
    role_map = {
        "admin": "Admin",
        "trader": "Trader",
        "quant": "Quant",
        "analyst": "Analyst",
        "viewer": "Viewer",
    }
    return role_map.get(v, value.strip())


def _live_cognito_groups() -> List[str]:
    user = get_user()
    if not user:
        return []

    user_pool_id = os.getenv("COGNITO_USER_POOL_ID", "")
    region = os.getenv("AWS_REGION", "ap-southeast-1")
    if not user_pool_id:
        return []

    username = (
        user.get("cognito:username")
        or user.get("username")
        or user.get("sub")
        or user.get("email")
    )
    if not username:
        return []

    try:
        cognito = boto3.client("cognito-idp", region_name=region)
        resp = cognito.admin_list_groups_for_user(
            UserPoolId=user_pool_id,
            Username=username,
        )
        return [_normalize_role_name(g["GroupName"]) for g in resp.get("Groups", [])]
    except Exception:
        return []


def get_groups() -> List[str]:
    user = get_user()
    if not user:
        return []

    # password fallback
    if "role" in user and user["role"]:
        return [_normalize_role_name(user["role"])]

    # live Cognito group lookup
    live_groups = _live_cognito_groups()
    if live_groups:
        return live_groups

    # groups from token/header
    groups = user.get("cognito:groups", [])
    if isinstance(groups, str):
        groups = [groups]
    elif not isinstance(groups, list):
        groups = []

    normalized_groups = [_normalize_role_name(g) for g in groups if str(g).strip()]
    if normalized_groups:
        return normalized_groups

    # Email fallback
    email = (get_email() or "").strip().lower()
    mapping = _email_role_map()

    if email and email in mapping:
        return [_normalize_role_name(mapping[email])]

    # Hard fallback admin
    if email == "chen_dpeng@hotmail.com":
        return ["Admin"]

    # Default authenticated users to Viewer
    return ["Viewer"]


def get_role(priority_order: Optional[List[str]] = None) -> Optional[str]:
    groups = get_groups()
    if not groups:
        return None

    if priority_order is None:
        priority_order = ["Admin", "Trader", "Quant", "Analyst", "Viewer"]

    normalized_priority = [_normalize_role_name(r) for r in priority_order]

    for role in normalized_priority:
        if role in groups:
            return role

    return groups[0]


def has_role(allowed_roles: List[str]) -> bool:
    role = get_role(priority_order=allowed_roles)
    return role in [_normalize_role_name(r) for r in allowed_roles]


def require_role(allowed_roles: List[str]) -> str:
    user = get_user()
    if not user:
        st.warning("Please log in via SSO.")
        st.stop()

    normalized_allowed = [_normalize_role_name(r) for r in allowed_roles]
    role = get_role(priority_order=normalized_allowed)

    if not role:
        st.error(f"Access denied. No valid role found. Email: {get_email()}")
        st.stop()

    if role not in normalized_allowed:
        st.error(f"Access denied. Your role: {role}. Allowed roles: {normalized_allowed}")
        st.stop()

    return role