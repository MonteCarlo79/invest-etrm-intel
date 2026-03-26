# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 17:58:21 2026

@author: dipeng.chen
"""

import base64
import json
import streamlit as st

def get_user_info():
    headers = st.context.headers

    if "x-amzn-oidc-data" not in headers:
        return None

    token = headers["x-amzn-oidc-data"]

    payload = token.split(".")[1]
    padded = payload + "=" * (4 - len(payload) % 4)

    decoded = base64.urlsafe_b64decode(padded)
    data = json.loads(decoded)

    return data


def get_user_role():
    user = get_user_info()
    if not user:
        return None

    groups = user.get("cognito:groups", [])

    if len(groups) == 0:
        return None

    return groups[0]


def require_role(allowed_roles):
    role = get_user_role()

    if role not in allowed_roles:
        st.error("Access denied")
        st.stop()

    return role