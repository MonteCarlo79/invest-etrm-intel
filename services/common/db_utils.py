# -*- coding: utf-8 -*-
"""
Created on Sun Mar 22 22:51:02 2026

@author: dipeng.chen
"""

from __future__ import annotations

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_pg_url() -> str:
    pgurl = os.getenv("PGURL")
    if not pgurl:
        raise ValueError("PGURL environment variable is not set")
    return pgurl


def get_engine() -> Engine:
    return create_engine(get_pg_url())