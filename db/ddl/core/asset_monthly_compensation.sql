# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 17:48:50 2026

@author: dipeng.chen
"""

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.asset_monthly_compensation (
    asset_code                 text        NOT NULL,
    effective_month            date        NOT NULL,
    compensation_yuan_per_mwh  numeric     NOT NULL,
    source_system              text,
    notes                      text,
    active_flag                boolean     NOT NULL DEFAULT TRUE,
    created_at                 timestamptz NOT NULL DEFAULT now(),
    updated_at                 timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_code, effective_month)
);