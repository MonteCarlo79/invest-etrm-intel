"""
DB upsert helpers for:
  staging.spot_interprov_flow     (inter-provincial trading rows)
  staging.spot_report_summaries   (AI-generated daily summaries)
"""
from __future__ import annotations

import logging
from typing import List

from services.knowledge_pool.db import get_conn

_log = logging.getLogger(__name__)

_INTERPROV_UPSERT = """
INSERT INTO staging.spot_interprov_flow (
    report_date, direction, metric_type,
    province_cn, province_share,
    price_yuan_kwh, price_chg_pct,
    time_period, total_vol_100gwh, source_pdf
) VALUES (
    %(report_date)s, %(direction)s, %(metric_type)s,
    %(province_cn)s, %(province_share)s,
    %(price_yuan_kwh)s, %(price_chg_pct)s,
    %(time_period)s, %(total_vol_100gwh)s, %(source_pdf)s
)
ON CONFLICT (report_date, direction, metric_type) DO UPDATE SET
    province_cn      = EXCLUDED.province_cn,
    province_share   = EXCLUDED.province_share,
    price_yuan_kwh   = COALESCE(EXCLUDED.price_yuan_kwh,   staging.spot_interprov_flow.price_yuan_kwh),
    price_chg_pct    = COALESCE(EXCLUDED.price_chg_pct,    staging.spot_interprov_flow.price_chg_pct),
    time_period      = EXCLUDED.time_period,
    total_vol_100gwh = COALESCE(EXCLUDED.total_vol_100gwh, staging.spot_interprov_flow.total_vol_100gwh),
    source_pdf       = EXCLUDED.source_pdf;
"""

_SUMMARY_UPSERT = """
INSERT INTO staging.spot_report_summaries (
    report_date, summary_text, model,
    prompt_tokens, completion_tokens, source_pdf
) VALUES (
    %(report_date)s, %(summary_text)s, %(model)s,
    %(prompt_tokens)s, %(completion_tokens)s, %(source_pdf)s
)
ON CONFLICT (report_date) DO UPDATE SET
    summary_text      = EXCLUDED.summary_text,
    model             = EXCLUDED.model,
    prompt_tokens     = EXCLUDED.prompt_tokens,
    completion_tokens = EXCLUDED.completion_tokens,
    source_pdf        = EXCLUDED.source_pdf;
"""


def upsert_interprov_rows(rows: List[dict]) -> int:
    """Upsert interprov flow rows.  Returns number of rows processed."""
    if not rows:
        return 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(_INTERPROV_UPSERT, row)
        conn.commit()
    return len(rows)


def upsert_summary(summary: dict) -> None:
    """Upsert one AI summary row."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_SUMMARY_UPSERT, summary)
        conn.commit()
