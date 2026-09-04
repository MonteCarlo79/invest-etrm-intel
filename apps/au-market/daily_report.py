"""Australia (NEM) daily market report — thin wrapper around shared template.

Exposes generate_report_pdf / send_daily_report_email with the same call signatures
as apps/gb-market/daily_report.py so the app_template.py scheduler can load this
module dynamically and call it without knowing about MarketConfig.
"""
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from services.au_knowledge.config import MARKET_CONFIG as _CFG
from services.intl_market_common.daily_report_template import (
    _get_conn,
    _get_latest_data_date as _tpl_latest,
    generate_report_pdf as _tpl_generate,
    send_daily_report_email as _tpl_send_email,
    send_daily_report_wecom as _tpl_send_wecom,
    run_daily_report as _tpl_run,
)


def _get_latest_data_date(conn):
    return _tpl_latest(conn, _CFG)


def generate_report_pdf(report_date=None):
    return _tpl_generate(_CFG, report_date)


def send_daily_report_email(pdf_bytes, report_date, to_email=None, from_email=None, ai_commentary=""):
    return _tpl_send_email(_CFG, pdf_bytes, report_date, to_email, from_email, ai_commentary)


def send_daily_report_wecom(pdf_bytes, report_date, webhook_url=None, ai_commentary=""):
    return _tpl_send_wecom(_CFG, pdf_bytes, report_date, webhook_url, ai_commentary)


def run_daily_report(to_email=None):
    return _tpl_run(_CFG, to_email)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    print(run_daily_report())
