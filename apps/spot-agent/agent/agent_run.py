"""
agent_run.py — DEPRECATED.  Do not use.

This file was an early prototype that required the OpenAI Agents SDK (openai-agents)
and referenced functions that were never implemented:
  - parse_pdf_tables        (not in tools_pdf.py)
  - digitize_hourly_chart   (not in tools_pdf.py — charts are images, not tables)
  - write_postgres          (not in tools_db.py)

It also assumed hourly price data could be extracted from PDF charts via vision models.
That is not supported by the current tooling and is not planned for the near term.

SUPPORTED ENTRYPOINT
====================
Use spot_ingest.py for all PDF ingestion:

  One-shot:
      python agent/spot_ingest.py --header agent/spot_header_bess.yaml --no-llm

  Watch mode (ongoing ingestion):
      python agent/spot_ingest.py --header agent/spot_header_bess.yaml --watch --interval 300

  See README.md for the full runbook.
"""

import sys

print(
    "\n[DEPRECATED] agent_run.py is not a supported entrypoint.\n"
    "\nUse spot_ingest.py instead:\n"
    "  python agent/spot_ingest.py --header agent/spot_header_bess.yaml --no-llm\n"
    "\nFor ongoing ingestion (watch mode):\n"
    "  python agent/spot_ingest.py --header agent/spot_header_bess.yaml --watch\n"
    "\nSee README.md for the full runbook.\n",
    file=sys.stderr,
)
sys.exit(1)
