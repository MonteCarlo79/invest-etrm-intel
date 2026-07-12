# services/knowledge_pool/jizhi_extractor.py
"""
Structured extraction and persistence for 136号文 机制竞价 bid data.

Tables managed:
  staging.jizhi_bids         — completed bid results (province × year × batch × tech_type)
  staging.jizhi_bid_winners  — 中标清单 (optional sub-table)
  staging.jizhi_upcoming     — upcoming bid calendar

Public API:
  ensure_tables(pg_url)
  extract_bids(text, api_key) -> list[dict]
  extract_upcoming(text, api_key) -> list[dict]
  save_bids(records, source_doc_id, pg_url) -> int
  save_upcoming(records, pg_url) -> int
"""
from __future__ import annotations
import logging

import psycopg2

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS staging.jizhi_bids (
    id                  SERIAL PRIMARY KEY,
    province            TEXT NOT NULL,
    year                INT  NOT NULL,
    batch               TEXT NOT NULL,
    tech_type           TEXT NOT NULL,
    price_floor         NUMERIC,
    price_cap           NUMERIC,
    mechanism_type      TEXT,
    mechanism_value     NUMERIC,
    supply_demand_ratio NUMERIC,
    cleared_price       NUMERIC,
    cleared_volume_gwh  NUMERIC,
    bid_date            DATE,
    verified            BOOLEAN NOT NULL DEFAULT FALSE,
    source_doc_id       INT,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (province, year, batch, tech_type)
);
CREATE TABLE IF NOT EXISTS staging.jizhi_bid_winners (
    id            SERIAL PRIMARY KEY,
    bid_id        INT NOT NULL REFERENCES staging.jizhi_bids(id) ON DELETE CASCADE,
    project_name  TEXT NOT NULL,
    operator      TEXT,
    capacity_mw   NUMERIC,
    cleared_price NUMERIC,
    tech_type     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_jizhi_winners_bid
    ON staging.jizhi_bid_winners(bid_id);
CREATE TABLE IF NOT EXISTS staging.jizhi_upcoming (
    id                   SERIAL PRIMARY KEY,
    province             TEXT NOT NULL,
    year                 INT  NOT NULL,
    batch                TEXT NOT NULL,
    tech_type            TEXT NOT NULL,
    price_floor          NUMERIC,
    price_cap            NUMERIC,
    target_volume_gwh    NUMERIC,
    supply_demand_ratio  NUMERIC,
    bid_open_date        DATE,
    bid_close_date       DATE,
    source_url           TEXT,
    announcement_date    DATE,
    verified             BOOLEAN NOT NULL DEFAULT FALSE,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (province, year, batch, tech_type, bid_open_date)
);
"""

_BIDS_TOOL = {
    "name": "save_bid_results",
    "description": "Save extracted 机制竞价 completed bid results from the document.",
    "input_schema": {
        "type": "object",
        "properties": {
            "bids": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "province":            {"type": "string",  "description": "Province name in Chinese, e.g. 广东"},
                        "year":                {"type": "integer", "description": "Year of the bidding, e.g. 2025"},
                        "batch":               {"type": "string",  "description": "One of: 存量, 增量_2025-12, 增量_2026-12, 增量_2027-12"},
                        "tech_type":           {"type": "string",  "description": "One of: 陆风, 海风, 光伏, 水电"},
                        "price_floor":         {"type": "number",  "description": "Minimum bid price in 元/kWh"},
                        "price_cap":           {"type": "number",  "description": "Maximum bid price in 元/kWh"},
                        "mechanism_type":      {"type": "string",  "description": "One of: 电量, 比例, 小时数"},
                        "mechanism_value":     {"type": "number",  "description": "Value in GWh (电量), % (比例), or hours (小时数)"},
                        "supply_demand_ratio": {"type": "number",  "description": "Supply-demand ratio, e.g. 1.35"},
                        "cleared_price":       {"type": "number",  "description": "Final cleared price in 元/kWh"},
                        "cleared_volume_gwh":  {"type": "number",  "description": "Total cleared volume in GWh"},
                        "bid_date":            {"type": "string",  "description": "Bid date as YYYY-MM-DD"},
                        "notes":               {"type": "string"},
                    },
                    "required": ["province", "year", "batch", "tech_type"],
                },
            }
        },
        "required": ["bids"],
    },
}

_BIDS_PROMPT = """\
Extract ALL 机制竞价 completed bid results from the document below.

Normalisation rules:
- batch: 存量 = grid-connected before 2025-05-31 \
; 增量_2025-12 = before 2025-12-31; 增量_2026-12 = before 2026-12-31; 增量_2027-12 = before 2027-12-31
- tech_type: 陆风 / 海风 / 光伏 / 水电  (map 风电/wind → 陆风 unless specifically 海风)
- prices in 元/kWh  (divide by 1000 if document uses 元/MWh)
- cleared_volume_gwh in GWh  (divide by 1000 if document uses TWh)
- bid_date as YYYY-MM-DD

Document:
{text}"""


def ensure_tables(pg_url: str) -> None:
    """Create jizhi_bids, jizhi_bid_winners, jizhi_upcoming if they don't exist."""
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
        logger.info("[jizhi] tables ensured")
    finally:
        conn.close()


def extract_bids(text: str, api_key: str) -> list[dict]:
    """Extract structured bid results from document text via Claude tool-use.

    Returns list of dicts with keys matching staging.jizhi_bids columns
    (excluding id, source_doc_id, created_at).
    Returns [] on failure or empty input.
    """
    if not api_key or not text.strip():
        return []
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4096,
            tools=[_BIDS_TOOL],
            tool_choice={"type": "tool", "name": "save_bid_results"},
            messages=[{
                "role": "user",
                "content": _BIDS_PROMPT.format(text=text[:15000]),
            }],
        )
        for block in response.content:
            if block.type == "tool_use" and block.name == "save_bid_results":
                return block.input.get("bids", [])
    except Exception as exc:
        logger.error("[jizhi] extract_bids failed: %s", exc)
    return []
