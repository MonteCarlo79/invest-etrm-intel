"""
services/exchange_reports/metrics_extractor.py

AI-powered structured data extraction from provincial exchange monthly reports.

Uses Claude to extract key market metrics from full report text, then upserts
to staging.exchange_monthly_metrics for cross-province comparison.

Common fields across all 9 provinces (where available):
  - Total traded volume (GWh)
  - Year-on-year change (%)
  - Average settlement/transaction price (yuan/MWh)
  - Peak / valley prices
  - Spot vs medium-long-term volume split
  - Renewable energy share (%)
  - Installed capacity (GW)
  - Max load (GW)
  - Market participant counts
  - Key highlights (free text, 2-3 sentences)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

# ── LLM provider config ───────────────────────────────────────────────────────
#
# Provider selection (first match wins):
#   1. DEEPSEEK_API_KEY set           → DeepSeek  (OpenAI-compatible, China-accessible)
#   2. BEDROCK_REGION set             → AWS Bedrock Claude
#   3. ANTHROPIC_API_KEY set          → Direct Anthropic API
#
# Optional overrides:
#   DEEPSEEK_MODEL   (default: deepseek-chat)
#   BEDROCK_MODEL_ID (default: anthropic.claude-haiku-4-5-20251001-v1:0)

_DEFAULT_DIRECT_MODEL   = "claude-haiku-4-5-20251001"
_DEFAULT_BEDROCK_MODEL  = "anthropic.claude-haiku-4-5-20251001-v1:0"
_DEFAULT_DEEPSEEK_MODEL = "deepseek-chat"
_DEEPSEEK_BASE_URL      = "https://api.deepseek.com"

# OpenAI-format tool schema for DeepSeek (same fields, different wrapper)
_TOOL_SCHEMA_OPENAI = {
    "type": "function",
    "function": {
        "name": "store_market_metrics",
        "description": (
            "Store structured metrics extracted from a Chinese provincial power exchange "
            "monthly report. Use null for any field not found in the report text."
        ),
        "parameters": None,  # filled in after _TOOL_SCHEMA is defined
    },
}


def _get_provider() -> str:
    """Return 'deepseek', 'bedrock', or 'anthropic'."""
    if os.environ.get("DEEPSEEK_API_KEY", "").strip():
        return "deepseek"
    if os.environ.get("BEDROCK_REGION", "").strip():
        return "bedrock"
    return "anthropic"


def _get_client(api_key: Optional[str] = None):
    """
    Return (client, model_id, provider) for LLM calls.
    provider is 'deepseek', 'bedrock', or 'anthropic'.
    """
    provider = _get_provider()

    if provider == "deepseek":
        from openai import OpenAI
        key = os.environ.get("DEEPSEEK_API_KEY", "")
        model_id = os.environ.get("DEEPSEEK_MODEL", _DEFAULT_DEEPSEEK_MODEL)
        client = OpenAI(api_key=key, base_url=_DEEPSEEK_BASE_URL)
        logger.debug("Using DeepSeek client, model=%s", model_id)
        return client, model_id, "deepseek"

    import anthropic
    if provider == "bedrock":
        region = os.environ.get("BEDROCK_REGION")
        model_id = os.environ.get("BEDROCK_MODEL_ID", _DEFAULT_BEDROCK_MODEL)
        client = anthropic.AnthropicBedrock(aws_region=region)
        logger.debug("Using Bedrock client in %s, model=%s", region, model_id)
        return client, model_id, "bedrock"

    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    client = anthropic.Anthropic(api_key=key)
    return client, _DEFAULT_DIRECT_MODEL, "anthropic"


# ── DB DDL ────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS staging.exchange_monthly_metrics (
    id                          SERIAL PRIMARY KEY,
    province                    TEXT NOT NULL,
    report_month                DATE NOT NULL,
    report_type                 TEXT NOT NULL DEFAULT 'monthly',

    -- Volume
    total_volume_gwh            NUMERIC(12,2),   -- 总成交/用电量 (亿千瓦时)
    volume_yoy_pct              NUMERIC(6,2),    -- 同比变化 (%)
    spot_volume_gwh             NUMERIC(12,2),   -- 现货成交量
    medium_longterm_volume_gwh  NUMERIC(12,2),   -- 中长期成交量

    -- Prices (yuan/MWh — 元/兆瓦时 = 元/千千瓦时 × 1000 if input in fen)
    avg_price_yuan_mwh          NUMERIC(8,2),    -- 加权平均价格
    peak_price_yuan_mwh         NUMERIC(8,2),    -- 峰段价格
    valley_price_yuan_mwh       NUMERIC(8,2),    -- 谷段价格
    spot_avg_price_yuan_mwh     NUMERIC(8,2),    -- 现货均价

    -- Generation mix
    renewable_pct               NUMERIC(6,2),    -- 新能源/可再生能源占比 (%)
    wind_pct                    NUMERIC(6,2),    -- 风电占比
    solar_pct                   NUMERIC(6,2),    -- 光伏/太阳能占比
    thermal_pct                 NUMERIC(6,2),    -- 火电占比
    hydro_pct                   NUMERIC(6,2),    -- 水电占比

    -- Capacity & load
    installed_capacity_gw       NUMERIC(10,2),   -- 装机容量 (万千瓦 → GW)
    max_load_gw                 NUMERIC(8,2),    -- 最大用电负荷
    avg_load_gw                 NUMERIC(8,2),    -- 平均负荷

    -- Market participants
    market_participants_total   INT,             -- 注册市场主体总数
    generators_count            INT,             -- 发电企业
    retailers_count             INT,             -- 售电公司
    consumers_count             INT,             -- 电力用户

    -- Text summary
    key_highlights              TEXT,            -- AI-generated 2-3 sentence summary

    -- Metadata
    exchange_report_id          INT REFERENCES staging.exchange_monthly_reports(id),
    extracted_at                TIMESTAMPTZ DEFAULT NOW(),
    extraction_model            TEXT DEFAULT 'claude-haiku-4-5-20251001',

    UNIQUE(province, report_month, report_type)
);
"""

_TABLES_INITIALIZED = False


def init_metrics_table(pg_url: Optional[str] = None) -> None:
    """Create metrics table if not exists."""
    global _TABLES_INITIALIZED
    if _TABLES_INITIALIZED:
        return
    import psycopg2
    url = pg_url or os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise RuntimeError("pg_url required")
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
    finally:
        conn.close()
    _TABLES_INITIALIZED = True


# ── Claude extraction ─────────────────────────────────────────────────────────

_TOOL_SCHEMA = {
    "name": "store_market_metrics",
    "description": (
        "Store structured metrics extracted from a Chinese provincial power exchange "
        "monthly report. Use null for any field not found in the report text."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "total_volume_gwh": {
                "type": ["number", "null"],
                "description": "Total electricity traded/consumed this month in 亿千瓦时 (GWh×100). "
                               "Look for 总成交量, 总用电量, 全社会用电量. Report in 亿千瓦时.",
            },
            "volume_yoy_pct": {
                "type": ["number", "null"],
                "description": "Year-on-year change percentage. Look for 同比增长/下降 X%.",
            },
            "spot_volume_gwh": {
                "type": ["number", "null"],
                "description": "Spot market traded volume in 亿千瓦时. Look for 现货成交量, 日前+实时成交.",
            },
            "medium_longterm_volume_gwh": {
                "type": ["number", "null"],
                "description": "Medium-long term contract volume in 亿千瓦时. Look for 中长期成交量.",
            },
            "avg_price_yuan_mwh": {
                "type": ["number", "null"],
                "description": "Weighted average settlement price in yuan/MWh (元/兆瓦时). "
                               "If report shows 元/千瓦时, multiply by 1000. "
                               "If 分/千瓦时, divide by 10. Look for 加权平均价, 结算均价, 成交均价.",
            },
            "peak_price_yuan_mwh": {
                "type": ["number", "null"],
                "description": "Peak period price yuan/MWh. Look for 峰段价格, 高峰, 峰时.",
            },
            "valley_price_yuan_mwh": {
                "type": ["number", "null"],
                "description": "Valley period price yuan/MWh. Look for 谷段价格, 低谷, 谷时.",
            },
            "spot_avg_price_yuan_mwh": {
                "type": ["number", "null"],
                "description": "Spot market average price yuan/MWh. Look for 现货均价, 日前均价.",
            },
            "renewable_pct": {
                "type": ["number", "null"],
                "description": "Renewable energy share as percentage of total generation. "
                               "Look for 新能源占比, 可再生能源占比.",
            },
            "wind_pct": {
                "type": ["number", "null"],
                "description": "Wind power percentage. Look for 风电占比.",
            },
            "solar_pct": {
                "type": ["number", "null"],
                "description": "Solar/PV percentage. Look for 光伏占比, 太阳能占比.",
            },
            "thermal_pct": {
                "type": ["number", "null"],
                "description": "Thermal power percentage. Look for 火电占比.",
            },
            "hydro_pct": {
                "type": ["number", "null"],
                "description": "Hydro power percentage. Look for 水电占比.",
            },
            "installed_capacity_gw": {
                "type": ["number", "null"],
                "description": "Total installed generation capacity in GW. "
                               "If report shows 万千瓦, divide by 100. Look for 装机容量, 装机规模.",
            },
            "max_load_gw": {
                "type": ["number", "null"],
                "description": "Maximum load in GW. Look for 最大负荷, 最高负荷. "
                               "If in 万千瓦, divide by 100.",
            },
            "avg_load_gw": {
                "type": ["number", "null"],
                "description": "Average load in GW. Look for 平均负荷.",
            },
            "market_participants_total": {
                "type": ["integer", "null"],
                "description": "Total registered market participants. Look for 注册市场主体, 市场主体数量.",
            },
            "generators_count": {
                "type": ["integer", "null"],
                "description": "Number of registered generators/power plants. Look for 发电企业, 发电厂.",
            },
            "retailers_count": {
                "type": ["integer", "null"],
                "description": "Number of electricity retailers. Look for 售电公司.",
            },
            "consumers_count": {
                "type": ["integer", "null"],
                "description": "Number of electricity consumers. Look for 电力用户, 购电用户.",
            },
            "key_highlights": {
                "type": "string",
                "description": (
                    "2-3 concise sentences in Chinese summarising the most notable market "
                    "developments this month: price trends, volume changes, renewable growth, "
                    "or any unusual market events."
                ),
            },
        },
        "required": ["key_highlights"],
    },
}

# Patch the OpenAI schema now that _TOOL_SCHEMA is defined
_TOOL_SCHEMA_OPENAI["function"]["parameters"] = _TOOL_SCHEMA["input_schema"]


def extract_metrics(
    full_text: str,
    province: str,
    report_month: date,
    api_key: Optional[str] = None,
) -> Optional[dict]:
    """
    Extract structured metrics from report full text via LLM (DeepSeek / Bedrock / Anthropic).

    Returns dict of metric fields (matching DB columns), or None on failure.
    """
    provider = _get_provider()
    if provider == "anthropic":
        api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            logger.warning("No LLM provider configured (set DEEPSEEK_API_KEY, BEDROCK_REGION, or ANTHROPIC_API_KEY)")
            return None

    # Truncate to ~12k chars (fits context comfortably, covers ~6 pages)
    text_sample = full_text[:12000]

    system_prompt = (
        "You are extracting structured data from a Chinese provincial power exchange "
        f"monthly market report. Province: {province}, Month: {report_month.strftime('%Y年%m月')}. "
        "Extract numerical values precisely as reported. "
        "For volume units: 亿千瓦时 = GWh×100. Always report volumes in 亿千瓦时. "
        "For prices: if given as 元/千瓦时, multiply by 1000 to get 元/兆瓦时 (yuan/MWh). "
        "If given as 分/千瓦时, divide by 10 to get yuan/MWh. "
        "Use null for any field not present in the text."
    )
    user_message = (
        f"Extract market metrics from this {province} power exchange report "
        f"for {report_month.strftime('%Y年%m月')}:\n\n{text_sample}"
    )

    try:
        client, model_id, provider = _get_client(api_key=api_key)

        if provider == "deepseek":
            # OpenAI-compatible function calling
            resp = client.chat.completions.create(
                model=model_id,
                tools=[_TOOL_SCHEMA_OPENAI],
                tool_choice={"type": "function", "function": {"name": "store_market_metrics"}},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_message},
                ],
            )
            tool_calls = resp.choices[0].message.tool_calls
            if tool_calls:
                return json.loads(tool_calls[0].function.arguments)

        else:
            # Anthropic SDK (direct or Bedrock)
            resp = client.messages.create(
                model=model_id,
                max_tokens=1024,
                system=system_prompt,
                tools=[_TOOL_SCHEMA],
                tool_choice={"type": "tool", "name": "store_market_metrics"},
                messages=[{"role": "user", "content": user_message}],
            )
            for block in resp.content:
                if block.type == "tool_use" and block.name == "store_market_metrics":
                    return block.input

    except Exception as exc:
        logger.error("Metrics extraction failed for %s %s: %s", province, report_month, exc)

    return None


# ── DB upsert ─────────────────────────────────────────────────────────────────

_METRIC_COLS = [
    "total_volume_gwh", "volume_yoy_pct",
    "spot_volume_gwh", "medium_longterm_volume_gwh",
    "avg_price_yuan_mwh", "peak_price_yuan_mwh", "valley_price_yuan_mwh", "spot_avg_price_yuan_mwh",
    "renewable_pct", "wind_pct", "solar_pct", "thermal_pct", "hydro_pct",
    "installed_capacity_gw", "max_load_gw", "avg_load_gw",
    "market_participants_total", "generators_count", "retailers_count", "consumers_count",
    "key_highlights",
]


def upsert_metrics(
    metrics: dict,
    province: str,
    report_month: date,
    report_type: str = "monthly",
    exchange_report_id: Optional[int] = None,
    model: str = "claude-haiku-4-5-20251001",
    pg_url: Optional[str] = None,
) -> int:
    """
    Upsert extracted metrics into staging.exchange_monthly_metrics.
    Returns the row id.
    """
    import psycopg2
    url = pg_url or os.environ.get("PGURL") or os.environ.get("DB_DSN")
    conn = psycopg2.connect(url)
    try:
        init_metrics_table(pg_url)
        cols = ["province", "report_month", "report_type", "exchange_report_id", "extraction_model"]
        vals = [province, report_month, report_type, exchange_report_id, model]
        for col in _METRIC_COLS:
            cols.append(col)
            vals.append(metrics.get(col))

        set_clause = ", ".join(
            f"{c} = EXCLUDED.{c}"
            for c in cols
            if c not in ("province", "report_month", "report_type")
        )

        sql = f"""
            INSERT INTO staging.exchange_monthly_metrics ({", ".join(cols)})
            VALUES ({", ".join(["%s"] * len(cols))})
            ON CONFLICT (province, report_month, report_type)
            DO UPDATE SET {set_clause}, extracted_at = NOW()
            RETURNING id
        """
        with conn.cursor() as cur:
            cur.execute(sql, vals)
            row_id = cur.fetchone()[0]
        conn.commit()
        return row_id
    finally:
        conn.close()


# ── Combined extract + upsert ─────────────────────────────────────────────────

def extract_and_store(
    full_text: str,
    province: str,
    report_month: date,
    report_type: str = "monthly",
    exchange_report_id: Optional[int] = None,
    api_key: Optional[str] = None,
    pg_url: Optional[str] = None,
) -> Optional[int]:
    """
    Extract metrics via Claude and store to DB.
    Returns metrics row id, or None if extraction failed.
    """
    metrics = extract_metrics(full_text, province, report_month, api_key=api_key)
    if metrics is None:
        return None
    return upsert_metrics(
        metrics=metrics,
        province=province,
        report_month=report_month,
        report_type=report_type,
        exchange_report_id=exchange_report_id,
        pg_url=pg_url,
    )


# ── Query helpers ─────────────────────────────────────────────────────────────

def get_metrics_table(
    year: Optional[int] = None,
    month: Optional[int] = None,
    pg_url: Optional[str] = None,
) -> list[dict]:
    """
    Return all metrics rows for a given year/month, ordered by province.
    If month is None, returns the latest month available per province.
    """
    import psycopg2
    url = pg_url or os.environ.get("PGURL") or os.environ.get("DB_DSN")
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            if month:
                cur.execute(
                    """
                    SELECT * FROM staging.exchange_monthly_metrics
                    WHERE EXTRACT(YEAR FROM report_month) = %s
                      AND EXTRACT(MONTH FROM report_month) = %s
                    ORDER BY province
                    """,
                    (year or date.today().year, month),
                )
            else:
                # Latest month per province
                cur.execute(
                    """
                    SELECT DISTINCT ON (province) *
                    FROM staging.exchange_monthly_metrics
                    WHERE (%s IS NULL OR EXTRACT(YEAR FROM report_month) = %s)
                    ORDER BY province, report_month DESC
                    """,
                    (year, year),
                )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


def get_available_months(pg_url: Optional[str] = None) -> list[str]:
    """Return distinct report_months as YYYY-MM strings, most recent first."""
    import psycopg2
    url = pg_url or os.environ.get("PGURL") or os.environ.get("DB_DSN")
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT TO_CHAR(report_month, 'YYYY-MM') "
                "FROM staging.exchange_monthly_metrics "
                "ORDER BY 1 DESC"
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
