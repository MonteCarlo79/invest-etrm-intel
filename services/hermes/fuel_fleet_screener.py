"""
Province Fuel Price + Generation Fleet Screener
================================================
Searches the knowledge base (staging.spot_knowledge_chunks) for per-province
fuel/fleet documents, then uses Claude to extract:
  - 动力煤价格 (yuan/t, 5500 kcal benchmark)
  - 天然气门站价 (yuan/m³, power-generation gas)
  - 煤电/气电装机结构 (2–4 efficiency segments with heat rate + VOM)

Upserts results via services.hermes.fuel_fleet_etl (status='draft' pending
human confirmation; conflicts flagged against confirmed rows).

Entry points:
  screen_fuel_fleet(pg_url, api_key, feishu, owner_open_id, year, provinces)
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from typing import Optional

import psycopg2

from services.hermes.fuel_fleet_etl import upsert_fuel_fleet_rows, _valid_segments

logger = logging.getLogger(__name__)

# ── Province list ─────────────────────────────────────────────────────────────

_SEARCH_PROVINCES = [
    "山东", "山西", "蒙西", "广东", "甘肃", "江苏",
    "浙江", "河北南网", "冀北", "河南", "新疆",
]

# ── KB keywords ────────────────────────────────────────────────────────────────

_FUEL_FLEET_KEYWORDS = ["动力煤", "煤价", "天然气门站价", "装机结构"]

_RATE_DELAY_SECONDS = 1  # delay between province queries

EXTRACTION_PROMPT = """你是电力市场数据提取助手。目标省份：{province}，年份：{year}。

从下面的资料中提取该省的：
1. 动力煤标杆/指数价（元/吨，5500大卡为准；若只有其他热值，按热值比例折算并说明）
2. 天然气门站价（元/立方米；取发电用气价格，非居民用气）
3. 煤电/气电装机结构，拆成 2–4 个效率段，每段给出：
   fuel（coal 或 gas）、capacity_mw、heat_rate_kj_kwh（供电煤耗×3600≈kJ/kWh；超超临界≈8200、超临界≈8700、亚临界≈9500、CCGT≈6400、OCGT≈9000 可作先验）、vom_yuan_mwh（变动运维费，煤≈12、气≈8 可作先验）、label

只输出一个 JSON 对象（不要 markdown）：
{{"coal_price_yuan_t": float|null, "gas_price_yuan_m3": float|null,
  "fleet_segments": [...], "confidence": "high|medium|low", "source_url": "来源文件名或URL"}}

资料（{n_chunks} 段）：
{context}"""

_EXTRACTION_SYSTEM = (
    "你是中国电力市场数据提取专家。从提供的文本中提取燃料价格与装机结构数据。"
    "只提取明确出现在文本中的数据，不要猜测。若文本无相关数据，返回null值。"
    "Respond ONLY with valid JSON, no other text."
)


# ── KB search ──────────────────────────────────────────────────────────────────

def _search_kb(province: str, keywords: list, pg_url: str, limit: int = 12) -> list:
    """
    Search knowledge base for chunks relevant to province + keywords.
    Returns list of (chunk_text, file_name) tuples.

    Strategy:
    1. Prefer chunks from docs whose filename contains the province name
    2. Fall back to chunks that mention the province in their text
    Keyword filter: at least one keyword must match in the chunk.
    """
    # Normalise province for search (strip parentheses variants)
    prov_short = province.split("（")[0].split("(")[0]

    # Build keyword filter — at least one keyword must match in chunk
    kw_parts = " OR ".join(["c.chunk_text ILIKE %s"] * len(keywords))

    # Priority 1: doc filename contains province + keyword in chunk
    # Priority 2: chunk text contains province + keyword in chunk
    sql = f"""
        SELECT c.chunk_text, COALESCE(d.file_name, '') AS file_name,
               CASE WHEN d.file_name ILIKE %s THEN 1 ELSE 2 END AS priority
        FROM staging.spot_knowledge_chunks c
        LEFT JOIN staging.spot_knowledge_docs d ON d.id = c.doc_id
        WHERE ({kw_parts})
          AND (d.file_name ILIKE %s OR c.chunk_text ILIKE %s
               OR d.region_province ILIKE %s)
        ORDER BY priority ASC, d.published_at DESC NULLS LAST, c.id DESC
        LIMIT %s
    """
    params = (
        [f"%{prov_short}%"]          # priority CASE WHEN
        + [f"%{kw}%" for kw in keywords]  # keyword filter
        + [f"%{prov_short}%", f"%{prov_short}%", f"%{prov_short}%", limit]
    )
    try:
        conn = psycopg2.connect(pg_url, connect_timeout=10)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = [(r[0], r[1]) for r in cur.fetchall()]  # drop priority column
        conn.close()
        return rows
    except Exception as exc:
        logger.warning("KB search error for %s: %s", province, exc)
        return []


# ── Claude extraction ──────────────────────────────────────────────────────────

def _claude_extract(
    province: str,
    kb_rows: list,    # list of (chunk_text, file_name)
    api_key: str,
    year: int,
) -> Optional[dict]:
    """
    Call Claude directly with KB context to extract fuel/fleet data.
    Falls back to Claude's training knowledge if kb_rows is empty.
    """
    if kb_rows:
        context_parts = []
        sources = set()
        for chunk_text, file_name in kb_rows[:8]:
            context_parts.append(chunk_text[:1200])
            if file_name:
                sources.add(file_name)
        context = "\n\n---\n\n".join(context_parts)
        source_hint = "; ".join(list(sources)[:3])
    else:
        # No KB data — ask Claude to use training knowledge
        context = f"（知识库中未找到{province}的相关文档，请根据你的训练知识回答，并将confidence设为low）"
        source_hint = "claude_training_knowledge"

    user_msg = EXTRACTION_PROMPT.format(
        province=province,
        year=year,
        n_chunks=len(kb_rows),
        context=context,
    )

    try:
        from shared.anthropic_client import make_client as _make_anthropic_client
        client = _make_anthropic_client(api_key)
        resp = client.messages.create(
            model="claude-sonnet-4-6",  # haiku-4-5 requires use-case form on this Bedrock account
            max_tokens=1024,
            system=_EXTRACTION_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = resp.content[0].text.strip()
        data = _extract_json(text)
        # Always prefer source_hint (actual KB filenames) over Claude's guessed source_url
        if data and source_hint:
            data["source_url"] = source_hint
        elif data and not data.get("source_url"):
            data["source_url"] = source_hint  # empty string / None → keep hint
        return data
    except Exception as exc:
        logger.error("Claude extraction failed for %s: %s", province, exc)
        return None


# ── JSON extraction ────────────────────────────────────────────────────────────

def _extract_json(text: str) -> Optional[dict]:
    """Extract first JSON object from agent response text.

    Order matters: the prompt instructs raw JSON (no markdown), so try the
    full response first; the conservative [^{}]+ pattern is LAST because it
    cannot span nested braces (e.g. fleet_segments: [{...}]) and would
    otherwise match only an inner dict.
    """
    # 1. Whole response is bare JSON (the instructed no-markdown path)
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        pass
    # 2-4. Locate a JSON block inside surrounding text
    patterns = [
        r'```(?:json)?\s*(\{[^`]+\})\s*```',  # fenced block
        r'(\{.*\})',                          # greedy — spans nested braces
        r'(\{[^{}]+\})',                      # conservative fallback, flat only
    ]
    for pat in patterns:
        m = re.search(pat, text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                continue
    return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _effective_date(data: dict, year: int) -> str:
    """Resolve effective_date: explicit date in data, else Jan 1 of target year."""
    eff = data.get("effective_date")
    if isinstance(eff, str) and re.match(r"^\d{4}-\d{2}-\d{2}", eff):
        return eff[:10]
    return f"{year}-01-01"


# ── Main screener ──────────────────────────────────────────────────────────────

def screen_fuel_fleet(
    pg_url: str,
    api_key: str,
    feishu=None,
    owner_open_id: str = "",
    year: Optional[int] = None,
    provinces: Optional[list] = None,
) -> dict:
    """
    Loop over provinces, search KB + call Claude to extract coal/gas prices and
    fleet composition, upsert results to marketdata.province_fuel_fleet (draft).

    Returns summary dict: {scanned, extracted, upserted, errors}.
    """
    year = year or datetime.now().year
    provinces = provinces or _SEARCH_PROVINCES
    summary = {"scanned": 0, "extracted": 0, "upserted": 0, "errors": []}

    logger.info(
        "fuel_fleet_screener: starting scan for %d provinces (year=%d)",
        len(provinces), year,
    )

    for province in provinces:
        summary["scanned"] += 1
        try:
            kb_rows = _search_kb(province, _FUEL_FLEET_KEYWORDS, pg_url)
            logger.info("fuel_fleet KB: %s → %d chunks found", province, len(kb_rows))
            data = _claude_extract(province, kb_rows, api_key, year)
            if data and _valid_segments(data.get("fleet_segments")):
                # Sanitise numeric fields — a non-numeric string from Claude
                # would raise inside the ETL and roll back the whole batch.
                row = {
                    "province": province,
                    "effective_date": _effective_date(data, year),
                    "coal_price_yuan_t": _safe_float(data.get("coal_price_yuan_t")),
                    "gas_price_yuan_m3": _safe_float(data.get("gas_price_yuan_m3")),
                    "fleet_segments": data["fleet_segments"],
                    "source": str(data.get("source_url", ""))[:200],
                    "notes": f"confidence={data.get('confidence')}",
                }
                summary["extracted"] += 1
                res = upsert_fuel_fleet_rows(
                    [row], pg_url,
                    source=f"fuel_fleet_screener:{str(data.get('source_url', ''))[:200]}",
                )
                summary["upserted"] += res["upserted"]
                summary["errors"].extend(res["errors"])
                logger.info(
                    "fuel_fleet upserted: %s coal=%s gas=%s segs=%d conf=%s",
                    province, row["coal_price_yuan_t"], row["gas_price_yuan_m3"],
                    len(row["fleet_segments"]), data.get("confidence"),
                )
            else:
                logger.info("fuel_fleet: no usable extraction for %s", province)
        except Exception as exc:
            logger.error("fuel_fleet failed for %s: %s", province, exc)
            summary["errors"].append(f"fuel_fleet/{province}: {exc}")

        time.sleep(_RATE_DELAY_SECONDS)

    logger.info(
        "fuel_fleet_screener done: extracted=%d/%d upserted=%d errors=%d",
        summary["extracted"], summary["scanned"],
        summary["upserted"], len(summary["errors"]),
    )

    # ── Feishu notification ──
    if feishu and owner_open_id:
        try:
            feishu.send_text(
                open_id=owner_open_id,
                text=f"⛽ 燃料装机扫描完成：{summary['extracted']}/{summary['scanned']} 提取，"
                     f"{summary['upserted']} 入库（draft，待确认）",
            )
        except Exception as exc:
            logger.warning("Failed to send Feishu notification: %s", exc)

    return summary
