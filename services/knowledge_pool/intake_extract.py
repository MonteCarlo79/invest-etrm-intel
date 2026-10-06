"""Market intel intake — batch → structured proposal via one vision call per ≤20 images."""
from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional

IMAGE_EXTS = {"png", "jpg", "jpeg", "webp"}
MIME = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "webp": "image/webp"}

# Mirrors services/lingfeng/run_daily.py::_ALL_MARKETS (+ 全国) — kept local to
# avoid importing the playwright-dependent lingfeng module into the app.
PROVINCES = [
    "河南", "新疆", "吉林", "海南", "湖北", "四川", "黑龙江", "福建", "浙江", "江苏",
    "广西", "安徽", "陕西", "贵州", "云南", "广东", "蒙东", "湖南", "宁夏", "辽宁",
    "河北南网", "甘肃", "蒙西", "山东", "山西", "冀北", "广州", "青海", "江西", "全国",
]

ROUTE_TYPES = ("capacity_comp_rate", "fr_market_params", "ancillary_revenue",
               "pipeline_stat", "spot_note", "quant_note")

CATEGORIES_ALLOWED = ("market_rules", "annual_report", "policy_doc", "technical_spec",
                      "research_report", "market_intel", "capacity_pipeline",
                      "ancillary_market", "other")

_GROUP = 20  # max images per vision call


class IntakeError(Exception):
    """Extraction failed (API error or unparseable response)."""


@dataclass
class Page:
    filename: str
    data: bytes
    kind: str                     # 'image' | 'text'
    text: str = ""                # pre-extracted for kind='text'
    error: Optional[str] = None


@dataclass
class RouteProposal:
    type: str
    province: Optional[str]
    content: str
    structured: dict = field(default_factory=dict)


@dataclass
class IntakeProposal:
    title: str
    province: Optional[str]
    category: str
    summary: str
    pages: list[tuple[int, str]]
    routes: list[RouteProposal]
    batch_hash: str
    raw_json: dict = field(default_factory=dict)


def batch_hash(pages_bytes: list[bytes]) -> str:
    h = hashlib.sha256()
    for digest in sorted(hashlib.sha256(b).hexdigest() for b in pages_bytes):
        h.update(digest.encode())
    return h.hexdigest()


_PROMPT_TMPL = """你在分析一组中文电力市场情报文件（同一主题，共 {n_pages} 页）。
输出严格 JSON（不要 markdown 围栏，不要任何额外文字）：
{{
  "title": "文档标题（主题+省份+日期，如可得）",
  "province": "省份（限选：{provinces}；无法判断为 null）",
  "category": "限选一个：{categories}",
  "summary": "150字内中文摘要",
  "pages": [{{"page_no": 页码整数, "text": "该页完整转录：表格逐行、图表读数与单位"}}],
  "routes": [{{"type": "...", "province": "省份或null", "content": "给对应分析师看的中文要点", "structured": {{...}}}}]
}}

路由类型（只输出文中真实存在的，宁缺毋滥）：
- capacity_comp_rate: 容量电价/容量补偿标准 → structured: {{"cap_comp_yuan_kw": 数值, "peak_duration_hours": 数值或null, "effective_date": "YYYY-MM-DD"}}
- fr_market_params: 调频容量价格(元/kW·h)/全省资金池 → structured: {{"fr_price_yuan_kw_h": 数值或null, "fr_pool_billion_yuan": 数值或null, "effective_date": "YYYY-MM-DD"}}
- ancillary_revenue: 已结算调频/辅助服务收入月度金额 → structured: {{"month": "YYYY-MM-01", "metric": "指标名", "amount_yuan": 数值（元）}}
- pipeline_stat: 装机/在库/备案/规划/缺口统计 → structured: {{"rows": [{{"metric": "英文蛇形名", "value": 数值, "unit": "GW|GWh|个|倍", "as_of_date": "YYYY-MM-DD", "source": "出处"}}]}}
- spot_note: 给现货策略分析师的供需/格局解读（structured 留空 {{}}）
- quant_note: 给储能量化分析师的收益机制/约束/需求体量解读（structured 留空 {{}}）

单位换算：万千瓦→GW 除以10；万元→元 乘10000；亿kWh→GWh 乘100。
页码从 {page_start} 开始连续编号。"""


def _call_vision(client, image_blocks: list[dict], text_pages: list[str],
                 page_start: int) -> dict:
    prompt = _PROMPT_TMPL.format(
        n_pages=len(image_blocks) + len(text_pages),
        provinces="、".join(PROVINCES), categories=" | ".join(CATEGORIES_ALLOWED),
        page_start=page_start,
    )
    if text_pages:
        prompt += "\n\n以下为文本页内容：\n" + "\n\n".join(text_pages)
    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=16384,
        messages=[{"role": "user", "content": image_blocks + [{"type": "text", "text": prompt}]}],
    )
    raw = resp.content[0].text.strip()
    try:
        start, end = raw.index("{"), raw.rindex("}") + 1
        return json.loads(raw[start:end])
    except (ValueError, json.JSONDecodeError) as exc:
        raise IntakeError(f"Extractor returned unparseable JSON: {exc}; raw head: {raw[:200]}") from exc


def _routes_from(data: dict) -> list[RouteProposal]:
    out = []
    for r in data.get("routes") or []:
        if r.get("type") not in ROUTE_TYPES:
            continue
        out.append(RouteProposal(
            type=r["type"], province=r.get("province"),
            content=str(r.get("content", "")), structured=r.get("structured") or {},
        ))
    return out


def extract_batch(pages: list[Page], api_key: str, *, _client=None) -> IntakeProposal:
    """One vision call per ≤20 image pages; groups merged into one proposal."""
    if _client is None:
        from shared.anthropic_client import make_client
        _client = make_client(api_key)

    images = [p for p in pages if p.kind == "image" and p.error is None]
    texts = [p for p in pages if p.kind == "text" and p.error is None]
    text_blobs = [f"【{p.filename}】\n{p.text}" for p in texts]

    groups = [images[i:i + _GROUP] for i in range(0, len(images), _GROUP)] or [[]]
    merged_pages: list[tuple[int, str]] = []
    merged_routes: list[RouteProposal] = []
    first: dict | None = None
    page_cursor = 1

    for gi, group in enumerate(groups):
        blocks = [
            {"type": "image",
             "source": {"type": "base64",
                        "media_type": MIME.get(p.filename.rsplit(".", 1)[-1].lower(), "image/jpeg"),
                        "data": base64.standard_b64encode(p.data).decode()}}
            for p in group
        ]
        data = _call_vision(_client, blocks, text_blobs if gi == 0 else [], page_cursor)
        if first is None:
            first = data
        for p in data.get("pages") or []:
            merged_pages.append((int(p.get("page_no", page_cursor)), str(p.get("text", ""))))
            page_cursor = max(page_cursor + 1, merged_pages[-1][0] + 1)
        merged_routes.extend(_routes_from(data))

    first = first or {}
    category = first.get("category")
    if category not in CATEGORIES_ALLOWED:
        category = "other"
    return IntakeProposal(
        title=str(first.get("title") or pages[0].filename if pages else "intake"),
        province=first.get("province") if first.get("province") in PROVINCES else None,
        category=category,
        summary=str(first.get("summary", "")),
        pages=merged_pages,
        routes=merged_routes,
        batch_hash=batch_hash([p.data for p in pages]),
        raw_json=first,
    )
