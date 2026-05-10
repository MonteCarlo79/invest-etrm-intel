"""
Reference document knowledge base — market rules, annual reports, policy docs.

Separate from the daily spot report pipeline (staging.spot_report_* tables).
Tables: staging.spot_knowledge_docs, staging.spot_knowledge_chunks

Usage:
    from services.knowledge_pool.knowledge_docs import (
        init_knowledge_tables,
        register_and_ingest,
        search_reference_docs,
        list_knowledge_docs,
        delete_knowledge_doc,
        CATEGORY_LABELS,
    )
"""
from __future__ import annotations

import datetime as dt
import hashlib
import io
import re
from typing import Optional

import pdfplumber

from .db import get_conn


# ── File-type extraction ──────────────────────────────────────────────────────

_VISION_MIME: dict[str, str] = {
    "jpg":  "image/jpeg",
    "jpeg": "image/jpeg",
    "png":  "image/png",
    "gif":  "image/gif",
    "webp": "image/webp",
}


def _describe_image(
    image_bytes: bytes,
    mime_type: str,
    api_key: str,
    context: str = "",
) -> str:
    """
    Send an image to Claude vision and return a text description.
    Used for standalone image files and embedded images/charts in PPTX.
    """
    import base64
    import anthropic

    prompt = (
        "Describe this image in detail for text indexing. "
        "If it is a chart or graph, state the chart type, title, axis labels, "
        "units, data series names, and summarise the key trends or values. "
        "If it contains text or tables, transcribe them. "
        "Be thorough — the description will be stored as a searchable text chunk."
    )
    if context:
        prompt = f"Context: {context}\n\n" + prompt

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": mime_type,
                        "data": base64.standard_b64encode(image_bytes).decode(),
                    },
                },
                {"type": "text", "text": prompt},
            ],
        }],
    )
    return resp.content[0].text.strip()


def _extract_pages_pdf(file_bytes: bytes) -> list[tuple[int, str]]:
    """Return [(page_no, text), ...] from a PDF."""
    pages = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            pages.append((i, page.extract_text() or ""))
    return pages


def _extract_pages_pptx(
    file_bytes: bytes,
    api_key: Optional[str] = None,
) -> list[tuple[int, str]]:
    """
    Return [(slide_no, text), ...] from a PPTX file.

    - Text shapes: extracted directly.
    - Chart shapes: data (title, series, categories) extracted as structured text.
    - Picture shapes: described via Claude vision if api_key is provided.
    """
    from pptx import Presentation
    from pptx.enum.shapes import MSO_SHAPE_TYPE

    prs = Presentation(io.BytesIO(file_bytes))
    pages = []
    for i, slide in enumerate(prs.slides, start=1):
        parts = []

        for shape in slide.shapes:
            # ── Text frames ────────────────────────────────────────────────
            if shape.has_text_frame:
                for para in shape.text_frame.paragraphs:
                    line = "".join(run.text for run in para.runs).strip()
                    if line:
                        parts.append(line)

            # ── Charts — extract data as structured text ───────────────────
            elif shape.has_chart:
                chart = shape.chart
                try:
                    title = (
                        chart.chart_title.text_frame.text.strip()
                        if chart.has_title else "Chart"
                    )
                    parts.append(f"[Chart: {title}]")
                    for series in chart.series:
                        try:
                            vals = list(series.values)
                            parts.append(f"  Series '{series.name}': {vals}")
                        except Exception:
                            parts.append(f"  Series '{series.name}'")
                except Exception:
                    parts.append("[Chart]")

            # ── Pictures — Claude vision description ───────────────────────
            elif shape.shape_type == MSO_SHAPE_TYPE.PICTURE and api_key:
                try:
                    img_bytes = shape.image.blob
                    mime = shape.image.content_type or "image/png"
                    desc = _describe_image(
                        img_bytes, mime, api_key,
                        context=f"Slide {i}",
                    )
                    parts.append(f"[Image on slide {i}]: {desc}")
                except Exception:
                    pass

        pages.append((i, "\n".join(parts)))
    return pages


def _extract_pages_image(
    file_bytes: bytes,
    filename: str,
    api_key: Optional[str] = None,
) -> list[tuple[int, str]]:
    """Describe a standalone image file via Claude vision."""
    ext = filename.rsplit(".", 1)[-1].lower()
    mime = _VISION_MIME.get(ext, "image/jpeg")
    if not api_key:
        return [(1, f"[Image: {filename} — set ANTHROPIC_API_KEY to enable vision description]")]
    desc = _describe_image(file_bytes, mime, api_key)
    return [(1, desc)]


def _extract_pages_txt(file_bytes: bytes) -> list[tuple[int, str]]:
    """Split plain text into pages of 100 lines each."""
    text = file_bytes.decode("utf-8", errors="replace")
    lines = text.splitlines()
    page_size = 100
    pages = []
    for i in range(0, max(len(lines), 1), page_size):
        block = "\n".join(lines[i:i + page_size]).strip()
        if block:
            pages.append((i // page_size + 1, block))
    return pages or [(1, "")]


def _extract_pages_docx(file_bytes: bytes) -> list[tuple[int, str]]:
    """Return paragraphs from a DOCX file, grouped into pages of 50 paragraphs."""
    from docx import Document
    doc = Document(io.BytesIO(file_bytes))
    paras = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    page_size = 50
    pages = []
    for i in range(0, max(len(paras), 1), page_size):
        block = "\n".join(paras[i:i + page_size])
        if block:
            pages.append((i // page_size + 1, block))
    return pages or [(1, "")]


def _extract_pages_xlsx(file_bytes: bytes) -> list[tuple[int, str]]:
    """Return one 'page' per sheet, with each row as a tab-separated line."""
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    pages = []
    for sheet_no, ws in enumerate(wb.worksheets, start=1):
        rows = []
        for row in ws.iter_rows(values_only=True):
            cells = [str(c) if c is not None else "" for c in row]
            line = "\t".join(cells).strip()
            if line:
                rows.append(line)
        if rows:
            pages.append((sheet_no, f"[Sheet: {ws.title}]\n" + "\n".join(rows)))
    wb.close()
    return pages or [(1, "")]


def _extract_pages_xls(file_bytes: bytes) -> list[tuple[int, str]]:
    """Return one 'page' per sheet from legacy .xls files."""
    import xlrd
    wb = xlrd.open_workbook(file_contents=file_bytes)
    pages = []
    for sheet_no in range(wb.nsheets):
        ws = wb.sheet_by_index(sheet_no)
        rows = []
        for r in range(ws.nrows):
            line = "\t".join(str(ws.cell_value(r, c)) for c in range(ws.ncols)).strip()
            if line:
                rows.append(line)
        if rows:
            pages.append((sheet_no + 1, f"[Sheet: {ws.name}]\n" + "\n".join(rows)))
    return pages or [(1, "")]


def _extract_pages(
    file_bytes: bytes,
    filename: str,
    api_key: Optional[str] = None,
) -> list[tuple[int, str]]:
    """Dispatch to the right extractor based on file extension."""
    ext = filename.rsplit(".", 1)[-1].lower()
    if ext in ("ppt", "pptx"):
        return _extract_pages_pptx(file_bytes, api_key=api_key)
    if ext == "txt":
        return _extract_pages_txt(file_bytes)
    if ext in ("doc", "docx"):
        return _extract_pages_docx(file_bytes)
    if ext == "xlsx":
        return _extract_pages_xlsx(file_bytes)
    if ext == "xls":
        return _extract_pages_xls(file_bytes)
    if ext in _VISION_MIME:
        return _extract_pages_image(file_bytes, filename, api_key=api_key)
    return _extract_pages_pdf(file_bytes)


# ── Category definitions ────────────────────────────────────────────────────

CATEGORIES: dict[str, list[str]] = {
    "market_rules": [
        "交易规则", "市场规则", "结算规则", "交易管理", "竞价规则", "报价规则",
        "交易细则", "市场运营规则", "现货交易规则",
        "market rule", "trading rule", "settlement rule", "bidding rule",
    ],
    "annual_report": [
        "年度报告", "年报", "运行年报", "运营报告", "年度运行", "年度总结",
        "年度回顾", "全年运行", "电力市场年度",
        "annual report", "annual operations", "annual review",
    ],
    "policy_doc": [
        "通知", "办法", "规定", "意见", "政策", "指导意见", "管理办法",
        "实施方案", "工作方案", "发改委", "能源局",
        "policy", "notice", "regulation", "directive", "guideline", "circular",
    ],
    "technical_spec": [
        "技术规范", "技术标准", "规程", "技术要求", "调度规程", "并网规范",
        "技术条件", "标准规范",
        "specification", "technical standard", "grid code", "technical requirement",
    ],
    "research_report": [
        "研究报告", "分析报告", "调研报告", "白皮书", "研究院", "研究所",
        "market analysis", "research report", "white paper",
    ],
}

CATEGORY_LABELS: dict[str, str] = {
    "market_rules":      "Market Rules",
    "annual_report":     "Annual Report",
    "policy_doc":        "Policy Document",
    "technical_spec":    "Technical Spec",
    "research_report":   "Research Report",
    "conversation_log":  "Conversation Log",
    "other":             "Other",
}

CATEGORY_LABELS_ZH: dict[str, str] = {
    "market_rules":      "交易规则",
    "annual_report":     "年度报告",
    "policy_doc":        "政策文件",
    "technical_spec":    "技术规范",
    "research_report":   "研究报告",
    "conversation_log":  "对话记录",
    "other":             "其他",
}


# ── DB setup ─────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS staging.spot_knowledge_docs (
    id              SERIAL PRIMARY KEY,
    file_name       TEXT NOT NULL,
    file_hash       TEXT UNIQUE NOT NULL,
    category        TEXT NOT NULL DEFAULT 'other',
    title           TEXT,
    doc_year        INT,
    file_size_bytes INT,
    page_count      INT DEFAULT 0,
    ingest_status   TEXT NOT NULL DEFAULT 'pending',
    parse_error     TEXT,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.spot_knowledge_chunks (
    id          SERIAL PRIMARY KEY,
    doc_id      INT NOT NULL REFERENCES staging.spot_knowledge_docs(id),
    page_no     INT,
    chunk_index INT NOT NULL,
    chunk_text  TEXT NOT NULL,
    UNIQUE(doc_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_skc_fts
    ON staging.spot_knowledge_chunks
    USING GIN(to_tsvector('simple', chunk_text));
"""


def init_knowledge_tables() -> None:
    """Create tables if they don't exist. Idempotent."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()


# ── Hashing ──────────────────────────────────────────────────────────────────

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ── Auto-categorization ───────────────────────────────────────────────────────

def _keyword_category(text: str) -> str:
    t = text.lower()
    for cat, keywords in CATEGORIES.items():
        for kw in keywords:
            if kw.lower() in t:
                return cat
    return "other"


def auto_categorize(
    filename: str,
    text_sample: str,
    api_key: Optional[str] = None,
) -> str:
    """
    Detect category from filename + first-page text.

    Step 1: keyword heuristic on filename + first 1000 chars of text.
    Step 2: if heuristic returns 'other' and api_key is set, ask Haiku.
    """
    combined = f"{filename}\n{text_sample[:1000]}"
    cat = _keyword_category(combined)
    if cat != "other" or not api_key:
        return cat

    # LLM fallback — Haiku is cheap and fast
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=20,
            system=(
                "Classify this Chinese electricity market document into exactly one category. "
                "Reply with only the category key, nothing else. "
                "Categories: market_rules | annual_report | policy_doc | "
                "technical_spec | research_report | other"
            ),
            messages=[{
                "role": "user",
                "content": f"Filename: {filename}\n\nText sample:\n{text_sample[:800]}",
            }],
        )
        cat_llm = (resp.content[0].text or "other").strip().lower()
        if cat_llm in CATEGORIES or cat_llm == "other":
            return cat_llm
    except Exception:
        pass

    return "other"


# ── Text chunking ─────────────────────────────────────────────────────────────

def _chunk_text(text: str, chunk_size: int = 500, overlap: int = 100) -> list[str]:
    text = text.strip()
    if not text:
        return []
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunks.append(text[start:end])
        if end == len(text):
            break
        start += chunk_size - overlap
    return chunks


def _infer_title(filename: str, first_page_text: str) -> str:
    """Strip file extension and year suffixes for a clean title."""
    stem = re.sub(r"[\(\（]\d{4}[\)\）]", "", filename)
    stem = re.sub(r"\.\w+$", "", stem).strip()
    return stem or filename


# ── Registration + ingestion ──────────────────────────────────────────────────

def register_and_ingest(
    file_bytes: bytes,
    filename: str,
    category_override: Optional[str] = None,
    api_key: Optional[str] = None,
) -> tuple[int, bool, str]:
    """
    Register and ingest a document from raw bytes (e.g. from Streamlit uploader).

    Returns:
        (doc_id, is_new, category)
        is_new=False means the file already existed (same SHA-256 hash).
    """
    init_knowledge_tables()

    file_hash = sha256_bytes(file_bytes)

    # Dedup check
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, category FROM staging.spot_knowledge_docs WHERE file_hash = %s",
                (file_hash,),
            )
            row = cur.fetchone()
    if row:
        return row[0], False, row[1]

    # Extract pages (PDF or PPTX)
    pages_text: list[tuple[int, str]] = []
    try:
        pages_text = _extract_pages(file_bytes, filename, api_key=api_key)
    except Exception as exc:
        # Register as failed so user gets feedback
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO staging.spot_knowledge_docs
                        (file_name, file_hash, category, file_size_bytes, ingest_status, parse_error)
                    VALUES (%s, %s, %s, %s, 'failed', %s)
                    RETURNING id
                    """,
                    (filename, file_hash, category_override or "other",
                     len(file_bytes), str(exc)),
                )
                doc_id = cur.fetchone()[0]
            conn.commit()
        return doc_id, True, category_override or "other"

    first_page_text = pages_text[0][1] if pages_text else ""
    category = category_override or auto_categorize(filename, first_page_text, api_key)
    title = _infer_title(filename, first_page_text)

    # Register doc and write chunks in one transaction
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging.spot_knowledge_docs
                    (file_name, file_hash, category, title, file_size_bytes,
                     page_count, ingest_status)
                VALUES (%s, %s, %s, %s, %s, %s, 'parsed')
                RETURNING id
                """,
                (filename, file_hash, category, title,
                 len(file_bytes), len(pages_text)),
            )
            doc_id = cur.fetchone()[0]

            chunk_index = 0
            inserts = []
            for page_no, text in pages_text:
                for chunk in _chunk_text(text):
                    inserts.append((doc_id, page_no, chunk_index, chunk))
                    chunk_index += 1

            if inserts:
                cur.executemany(
                    """
                    INSERT INTO staging.spot_knowledge_chunks
                        (doc_id, page_no, chunk_index, chunk_text)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (doc_id, chunk_index) DO NOTHING
                    """,
                    inserts,
                )
        conn.commit()

    return doc_id, True, category


# ── Retrieval ─────────────────────────────────────────────────────────────────

def search_reference_docs(
    query: str,
    category: Optional[str] = None,
    limit: int = 5,
) -> list[dict]:
    """
    Full-text search over staging.spot_knowledge_chunks.

    Returns list of dicts:
        doc_id, file_name, category, page_no, chunk_text, rank
    """
    init_knowledge_tables()

    conditions = ["d.active = TRUE"]
    params: list = []

    if len(query) <= 4:
        conditions.append("c.chunk_text ILIKE %s")
        params.append(f"%{query}%")
        rank_expr = "1.0::float"
    else:
        conditions.append(
            "to_tsvector('simple', c.chunk_text) @@ plainto_tsquery('simple', %s)"
        )
        params.append(query)
        rank_expr = (
            "ts_rank(to_tsvector('simple', c.chunk_text), "
            "plainto_tsquery('simple', %s))"
        )
        params.append(query)

    if category:
        conditions.append("d.category = %s")
        params.append(category)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT d.id AS doc_id, d.file_name, d.category,
               c.page_no, c.chunk_text,
               {rank_expr} AS rank
        FROM staging.spot_knowledge_chunks c
        JOIN staging.spot_knowledge_docs d ON d.id = c.doc_id
        WHERE {where}
        ORDER BY rank DESC
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── Doc management ────────────────────────────────────────────────────────────

def list_knowledge_docs() -> list[dict]:
    """Return all active knowledge docs ordered by most recently added."""
    init_knowledge_tables()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, file_name, category, title, page_count,
                       ingest_status, created_at
                FROM staging.spot_knowledge_docs
                WHERE active = TRUE
                ORDER BY created_at DESC
                """,
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def delete_knowledge_doc(doc_id: int) -> None:
    """Soft-delete a document and its chunks remain but are excluded from queries."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE staging.spot_knowledge_docs SET active=FALSE WHERE id=%s",
                (doc_id,),
            )
        conn.commit()


# ── Conversation logging ──────────────────────────────────────────────────────

def log_conversation_turn(user_msg: str, agent_reply: str) -> None:
    """
    Append a Q&A turn to today's conversation log document.

    One document per calendar day (keyed by filename); each turn is one chunk.
    Logs are searchable by the agent via search_reference_docs(category='conversation_log').
    """
    init_knowledge_tables()

    today = dt.date.today().isoformat()
    file_name = f"conversation_log_{today}.md"
    # Stable per-day hash — same day always resolves to the same doc
    file_hash = sha256_bytes(f"__conv_log__{today}".encode())

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Ensure today's log doc exists (idempotent)
            cur.execute(
                """
                INSERT INTO staging.spot_knowledge_docs
                    (file_name, file_hash, category, title, ingest_status)
                VALUES (%s, %s, 'conversation_log', %s, 'parsed')
                ON CONFLICT (file_hash) DO NOTHING
                """,
                (file_name, file_hash, f"Agent Conversation Log {today}"),
            )
            cur.execute(
                "SELECT id FROM staging.spot_knowledge_docs WHERE file_hash = %s",
                (file_hash,),
            )
            doc_id = cur.fetchone()[0]

            # Next available chunk index for this doc
            cur.execute(
                "SELECT COALESCE(MAX(chunk_index), -1) + 1 "
                "FROM staging.spot_knowledge_chunks WHERE doc_id = %s",
                (doc_id,),
            )
            next_idx = cur.fetchone()[0]

            chunk_text = (
                f"[User]: {user_msg}\n\n"
                f"[Agent]: {agent_reply}"
            )
            cur.execute(
                """
                INSERT INTO staging.spot_knowledge_chunks
                    (doc_id, page_no, chunk_index, chunk_text)
                VALUES (%s, 1, %s, %s)
                ON CONFLICT (doc_id, chunk_index) DO NOTHING
                """,
                (doc_id, next_idx, chunk_text),
            )
        conn.commit()
