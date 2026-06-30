"""
Daily and monthly China power market PDF report generator.

Queries the Strategist knowledge base for recent articles/docs, uses Claude Sonnet
to synthesise structured report content, renders to PDF via ReportLab, and sends
via Feishu as a file message.

Schedules (set in app.py):
  Daily   — 07:00 UTC (15:00 Beijing), after the 06:00 news screener digest
  Monthly — 1st of each month, 09:00 UTC (17:00 Beijing)

Entry points:
  send_daily_report(pg_url, api_key, feishu, owner_open_id)
  send_monthly_report(pg_url, api_key, feishu, owner_open_id, year=None, month=None)
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2

logger = logging.getLogger(__name__)

# ── CJK font registration ─────────────────────────────────────────────────────
# Use ReportLab's built-in STSong-Light CIDFont — always available, no external
# font files needed. This is the same approach used in mengxi_ranking_report.py.

_FONT_REGULAR = "STSong-Light"
_FONT_BOLD    = "STSong-Light"  # CID fonts have no bold variant; use same font

def _register_cjk_fonts() -> None:
    global _FONT_REGULAR, _FONT_BOLD
    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.cidfonts import UnicodeCIDFont
        pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
        logger.info("CJK font registered: STSong-Light (built-in CIDFont)")
    except Exception as exc:
        logger.warning("STSong-Light registration failed, falling back to Helvetica: %s", exc)
        _FONT_REGULAR = "Helvetica"
        _FONT_BOLD    = "Helvetica-Bold"


_register_cjk_fonts()


# ── DB helpers ────────────────────────────────────────────────────────────────

def _query_articles(
    pg_url: str,
    from_dt: datetime,
    to_dt: Optional[datetime] = None,
    pub_from_dt: Optional[datetime] = None,
    stmt_timeout_ms: int = 15000,
) -> list[dict]:
    """
    Query staging.spot_knowledge_docs for articles in the given window.

    For daily reports (pub_from_dt provided):
      Primary filter: published_at >= pub_from_dt  (catches recently published articles)
      Fallback:       published_at IS NULL AND created_at >= from_dt
      This prevents backfilled old articles (pub March, ingested today) from
      contaminating today's daily report.

    For monthly reports (to_dt provided):
      Filters by created_at range (calendar month window).

    Returns list of dicts sorted by relevance_score DESC, published_at DESC.
    """
    conn = psycopg2.connect(pg_url, options=f"-c statement_timeout={stmt_timeout_ms}")
    try:
        with conn.cursor() as cur:
            if to_dt:
                # Monthly: calendar month window by created_at
                cur.execute(
                    """
                    SELECT title, source_name, relevance_score, ai_summary,
                           published_at, region_bucket, category
                    FROM staging.spot_knowledge_docs
                    WHERE created_at >= %s AND created_at < %s
                    ORDER BY COALESCE(relevance_score, 0) DESC,
                             COALESCE(published_at, created_at) DESC
                    LIMIT 120
                    """,
                    (from_dt, to_dt),
                )
            elif pub_from_dt is not None:
                # Daily: published_at as primary filter; created_at fallback for NULL pub_at
                cur.execute(
                    """
                    SELECT title, source_name, relevance_score, ai_summary,
                           published_at, region_bucket, category
                    FROM staging.spot_knowledge_docs
                    WHERE (published_at >= %s)
                       OR (published_at IS NULL AND created_at >= %s)
                    ORDER BY COALESCE(relevance_score, 0) DESC,
                             COALESCE(published_at, created_at) DESC
                    LIMIT 120
                    """,
                    (pub_from_dt, from_dt),
                )
            else:
                # Legacy fallback: created_at only
                cur.execute(
                    """
                    SELECT title, source_name, relevance_score, ai_summary,
                           published_at, region_bucket, category
                    FROM staging.spot_knowledge_docs
                    WHERE created_at >= %s
                    ORDER BY COALESCE(relevance_score, 0) DESC,
                             COALESCE(published_at, created_at) DESC
                    LIMIT 120
                    """,
                    (from_dt,),
                )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


# ── AI content generation ─────────────────────────────────────────────────────

_DAILY_PROMPT = """\
你是一位中国电力市场资深分析师。请根据以下今日收集的新闻资讯，撰写一份简洁专业的日报。

今日资讯（按相关度排序）：
{articles_text}

请以JSON格式输出，结构如下（仅输出JSON，不要加代码块或注释）：
{{
  "executive_summary": "2-3句话的今日市场核心动态综述",
  "sections": [
    {{
      "title": "政策与监管",
      "items": [
        {{
          "title": "文章标题",
          "content": "2-4句话的分析，结合文章内容和市场意义",
          "source": "来源机构",
          "date": "发布日期（如有）"
        }}
      ]
    }},
    {{
      "title": "市场动态",
      "items": [...]
    }},
    {{
      "title": "储能与新能源",
      "items": [...]
    }},
    {{
      "title": "其他动态",
      "items": [...]
    }}
  ]
}}

要求：
- 只包含有实质内容的章节（无内容的章节不输出）
- 每个section至少1个item，每个item的content不少于50字
- 政策监管类文章放"政策与监管"，价格/交易类放"市场动态"，储能/新能源/BESS放"储能与新能源"
- 文章相关度低于4分的可忽略
- 语言简洁专业，适合能源从业者
"""

_MONTHLY_PROMPT = """\
你是中国电力市场资深分析师，为投资者和行业从业者撰写月度政策研究报告。

本报告定位：专家深度分析，不是新闻摘要。每个议题需有政策背景、关键内容解读、市场影响分析、储能/BESS投资启示，体现专业判断。

{period}收集的市场资讯（共{n_articles}篇，按相关度排序）：
{articles_text}

请从中识别本月最重要的4-6个议题，每个议题进行深度分析。按三类组织：
- 市场快讯：重要市场事件、政策发布、规则变化
- 市场洞察：行业数据趋势、市场结构深度分析
- 政策追踪：具体政策落地、影响测算、投资逻辑

请以JSON格式输出（仅输出JSON，不要加代码块，不要有任何JSON之外的文字）：
{{
  "executive_summary": "2-3句话总结本月最重要的1-2个核心主题及其对储能行业的影响",
  "highlights": [
    {{"category": "市场快讯", "title": "议题标题", "teaser": "一句话说明为何重要"}},
    {{"category": "市场洞察", "title": "议题标题", "teaser": "一句话说明核心发现"}}
  ],
  "articles": [
    {{
      "category": "市场快讯",
      "title": "议题完整标题",
      "body": "正文分析，200-350字。段落间用[P]分隔。内容包括：背景与政策来源、核心内容要点、市场影响分析、对储能BESS行业的具体影响或投资启示。使用专业但易懂的语言，可引用具体数字。"
    }}
  ]
}}

要求：
- highlights与articles一一对应，顺序相同
- 每篇article的body必须200字以上，体现分析深度
- 优先选择与储能、电力现货、容量电价、新能源政策直接相关的议题
- body中用[P]表示段落分隔（不要用换行符）
- category必须是：市场快讯、市场洞察、政策追踪 三者之一
"""


def _clean(s: str) -> str:
    """Strip newlines/tabs from a string so it's safe inside JSON values."""
    return s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ").strip()


def _format_articles_for_prompt(articles: list[dict], max_chars: int = 12000) -> str:
    """Format articles as numbered list for the AI prompt."""
    lines = []
    total = 0
    for i, a in enumerate(articles, 1):
        title = _clean((a.get("title") or "（无标题）")[:100])
        source = _clean(a.get("source_name") or "未知来源")
        score = a.get("relevance_score")
        score_str = f"[相关度{score}]" if score is not None else ""
        summary = _clean((a.get("ai_summary") or "")[:150])
        pub = ""
        if a.get("published_at"):
            pub = a["published_at"].strftime("%Y-%m-%d")
        line = f"{i}. {score_str} 【{source}】{title}（{pub}）"
        if summary:
            line += f"\n   摘要：{summary}"
        lines.append(line)
        total += len(line)
        if total > max_chars:
            lines.append(f"（另有 {len(articles) - i} 篇文章因篇幅限制未展示）")
            break
    return "\n\n".join(lines)


def _call_claude_json(api_key: str, prompt: str, max_tokens: int) -> dict | None:
    """
    Call Claude Sonnet with a JSON-only system prompt.
    Returns parsed dict, or None if all parse attempts fail.
    """
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=max_tokens,
        system=(
            "You are a JSON-only output assistant. "
            "Respond with a single valid JSON object and nothing else. "
            "No markdown fences, no explanations, no text outside the JSON. "
            "Start with { and end with }. "
            "Use standard ASCII double-quotes for all string values. "
            "Never include literal newline characters inside string values — use [P] as paragraph separator instead."
        ),
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    raw = re.sub(r"\n?```(?:json)?\s*$", "", raw).strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw).strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("JSON parse failed (%s), trying regex extract. raw[:300]=%s", e, raw[:300])

    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass

    logger.warning("All JSON parse attempts failed. raw[:500]=%s", raw[:500])
    return None


def _generate_report_content(articles: list[dict], api_key: str, report_type: str, period_str: str) -> dict:
    """
    Call Claude Sonnet to generate structured report content.
    Returns parsed dict. Daily uses sections[]; monthly uses articles[].
    """
    if not articles:
        return {
            "executive_summary": f"{period_str}暂无相关新闻资讯录入知识库。",
            "sections": [],
            "articles": [],
        }

    # Daily: 50 articles, 8000 chars input, 6000 tokens output
    max_arts  = 50
    max_chars = 8000
    max_out   = 6000

    articles_for_prompt = articles[:max_arts]
    articles_text = _format_articles_for_prompt(articles_for_prompt, max_chars=max_chars)
    prompt = _DAILY_PROMPT.format(articles_text=articles_text)

    try:
        result = _call_claude_json(api_key, prompt, max_out)
        if result:
            return result
        return {
            "executive_summary": "报告结构化内容生成失败，请稍后重试。",
            "sections": [],
        }
    except Exception as exc:
        logger.error("Report content generation failed: %s", exc, exc_info=True)
        return {
            "executive_summary": f"报告生成失败（{exc}）。",
            "sections": [],
        }


def _generate_monthly_content(articles: list[dict], api_key: str, period_str: str) -> dict:
    """
    Generate monthly expert-analyst report content using a simplified JSON structure.
    Returns dict with executive_summary, highlights[], articles[].
    """
    if not articles:
        return {
            "executive_summary": f"{period_str}暂无相关新闻资讯录入知识库。",
            "highlights": [],
            "articles": [],
        }

    # Use top 30 highest-relevance articles — enough context, keeps prompt lean
    articles_for_prompt = articles[:30]
    articles_text = _format_articles_for_prompt(articles_for_prompt, max_chars=6000)
    prompt = _MONTHLY_PROMPT.format(
        period=period_str,
        n_articles=len(articles_for_prompt),
        articles_text=articles_text,
    )

    try:
        result = _call_claude_json(api_key, prompt, max_tokens=6000)
        if result and result.get("articles"):
            return result
        logger.warning("Monthly content generation returned empty or unparseable result")
        return {
            "executive_summary": "月报内容生成失败，请稍后重试。",
            "highlights": [],
            "articles": [],
        }
    except Exception as exc:
        logger.error("Monthly content generation failed: %s", exc, exc_info=True)
        return {
            "executive_summary": f"月报生成失败（{exc}）。",
            "highlights": [],
            "articles": [],
        }


# ── PDF builder ───────────────────────────────────────────────────────────────

def _esc(text: str) -> str:
    """Escape XML entities for ReportLab Paragraph."""
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_pdf(report: dict, report_type: str, period_str: str) -> bytes:
    """Render the structured report dict to a PDF. Returns raw bytes."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        HRFlowable, PageBreak, Paragraph, SimpleDocTemplate, Spacer,
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2.2 * cm, rightMargin=2.2 * cm,
        topMargin=2.5 * cm, bottomMargin=2.5 * cm,
    )

    # ── Colour palette ────────────────────────────────────────────────────────
    NAVY    = colors.HexColor("#1a3a5c")
    STEEL   = colors.HexColor("#3a6a9c")
    RULE    = colors.HexColor("#b0c8e0")
    MUTED   = colors.HexColor("#777777")
    WHITE   = colors.white

    # ── Styles ────────────────────────────────────────────────────────────────
    cover_title = ParagraphStyle("cover_title",  fontName=_FONT_BOLD,    fontSize=26, leading=34, alignment=1, textColor=NAVY, spaceAfter=10)
    cover_sub   = ParagraphStyle("cover_sub",    fontName=_FONT_REGULAR, fontSize=13, leading=18, alignment=1, textColor=STEEL, spaceAfter=6)
    cover_org   = ParagraphStyle("cover_org",    fontName=_FONT_REGULAR, fontSize=10, leading=14, alignment=1, textColor=MUTED)
    sec_header  = ParagraphStyle("sec_header",   fontName=_FONT_BOLD,    fontSize=13, leading=18, textColor=NAVY,  spaceBefore=18, spaceAfter=6)
    item_title  = ParagraphStyle("item_title",   fontName=_FONT_BOLD,    fontSize=10, leading=15, textColor=STEEL, spaceBefore=12, spaceAfter=3)
    body_text   = ParagraphStyle("body_text",    fontName=_FONT_REGULAR, fontSize=9.5, leading=15, spaceAfter=5)
    meta_text   = ParagraphStyle("meta_text",    fontName=_FONT_REGULAR, fontSize=8,  leading=11, textColor=MUTED, spaceAfter=3)
    summary_txt = ParagraphStyle("summary_txt",  fontName=_FONT_REGULAR, fontSize=10, leading=16, spaceAfter=6, leftIndent=6, rightIndent=6)

    story = []

    # ── Cover page ─────────────────────────────────────────────────────────────
    story.append(Spacer(1, 3.5 * cm))
    title = "中国电力市场日报" if report_type == "daily" else "中国电力市场月报"
    story.append(Paragraph(_esc(title), cover_title))
    story.append(Spacer(1, 0.4 * cm))
    story.append(Paragraph(_esc(period_str), cover_sub))
    story.append(Spacer(1, 0.3 * cm))
    story.append(Paragraph("电力市场调研系列研究小组", cover_org))
    story.append(Spacer(1, 1.5 * cm))
    story.append(HRFlowable(width="100%", thickness=2, color=NAVY))
    story.append(Spacer(1, 0.8 * cm))

    # Executive summary on cover
    if report.get("executive_summary"):
        story.append(Paragraph("摘要", sec_header))
        story.append(Paragraph(_esc(report["executive_summary"]), summary_txt))

    story.append(PageBreak())

    # ── Content sections ───────────────────────────────────────────────────────
    for section in report.get("sections", []):
        sec_title = section.get("title", "")
        if not sec_title:
            continue
        story.append(Paragraph(_esc(sec_title), sec_header))
        story.append(HRFlowable(width="100%", thickness=1, color=RULE))
        story.append(Spacer(1, 0.15 * cm))

        for item in section.get("items", []):
            if item.get("title"):
                story.append(Paragraph(_esc(item["title"]), item_title))
            # Meta line: source + date
            meta_parts = []
            if item.get("source"):
                meta_parts.append(f"来源：{item['source']}")
            if item.get("date"):
                meta_parts.append(str(item["date"]))
            if meta_parts:
                story.append(Paragraph("  ·  ".join(meta_parts), meta_text))
            if item.get("content"):
                story.append(Paragraph(_esc(item["content"]), body_text))
            story.append(Spacer(1, 0.1 * cm))

        story.append(Spacer(1, 0.4 * cm))

    # Footer note
    story.append(Spacer(1, 0.5 * cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=RULE))
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    story.append(Paragraph(
        f"本报告由AI自动生成 · {generated_at} · 数据来源：知识库新闻资讯",
        meta_text,
    ))

    doc.build(story)
    return buf.getvalue()


def _build_monthly_pdf(report: dict, period_str: str) -> bytes:
    """
    Render monthly expert-analyst report to PDF.
    Template: cover → 月度看点 TOC → per-article pages (category label + title + body).
    """
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        HRFlowable, PageBreak, Paragraph, SimpleDocTemplate, Spacer,
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2.5 * cm, rightMargin=2.5 * cm,
        topMargin=2.5 * cm, bottomMargin=2.5 * cm,
    )

    NAVY   = colors.HexColor("#1a3a5c")
    STEEL  = colors.HexColor("#2d6a9f")
    TEAL   = colors.HexColor("#1a7a6e")
    RULE   = colors.HexColor("#b0c8e0")
    MUTED  = colors.HexColor("#777777")
    ORANGE = colors.HexColor("#c47a2a")

    # Category → colour mapping
    _CAT_COLOR = {
        "市场快讯": STEEL,
        "市场洞察": TEAL,
        "政策追踪": ORANGE,
    }

    cover_title  = ParagraphStyle("m_cover_title",  fontName=_FONT_BOLD,    fontSize=28, leading=36, alignment=1, textColor=NAVY,  spaceAfter=8)
    cover_sub    = ParagraphStyle("m_cover_sub",     fontName=_FONT_REGULAR, fontSize=14, leading=20, alignment=1, textColor=STEEL, spaceAfter=6)
    cover_org    = ParagraphStyle("m_cover_org",     fontName=_FONT_REGULAR, fontSize=10, leading=14, alignment=1, textColor=MUTED)
    summary_txt  = ParagraphStyle("m_summary_txt",   fontName=_FONT_REGULAR, fontSize=10, leading=16, spaceAfter=6, leftIndent=8, rightIndent=8)
    toc_header   = ParagraphStyle("m_toc_header",    fontName=_FONT_BOLD,    fontSize=16, leading=22, textColor=NAVY, spaceBefore=0, spaceAfter=12)
    cat_label    = ParagraphStyle("m_cat_label",     fontName=_FONT_REGULAR, fontSize=10, leading=14, textColor=STEEL, spaceAfter=2)
    toc_title    = ParagraphStyle("m_toc_title",     fontName=_FONT_BOLD,    fontSize=11, leading=16, textColor=NAVY,  spaceAfter=2)
    toc_teaser   = ParagraphStyle("m_toc_teaser",    fontName=_FONT_REGULAR, fontSize=9,  leading=13, textColor=MUTED, spaceAfter=10, leftIndent=8)
    art_cat      = ParagraphStyle("m_art_cat",       fontName=_FONT_REGULAR, fontSize=10, leading=14, textColor=STEEL, spaceAfter=6)
    art_title    = ParagraphStyle("m_art_title",     fontName=_FONT_BOLD,    fontSize=15, leading=22, textColor=NAVY,  alignment=1, spaceAfter=14)
    body_text    = ParagraphStyle("m_body_text",     fontName=_FONT_REGULAR, fontSize=10, leading=17, spaceAfter=8, firstLineIndent=20)
    meta_text    = ParagraphStyle("m_meta_text",     fontName=_FONT_REGULAR, fontSize=8,  leading=11, textColor=MUTED, spaceAfter=3)

    story = []

    # ── Cover page ────────────────────────────────────────────────────────────
    story.append(Spacer(1, 4 * cm))
    story.append(Paragraph("中国电力市场政策研究报告", cover_title))
    story.append(Spacer(1, 0.5 * cm))
    story.append(Paragraph(_esc(period_str), cover_sub))
    story.append(Spacer(1, 0.3 * cm))
    story.append(Paragraph("电力市场体系政策研究团队", cover_org))
    story.append(Spacer(1, 2 * cm))
    story.append(HRFlowable(width="100%", thickness=2, color=NAVY))
    story.append(Spacer(1, 0.8 * cm))

    if report.get("executive_summary"):
        story.append(Paragraph("摘要", ParagraphStyle("m_abs_hdr", fontName=_FONT_BOLD, fontSize=11,
                                                       leading=16, textColor=NAVY, spaceAfter=6, alignment=1)))
        story.append(Paragraph(_esc(report["executive_summary"]), summary_txt))

    story.append(PageBreak())

    # ── 月度看点 (TOC) page ───────────────────────────────────────────────────
    story.append(Paragraph("月度看点：", toc_header))
    story.append(HRFlowable(width="100%", thickness=0.5, color=RULE))
    story.append(Spacer(1, 0.3 * cm))

    # Group highlights by category for display
    highlights = report.get("highlights") or []
    articles   = report.get("articles") or []
    # If highlights missing, derive from articles
    if not highlights and articles:
        highlights = [{"category": a.get("category", ""), "title": a.get("title", ""), "teaser": ""}
                      for a in articles]

    current_cat = None
    for h in highlights:
        cat = h.get("category", "")
        if cat != current_cat:
            current_cat = cat
            cat_color = _CAT_COLOR.get(cat, STEEL)
            cat_style = ParagraphStyle(f"toc_cat_{cat}", fontName=_FONT_BOLD, fontSize=11,
                                        leading=16, textColor=cat_color, spaceBefore=12, spaceAfter=4)
            story.append(Paragraph(_esc(f"{cat}："), cat_style))
            story.append(HRFlowable(width="100%", thickness=0.5, color=RULE))

        story.append(Paragraph(_esc(h.get("title", "")), toc_title))
        if h.get("teaser"):
            story.append(Paragraph(_esc(h["teaser"]), toc_teaser))

    story.append(PageBreak())

    # ── Per-article pages ─────────────────────────────────────────────────────
    for art in articles:
        cat = art.get("category", "")
        title = art.get("title", "")
        body = art.get("body", "")
        if not title or not body:
            continue

        cat_color = _CAT_COLOR.get(cat, STEEL)
        # Category label (italic style — use muted colour matching category)
        art_cat_dyn = ParagraphStyle(f"art_cat_{cat}", fontName=_FONT_REGULAR, fontSize=10,
                                      leading=14, textColor=cat_color, spaceAfter=4)
        story.append(Paragraph(_esc(f"{cat}："), art_cat_dyn))
        story.append(Spacer(1, 0.2 * cm))
        story.append(Paragraph(_esc(title), art_title))
        story.append(HRFlowable(width="80%", thickness=1, color=cat_color))
        story.append(Spacer(1, 0.4 * cm))

        # Body: split on [P] separator into paragraphs
        paragraphs = [p.strip() for p in body.split("[P]") if p.strip()]
        if not paragraphs:
            paragraphs = [body]
        for para in paragraphs:
            story.append(Paragraph(_esc(para), body_text))

        story.append(PageBreak())

    # ── Footer on last page ───────────────────────────────────────────────────
    # Remove trailing PageBreak if present
    if story and isinstance(story[-1], PageBreak):
        story.pop()

    story.append(Spacer(1, 0.5 * cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=RULE))
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    story.append(Paragraph(
        f"本报告由AI自动生成 · {generated_at} · 数据来源：知识库新闻资讯",
        meta_text,
    ))

    doc.build(story)
    return buf.getvalue()


# ── Entry points ──────────────────────────────────────────────────────────────

def send_daily_report(
    pg_url: str,
    api_key: str,
    feishu,
    owner_open_id: str,
) -> None:
    """
    Generate and send a daily power market PDF report via Feishu.
    Queries articles published in the last 26h to cover yesterday's late articles.
    """
    now_utc = datetime.now(timezone.utc)
    beijing_now = now_utc + timedelta(hours=8)
    # published_at window: 3 days (catches yesterday + today articles by publication date)
    pub_from_dt = now_utc - timedelta(days=3)
    # created_at fallback window: 30h (catches articles with NULL published_at)
    from_dt = now_utc - timedelta(hours=30)
    period_str = beijing_now.strftime("%Y年%m月%d日")

    logger.info("Generating daily market report for %s", period_str)
    try:
        articles = _query_articles(pg_url, from_dt, pub_from_dt=pub_from_dt)
        logger.info("Daily report: %d articles found (pub>=3d OR created>=30h)", len(articles))
        # Fallback: if very few articles found (screener not yet run today),
        # extend windows to 7 days / 72h
        if len(articles) < 5:
            articles = _query_articles(
                pg_url,
                now_utc - timedelta(hours=72),
                pub_from_dt=now_utc - timedelta(days=7),
            )
            logger.info("Daily report: extended to 7d/72h window, %d articles found", len(articles))

        report = _generate_report_content(articles, api_key, "daily", period_str)
        pdf_bytes = _build_pdf(report, "daily", period_str)

        filename = f"电力市场日报_{beijing_now.strftime('%Y%m%d')}.pdf"
        file_key = feishu.upload_file(pdf_bytes, filename, file_type="pdf")

        # Send a brief card first, then the PDF file
        n_articles = len(articles)
        n_sections = len(report.get("sections", []))
        feishu.send_card(
            owner_open_id,
            {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": f"📄 电力市场日报 — {period_str}"},
                    "template": "blue",
                },
                "elements": [
                    {"tag": "markdown", "content": report.get("executive_summary", "")},
                    {"tag": "hr"},
                    {"tag": "markdown", "content": f"共 **{n_articles}** 篇资讯 · **{n_sections}** 个章节 · PDF文件见下方"},
                ],
            },
        )
        feishu.send_file(owner_open_id, file_key)
        logger.info("Daily report sent: %s (%d bytes)", filename, len(pdf_bytes))

    except Exception as exc:
        logger.error("Daily report failed: %s", exc, exc_info=True)
        if feishu and owner_open_id:
            try:
                feishu.send_text(owner_open_id, f"⚠️ 电力市场日报生成失败：{exc}")
            except Exception:
                pass


def send_monthly_report(
    pg_url: str,
    api_key: str,
    feishu,
    owner_open_id: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> None:
    """
    Generate and send a monthly power market PDF report via Feishu.
    Defaults to the previous calendar month.
    """
    now_utc = datetime.now(timezone.utc)
    beijing_now = now_utc + timedelta(hours=8)

    if year is None or month is None:
        # Default to previous month
        first_of_this_month = beijing_now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_month_end = first_of_this_month - timedelta(seconds=1)
        year = last_month_end.year
        month = last_month_end.month

    from_dt = datetime(year, month, 1, tzinfo=timezone.utc) - timedelta(hours=8)  # Beijing→UTC
    if month == 12:
        to_dt = datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(hours=8)
    else:
        to_dt = datetime(year, month + 1, 1, tzinfo=timezone.utc) - timedelta(hours=8)

    period_str = f"{year}年{month}月"
    logger.info("Generating monthly market report for %s", period_str)

    try:
        # Monthly query spans a full calendar month — needs a longer timeout (60s)
        articles = _query_articles(pg_url, from_dt, to_dt, stmt_timeout_ms=60000)
        logger.info("Monthly report: %d articles found for %s", len(articles), period_str)

        report = _generate_monthly_content(articles, api_key, period_str)
        n_articles_in_report = len(report.get("articles") or [])
        logger.info("Monthly report: generated %d analytical articles", n_articles_in_report)

        pdf_bytes = _build_monthly_pdf(report, period_str)

        filename = f"电力市场月报_{year}{month:02d}.pdf"
        file_key = feishu.upload_file(pdf_bytes, filename, file_type="pdf")

        # Card summary: executive summary + article list
        art_list = "\n".join(
            f"**{a.get('category', '')}** · {a.get('title', '')}"
            for a in (report.get("articles") or [])
        )
        feishu.send_card(
            owner_open_id,
            {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": f"📊 电力市场月报 — {period_str}"},
                    "template": "green",
                },
                "elements": [
                    {"tag": "markdown", "content": report.get("executive_summary", "")},
                    {"tag": "hr"},
                    {"tag": "markdown", "content": art_list or ""},
                    {"tag": "hr"},
                    {"tag": "markdown", "content": f"共 **{n_articles_in_report}** 篇深度分析 · PDF月报见下方"},
                ],
            },
        )
        feishu.send_file(owner_open_id, file_key)
        logger.info("Monthly report sent: %s (%d bytes)", filename, len(pdf_bytes))

    except Exception as exc:
        logger.error("Monthly report failed: %s", exc, exc_info=True)
        if feishu and owner_open_id:
            try:
                feishu.send_text(owner_open_id, f"⚠️ {period_str}电力市场月报生成失败：{exc}")
            except Exception:
                pass
