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
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=15000")
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
你是一位中国电力市场资深分析师。请根据以下本月收集的新闻资讯和研究报告，撰写一份全面的月度市场报告。

本月资讯（按相关度排序，共{n_articles}篇）：
{articles_text}

请以JSON格式输出，结构如下（仅输出JSON，不要加代码块或注释）：
{{
  "executive_summary": "3-5句话的本月市场总体回顾，涵盖政策、价格、储能等核心主题",
  "sections": [
    {{
      "title": "市场资讯",
      "items": [
        {{
          "title": "事件/政策名称",
          "content": "详细分析，3-6句话，含背景、内容要点、市场影响",
          "source": "来源",
          "date": "日期"
        }}
      ]
    }},
    {{
      "title": "市场动态",
      "items": [
        {{
          "title": "动态标题",
          "content": "数据分析和趋势判断，3-5句话",
          "source": "来源",
          "date": "日期"
        }}
      ]
    }},
    {{
      "title": "储能与新能源行业",
      "items": [...]
    }},
    {{
      "title": "重点追踪",
      "items": [
        {{
          "title": "持续关注事项",
          "content": "说明该事项的背景及未来关注要点"
        }}
      ]
    }}
  ]
}}

要求：
- 月报应比日报更深入，每个item的content不少于100字
- "市场资讯"聚焦重大政策和监管变化（3-5项）
- "市场动态"聚焦价格、容量、交易量等数据趋势（2-4项）
- "储能与新能源行业"聚焦行业发展和重要项目（2-4项）
- "重点追踪"列出2-3个持续关注的长期议题
- 只输出有内容的章节
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


def _generate_report_content(articles: list[dict], api_key: str, report_type: str, period_str: str) -> dict:
    """
    Call Claude Sonnet to generate structured report content.
    Returns parsed dict with executive_summary and sections.
    """
    import anthropic

    if not articles:
        return {
            "executive_summary": f"{period_str}暂无相关新闻资讯录入知识库。",
            "sections": [],
        }

    # Cap articles: daily 50 highest-relevance, monthly 100
    # Keeps prompt within safe token limits and avoids truncated JSON responses
    max_arts = 50 if report_type == "daily" else 100
    articles_for_prompt = articles[:max_arts]
    articles_text = _format_articles_for_prompt(articles_for_prompt)

    if report_type == "daily":
        prompt = _DAILY_PROMPT.format(articles_text=articles_text)
    else:
        prompt = _MONTHLY_PROMPT.format(n_articles=len(articles_for_prompt), articles_text=articles_text)

    client = anthropic.Anthropic(api_key=api_key)
    try:
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=6000,
            system=(
                "You are a JSON-only output assistant. "
                "You MUST respond with a single valid JSON object and nothing else. "
                "Do NOT include markdown fences, explanations, or any text outside the JSON. "
                "Start your response with { and end with }. "
                "All string values must use standard ASCII double-quotes. "
                "Do not include newline characters inside string values — use a space instead."
            ),
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()

        # Strip trailing markdown fence if the model added one after the JSON
        raw = re.sub(r"\n?```\s*$", "", raw).strip()

        try:
            return json.loads(raw)
        except json.JSONDecodeError as first_err:
            logger.warning("First JSON parse failed (%s), trying regex extract", first_err)

        # Fallback: extract the outermost {...} block
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass

        logger.warning("All JSON parse attempts failed — raw starts: %s", raw[:200])
        return {
            "executive_summary": "报告结构化内容生成失败，请稍后重试。",
            "sections": [],
        }
    except Exception as exc:
        logger.warning("Report content generation failed: %s", exc)
        return {
            "executive_summary": f"报告生成失败（{exc}）。请检查API密钥和网络连接。",
            "sections": [],
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
        articles = _query_articles(pg_url, from_dt, to_dt)
        logger.info("Monthly report: %d articles found for %s", len(articles), period_str)

        report = _generate_report_content(articles, api_key, "monthly", period_str)
        pdf_bytes = _build_pdf(report, "monthly", period_str)

        filename = f"电力市场月报_{year}{month:02d}.pdf"
        file_key = feishu.upload_file(pdf_bytes, filename, file_type="pdf")

        n_articles = len(articles)
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
                    {"tag": "markdown", "content": f"本月共收录 **{n_articles}** 篇资讯 · PDF月报见下方"},
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
