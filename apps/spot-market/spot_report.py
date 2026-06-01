"""China Spot Market Daily Report — PDF generation, email, and WeCom delivery.

Generates a two-section PDF:
  1. Provincial DA / RT average prices table (most recent date in DB)
  2. AI-generated market commentary (3–5 sentences, Haiku)

Email is sent via SMTP (same env vars as GB market):
  SMTP_HOST          (default: smtp.gmail.com)
  SMTP_PORT          (default: 587)
  SMTP_USER          — sender Gmail address
  SMTP_PASSWORD      — Gmail App Password
  REPORT_FROM_EMAIL  — From: address (defaults to SMTP_USER)
  REPORT_TO_EMAIL    — default recipient (defaults to chen_dpeng@hotmail.com)

WeCom delivery (3-step: upload media → markdown summary → file):
  WECOM_WEBHOOK_URL  — comma-separated list of bot webhook URLs (env fallback)
  Webhook URLs are also persisted in DB table staging.spot_report_webhooks
  so they survive ECS container restarts.
"""
from __future__ import annotations

import logging
import os
import smtplib
import ssl
from datetime import date, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO
from pathlib import Path

import pandas as pd
import psycopg2
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

logger = logging.getLogger(__name__)


def _register_cjk_font() -> str:
    """Find and register a CJK TrueType/OpenType font with ReportLab.

    Tries system fonts via matplotlib first (works on both Linux/Docker and Windows),
    then falls back to the built-in STSong-Light CID font.  The CID font embeds no
    glyph outlines so only viewers with Adobe CJK packs can render it — TTFont is
    strongly preferred for portable PDFs.

    Returns the ReportLab font name to use.
    """
    try:
        import matplotlib.font_manager as _mfm

        # Preferred families (order matters — first match wins)
        _families = [
            "Noto Sans CJK SC",
            "Noto Sans SC",
            "Microsoft YaHei",
            "SimHei",
            "SimSun",
            "STHeiti",
            "WenQuanYi Micro Hei",
            "Arial Unicode MS",
        ]
        for _fam in _families:
            try:
                _fp = _mfm.findfont(
                    _mfm.FontProperties(family=_fam), fallback_to_default=False
                )
                if _fp and os.path.exists(_fp) and "DejaVu" not in _fp:
                    pdfmetrics.registerFont(TTFont("CJKFont", _fp))
                    logger.debug("ReportLab CJK font: %s (%s)", _fam, _fp)
                    return "CJKFont"
            except Exception:
                pass

        # File-path scan: look for Noto/WQY font files directly
        for _fp in _mfm.findSystemFonts():
            _bn = os.path.basename(_fp).lower()
            if any(
                k in _bn
                for k in ("notocjksc", "notosanscjksc", "notosanscjk",
                           "noto_cjk", "wqymicro", "wenquanyi")
            ):
                try:
                    pdfmetrics.registerFont(TTFont("CJKFont", _fp))
                    logger.debug("ReportLab CJK font (scan): %s", _fp)
                    return "CJKFont"
                except Exception:
                    pass
    except Exception:
        pass

    # Hard-coded Windows fallback paths (SimHei is a plain .ttf — no subfont index)
    for _wpath in (
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\msyh.ttf",
    ):
        if os.path.exists(_wpath):
            try:
                pdfmetrics.registerFont(TTFont("CJKFont", _wpath))
                return "CJKFont"
            except Exception:
                pass

    # Last resort: built-in CID font (no glyph embedding — works only with Adobe CJK pack)
    pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
    logger.warning(
        "No CJK TTFont found; using STSong-Light CID — Chinese may not display in all viewers"
    )
    return "STSong-Light"


_CJK = _register_cjk_font()

_DEFAULT_RECIPIENT = "chen_dpeng@hotmail.com"

# ---------------------------------------------------------------------------
# DB helpers (standalone — no Streamlit dependency)
# ---------------------------------------------------------------------------

def _get_conn():
    url = (
        os.environ.get("PGURL")
        or os.environ.get("DB_DSN")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    conn = psycopg2.connect(url, connect_timeout=10)
    conn.autocommit = True
    return conn


def _query(conn, sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql(sql, conn, params=params)


# ---------------------------------------------------------------------------
# Webhook persistence helpers
# ---------------------------------------------------------------------------

_WEBHOOK_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging.spot_report_webhooks (
    id          SERIAL PRIMARY KEY,
    label       TEXT NOT NULL DEFAULT '',
    webhook_url TEXT NOT NULL UNIQUE,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def ensure_webhook_table(conn=None) -> None:
    """Create staging.spot_report_webhooks if it doesn't exist."""
    _close = conn is None
    if conn is None:
        conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_WEBHOOK_TABLE_SQL)
    finally:
        if _close:
            conn.close()


def list_webhooks(conn=None) -> list[dict]:
    """Return all webhook rows as list of dicts."""
    _close = conn is None
    if conn is None:
        conn = _get_conn()
    try:
        df = _query(conn,
                    "SELECT id, label, webhook_url, enabled "
                    "FROM staging.spot_report_webhooks ORDER BY id")
        if df.empty:
            return []
        return df.to_dict("records")
    except Exception:
        return []
    finally:
        if _close:
            conn.close()


def upsert_webhook(webhook_url: str, label: str = "", enabled: bool = True,
                   conn=None) -> None:
    _close = conn is None
    if conn is None:
        conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO staging.spot_report_webhooks (label, webhook_url, enabled, updated_at)
                   VALUES (%s, %s, %s, NOW())
                   ON CONFLICT (webhook_url) DO UPDATE
                     SET label = EXCLUDED.label,
                         enabled = EXCLUDED.enabled,
                         updated_at = NOW()""",
                (label, webhook_url, enabled),
            )
    finally:
        if _close:
            conn.close()


def delete_webhook(webhook_id: int, conn=None) -> None:
    _close = conn is None
    if conn is None:
        conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.spot_report_webhooks WHERE id = %s",
                        (webhook_id,))
    finally:
        if _close:
            conn.close()


def get_enabled_webhook_urls(conn=None) -> list[str]:
    """Return enabled webhook URLs from DB, falling back to env var."""
    rows = list_webhooks(conn)
    db_urls = [r["webhook_url"] for r in rows if r.get("enabled")]
    if db_urls:
        return db_urls
    # Fall back to env var
    env_raw = os.environ.get("WECOM_WEBHOOK_URL", "")
    return [u.strip() for u in env_raw.split(",") if u.strip()]


# ---------------------------------------------------------------------------
# Data queries
# ---------------------------------------------------------------------------

def _get_latest_data_date(conn) -> date:
    df = _query(conn,
                "SELECT MAX(report_date) AS latest FROM spot_daily")
    if df.empty or pd.isna(df.iloc[0]["latest"]):
        return date.today() - timedelta(days=1)
    return pd.Timestamp(df.iloc[0]["latest"]).date()


def _get_latest_pdf_name(conn, report_date: date) -> str | None:
    """Return the source PDF name for the most recent data, if stored."""
    try:
        df = _query(conn,
                    "SELECT source_pdf FROM spot_daily "
                    "WHERE report_date = %s AND source_pdf IS NOT NULL "
                    "LIMIT 1",
                    (report_date,))
        if not df.empty:
            return str(df.iloc[0]["source_pdf"])
    except Exception:
        pass
    return None


def _get_provincial_prices(conn, report_date: date) -> pd.DataFrame:
    """Return DA/RT avg prices for all provinces on report_date."""
    return _query(conn,
                  "SELECT province_cn, province_en, da_avg, rt_avg "
                  "FROM spot_daily "
                  "WHERE report_date = %s "
                  "ORDER BY province_cn",
                  (report_date,))


def _get_prev_prices(conn, report_date: date) -> pd.DataFrame:
    """Return previous date's DA/RT prices for day-on-day comparison."""
    df = _query(conn,
                "SELECT MAX(report_date) AS prev FROM spot_daily "
                "WHERE report_date < %s",
                (report_date,))
    if df.empty or pd.isna(df.iloc[0]["prev"]):
        return pd.DataFrame()
    prev_date = pd.Timestamp(df.iloc[0]["prev"]).date()
    return _get_provincial_prices(conn, prev_date)


def _get_national_summary(conn, report_date: date) -> dict:
    """Return national averages and extreme provinces."""
    df = _query(conn,
                "SELECT province_cn, da_avg, rt_avg FROM spot_daily "
                "WHERE report_date = %s AND da_avg IS NOT NULL AND rt_avg IS NOT NULL",
                (report_date,))
    if df.empty:
        return {}
    avg_da = float(df["da_avg"].mean())
    avg_rt = float(df["rt_avg"].mean())
    max_da_row = df.loc[df["da_avg"].idxmax()]
    min_da_row = df.loc[df["da_avg"].idxmin()]
    max_rt_row = df.loc[df["rt_avg"].idxmax()]
    min_rt_row = df.loc[df["rt_avg"].idxmin()]
    return {
        "avg_da": avg_da,
        "avg_rt": avg_rt,
        "max_da_prov": max_da_row["province_cn"],
        "max_da": float(max_da_row["da_avg"]),
        "min_da_prov": min_da_row["province_cn"],
        "min_da": float(min_da_row["da_avg"]),
        "max_rt_prov": max_rt_row["province_cn"],
        "max_rt": float(max_rt_row["rt_avg"]),
        "min_rt_prov": min_rt_row["province_cn"],
        "min_rt": float(min_rt_row["rt_avg"]),
        "n_provinces": len(df),
    }


# ---------------------------------------------------------------------------
# AI commentary
# ---------------------------------------------------------------------------

def _generate_ai_commentary(
    report_date: date,
    prices: pd.DataFrame,
    summary: dict,
    prev_prices: pd.DataFrame | None = None,
) -> str:
    """Call Claude Haiku to generate a concise Chinese + English market summary.

    Returns plain text. Returns empty string on any error (report still sends).
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — skipping AI commentary")
        return ""
    try:
        import anthropic as _anthropic
    except ImportError:
        logger.warning("anthropic package not available — skipping AI commentary")
        return ""

    if prices.empty or not summary:
        return ""

    # Build data snapshot
    lines: list[str] = [
        f"China spot electricity market data for {report_date.strftime('%Y-%m-%d')} "
        f"({summary.get('n_provinces', '?')} provinces):",
        f"National avg DA: {summary.get('avg_da', 0):.4f} ¥/kWh, "
        f"National avg RT: {summary.get('avg_rt', 0):.4f} ¥/kWh",
        f"Highest DA: {summary.get('max_da_prov','?')} {summary.get('max_da',0):.4f}, "
        f"Lowest DA: {summary.get('min_da_prov','?')} {summary.get('min_da',0):.4f}",
        f"Highest RT: {summary.get('max_rt_prov','?')} {summary.get('max_rt',0):.4f}, "
        f"Lowest RT: {summary.get('min_rt_prov','?')} {summary.get('min_rt',0):.4f}",
    ]

    # Day-on-day comparison
    if prev_prices is not None and not prev_prices.empty and not prices.empty:
        try:
            merged = prices.merge(prev_prices, on="province_cn", suffixes=("", "_prev"))
            da_chg = (merged["da_avg"] - merged["da_avg_prev"]).mean()
            rt_chg = (merged["rt_avg"] - merged["rt_avg_prev"]).mean()
            lines.append(
                f"Day-on-day avg change: DA {da_chg:+.4f} ¥/kWh, RT {rt_chg:+.4f} ¥/kWh"
            )
            # Notable movers
            merged["da_chg_abs"] = (merged["da_avg"] - merged["da_avg_prev"]).abs()
            top_mover = merged.nlargest(1, "da_chg_abs").iloc[0]
            actual_chg = float(top_mover["da_avg"] - top_mover["da_avg_prev"])
            lines.append(
                f"Largest DA move: {top_mover['province_cn']} {actual_chg:+.4f} ¥/kWh"
            )
        except Exception:
            pass

    # Spread analysis
    if not prices.empty:
        try:
            prices_copy = prices.copy()
            prices_copy["spread"] = prices_copy["da_avg"] - prices_copy["rt_avg"]
            high_spread = prices_copy.nlargest(3, "spread")[["province_cn", "spread"]]
            parts = [f"{r['province_cn']} {float(r['spread']):+.4f}" for _, r in high_spread.iterrows()]
            lines.append(f"Top DA−RT spread provinces: {', '.join(parts)}")
        except Exception:
            pass

    data_snapshot = "\n".join(lines)
    prompt = (
        "You are an electricity market analyst. Write a concise market commentary "
        "in BOTH Chinese (3–4 sentences) AND English (3–4 sentences) about the "
        "China spot electricity market based on the data below.\n\n"
        "Format:\n"
        "【市场综述】\n"
        "<Chinese commentary here>\n\n"
        "【Market Summary】\n"
        "<English commentary here>\n\n"
        "Focus on: national price level, notable provincial divergences, "
        "DA vs RT spread implications for BESS arbitrage, and day-on-day trends.\n"
        "Be precise with numbers. No preamble.\n\n"
        f"Data:\n{data_snapshot}"
    )

    try:
        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        logger.info("AI commentary generated (%d chars)", len(text))
        return text
    except Exception as exc:
        logger.warning("AI commentary generation failed: %s", exc)
        return f"[AI commentary unavailable: {exc}]"


# ---------------------------------------------------------------------------
# PDF builder
# ---------------------------------------------------------------------------

_PAGE_W, _PAGE_H = A4
_MARGIN = 1.8 * cm

_GREY_HEADER  = colors.HexColor("#1F3864")
_GREY_ALT     = colors.HexColor("#EBF0FA")
_ACCENT       = colors.HexColor("#2E75B6")
_LIGHT_BORDER = colors.HexColor("#B8CCE4")
_GREEN        = colors.HexColor("#1a7f37")
_RED          = colors.HexColor("#cf222e")
_ORANGE       = colors.HexColor("#e46c0a")


def _styles():
    ss = getSampleStyleSheet()
    title   = ParagraphStyle("rpt_title",  parent=ss["Title"],
                              fontName=_CJK, fontSize=18,
                              textColor=_GREY_HEADER, spaceAfter=4)
    h1      = ParagraphStyle("rpt_h1",     parent=ss["Heading1"],
                              fontName=_CJK, fontSize=12,
                              textColor=_GREY_HEADER, spaceBefore=12, spaceAfter=4)
    body    = ParagraphStyle("rpt_body",   parent=ss["Normal"],
                              fontName=_CJK, fontSize=9)
    caption = ParagraphStyle("rpt_caption",parent=ss["Normal"],
                              fontName=_CJK, fontSize=8,
                              textColor=colors.grey, spaceAfter=4)
    cell    = ParagraphStyle("rpt_cell",   parent=ss["Normal"],
                              fontName=_CJK, fontSize=7.5, leading=9)
    ai_body = ParagraphStyle("rpt_ai",     parent=ss["Normal"],
                              fontName=_CJK, fontSize=9, leading=13, spaceBefore=4)
    return title, h1, body, caption, cell, ai_body


def _fmt(val, decimals=4, prefix="", suffix=""):
    if val is None or (isinstance(val, float) and val != val):
        return "—"
    return f"{prefix}{val:,.{decimals}f}{suffix}"


def _make_table(headers: list[str], rows: list[list], col_widths: list[float],
                extra_styles: list | None = None) -> Table:
    data = [headers] + rows
    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    style = [
        ("BACKGROUND",  (0, 0), (-1, 0),  _GREY_HEADER),
        ("TEXTCOLOR",   (0, 0), (-1, 0),  colors.white),
        ("FONTNAME",    (0, 0), (-1, 0),  _CJK),
        ("FONTSIZE",    (0, 0), (-1, 0),  8),
        ("FONTNAME",    (0, 1), (-1, -1), _CJK),
        ("FONTSIZE",    (0, 1), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _GREY_ALT]),
        ("GRID",        (0, 0), (-1, -1), 0.4, _LIGHT_BORDER),
        ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",  (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING",(0, 0),(-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",(0, 0), (-1, -1), 5),
    ]
    if extra_styles:
        style.extend(extra_styles)
    tbl.setStyle(TableStyle(style))
    return tbl


def _build_pdf(
    buf: BytesIO,
    report_date: date,
    prices: pd.DataFrame,
    summary: dict,
    prev_prices: pd.DataFrame | None = None,
    ai_commentary: str = "",
    source_pdf: str | None = None,
) -> None:
    title_s, h1_s, body_s, caption_s, cell_s, ai_s = _styles()
    usable_w = _PAGE_W - 2 * _MARGIN
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=_MARGIN, rightMargin=_MARGIN,
        topMargin=_MARGIN, bottomMargin=_MARGIN,
    )
    story = []

    # ── Title block ──────────────────────────────────────────────────────────
    story.append(Paragraph("中国电力现货市场日报", title_s))
    story.append(Paragraph("China Spot Electricity Market Daily Report", title_s))
    story.append(Paragraph(
        f"报告日期 Report date: <b>{report_date.strftime('%Y年%m月%d日 (%A, %d %B %Y)')}</b> "
        f"&nbsp;|&nbsp; 生成 Generated: {date.today().strftime('%Y-%m-%d')}",
        body_s,
    ))
    if source_pdf:
        story.append(Paragraph(f"数据来源 Source: {source_pdf}", caption_s))
    story.append(HRFlowable(width="100%", thickness=2, color=_GREY_HEADER, spaceAfter=8))

    # ── National KPI strip ───────────────────────────────────────────────────
    if summary:
        story.append(Paragraph("全国市场概览 National Overview", h1_s))
        kpi_data = [
            ["全国均价 Avg DA", "全国均价 Avg RT", "最高日前 Max DA", "最低日前 Min DA"],
            [
                _fmt(summary.get("avg_da"), suffix=" ¥/kWh"),
                _fmt(summary.get("avg_rt"), suffix=" ¥/kWh"),
                f"{summary.get('max_da_prov','?')} {_fmt(summary.get('max_da'))}",
                f"{summary.get('min_da_prov','?')} {_fmt(summary.get('min_da'))}",
            ],
        ]
        kpi_tbl = Table(kpi_data, colWidths=[usable_w / 4] * 4)
        kpi_tbl.setStyle(TableStyle([
            ("BACKGROUND",  (0, 0), (-1, 0), _GREY_HEADER),
            ("TEXTCOLOR",   (0, 0), (-1, 0), colors.white),
            ("FONTNAME",    (0, 0), (-1, 0), _CJK),
            ("FONTSIZE",    (0, 0), (-1, 0), 8),
            ("FONTNAME",    (0, 1), (-1, 1), _CJK),
            ("FONTSIZE",    (0, 1), (-1, 1), 9),
            ("BACKGROUND",  (0, 1), (-1, 1), _GREY_ALT),
            ("ALIGN",       (0, 0), (-1, -1), "CENTER"),
            ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING",  (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING",(0, 0),(-1, -1), 5),
            ("GRID",        (0, 0), (-1, -1), 0.4, _LIGHT_BORDER),
        ]))
        story.append(kpi_tbl)
        story.append(Spacer(1, 0.4 * cm))

    # ── Section 1: Provincial price table ────────────────────────────────────
    story.append(Paragraph("1. 各省日前/实时均价 Provincial DA / RT Prices (¥/kWh)", h1_s))
    story.append(Paragraph(
        "单位：元/千瓦时。DA=日前均价，RT=实时均价，差价=DA−RT（正值代表日前溢价）。"
        "↑↓ 表示较前一日变化。 "
        "Units: ¥/kWh. Spread = DA − RT (positive = DA premium over RT).",
        caption_s,
    ))

    if prices.empty:
        story.append(Paragraph("No price data available for this date.", body_s))
    else:
        # Build prev price lookup
        prev_lookup: dict[str, tuple[float, float]] = {}
        if prev_prices is not None and not prev_prices.empty:
            for _, row in prev_prices.iterrows():
                prev_lookup[str(row["province_cn"])] = (
                    row.get("da_avg"), row.get("rt_avg"),
                )

        headers = ["省份 Province", "日前 DA", "实时 RT", "差价 Spread",
                   "DA环比 Chg", "RT环比 Chg"]
        col_w = [w * cm for w in [4.8, 2.5, 2.5, 2.5, 2.5, 2.5]]

        def _chg_str(curr, prev) -> str:
            if curr is None or prev is None:
                return "—"
            try:
                c, p = float(curr), float(prev)
                return f"{c - p:+.4f}"
            except (TypeError, ValueError):
                return "—"

        rows = []
        extra_s: list = []
        for tbl_row_idx, (_, row) in enumerate(prices.iterrows(), 1):
            prov = str(row.get("province_cn", ""))
            da   = row.get("da_avg")
            rt   = row.get("rt_avg")
            spread = (float(da) - float(rt)) if (
                da is not None and rt is not None and not pd.isna(da) and not pd.isna(rt)
            ) else None
            p_da, p_rt = prev_lookup.get(prov, (None, None))
            da_chg_str = _chg_str(da, p_da)
            rt_chg_str = _chg_str(rt, p_rt)

            # Colour DA change col
            if p_da is not None and da is not None:
                try:
                    if float(da) > float(p_da):
                        extra_s.append(("TEXTCOLOR", (4, tbl_row_idx), (4, tbl_row_idx), _GREEN))
                    elif float(da) < float(p_da):
                        extra_s.append(("TEXTCOLOR", (4, tbl_row_idx), (4, tbl_row_idx), _RED))
                except Exception:
                    pass
            # Colour RT change col
            if p_rt is not None and rt is not None:
                try:
                    if float(rt) > float(p_rt):
                        extra_s.append(("TEXTCOLOR", (5, tbl_row_idx), (5, tbl_row_idx), _GREEN))
                    elif float(rt) < float(p_rt):
                        extra_s.append(("TEXTCOLOR", (5, tbl_row_idx), (5, tbl_row_idx), _RED))
                except Exception:
                    pass

            rows.append([
                prov,
                _fmt(da),
                _fmt(rt),
                _fmt(spread),
                da_chg_str,
                rt_chg_str,
            ])

        story.append(_make_table(headers, rows, col_w, extra_styles=extra_s))
        story.append(Paragraph(
            f"共 {len(prices)} 个省份。 {len(prices)} provinces reported.",
            caption_s,
        ))

    story.append(Spacer(1, 0.4 * cm))

    # ── Section 2: AI Commentary (always shown; data fallback if AI unavailable) ──
    story.append(Paragraph("2. AI 市场分析 Market Analytics", h1_s))
    story.append(Paragraph(
        "由 Claude (Anthropic) 自动生成，基于当日市场数据。"
        " AI-generated based on today's market data. Powered by Claude.",
        caption_s,
    ))
    story.append(Spacer(1, 0.15 * cm))
    if ai_commentary:
        for para_text in [p.strip() for p in ai_commentary.split("\n\n") if p.strip()]:
            story.append(Paragraph(para_text, ai_s))
            story.append(Spacer(1, 0.1 * cm))
    elif summary:
        # Machine-generated fallback when AI call is unavailable
        fallback_lines = [
            f"【市场综述 Market Highlights — {report_date.strftime('%Y-%m-%d')}】",
            f"全国日前均价 National avg DA: {summary.get('avg_da', 0):.4f} ¥/kWh　"
            f"全国实时均价 National avg RT: {summary.get('avg_rt', 0):.4f} ¥/kWh",
            f"最高日前省份 Highest DA: {summary.get('max_da_prov','?')} "
            f"{summary.get('max_da', 0):.4f} ¥/kWh　"
            f"最低日前省份 Lowest DA: {summary.get('min_da_prov','?')} "
            f"{summary.get('min_da', 0):.4f} ¥/kWh",
            f"最高实时省份 Highest RT: {summary.get('max_rt_prov','?')} "
            f"{summary.get('max_rt', 0):.4f} ¥/kWh　"
            f"最低实时省份 Lowest RT: {summary.get('min_rt_prov','?')} "
            f"{summary.get('min_rt', 0):.4f} ¥/kWh",
            f"共报告 {summary.get('n_provinces', '?')} 个省份数据。"
            f" {summary.get('n_provinces', '?')} provinces reported.",
        ]
        for line in fallback_lines:
            story.append(Paragraph(line, ai_s))
            story.append(Spacer(1, 0.1 * cm))
    else:
        story.append(Paragraph("No market data available for commentary.", ai_s))

    story.append(Spacer(1, 0.5 * cm))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
    story.append(Paragraph(
        "本报告由 BESS Platform 自动生成。 "
        "Auto-generated by BESS Platform spot market intelligence system.",
        caption_s,
    ))
    doc.build(story)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_report_pdf(report_date: date | None = None) -> tuple[bytes, str]:
    """Generate the daily spot market report PDF. Returns (pdf_bytes, ai_commentary)."""
    conn = _get_conn()
    try:
        if report_date is None:
            report_date = _get_latest_data_date(conn)

        logger.info("Generating spot report for %s", report_date)
        prices      = _get_provincial_prices(conn, report_date)
        summary     = _get_national_summary(conn, report_date)
        prev_prices = _get_prev_prices(conn, report_date)
        source_pdf  = _get_latest_pdf_name(conn, report_date)
    finally:
        conn.close()

    ai_commentary = _generate_ai_commentary(report_date, prices, summary, prev_prices)

    buf = BytesIO()
    _build_pdf(buf, report_date, prices, summary, prev_prices, ai_commentary, source_pdf)
    pdf_bytes = buf.getvalue()
    logger.info("Spot report PDF generated: %d bytes", len(pdf_bytes))
    return pdf_bytes, ai_commentary


def send_report_email(
    pdf_bytes: bytes,
    report_date: date,
    to_email: str | None = None,
    ai_commentary: str = "",
) -> None:
    """Send the spot market PDF report via SMTP.

    ``to_email`` accepts a single address or comma-separated list.
    """
    raw_to  = to_email or os.environ.get("REPORT_TO_EMAIL", _DEFAULT_RECIPIENT)
    to_list = [e.strip() for e in raw_to.split(",") if e.strip()] or [_DEFAULT_RECIPIENT]

    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASSWORD", "")
    from_email = os.environ.get("REPORT_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_pass:
        raise RuntimeError(
            "SMTP credentials not configured. Set SMTP_USER and SMTP_PASSWORD."
        )

    subject  = f"中国电力现货市场日报 {report_date.strftime('%Y-%m-%d')}"
    filename = f"spot_market_report_{report_date.isoformat()}.pdf"

    msg = MIMEMultipart()
    msg["From"]    = from_email
    msg["To"]      = ", ".join(to_list)
    msg["Subject"] = subject

    body_text = (
        f"请查收 {report_date.strftime('%Y-%m-%d')} 中国电力现货市场日报。\n"
        f"Please find attached the China Spot Electricity Market Daily Report "
        f"for {report_date.strftime('%Y-%m-%d')}.\n\n"
        "Contents:\n"
        "  1. Provincial DA / RT average prices (all reporting provinces)\n"
        "  2. AI market commentary (Chinese + English)\n\n"
    )
    if ai_commentary:
        body_text += "── AI Market Analytics ──\n\n" + ai_commentary + "\n\n"
    body_text += "Generated by BESS Platform spot market intelligence system."
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
    attachment.add_header("Content-Disposition", "attachment", filename=filename)
    msg.attach(attachment)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_email, to_list, msg.as_string())

    logger.info("Spot report emailed to %s", ", ".join(to_list))


def send_report_wecom(
    pdf_bytes: bytes,
    report_date: date,
    webhook_urls: list[str] | None = None,
    ai_commentary: str = "",
) -> None:
    """Send the spot market report to WeCom groups.

    ``webhook_urls``: list of bot webhook URLs. Falls back to DB then env var.
    Each webhook receives upload → markdown → file (3-step delivery).
    """
    import re
    import requests

    if not webhook_urls:
        webhook_urls = get_enabled_webhook_urls()
    if not webhook_urls:
        raise RuntimeError(
            "No WeCom webhook URLs configured. "
            "Add URLs in Data Management → Daily Report tab, "
            "or set WECOM_WEBHOOK_URL env var."
        )

    filename = f"spot_market_report_{report_date.isoformat()}.pdf"
    header = f"## 中国电力现货市场日报 {report_date.strftime('%Y-%m-%d')}\n\n"
    if ai_commentary:
        body = ai_commentary[:3500]
        if len(ai_commentary) > 3500:
            body += "\n\n*(内容已截断，完整报告见附件 PDF)*"
    else:
        body = "详见附件 PDF — 各省日前/实时均价及AI市场分析。"
    markdown_content = header + body

    errors = []
    for url in webhook_urls:
        m = re.search(r"key=([0-9a-f-]+)", url)
        if not m:
            errors.append(f"Cannot extract key from URL: {url}")
            continue
        key = m.group(1)
        try:
            # 1. Upload PDF
            upload_resp = requests.post(
                f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media"
                f"?key={key}&type=file",
                files={"media": (filename, pdf_bytes, "application/pdf")},
                timeout=30,
            )
            upload_resp.raise_for_status()
            upload_data = upload_resp.json()
            if upload_data.get("errcode", 0) != 0:
                raise RuntimeError(f"Upload failed: {upload_data}")
            media_id = upload_data["media_id"]

            # 2. Markdown summary
            md_resp = requests.post(
                url,
                json={"msgtype": "markdown", "markdown": {"content": markdown_content}},
                timeout=10,
            )
            md_resp.raise_for_status()

            # 3. PDF file
            file_resp = requests.post(
                url,
                json={"msgtype": "file", "file": {"media_id": media_id}},
                timeout=10,
            )
            file_resp.raise_for_status()
            if file_resp.json().get("errcode", 0) != 0:
                raise RuntimeError(f"File send failed: {file_resp.json()}")

            logger.info("Spot report sent to WeCom (key=...%s) for %s", key[-8:], report_date)
        except Exception as exc:
            logger.error("WeCom send failed (key=...%s): %s", key[-8:], exc)
            errors.append(str(exc))

    if errors and len(errors) == len(webhook_urls):
        raise RuntimeError(f"All WeCom sends failed: {errors}")


def run_daily_report(to_email: str | None = None,
                     wecom_urls: list[str] | None = None,
                     report_date: "date | None" = None) -> dict:
    """End-to-end: generate PDF, email, and optionally WeCom. Returns status dict."""
    import time
    t0 = time.time()
    try:
        if report_date is None:
            conn = _get_conn()
            try:
                report_date = _get_latest_data_date(conn)
            finally:
                conn.close()

        pdf_bytes, ai_commentary = generate_report_pdf(report_date)
        try:
            from services.common.report_library import save_report as _save_report
            _save_report("spot", report_date, pdf_bytes,
                         f"spot_market_report_{report_date.isoformat()}.pdf")
        except Exception as _lib_exc:
            logger.warning("Report library save failed: %s", _lib_exc)
        send_report_email(pdf_bytes, report_date, to_email, ai_commentary=ai_commentary)

        wecom_result = "skipped"
        urls = wecom_urls or get_enabled_webhook_urls()
        if urls:
            try:
                send_report_wecom(pdf_bytes, report_date, urls, ai_commentary=ai_commentary)
                wecom_result = f"sent to {len(urls)} webhook(s)"
            except Exception as _wc_exc:
                logger.error("WeCom send failed: %s", _wc_exc)
                wecom_result = f"failed: {_wc_exc}"

        return {
            "status": "success",
            "date": str(report_date),
            "size_bytes": len(pdf_bytes),
            "wecom": wecom_result,
            "duration": round(time.time() - t0, 1),
        }
    except Exception as exc:
        logger.error("Spot daily report failed: %s", exc, exc_info=True)
        return {
            "status": "error",
            "date": str(report_date),
            "error": str(exc),
            "duration": round(time.time() - t0, 1),
        }
