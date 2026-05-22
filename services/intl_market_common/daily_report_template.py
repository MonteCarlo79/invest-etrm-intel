"""International market daily report — PDF generation and email/WeCom delivery.

Shared template for AU, ERCOT, PJM, CAISO market apps.
Generates a three-section PDF:
  1. Top 10 BESS performers (yesterday, normalised to £/MWh installed/yr)
  2. Daily average revenue breakdown by market stream
  3. Market summary (spot price stats + ancillary clearing prices)
  4. AI commentary (Claude Haiku)

Usage (from market-specific daily_report.py):
    from services.intl_market_common.daily_report_template import run_daily_report
    from services.au_knowledge.config import MARKET_CONFIG
    run_daily_report(MARKET_CONFIG)
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

import pandas as pd
import psycopg2
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from services.intl_market_common.market_config import MarketConfig

logger = logging.getLogger(__name__)

_DEFAULT_RECIPIENT = "chen_dpeng@hotmail.com"

# Revenue stream display labels (covers AU, ERCOT, PJM, CAISO common names)
_MARKET_LABELS: dict[str, str] = {
    # Generic / cross-market
    "wholesale":       "Wholesale (Spot)",
    "energy":          "Energy",
    "total":           "Total (all markets)",
    # AU FCAS
    "fcas":            "FCAS (all)",
    "fcas_reg":        "FCAS Regulation",
    "fcas_cont":       "FCAS Contingency",
    "fcas_raise":      "FCAS Raise",
    "fcas_lower":      "FCAS Lower",
    "regulation_raise":"Regulation Raise",
    "regulation_lower":"Regulation Lower",
    "contingency":     "FCAS Contingency",
    # ERCOT
    "reg_up":          "Regulation Up",
    "reg_down":        "Regulation Down",
    "rrs":             "RRS",
    "ecrs":            "ECRS",
    "nonspin":         "Non-Spin Reserve",
    # PJM
    "regulation":      "Regulation",
    "sync_reserve":    "Sync. Reserve",
    "primary_reserve": "Primary Reserve",
    # CAISO
    "spin":            "Spinning Reserve",
    "nonspin_reserve": "Non-Spin Reserve",
    "flex_ramp":       "Flex Ramp",
}

# Markets treated as "wholesale" for top-10 aggregation
_WHOLESALE_KEYS = {"wholesale", "energy", "spot", "nem_spot", "rt_energy", "da_energy",
                   "rt_price", "da_price"}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_conn():
    url = (
        os.environ.get("PGURL")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    conn = psycopg2.connect(url, connect_timeout=10)
    conn.autocommit = True
    return conn


def _query(conn, sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql(sql, conn, params=params)


# ---------------------------------------------------------------------------
# Data queries
# ---------------------------------------------------------------------------

def _get_top_performers(conn, cfg: MarketConfig, report_date: date, top_n: int = 10) -> pd.DataFrame:
    prefix = cfg.table_prefix
    return _query(
        conn,
        f"WITH lb AS ( "
        f"  SELECT asset, "
        f"    SUM(CASE WHEN market != 'total' THEN revenue ELSE 0 END) AS total_revenue, "
        f"    SUM(CASE WHEN LOWER(market) IN ('wholesale','energy','spot','nem_spot','rt_energy') "
        f"             THEN revenue ELSE 0 END) AS wholesale, "
        f"    SUM(CASE WHEN LOWER(market) NOT IN ('wholesale','energy','spot','nem_spot','rt_energy','total') "
        f"             THEN revenue ELSE 0 END) AS ancillary, "
        f"    AVG(rated_power) AS rated_power_mw, "
        f"    AVG(energy_capacity) AS energy_capacity_mwh "
        f"  FROM intl_market.{prefix}bess_leaderboard "
        f"  WHERE settlement_date = %s "
        f"  GROUP BY asset ORDER BY total_revenue DESC LIMIT %s "
        f"), "
        f"op AS (SELECT DISTINCT ON (asset) asset, value AS operator "
        f"       FROM intl_market.{prefix}bess_assets WHERE history_table='operator' "
        f"       ORDER BY asset, date_from DESC NULLS LAST), "
        f"ow AS (SELECT DISTINCT ON (asset) asset, value AS owner "
        f"       FROM intl_market.{prefix}bess_assets WHERE history_table='owner' "
        f"       ORDER BY asset, date_from DESC NULLS LAST) "
        f"SELECT lb.asset, ow.owner, op.operator, lb.rated_power_mw, "
        f"  CASE WHEN lb.energy_capacity_mwh > 0 AND lb.rated_power_mw > 0 "
        f"       THEN lb.energy_capacity_mwh / lb.rated_power_mw ELSE NULL END AS duration_h, "
        f"  lb.total_revenue, lb.wholesale, lb.ancillary "
        f"FROM lb "
        f"LEFT JOIN op ON op.asset = lb.asset "
        f"LEFT JOIN ow ON ow.asset = lb.asset "
        f"ORDER BY lb.total_revenue DESC",
        (report_date, top_n),
    )


def _get_revenue_breakdown(conn, cfg: MarketConfig, report_date: date) -> pd.DataFrame:
    """Industry-average revenue index for a single date."""
    prefix = cfg.table_prefix
    # Try duration IS NULL (aggregate row) first; fall back to avg across durations
    df = _query(
        conn,
        f"SELECT market, AVG(revenue_permw) AS revenue_permw, AVG(revenue_permwh) AS revenue_permwh "
        f"FROM intl_market.{prefix}bess_daily_index "
        f"WHERE settlement_date = %s "
        f"GROUP BY market ORDER BY market",
        (report_date,),
    )
    return df


def _get_market_summary(conn, cfg: MarketConfig, report_date: date) -> dict:
    """Spot price stats and ancillary clearing prices."""
    prefix = cfg.table_prefix
    spot = _query(
        conn,
        f"SELECT region, "
        f"  AVG(spot_price) AS avg, MIN(spot_price) AS min, "
        f"  MAX(spot_price) AS max, STDDEV(spot_price) AS stddev "
        f"FROM intl_market.{prefix}spot_price "
        f"WHERE settlement_date = %s "
        f"GROUP BY region ORDER BY region",
        (report_date,),
    )
    ancillary = _query(
        conn,
        f"SELECT service, AVG(clearing_price) AS avg_price, AVG(volume_mw) AS avg_volume "
        f"FROM intl_market.{prefix}ancillary_results "
        f"WHERE settlement_date = %s "
        f"GROUP BY service ORDER BY service",
        (report_date,),
    )
    return {"spot": spot, "ancillary": ancillary}


def _get_prev_data_date(conn, cfg: MarketConfig, report_date: date) -> date | None:
    prefix = cfg.table_prefix
    df = _query(
        conn,
        f"SELECT MAX(settlement_date) AS prev FROM intl_market.{prefix}bess_leaderboard "
        f"WHERE settlement_date < %s",
        (report_date,),
    )
    if df.empty or pd.isna(df.iloc[0]["prev"]):
        return None
    return pd.Timestamp(df.iloc[0]["prev"]).date()


def _get_all_rankings(conn, cfg: MarketConfig, ref_date: date) -> dict:
    prefix = cfg.table_prefix
    df = _query(
        conn,
        f"SELECT asset, SUM(revenue) AS total_rev "
        f"FROM intl_market.{prefix}bess_leaderboard WHERE settlement_date = %s "
        f"GROUP BY asset ORDER BY total_rev DESC",
        (ref_date,),
    )
    return {row["asset"]: i + 1 for i, row in df.iterrows()}


def _get_latest_data_date(conn, cfg: MarketConfig) -> date:
    prefix = cfg.table_prefix
    df = _query(
        conn,
        f"SELECT MAX(settlement_date) AS latest FROM intl_market.{prefix}bess_leaderboard",
    )
    if df.empty or pd.isna(df.iloc[0]["latest"]):
        return date.today() - timedelta(days=1)
    return pd.Timestamp(df.iloc[0]["latest"]).date()


# ---------------------------------------------------------------------------
# AI commentary
# ---------------------------------------------------------------------------

def _generate_ai_commentary(
    cfg: MarketConfig,
    report_date: date,
    performers: pd.DataFrame,
    revenue: pd.DataFrame,
    market: dict,
    prev_revenue: pd.DataFrame | None = None,
) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ""
    try:
        import anthropic as _anthropic
    except ImportError:
        return ""

    cur = cfg.currency_sym

    def _n(v, d=0.0):
        if v is None:
            return d
        try:
            f = float(v)
            return d if f != f else f
        except (TypeError, ValueError):
            return d

    lines: list[str] = [f"{cfg.name} BESS market data for {report_date.strftime('%d %b %Y')}:"]

    spot_df = market.get("spot", pd.DataFrame())
    if not spot_df.empty:
        for _, r in spot_df.iterrows():
            avg = _n(r.get("avg"))
            if avg:
                lines.append(
                    f"Spot price ({r.get('region','')}):"
                    f" avg {cur}{avg:.2f}/MWh,"
                    f" min {cur}{_n(r.get('min')):.2f},"
                    f" max {cur}{_n(r.get('max')):.2f}/MWh"
                )

    anc_df = market.get("ancillary", pd.DataFrame())
    if not anc_df.empty:
        parts = []
        for _, r in anc_df.iterrows():
            svc = r.get("service", "")
            p = _n(r.get("avg_price"))
            if svc and p:
                parts.append(f"{svc} {cur}{p:.2f}/MW")
        if parts:
            lines.append(f"{cfg.ancillary_label} clearing: {'; '.join(parts)}")

    if not revenue.empty:
        try:
            rev_dict = {k: _n(v) for k, v in revenue.set_index("market")["revenue_permw"].to_dict().items()}
            rev_parts = [f"{m}: {cur}{v:,.2f}/MW" for m, v in rev_dict.items() if v and m != "total"]
            if rev_parts:
                lines.append(f"Revenue index: {', '.join(rev_parts[:5])}")
        except Exception:
            pass

    if not performers.empty:
        top1 = performers.iloc[0]
        t1_rev = _n(top1.get("total_revenue"))
        t1_ws  = _n(top1.get("wholesale"))
        t1_anc = _n(top1.get("ancillary"))
        t1_tot = t1_ws + t1_anc or 1
        lines.append(
            f"#1 asset: {top1.get('asset','?')} "
            f"{cur}{t1_rev:,.0f} "
            f"[wholesale {t1_ws/t1_tot*100:.0f}%, ancillary {t1_anc/t1_tot*100:.0f}%]"
        )

    data_snapshot = "\n".join(lines)
    prompt = (
        f"You are a {cfg.name} BESS market analyst. Write a summary under 100 words.\n\n"
        f"Focus on:\n"
        f"1. Top performer: name and revenue mix (wholesale vs {cfg.ancillary_label}).\n"
        f"2. What spot price and ancillary data implies for BESS today.\n\n"
        f"No headers, no bullets, no preamble. Numbers only. Under 100 words.\n\n"
        f"Data:\n{data_snapshot}"
    )
    try:
        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as exc:
        logger.warning("AI commentary failed: %s", exc)
        return ""


# ---------------------------------------------------------------------------
# PDF helpers
# ---------------------------------------------------------------------------

_PAGE_W, _PAGE_H = A4
_MARGIN = 2 * cm

_GREY_HEADER  = colors.HexColor("#1F3864")
_GREY_ALT     = colors.HexColor("#EBF0FA")
_ACCENT       = colors.HexColor("#2E75B6")
_LIGHT_BORDER = colors.HexColor("#B8CCE4")
_GREEN        = colors.HexColor("#1a7f37")
_RED          = colors.HexColor("#cf222e")


def _styles():
    ss = getSampleStyleSheet()
    title   = ParagraphStyle("rpt_title",  parent=ss["Title"],    fontSize=20, textColor=_GREY_HEADER, spaceAfter=6)
    h1      = ParagraphStyle("rpt_h1",     parent=ss["Heading1"], fontSize=13, textColor=_GREY_HEADER, spaceBefore=14, spaceAfter=4)
    h2      = ParagraphStyle("rpt_h2",     parent=ss["Heading2"], fontSize=11, textColor=_ACCENT, spaceBefore=8, spaceAfter=3)
    body    = ParagraphStyle("rpt_body",   parent=ss["Normal"],   fontSize=9)
    caption = ParagraphStyle("rpt_caption",parent=ss["Normal"],   fontSize=8, textColor=colors.grey, spaceAfter=4)
    cell    = ParagraphStyle("rpt_cell",   parent=ss["Normal"],   fontSize=7.5, leading=9)
    return title, h1, h2, body, caption, cell


def _c_rate(duration_h) -> str:
    if duration_h is None or (isinstance(duration_h, float) and pd.isna(duration_h)):
        return "—"
    d = float(duration_h)
    if d <= 0:
        return "—"
    return f"{1/d:.4g}C"


def _fmt(val, decimals=1, prefix="", suffix=""):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return f"{prefix}{val:,.{decimals}f}{suffix}"


def _per_cap_yr(raw, power_mw, duration_h) -> str:
    """Daily raw revenue → annualised per MWh installed."""
    try:
        e_cap = float(power_mw) * float(duration_h)
        if e_cap <= 0:
            return "—"
        return _fmt(float(raw) / e_cap * 365, 0)
    except (TypeError, ValueError):
        return "—"


def _make_table(headers, rows, col_widths, extra_styles=None) -> Table:
    data = [headers] + rows
    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    style = [
        ("BACKGROUND",  (0, 0), (-1, 0),  _GREY_HEADER),
        ("TEXTCOLOR",   (0, 0), (-1, 0),  colors.white),
        ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, 0),  8),
        ("FONTNAME",    (0, 1), (-1, -1), "Helvetica"),
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


# ---------------------------------------------------------------------------
# PDF builder
# ---------------------------------------------------------------------------

def _build_pdf(
    buf: BytesIO,
    cfg: MarketConfig,
    report_date: date,
    performers: pd.DataFrame,
    revenue: pd.DataFrame,
    market: dict,
    prev_rankings: dict | None = None,
    prev_revenue: pd.DataFrame | None = None,
    ai_commentary: str = "",
) -> None:
    title_s, h1_s, h2_s, body_s, caption_s, cell_s = _styles()
    cur = cfg.currency_sym
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=_MARGIN, rightMargin=_MARGIN,
        topMargin=_MARGIN, bottomMargin=_MARGIN,
    )
    story = []

    # ── Title ─────────────────────────────────────────────────────────────────
    story.append(Paragraph(f"{cfg.name} BESS Daily Market Report", title_s))
    story.append(Paragraph(
        f"Report date: <b>{report_date.strftime('%A, %d %B %Y')}</b> &nbsp;|&nbsp; "
        f"Generated: {date.today().strftime('%d %b %Y')} &nbsp;|&nbsp; "
        f"Operator: {cfg.system_operator}",
        body_s,
    ))
    story.append(HRFlowable(width="100%", thickness=2, color=_GREY_HEADER, spaceAfter=10))

    # ── Section 1: Top 10 performers ─────────────────────────────────────────
    story.append(Paragraph("1. Top 10 BESS Performers", h1_s))
    story.append(Paragraph(
        f"Top 10 assets by total revenue for {report_date.strftime('%d %b %Y')}. "
        f"Revenue columns are in {cur}/MWh installed/yr "
        f"(today's {cur} ÷ energy capacity MWh × 365) — capacity-normalised yield. "
        "Dur. = C-rate (1C = 1 h, 0.5C = 2 h, 0.25C = 4 h). "
        f"Ancillary = {cfg.ancillary_label}.",
        caption_s,
    ))

    if performers.empty:
        story.append(Paragraph("No leaderboard data available for this date.", body_s))
    else:
        headers = ["#", "Asset", "Owner", "Operator", "MW", "Dur.",
                   "Total", "Wholesale", "Ancillary"]
        col_w = [w * cm for w in [0.5, 3.8, 2.2, 2.0, 0.8, 1.1, 1.7, 1.7, 1.7]]
        performers = performers.sort_values("total_revenue", ascending=False).reset_index(drop=True)
        rows = []
        extra_s = []
        for rank, (_, row) in enumerate(performers.iterrows(), 1):
            asset = str(row.get("asset", ""))
            pw = row.get("rated_power_mw")
            dh = row.get("duration_h")
            prev_rank = (prev_rankings or {}).get(asset)
            tbl_row = rank
            if prev_rank is not None:
                if rank < prev_rank:
                    extra_s.append(("TEXTCOLOR", (0, tbl_row), (0, tbl_row), _GREEN))
                elif rank > prev_rank:
                    extra_s.append(("TEXTCOLOR", (0, tbl_row), (0, tbl_row), _RED))
            rows.append([
                str(rank),
                Paragraph(asset, cell_s),
                Paragraph(str(row.get("owner") or ""), cell_s),
                Paragraph(str(row.get("operator") or ""), cell_s),
                _fmt(pw, 0),
                _c_rate(dh),
                _per_cap_yr(row.get("total_revenue"), pw, dh),
                _per_cap_yr(row.get("wholesale"),     pw, dh),
                _per_cap_yr(row.get("ancillary"),     pw, dh),
            ])
        story.append(_make_table(headers, rows, col_w, extra_styles=extra_s))
        story.append(Paragraph(
            f"Revenue columns: {cur}/MWh installed/yr — daily gross revenue ÷ energy capacity × 365.",
            caption_s,
        ))

    story.append(Spacer(1, 0.4 * cm))

    # ── Section 2: Revenue breakdown ──────────────────────────────────────────
    story.append(Paragraph("2. Daily Average Revenue Breakdown", h1_s))
    story.append(Paragraph(
        f"Modo industry-average index for all {cfg.name} BESS. "
        f"{cur}/MW/day = revenue per rated MW. "
        f"{cur}/MWh/yr = revenue per installed MWh × 365 (annualised yield).",
        caption_s,
    ))

    if revenue.empty:
        story.append(Paragraph("No revenue index data available for this date.", body_s))
    else:
        non_total = revenue[revenue["market"] != "total"].sort_values("revenue_permw", ascending=False)
        total_row = revenue[revenue["market"] == "total"]

        headers2 = ["Market Stream", f"{cur}/MW/day", f"{cur}/MWh/yr", f"Prev {cur}/MWh/yr"]
        col_w2   = [6.0 * cm, 2.8 * cm, 4.1 * cm, 4.1 * cm]
        rows2    = []
        extra_s2 = []

        def _rev_row(market_key, label, permw, permwh):
            curr_val = float(permwh) * 365 if pd.notna(permwh) else None
            prev_val = None
            if prev_revenue is not None and not prev_revenue.empty:
                pm = prev_revenue[prev_revenue["market"] == market_key]
                if not pm.empty and pd.notna(pm.iloc[0].get("revenue_permwh")):
                    prev_val = float(pm.iloc[0]["revenue_permwh"]) * 365
            if curr_val is not None and prev_val is not None:
                sym = "↑" if curr_val > prev_val else ("↓" if curr_val < prev_val else "")
                curr_str = f"{sym} {_fmt(curr_val, 0, cur)}" if sym else _fmt(curr_val, 0, cur)
                color = _GREEN if curr_val > prev_val else (_RED if curr_val < prev_val else None)
            else:
                curr_str = _fmt(curr_val, 0, cur)
                color = None
            return [label, _fmt(permw, 2, cur), curr_str, _fmt(prev_val, 0, cur)], color

        for _, row in non_total.iterrows():
            idx = len(rows2) + 1
            label = _MARKET_LABELS.get(row["market"], row["market"])
            cells, color = _rev_row(row["market"], label, row["revenue_permw"], row.get("revenue_permwh"))
            rows2.append(cells)
            if color:
                extra_s2.append(("TEXTCOLOR", (2, idx), (2, idx), color))

        if not total_row.empty:
            tr = total_row.iloc[0]
            idx = len(rows2) + 1
            cells, color = _rev_row("total", "— Total —", tr["revenue_permw"], tr.get("revenue_permwh"))
            rows2.append(cells)
            if color:
                extra_s2.append(("TEXTCOLOR", (2, idx), (2, idx), color))

        tbl2 = _make_table(headers2, rows2, col_w2, extra_styles=extra_s2)
        if not total_row.empty:
            last = len(rows2)
            tbl2.setStyle(TableStyle([
                ("FONTNAME",   (0, last), (-1, last), "Helvetica-Bold"),
                ("BACKGROUND", (0, last), (-1, last), colors.HexColor("#D6E4F0")),
            ]))
        story.append(tbl2)

    story.append(Spacer(1, 0.4 * cm))

    # ── Section 3: Market summary ─────────────────────────────────────────────
    story.append(Paragraph("3. Market Summary", h1_s))

    # Spot price
    story.append(Paragraph(f"{cfg.wholesale_label} Spot Price ({cur}/MWh)", h2_s))
    spot_df = market.get("spot", pd.DataFrame())
    if spot_df.empty or spot_df.iloc[0].isna().all():
        story.append(Paragraph("No spot price data available.", body_s))
    else:
        sp_rows = []
        for _, r in spot_df.iterrows():
            sp_rows.append([
                str(r.get("region", cfg.system_operator)),
                _fmt(r.get("avg"), 2, cur),
                _fmt(r.get("min"), 2, cur),
                _fmt(r.get("max"), 2, cur),
                _fmt(r.get("stddev"), 2, cur),
            ])
        story.append(_make_table(
            ["Region", "Avg /MWh", "Min /MWh", "Max /MWh", "Std Dev"],
            sp_rows,
            [3.0 * cm, 3.0 * cm, 3.0 * cm, 3.0 * cm, 3.0 * cm],
        ))

    story.append(Spacer(1, 0.2 * cm))

    # Ancillary clearing
    story.append(Paragraph(f"{cfg.ancillary_label} Clearing Prices", h2_s))
    anc_df = market.get("ancillary", pd.DataFrame())
    if anc_df.empty:
        story.append(Paragraph("No ancillary clearing data available.", body_s))
    else:
        anc_rows = []
        for _, row in anc_df.iterrows():
            anc_rows.append([
                str(row.get("service", "")),
                _fmt(row.get("avg_price"), 2, cur),
                _fmt(row.get("avg_volume"), 1, suffix=" MW"),
            ])
        story.append(_make_table(
            ["Service", f"Avg Clearing Price ({cur}/MW)", "Avg Volume"],
            anc_rows,
            [6.0 * cm, 5.5 * cm, 4.5 * cm],
        ))

    # ── Section 4: AI commentary ──────────────────────────────────────────────
    if ai_commentary:
        story.append(Spacer(1, 0.4 * cm))
        story.append(Paragraph("4. Market Analytics", h1_s))
        story.append(Paragraph(
            "AI-generated commentary based on today's market data. Powered by Claude (Anthropic).",
            caption_s,
        ))
        for para_text in [p.strip() for p in ai_commentary.split("\n\n") if p.strip()]:
            story.append(Paragraph(para_text, body_s))
            story.append(Spacer(1, 0.15 * cm))

    story.append(Spacer(1, 0.6 * cm))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
    story.append(Paragraph(
        f"Generated automatically by the {cfg.name} Market Intelligence platform. "
        f"Data sourced from Modo Energy and {cfg.system_operator}.",
        caption_s,
    ))
    doc.build(story)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_report_pdf(cfg: MarketConfig, report_date: date | None = None) -> tuple[bytes, str]:
    """Generate PDF. Returns (pdf_bytes, ai_commentary)."""
    conn = _get_conn()
    try:
        if report_date is None:
            report_date = _get_latest_data_date(conn, cfg)
        logger.info("[%s] Generating daily report for %s", cfg.code, report_date)
        performers   = _get_top_performers(conn, cfg, report_date)
        revenue      = _get_revenue_breakdown(conn, cfg, report_date)
        market_data  = _get_market_summary(conn, cfg, report_date)
        prev_date    = _get_prev_data_date(conn, cfg, report_date)
        prev_rankings = _get_all_rankings(conn, cfg, prev_date) if prev_date else None
        prev_revenue  = _get_revenue_breakdown(conn, cfg, prev_date) if prev_date else None
    finally:
        conn.close()

    ai_commentary = _generate_ai_commentary(cfg, report_date, performers, revenue, market_data, prev_revenue)

    buf = BytesIO()
    _build_pdf(buf, cfg, report_date, performers, revenue, market_data,
               prev_rankings=prev_rankings, prev_revenue=prev_revenue,
               ai_commentary=ai_commentary)
    return buf.getvalue(), ai_commentary


def send_daily_report_email(
    cfg: MarketConfig,
    pdf_bytes: bytes,
    report_date: date,
    to_email: str | None = None,
    from_email: str | None = None,
    ai_commentary: str = "",
) -> None:
    raw_to    = to_email or os.environ.get("REPORT_TO_EMAIL", _DEFAULT_RECIPIENT)
    to_list   = [e.strip() for e in raw_to.split(",") if e.strip()] or [_DEFAULT_RECIPIENT]
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASSWORD", "")
    from_email = from_email or os.environ.get("REPORT_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_pass:
        raise RuntimeError("SMTP credentials not configured (SMTP_USER / SMTP_PASSWORD).")

    cur  = cfg.currency_sym
    subject  = f"{cfg.name} BESS Daily Market Report — {report_date.strftime('%d %b %Y')}"
    filename = f"{cfg.code}_market_report_{report_date.isoformat()}.pdf"

    msg = MIMEMultipart()
    msg["From"]    = from_email
    msg["To"]      = ", ".join(to_list)
    msg["Subject"] = subject

    body_text = (
        f"Please find attached the {cfg.name} BESS Daily Market Report for "
        f"{report_date.strftime('%d %b %Y')}.\n\n"
        "Contents:\n"
        "  1. Top 10 BESS performers\n"
        "  2. Daily average revenue breakdown by market stream\n"
        f"  3. Market summary ({cfg.wholesale_label} spot price, {cfg.ancillary_label})\n"
        "  4. AI Market Analytics (Claude)\n\n"
    )
    if ai_commentary:
        body_text += "── Market Analytics ──\n\n" + ai_commentary + "\n\n"
    body_text += f"Generated by {cfg.name} Market Intelligence platform."
    msg.attach(MIMEText(body_text, "plain"))

    att = MIMEApplication(pdf_bytes, _subtype="pdf")
    att.add_header("Content-Disposition", "attachment", filename=filename)
    msg.attach(att)

    ctx = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls(context=ctx)
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_email, to_list, msg.as_string())

    logger.info("[%s] Report emailed to %s", cfg.code, ", ".join(to_list))


def send_daily_report_wecom(
    cfg: MarketConfig,
    pdf_bytes: bytes,
    report_date: date,
    webhook_url: str | None = None,
    ai_commentary: str = "",
) -> None:
    import re
    import requests

    raw = webhook_url or os.environ.get("WECOM_WEBHOOK_URL", "")
    if not raw:
        raise RuntimeError("WeCom webhook URL not configured (WECOM_WEBHOOK_URL).")

    urls = [u.strip() for u in raw.split(",") if u.strip()]
    filename = f"{cfg.code}_market_report_{report_date.isoformat()}.pdf"
    header = f"## {cfg.name} BESS Daily Market Report — {report_date.strftime('%d %b %Y')}\n\n"
    body = (ai_commentary[:3500] if ai_commentary else
            "See attached PDF for top performers, revenue breakdown, and market summary.")
    if ai_commentary and len(ai_commentary) > 3500:
        body += "\n\n*(truncated — see attached PDF for full report)*"
    markdown_content = header + body

    errors = []
    for url in urls:
        m = re.search(r"key=([0-9a-f-]+)", url)
        if not m:
            errors.append(f"No key in URL: {url}")
            continue
        key = m.group(1)
        try:
            up = requests.post(
                f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={key}&type=file",
                files={"media": (filename, pdf_bytes, "application/pdf")},
                timeout=30,
            )
            up.raise_for_status()
            up_data = up.json()
            if up_data.get("errcode", 0) != 0:
                raise RuntimeError(f"Upload failed: {up_data}")
            media_id = up_data["media_id"]

            requests.post(url, json={"msgtype": "markdown", "markdown": {"content": markdown_content}}, timeout=10).raise_for_status()
            fr = requests.post(url, json={"msgtype": "file", "file": {"media_id": media_id}}, timeout=10)
            fr.raise_for_status()
            if fr.json().get("errcode", 0) != 0:
                raise RuntimeError(f"File send failed: {fr.json()}")

            logger.info("[%s] WeCom report sent (...%s)", cfg.code, key[-8:])
        except Exception as exc:
            logger.error("[%s] WeCom failed (...%s): %s", cfg.code, key[-8:], exc)
            errors.append(str(exc))

    if errors and len(errors) == len(urls):
        raise RuntimeError(f"All WeCom sends failed: {errors}")


def run_daily_report(cfg: MarketConfig, to_email: str | None = None) -> dict:
    """End-to-end: generate PDF, send email. Returns status dict."""
    import time
    t0 = time.time()
    report_date = None
    try:
        conn = _get_conn()
        try:
            report_date = _get_latest_data_date(conn, cfg)
        finally:
            conn.close()
        pdf_bytes, ai_commentary = generate_report_pdf(cfg, report_date)
        send_daily_report_email(cfg, pdf_bytes, report_date, to_email, ai_commentary=ai_commentary)
        return {"status": "success", "date": str(report_date), "size_bytes": len(pdf_bytes),
                "duration": round(time.time() - t0, 1)}
    except Exception as exc:
        logger.error("[%s] Daily report failed: %s", cfg.code if cfg else "?", exc, exc_info=True)
        return {"status": "error", "date": str(report_date), "error": str(exc),
                "duration": round(time.time() - t0, 1)}
