"""Daily GB Market Report — PDF generation and email delivery.

Generates a three-section PDF:
  1. Top 10 BESS performers (yesterday)
  2. Daily average revenue breakdown by market stream
  3. Market summary (system price, EPEX DA, NIV, DX ancillary clearing prices)

Email is sent via SMTP.  Configure with env vars:
  SMTP_HOST          (default: smtp.gmail.com)
  SMTP_PORT          (default: 587)
  SMTP_USER          — sender email address / SMTP username
  SMTP_PASSWORD      — SMTP password or app password
  REPORT_FROM_EMAIL  — From: address (defaults to SMTP_USER)
  REPORT_TO_EMAIL    — override recipient (defaults to chen_dpeng@hotmail.com)
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

logger = logging.getLogger(__name__)

_DEFAULT_RECIPIENT = "chen_dpeng@hotmail.com"


# ---------------------------------------------------------------------------
# AI market commentary
# ---------------------------------------------------------------------------

def _generate_ai_commentary(
    report_date: date,
    performers: pd.DataFrame,
    revenue: pd.DataFrame,
    market: dict,
    prev_revenue: pd.DataFrame | None = None,
) -> str:
    """Call Claude to generate a 3–4 paragraph market analytics commentary.

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

    # ── Build data snapshot for the prompt ───────────────────────────────────
    # Helper: coerce None/NaN to a safe numeric default for format specs
    def _n(v, d=0.0):
        if v is None:
            return d
        try:
            f = float(v)
            return d if f != f else f   # f != f is True only for NaN
        except (TypeError, ValueError):
            return d

    lines: list[str] = [f"GB BESS market data for {report_date.strftime('%d %b %Y')}:"]

    # EPEX DA prices
    epex_df = market.get("epex", pd.DataFrame())
    if not epex_df.empty:
        r = epex_df.iloc[0]
        bl = _n(r.get("baseload"))
        pk = _n(r.get("peakload"))
        op = _n(r.get("offpeak"))
        if bl:
            msg = f"EPEX DA: baseload £{bl:.2f}/MWh"
            if pk:
                msg += f", peak £{pk:.2f}/MWh"
            if op:
                msg += f", off-peak £{op:.2f}/MWh"
            lines.append(msg)
        if pk and op:
            lines.append(f"Peak/off-peak spread: £{pk - op:.2f}/MWh")

    # System price
    sp_df = market.get("system_price", pd.DataFrame())
    if not sp_df.empty:
        r = sp_df.iloc[0]
        avg = _n(r.get("avg"))
        if avg:
            lines.append(
                f"System price: avg £{avg:.2f}/MWh, "
                f"min £{_n(r.get('min')):.2f}/MWh, "
                f"max £{_n(r.get('max')):.2f}/MWh, "
                f"std dev £{_n(r.get('stddev')):.2f}/MWh"
            )

    # NIV
    niv_df = market.get("niv", pd.DataFrame())
    if not niv_df.empty:
        niv = _n(niv_df.iloc[0].get("avg_niv"))
        if niv:
            direction = "long (over-generation)" if niv > 0 else "short (under-generation)"
            lines.append(f"Average NIV: {niv:.1f} MWh per SP — system was {direction}")

    # DX ancillary
    dx_df = market.get("dx", pd.DataFrame())
    if not dx_df.empty:
        dx_parts = []
        for _, row in dx_df.iterrows():
            svc = row.get("service", "")
            p   = _n(row.get("avg_price"))
            v   = _n(row.get("avg_volume"))
            if svc and p:
                dx_parts.append(f"{svc} £{p:.2f}/MW ({v:.0f} MW)")
        if dx_parts:
            lines.append(f"DX ancillary clearing: {'; '.join(dx_parts)}")

    # Revenue breakdown (today) — gb_bess_daily_index has one row per market
    if not revenue.empty:
        try:
            rev_dict = {
                k: _n(v)
                for k, v in revenue.set_index("market")["revenue_permw"].to_dict().items()
            }
            key_mkts = ["wholesale", "bm", "dch", "dcl", "dmh", "dml"]
            rev_parts = [
                f"{m}: £{rev_dict[m]:,.2f}/MW"
                for m in key_mkts
                if m in rev_dict and rev_dict[m]
            ]
            if rev_parts:
                lines.append(f"Market revenue index (£/MW): {', '.join(rev_parts)}")
        except Exception:
            pass

    # Day-on-day wholesale revenue change
    if prev_revenue is not None and not prev_revenue.empty and not revenue.empty:
        try:
            def _ws(df):
                d = {k: _n(v) for k, v in df.set_index("market")["revenue_permw"].to_dict().items()}
                return d.get("wholesale", 0.0)
            today_ws = _ws(revenue)
            prev_ws  = _ws(prev_revenue)
            if prev_ws:
                pct = (today_ws - prev_ws) / abs(prev_ws) * 100
                lines.append(f"Wholesale £/MW day-on-day change: {pct:+.1f}%")
        except Exception:
            pass

    # Top performers with revenue mix
    if not performers.empty:
        top = performers.head(10)
        top1 = top.iloc[0]
        t1_rev   = _n(top1.get("total_revenue"))
        t1_anc   = _n(top1.get("ancillary"))
        t1_ws    = _n(top1.get("wholesale"))
        t1_bm    = _n(top1.get("bm"))
        t1_res   = _n(top1.get("reserve"))
        t1_other = _n(top1.get("other"))
        t1_total = t1_anc + t1_ws + t1_bm + t1_res + t1_other or 1
        lines.append(
            f"#1 asset: {top1.get('asset','?')} "
            f"£{t1_rev:,.0f} "
            f"[ancillary {t1_anc/t1_total*100:.0f}%, "
            f"wholesale {t1_ws/t1_total*100:.0f}%, "
            f"BM {t1_bm/t1_total*100:.0f}%, "
            f"reserve {t1_res/t1_total*100:.0f}%]"
        )
        rest = top.iloc[1:]
        if not rest.empty:
            r_anc   = rest["ancillary"].apply(_n).mean()
            r_ws    = rest["wholesale"].apply(_n).mean()
            r_bm    = rest["bm"].apply(_n).mean()
            r_res   = rest["reserve"].apply(_n).mean()
            r_other = rest["other"].apply(_n).mean()
            r_total = r_anc + r_ws + r_bm + r_res + r_other or 1
            r_rev   = rest["total_revenue"].apply(_n).mean()
            lines.append(
                f"Avg #2-{len(rest)+1}: "
                f"£{r_rev:,.0f} "
                f"[ancillary {r_anc/r_total*100:.0f}%, "
                f"wholesale {r_ws/r_total*100:.0f}%, "
                f"BM {r_bm/r_total*100:.0f}%, "
                f"reserve {r_res/r_total*100:.0f}%]"
            )
            rev_premium = (t1_rev / r_rev - 1) * 100 if r_rev else 0
            lines.append(f"#1 revenue premium over avg rest: {rev_premium:+.0f}%")

    data_snapshot = "\n".join(lines)

    prompt = (
        "You are a GB BESS market analyst. Write a single summary under 100 words.\n\n"
        "Focus ONLY on:\n"
        "1. Top performer: name, rank, and what revenue mix drove its lead "
        "(e.g. heavy ancillary vs wholesale vs BM — be specific with %s).\n"
        "2. How the #1 revenue combination differs from the rest of the top 10.\n"
        "3. One sentence on what the market data (price/NIV/DX) implies for BESS today.\n\n"
        "No headers, no bullets, no preamble. Precise numbers only. Under 100 words.\n\n"
        f"Data:\n{data_snapshot}"
    )

    try:
        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        logger.info("AI commentary generated (%d chars)", len(text))
        return text
    except Exception as exc:
        logger.warning("AI commentary generation failed: %s", exc)
        return ""

# ---------------------------------------------------------------------------
# DB helpers (standalone, no Streamlit cache)
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

def _get_top_performers(conn, report_date: date, top_n: int = 10) -> pd.DataFrame:
    return _query(
        conn,
        "WITH per_sp AS ( "
        "  SELECT settlement_date, settlement_period, asset, "
        "    SUM(CASE WHEN market != '_test_' THEN revenue ELSE 0 END) AS total_rev, "
        "    AVG(rated_power) AS rated_power, "
        "    SUM(CASE WHEN market='wholesale' THEN revenue ELSE 0 END) AS wholesale, "
        "    SUM(CASE WHEN market IN ('dch','dcl','dmh','dml','drh','drl') THEN revenue ELSE 0 END) AS ancillary, "
        "    SUM(CASE WHEN market='bm' THEN revenue ELSE 0 END) AS bm, "
        "    SUM(CASE WHEN market IN ('nbr','nqr','nsr','pbr','pqr','psr') THEN revenue ELSE 0 END) AS reserve, "
        "    SUM(CASE WHEN market NOT IN ('wholesale','dch','dcl','dmh','dml','drh','drl','bm',"
        "      'nbr','nqr','nsr','pbr','pqr','psr','_test_') THEN revenue ELSE 0 END) AS other "
        "  FROM intl_market.gb_bess_leaderboard "
        "  WHERE settlement_date = %s "
        "  GROUP BY settlement_date, settlement_period, asset "
        "), "
        "lb AS ( "
        "  SELECT asset, "
        "    SUM(total_rev) AS total_revenue, "
        "    SUM(wholesale) AS wholesale, SUM(ancillary) AS ancillary, "
        "    SUM(bm) AS bm, SUM(reserve) AS reserve, SUM(other) AS other, "
        "    AVG(rated_power) AS rated_power_mw "
        "  FROM per_sp GROUP BY asset ORDER BY total_revenue DESC LIMIT %s "
        "), "
        "op AS ( "
        "  SELECT DISTINCT ON (asset) asset, value AS operator "
        "  FROM intl_market.gb_bess_assets WHERE history_table='operator' "
        "  ORDER BY asset, valid_from DESC "
        "), "
        "ow AS ( "
        "  SELECT DISTINCT ON (asset) asset, value AS owner "
        "  FROM intl_market.gb_bess_assets WHERE history_table='owner' "
        "  ORDER BY asset, valid_from DESC "
        "), "
        "dur AS ( "
        "  SELECT DISTINCT ON (asset) asset, "
        "    CAST(value AS NUMERIC) AS energy_mwh "
        "  FROM intl_market.gb_bess_assets WHERE history_table='energy_capacity' "
        "  ORDER BY asset, valid_from DESC "
        ") "
        "SELECT lb.asset, ow.owner, op.operator, lb.rated_power_mw, "
        "  CASE WHEN dur.energy_mwh > 0 AND lb.rated_power_mw > 0 "
        "       THEN dur.energy_mwh / lb.rated_power_mw ELSE NULL END AS duration_h, "
        "  lb.total_revenue, lb.wholesale, lb.ancillary, lb.bm, lb.reserve, lb.other "
        "FROM lb "
        "LEFT JOIN op  ON op.asset  = lb.asset "
        "LEFT JOIN ow  ON ow.asset  = lb.asset "
        "LEFT JOIN dur ON dur.asset = lb.asset "
        "ORDER BY lb.total_revenue DESC",
        (report_date, top_n),
    )


def _get_revenue_breakdown(conn, report_date: date) -> pd.DataFrame:
    """Market avg revenue breakdown for a single date (duration='*' = any duration)."""
    return _query(
        conn,
        "SELECT market, revenue_permw, revenue_permwh "
        "FROM intl_market.gb_bess_daily_index "
        "WHERE settlement_date = %s AND duration = '*' "
        "ORDER BY market",
        (report_date,),
    )


def _get_market_summary(conn, report_date: date) -> dict:
    """System price stats, EPEX DA, NIV, DX clearing prices for a single date."""
    # System price
    sp = _query(
        conn,
        "SELECT AVG(system_price) AS avg, MIN(system_price) AS min, "
        "MAX(system_price) AS max, STDDEV(system_price) AS stddev "
        "FROM intl_market.gb_system_price WHERE date = %s",
        (report_date,),
    )
    # EPEX DA
    epex = _query(
        conn,
        "SELECT MAX(daily_baseload) AS baseload, MAX(daily_peakload) AS peakload, "
        "MAX(daily_offpeak) AS offpeak "
        "FROM intl_market.gb_epex_da_hh WHERE delivery_date = %s",
        (report_date,),
    )
    # NIV
    niv = _query(
        conn,
        "SELECT AVG(niv) AS avg_niv FROM intl_market.gb_niv WHERE date = %s",
        (report_date,),
    )
    # DX ancillary — avg clearing price per service
    dx = _query(
        conn,
        "SELECT service, AVG(clearing_price) AS avg_price, "
        "AVG(cleared_volume) AS avg_volume "
        "FROM intl_market.gb_dx_results WHERE efa_date = %s "
        "GROUP BY service ORDER BY service",
        (report_date,),
    )
    return {"system_price": sp, "epex": epex, "niv": niv, "dx": dx}


def _get_prev_data_date(conn, report_date: date) -> date | None:
    """Most recent settlement date before report_date that has leaderboard data."""
    df = _query(
        conn,
        "SELECT MAX(settlement_date) AS prev FROM intl_market.gb_bess_leaderboard "
        "WHERE settlement_date < %s",
        (report_date,),
    )
    if df.empty or pd.isna(df.iloc[0]["prev"]):
        return None
    return pd.Timestamp(df.iloc[0]["prev"]).date()


def _get_all_rankings(conn, ref_date: date) -> dict:
    """Returns {asset: rank} for ref_date ranked by total revenue desc."""
    df = _query(
        conn,
        "SELECT asset, SUM(revenue) AS total_rev "
        "FROM intl_market.gb_bess_leaderboard WHERE settlement_date = %s "
        "GROUP BY asset ORDER BY total_rev DESC",
        (ref_date,),
    )
    return {row["asset"]: i + 1 for i, row in df.iterrows()}


# ---------------------------------------------------------------------------
# PDF builder
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
    title   = ParagraphStyle("rpt_title",  parent=ss["Title"],  fontSize=20, textColor=_GREY_HEADER, spaceAfter=6)
    h1      = ParagraphStyle("rpt_h1",     parent=ss["Heading1"], fontSize=13, textColor=_GREY_HEADER, spaceBefore=14, spaceAfter=4)
    h2      = ParagraphStyle("rpt_h2",     parent=ss["Heading2"], fontSize=11, textColor=_ACCENT, spaceBefore=8, spaceAfter=3)
    body    = ParagraphStyle("rpt_body",   parent=ss["Normal"],   fontSize=9)
    caption = ParagraphStyle("rpt_caption",parent=ss["Normal"],   fontSize=8, textColor=colors.grey, spaceAfter=4)
    cell    = ParagraphStyle("rpt_cell",   parent=ss["Normal"],   fontSize=7.5, leading=9)
    return title, h1, h2, body, caption, cell


def _c_rate(duration_h) -> str:
    """Return C-rate string for a given duration in hours (e.g. 2h → '0.5C')."""
    if duration_h is None or (isinstance(duration_h, float) and pd.isna(duration_h)):
        return "—"
    d = float(duration_h)
    if d <= 0:
        return "—"
    c = 1.0 / d
    # Format: drop trailing zeros — 1.0 → "1C", 0.5 → "0.5C", 0.25 → "0.25C"
    c_str = f"{c:.4g}"
    return f"{c_str}C"


def _fmt(val, decimals=1, prefix="", suffix=""):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return f"{prefix}{val:,.{decimals}f}{suffix}"


def _make_table(headers: list[str], rows: list[list], col_widths: list[float],
                extra_styles: list | None = None) -> Table:
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


def _build_pdf(buf: BytesIO, report_date: date,
               performers: pd.DataFrame,
               revenue: pd.DataFrame,
               market: dict,
               prev_rankings: dict | None = None,
               prev_revenue: pd.DataFrame | None = None,
               ai_commentary: str = "") -> None:
    title_s, h1_s, h2_s, body_s, caption_s, cell_s = _styles()
    usable_w = _PAGE_W - 2 * _MARGIN
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=_MARGIN, rightMargin=_MARGIN,
        topMargin=_MARGIN, bottomMargin=_MARGIN,
    )
    story = []

    # ── Title block ──────────────────────────────────────────────────────────
    story.append(Paragraph("GB BESS Daily Market Report", title_s))
    story.append(Paragraph(
        f"Report date: <b>{report_date.strftime('%A, %d %B %Y')}</b> &nbsp;|&nbsp; "
        f"Generated: {date.today().strftime('%d %b %Y')}",
        body_s,
    ))
    story.append(HRFlowable(width="100%", thickness=2, color=_GREY_HEADER, spaceAfter=10))

    # ── Section 1: Top 10 BESS performers ────────────────────────────────────
    story.append(Paragraph("1. Top 10 BESS Performers", h1_s))
    story.append(Paragraph(
        f"Total revenue by asset for {report_date.strftime('%d %b %Y')}, "
        "ranked by total £ across all settlement periods (excl. test data). "
        "Dur. = storage C-rate (1C = 1 h, 0.5C = 2 h, 0.25C = 4 h). "
        "Ancillary = DC/DM/DR products; Reserve = balancing reserve products; "
        "Other = CM, Triads, Imbalance, SOC, etc. Total = sum of all columns.",
        caption_s,
    ))

    if performers.empty:
        story.append(Paragraph("No leaderboard data available for this date.", body_s))
    else:
        # 12 columns; widths sum to 17.0 cm (A4 portrait, 2 cm margins each side)
        headers = ["#", "Asset", "Owner", "Operator", "MW", "Dur.",
                   "Total £", "Wholesale", "Ancillary", "BM", "Reserve", "Other"]
        col_w = [w * cm for w in [0.5, 3.3, 2.0, 1.8, 0.8, 1.3,
                                   1.5, 1.4, 1.3, 1.2, 1.2, 0.7]]
        # Ensure sorted by total_revenue descending (belt-and-suspenders over SQL ORDER BY)
        performers = performers.sort_values("total_revenue", ascending=False).reset_index(drop=True)
        rows = []
        extra_s = []
        for rank, (_, row) in enumerate(performers.iterrows(), 1):
            asset = str(row.get("asset", ""))
            # Colour rank cell green/red if rank changed vs prior day
            prev_rank = (prev_rankings or {}).get(asset)
            tbl_row = rank  # header is row 0; data rows start at 1
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
                _fmt(row.get("rated_power_mw"), 0),
                _c_rate(row.get("duration_h")),
                _fmt(row.get("total_revenue"), 0, "£"),
                _fmt(row.get("wholesale"), 0, "£"),
                _fmt(row.get("ancillary"), 0, "£"),
                _fmt(row.get("bm"), 0, "£"),
                _fmt(row.get("reserve"), 0, "£"),
                _fmt(row.get("other"), 0, "£"),
            ])
        story.append(_make_table(headers, rows, col_w, extra_styles=extra_s))

    story.append(Spacer(1, 0.4 * cm))

    # ── Section 2: Revenue breakdown by market stream ────────────────────────
    story.append(Paragraph("2. Daily Average Revenue Breakdown", h1_s))
    story.append(Paragraph(
        "Modo industry-average index for all GB BESS (any duration). "
        "£/MW/day = revenue per rated MW for the date. "
        "£/MWh/yr = revenue per installed MWh of storage capacity, annualised "
        "(today's £/MWh × 365) — standard BESS revenue yield metric.",
        caption_s,
    ))

    if revenue.empty:
        story.append(Paragraph("No revenue index data available for this date.", body_s))
    else:
        _MARKET_LABELS = {
            "wholesale": "Wholesale (EPEX)",
            "frequency_response": "Frequency Response",
            "bm": "Balancing Mechanism",
            "imbalance": "Imbalance",
            "reserve": "Reserve",
            "total": "Total (all markets)",
        }
        # Separate out 'total' row and sort remainder
        non_total = revenue[revenue["market"] != "total"].sort_values("revenue_permw", ascending=False)
        total_row = revenue[revenue["market"] == "total"]

        # col 2 = current £/MWh/yr (with ↑↓), col 3 = prev £/MWh/yr
        headers2 = ["Market Stream", "£/MW/day", "£/MWh/yr", "Prev £/MWh/yr"]
        col_w2   = [6.0 * cm, 2.8 * cm, 4.1 * cm, 4.1 * cm]
        rows2 = []
        extra_s2 = []

        def _rev_row(market_key, label, permw, permwh):
            curr_val = float(permwh) * 365 if pd.notna(permwh) else None
            prev_val = None
            if prev_revenue is not None and not prev_revenue.empty:
                pm = prev_revenue[prev_revenue["market"] == market_key]
                if not pm.empty and pd.notna(pm.iloc[0].get("revenue_permwh")):
                    prev_val = float(pm.iloc[0]["revenue_permwh"]) * 365
            if curr_val is not None and prev_val is not None:
                if curr_val > prev_val:
                    curr_str = f"↑ {_fmt(curr_val, 0, '£')}"
                    color = _GREEN
                elif curr_val < prev_val:
                    curr_str = f"↓ {_fmt(curr_val, 0, '£')}"
                    color = _RED
                else:
                    curr_str = _fmt(curr_val, 0, "£")
                    color = None
            else:
                curr_str = _fmt(curr_val, 0, "£")
                color = None
            return [label, _fmt(permw, 2, "£"), curr_str, _fmt(prev_val, 0, "£")], color

        for _, row in non_total.iterrows():
            data_row_idx = len(rows2) + 1  # +1 for header
            cells, color = _rev_row(
                row["market"],
                _MARKET_LABELS.get(row["market"], row["market"]),
                row["revenue_permw"],
                row.get("revenue_permwh"),
            )
            rows2.append(cells)
            if color:
                extra_s2.append(("TEXTCOLOR", (2, data_row_idx), (2, data_row_idx), color))

        if not total_row.empty:
            tr = total_row.iloc[0]
            data_row_idx = len(rows2) + 1
            cells, color = _rev_row(
                "total", "— Total —", tr["revenue_permw"], tr.get("revenue_permwh"),
            )
            rows2.append(cells)
            if color:
                extra_s2.append(("TEXTCOLOR", (2, data_row_idx), (2, data_row_idx), color))

        tbl2 = _make_table(headers2, rows2, col_w2, extra_styles=extra_s2)
        # Bold the last row (Total)
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

    # System price
    story.append(Paragraph("System Price (£/MWh)", h2_s))
    sp_df = market["system_price"]
    if sp_df.empty or sp_df.iloc[0].isna().all():
        story.append(Paragraph("No system price data.", body_s))
    else:
        r = sp_df.iloc[0]
        sp_rows = [[
            _fmt(r.get("avg"), 2, "£"),
            _fmt(r.get("min"), 2, "£"),
            _fmt(r.get("max"), 2, "£"),
            _fmt(r.get("stddev"), 2, "£"),
        ]]
        story.append(_make_table(
            ["Avg £/MWh", "Min £/MWh", "Max £/MWh", "Std Dev"],
            sp_rows,
            [4.0 * cm] * 4,
        ))

    story.append(Spacer(1, 0.2 * cm))

    # EPEX DA
    story.append(Paragraph("EPEX Day-Ahead Prices (£/MWh)", h2_s))
    epex_df = market["epex"]
    if epex_df.empty or epex_df.iloc[0].isna().all():
        story.append(Paragraph("No EPEX DA data.", body_s))
    else:
        r = epex_df.iloc[0]
        ep_rows = [[
            _fmt(r.get("baseload"), 2, "£"),
            _fmt(r.get("peakload"), 2, "£"),
            _fmt(r.get("offpeak"), 2, "£"),
        ]]
        story.append(_make_table(
            ["Baseload", "Peak", "Off-peak"],
            ep_rows,
            [5.0 * cm, 5.0 * cm, 6.0 * cm],
        ))

    story.append(Spacer(1, 0.2 * cm))

    # NIV
    story.append(Paragraph("Net Imbalance Volume (MWh)", h2_s))
    niv_df = market["niv"]
    if niv_df.empty or pd.isna(niv_df.iloc[0].get("avg_niv")):
        story.append(Paragraph("No NIV data.", body_s))
    else:
        niv_val = niv_df.iloc[0]["avg_niv"]
        story.append(Paragraph(f"Average NIV per settlement period: <b>{_fmt(niv_val, 1)} MWh</b>", body_s))

    story.append(Spacer(1, 0.2 * cm))

    # DX Ancillary
    story.append(Paragraph("DX Ancillary Clearing Prices", h2_s))
    dx_df = market["dx"]
    if dx_df.empty:
        story.append(Paragraph("No DX ancillary data.", body_s))
    else:
        dx_rows = []
        for _, row in dx_df.iterrows():
            dx_rows.append([
                str(row.get("service", "")),
                _fmt(row.get("avg_price"), 2, "£"),
                _fmt(row.get("avg_volume"), 1, suffix=" MW"),
            ])
        story.append(_make_table(
            ["Service", "Avg Clearing Price (£/MW)", "Avg Cleared Volume"],
            dx_rows,
            [6.0 * cm, 5.5 * cm, 4.5 * cm],
        ))

    # ── Section 4: AI Market Analytics ───────────────────────────────────────
    if ai_commentary:
        story.append(Spacer(1, 0.4 * cm))
        story.append(Paragraph("4. Market Analytics", h1_s))
        story.append(Paragraph(
            "AI-generated commentary based on today's market data. "
            "Powered by Claude (Anthropic).",
            caption_s,
        ))
        story.append(Spacer(1, 0.1 * cm))
        # Split into paragraphs and render each
        for para_text in [p.strip() for p in ai_commentary.split("\n\n") if p.strip()]:
            story.append(Paragraph(para_text, body_s))
            story.append(Spacer(1, 0.15 * cm))

    story.append(Spacer(1, 0.6 * cm))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
    story.append(Paragraph(
        "This report is generated automatically by the GB Market Intelligence platform. "
        "Data sourced from Modo Energy, Elexon, and EPEX SPOT.",
        caption_s,
    ))

    doc.build(story)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _get_latest_data_date(conn) -> date:
    """Return the most recent date that has leaderboard data in the DB."""
    df = _query(
        conn,
        "SELECT MAX(settlement_date) AS latest FROM intl_market.gb_bess_leaderboard",
    )
    if df.empty or pd.isna(df.iloc[0]["latest"]):
        return date.today() - timedelta(days=1)
    return pd.Timestamp(df.iloc[0]["latest"]).date()


def generate_report_pdf(report_date: date | None = None) -> bytes:
    """Generate the daily GB market report PDF. Returns PDF bytes."""
    conn = _get_conn()
    try:
        if report_date is None:
            report_date = _get_latest_data_date(conn)

        logger.info("Generating daily report for %s", report_date)
        performers  = _get_top_performers(conn, report_date)
        revenue     = _get_revenue_breakdown(conn, report_date)
        market_data = _get_market_summary(conn, report_date)

        prev_date     = _get_prev_data_date(conn, report_date)
        prev_rankings = _get_all_rankings(conn, prev_date) if prev_date else None
        prev_revenue  = _get_revenue_breakdown(conn, prev_date) if prev_date else None
        logger.info("Prev data date: %s", prev_date)
    finally:
        conn.close()

    ai_commentary = _generate_ai_commentary(
        report_date, performers, revenue, market_data, prev_revenue
    )

    buf = BytesIO()
    _build_pdf(buf, report_date, performers, revenue, market_data,
               prev_rankings=prev_rankings, prev_revenue=prev_revenue,
               ai_commentary=ai_commentary)
    pdf_bytes = buf.getvalue()
    logger.info("PDF generated: %d bytes", len(pdf_bytes))
    return pdf_bytes, ai_commentary


def send_daily_report_email(
    pdf_bytes: bytes,
    report_date: date,
    to_email: str | None = None,
    from_email: str | None = None,
    ai_commentary: str = "",
) -> None:
    """Send the PDF report via SMTP.

    ``to_email`` accepts a single address or a comma-separated list.
    The ``REPORT_TO_EMAIL`` env var likewise accepts comma-separated addresses.
    ``from_email`` overrides the REPORT_FROM_EMAIL env var when provided.
    """
    raw_to     = to_email or os.environ.get("REPORT_TO_EMAIL", _DEFAULT_RECIPIENT)
    to_list    = [e.strip() for e in raw_to.split(",") if e.strip()]
    if not to_list:
        to_list = [_DEFAULT_RECIPIENT]
    smtp_host  = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port  = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user  = os.environ.get("SMTP_USER", "")
    smtp_pass  = os.environ.get("SMTP_PASSWORD", "")
    from_email = from_email or os.environ.get("REPORT_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_pass:
        raise RuntimeError(
            "SMTP credentials not configured. "
            "Set SMTP_USER and SMTP_PASSWORD environment variables."
        )

    subject = f"GB BESS Daily Market Report — {report_date.strftime('%d %b %Y')}"
    filename = f"gb_market_report_{report_date.isoformat()}.pdf"

    msg = MIMEMultipart()
    msg["From"]    = from_email
    msg["To"]      = ", ".join(to_list)
    msg["Subject"] = subject

    body_text = (
        f"Please find attached the GB BESS Daily Market Report for {report_date.strftime('%d %b %Y')}.\n\n"
        "Contents:\n"
        "  1. Top 10 BESS performers (yesterday)\n"
        "  2. Daily average revenue breakdown by market stream\n"
        "  3. Market summary (system price, EPEX DA, NIV, DX ancillary)\n"
        "  4. AI Market Analytics (Claude)\n\n"
    )
    if ai_commentary:
        body_text += "── Market Analytics ──\n\n" + ai_commentary + "\n\n"
    body_text += "Generated by GB Market Intelligence platform."
    msg.attach(MIMEText(body_text, "plain"))

    attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
    attachment.add_header("Content-Disposition", "attachment", filename=filename)
    msg.attach(attachment)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_email, to_list, msg.as_string())

    logger.info("Report emailed to %s", ", ".join(to_list))


def send_daily_report_wecom(
    pdf_bytes: bytes,
    report_date: date,
    webhook_url: str | None = None,
    ai_commentary: str = "",
) -> None:
    """Send the daily report to one or more WeCom groups via bot webhook.

    ``webhook_url`` accepts a single URL or a comma-separated list of URLs.
    Falls back to the ``WECOM_WEBHOOK_URL`` env var (also comma-separated).
    Each webhook receives its own file upload + markdown + file message.
    """
    import re
    import requests

    raw = webhook_url or os.environ.get("WECOM_WEBHOOK_URL", "")
    if not raw:
        raise RuntimeError(
            "WeCom webhook URL not configured. "
            "Set WECOM_WEBHOOK_URL environment variable or pass webhook_url."
        )

    urls = [u.strip() for u in raw.split(",") if u.strip()]
    if not urls:
        raise RuntimeError("No valid WeCom webhook URLs found.")

    filename = f"gb_market_report_{report_date.isoformat()}.pdf"
    header = f"## GB BESS Daily Market Report — {report_date.strftime('%d %b %Y')}\n\n"
    if ai_commentary:
        body = ai_commentary[:3500]
        if len(ai_commentary) > 3500:
            body += "\n\n*(truncated — see attached PDF for full report)*"
    else:
        body = "See attached PDF for top performers, revenue breakdown, and market summary."
    markdown_content = header + body

    errors = []
    for url in urls:
        m = re.search(r"key=([0-9a-f-]+)", url)
        if not m:
            errors.append(f"Could not extract key from URL: {url}")
            continue
        key = m.group(1)
        try:
            # Upload PDF (each webhook needs its own media_id)
            upload_resp = requests.post(
                f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={key}&type=file",
                files={"media": (filename, pdf_bytes, "application/pdf")},
                timeout=30,
            )
            upload_resp.raise_for_status()
            upload_data = upload_resp.json()
            if upload_data.get("errcode", 0) != 0:
                raise RuntimeError(f"Upload failed: {upload_data}")
            media_id = upload_data["media_id"]

            # Markdown summary
            md_resp = requests.post(
                url,
                json={"msgtype": "markdown", "markdown": {"content": markdown_content}},
                timeout=10,
            )
            md_resp.raise_for_status()
            if md_resp.json().get("errcode", 0) != 0:
                logger.warning("[daily_report] WeCom markdown warning (%s): %s", key[:8], md_resp.json())

            # PDF file
            file_resp = requests.post(
                url,
                json={"msgtype": "file", "file": {"media_id": media_id}},
                timeout=10,
            )
            file_resp.raise_for_status()
            if file_resp.json().get("errcode", 0) != 0:
                raise RuntimeError(f"File send failed: {file_resp.json()}")

            logger.info("[daily_report] WeCom report sent for %s (key=...%s)", report_date, key[-8:])
        except Exception as exc:
            logger.error("[daily_report] WeCom send failed for key=...%s: %s", key[-8:], exc)
            errors.append(str(exc))

    if errors and len(errors) == len(urls):
        raise RuntimeError(f"All WeCom sends failed: {errors}")


def run_daily_report(to_email: str | None = None) -> dict:
    """End-to-end: generate PDF and send email. Returns status dict."""
    import time
    t0 = time.time()
    report_date = None
    try:
        # Resolve latest available data date rather than blindly using yesterday.
        _conn = _get_conn()
        try:
            report_date = _get_latest_data_date(_conn)
        finally:
            _conn.close()
        pdf_bytes, ai_commentary = generate_report_pdf(report_date)
        send_daily_report_email(pdf_bytes, report_date, to_email, ai_commentary=ai_commentary)
        return {"status": "success", "date": str(report_date), "size_bytes": len(pdf_bytes),
                "duration": round(time.time() - t0, 1)}
    except Exception as exc:
        logger.error("Daily report failed: %s", exc, exc_info=True)
        return {"status": "error", "date": str(report_date), "error": str(exc),
                "duration": round(time.time() - t0, 1)}
