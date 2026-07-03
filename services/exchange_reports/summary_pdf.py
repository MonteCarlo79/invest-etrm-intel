"""
services/exchange_reports/summary_pdf.py

Generate a multi-province exchange monthly report summary PDF.

Usage:
    from services.exchange_reports.summary_pdf import build_summary_pdf
    pdf_bytes = build_summary_pdf(metrics_rows, month_label="2026年3月")
"""
from __future__ import annotations

import io
from datetime import date
from typing import Optional

# ── Column definitions ────────────────────────────────────────────────────────

# (db_col, display_zh, display_en, unit, fmt)
_COLUMNS = [
    ("total_volume_gwh",            "总成交量",         "Total Volume",       "亿kWh",   "{:.1f}"),
    ("volume_yoy_pct",              "同比",             "YoY",                "%",       "{:+.1f}%"),
    ("avg_price_yuan_mwh",          "均价",             "Avg Price",          "元/MWh",  "{:.1f}"),
    ("peak_price_yuan_mwh",         "峰段价",           "Peak",               "元/MWh",  "{:.1f}"),
    ("valley_price_yuan_mwh",       "谷段价",           "Valley",             "元/MWh",  "{:.1f}"),
    ("spot_volume_gwh",             "现货量",           "Spot Vol",           "亿kWh",   "{:.1f}"),
    ("spot_avg_price_yuan_mwh",     "现货均价",         "Spot Price",         "元/MWh",  "{:.1f}"),
    ("renewable_pct",               "新能源占比",       "Renewable%",         "%",       "{:.1f}%"),
    ("installed_capacity_gw",       "装机容量",         "Capacity",           "GW",      "{:.1f}"),
    ("max_load_gw",                 "最大负荷",         "Max Load",           "GW",      "{:.1f}"),
    ("market_participants_total",   "市场主体",         "Participants",       "户",      "{:,}"),
]

_NA = "—"


def _fmt(val, fmt: str) -> str:
    if val is None:
        return _NA
    try:
        return fmt.format(float(val))
    except (TypeError, ValueError):
        return _NA


# ── PDF builder ───────────────────────────────────────────────────────────────

def build_summary_pdf(
    metrics_rows: list[dict],
    month_label: str = "",
    title: str = "省级电力交易月报数据汇总",
) -> bytes:
    """
    Build a ReportLab PDF summary table from a list of metrics dicts.

    Returns PDF bytes.
    """
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        SimpleDocTemplate, Table, TableStyle, Paragraph,
        Spacer, HRFlowable,
    )
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.cidfonts import UnicodeCIDFont

    # Register CJK font
    try:
        pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
        _font = "STSong-Light"
    except Exception:
        _font = "Helvetica"

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(A4),
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=14 * mm,
        bottomMargin=14 * mm,
        title=title,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ExchangeTitle",
        parent=styles["Normal"],
        fontName=_font,
        fontSize=14,
        leading=18,
        spaceAfter=4,
    )
    sub_style = ParagraphStyle(
        "ExchangeSub",
        parent=styles["Normal"],
        fontName=_font,
        fontSize=9,
        textColor=colors.HexColor("#666666"),
        spaceAfter=8,
    )
    cell_style = ParagraphStyle(
        "Cell",
        parent=styles["Normal"],
        fontName=_font,
        fontSize=8,
        leading=11,
    )
    highlight_style = ParagraphStyle(
        "Highlight",
        parent=styles["Normal"],
        fontName=_font,
        fontSize=7.5,
        leading=11,
        textColor=colors.HexColor("#333333"),
    )

    _navy = colors.HexColor("#1a3a5c")
    _light_blue = colors.HexColor("#e8f0f8")
    _alt_row = colors.HexColor("#f5f8fc")
    _border = colors.HexColor("#c0cfe0")

    story = []

    # Title block
    story.append(Paragraph(title, title_style))
    sub_text = month_label if month_label else ""
    if sub_text:
        sub_text += "  |  "
    sub_text += f"共 {len(metrics_rows)} 个省份"
    story.append(Paragraph(sub_text, sub_style))
    story.append(HRFlowable(width="100%", thickness=1, color=_navy))
    story.append(Spacer(1, 4 * mm))

    # ── Main metrics table ────────────────────────────────────────────────────
    # Header row: province + each metric (ZH name + unit)
    header_zh = ["省份"]
    header_en = ["Province"]
    for _, zh, en, unit, _ in _COLUMNS:
        header_zh.append(zh)
        header_en.append(unit)

    data = [header_zh, header_en]
    for row in sorted(metrics_rows, key=lambda r: r.get("province", "")):
        cells = [row.get("province", "")]
        for col, _, _, _, fmt in _COLUMNS:
            cells.append(_fmt(row.get(col), fmt))
        data.append(cells)

    # Column widths: province wider, others equal
    page_w = landscape(A4)[0] - 24 * mm
    prov_w = 22 * mm
    remaining = page_w - prov_w
    col_w = remaining / len(_COLUMNS)
    col_widths = [prov_w] + [col_w] * len(_COLUMNS)

    tbl = Table(data, colWidths=col_widths, repeatRows=2)
    ts = TableStyle([
        # Header rows
        ("BACKGROUND", (0, 0), (-1, 0), _navy),
        ("TEXTCOLOR",  (0, 0), (-1, 0), colors.white),
        ("FONTNAME",   (0, 0), (-1, 0), _font),
        ("FONTSIZE",   (0, 0), (-1, 0), 8),
        ("ALIGN",      (0, 0), (-1, 0), "CENTER"),
        ("VALIGN",     (0, 0), (-1, 0), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, 1), [_light_blue]),
        ("FONTNAME",   (0, 1), (-1, 1), _font),
        ("FONTSIZE",   (0, 1), (-1, 1), 7),
        ("TEXTCOLOR",  (0, 1), (-1, 1), colors.HexColor("#555555")),
        ("ALIGN",      (0, 1), (-1, 1), "CENTER"),
        # Data rows
        ("FONTNAME",   (0, 2), (-1, -1), _font),
        ("FONTSIZE",   (0, 2), (-1, -1), 8),
        ("ALIGN",      (1, 2), (-1, -1), "RIGHT"),
        ("ALIGN",      (0, 2), (0, -1), "LEFT"),
        ("VALIGN",     (0, 2), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 2), (-1, -1), [colors.white, _alt_row]),
        # Province column bold
        ("FONTNAME",   (0, 2), (0, -1), _font),
        # Grid
        ("GRID",       (0, 0), (-1, -1), 0.3, _border),
        ("LINEBELOW",  (0, 1), (-1, 1), 1, _navy),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING",  (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
    ])
    tbl.setStyle(ts)
    story.append(tbl)
    story.append(Spacer(1, 6 * mm))

    # ── Highlights section ────────────────────────────────────────────────────
    highlights = [
        (r["province"], r["key_highlights"])
        for r in sorted(metrics_rows, key=lambda r: r.get("province", ""))
        if r.get("key_highlights")
    ]
    if highlights:
        story.append(HRFlowable(width="100%", thickness=0.5, color=_border))
        story.append(Spacer(1, 3 * mm))
        story.append(Paragraph("各省市场要点", title_style))
        story.append(Spacer(1, 2 * mm))

        hi_data = []
        for prov, hl in highlights:
            hi_data.append([
                Paragraph(prov, cell_style),
                Paragraph(hl, highlight_style),
            ])

        hi_tbl = Table(hi_data, colWidths=[22 * mm, page_w - 22 * mm])
        hi_tbl.setStyle(TableStyle([
            ("FONTNAME",  (0, 0), (-1, -1), _font),
            ("FONTSIZE",  (0, 0), (-1, -1), 8),
            ("VALIGN",    (0, 0), (-1, -1), "TOP"),
            ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, _alt_row]),
            ("GRID",      (0, 0), (-1, -1), 0.3, _border),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING",  (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(hi_tbl)

    doc.build(story)
    return buf.getvalue()
