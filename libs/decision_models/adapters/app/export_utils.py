"""
libs/decision_models/adapters/app/export_utils.py

Shared helpers for generating Excel (.xlsx) and PDF downloads from the
Mengxi BESS dashboard pages.  All functions return bytes so they can be
passed directly to st.download_button().

PDF generation requires reportlab:
    pip install reportlab
If not installed, to_pdf_bytes_* functions return None and callers should
hide the PDF download button.

Excel generation uses openpyxl (ships with pandas) — always available.
"""
from __future__ import annotations

import io
from typing import Dict, List, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Excel
# ---------------------------------------------------------------------------

def to_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    """
    Write one or more DataFrames to a multi-sheet .xlsx workbook.

    Parameters
    ----------
    sheets : dict of {sheet_name: DataFrame}
             Insertion order determines sheet order.
             Sheet names are truncated to 31 chars (Excel limit).

    Returns
    -------
    bytes — raw content of the .xlsx file, ready for st.download_button.
    """
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            safe_name = str(sheet_name)[:31]
            df.to_excel(writer, sheet_name=safe_name, index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# PDF — from markdown string
# ---------------------------------------------------------------------------

def reportlab_available() -> bool:
    try:
        import reportlab  # noqa: F401
        return True
    except ImportError:
        return False


def to_pdf_bytes_from_markdown(title: str, markdown_str: str) -> Optional[bytes]:
    """
    Convert a markdown strategy report string to a structured PDF.

    Handles:
      - # / ## / ### headings
      - Bullet lists (- item)
      - Code blocks (``` ... ```)
      - Markdown tables (| col | col |) rendered as proper reportlab Tables
      - Bold (**text**) and italic (_text_)
      - Horizontal rules (---)

    Returns None if reportlab is not installed.
    """
    if not reportlab_available():
        return None

    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        HRFlowable, Paragraph, Preformatted, SimpleDocTemplate, Spacer,
        Table, TableStyle,
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=20 * mm, rightMargin=20 * mm,
        topMargin=20 * mm, bottomMargin=20 * mm,
    )
    styles = getSampleStyleSheet()
    code_style = ParagraphStyle(
        "sm_code", parent=styles["Code"],
        fontSize=6.5, leading=8.5, fontName="Courier",
        leftIndent=4,
    )
    bullet_style = ParagraphStyle(
        "bullet", parent=styles["Normal"],
        leftIndent=12, firstLineIndent=0,
    )

    story: list = []
    in_code = False
    code_buf: List[str] = []
    table_buf: List[str] = []

    def _flush_code():
        nonlocal code_buf
        if code_buf:
            story.append(Preformatted("\n".join(code_buf), code_style))
            story.append(Spacer(1, 2 * mm))
        code_buf = []

    def _is_md_separator(line: str) -> bool:
        """Detect separator rows like |---|---| or |:---|---:|."""
        cells = line.strip().strip("|").split("|")
        return all(
            cell.strip().replace(":", "").replace("-", "") == ""
            for cell in cells
        )

    def _flush_md_table():
        nonlocal table_buf
        if not table_buf:
            return
        rows: List[List[str]] = []
        for tl in table_buf:
            if _is_md_separator(tl):
                continue
            cells = [c.strip() for c in tl.strip().strip("|").split("|")]
            rows.append(cells)
        table_buf = []
        if not rows:
            return
        # Pad all rows to the same column count
        n_cols = max(len(r) for r in rows)
        for r in rows:
            while len(r) < n_cols:
                r.append("")
        tbl = Table(rows, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#3a3a3a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 7),
            ("LEADING", (0, 0), (-1, -1), 9),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1),
             [colors.white, colors.HexColor("#f7f7f7")]),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 3 * mm))

    for raw in markdown_str.splitlines():
        line = raw.rstrip()

        if line.startswith("```"):
            _flush_md_table()
            if in_code:
                _flush_code()
                in_code = False
            else:
                in_code = True
            continue

        if in_code:
            code_buf.append(line)
            continue

        # Collect markdown table lines
        if line.startswith("| ") or (line.startswith("|") and "|" in line[1:]):
            table_buf.append(line)
            continue

        # Non-table line: flush any buffered table first
        _flush_md_table()

        if line.startswith("# "):
            story.append(Paragraph(_esc(line[2:]), styles["Heading1"]))
        elif line.startswith("## "):
            story.append(HRFlowable(width="100%", thickness=0.4, color=colors.grey))
            story.append(Paragraph(_esc(line[3:]), styles["Heading2"]))
        elif line.startswith("### "):
            story.append(Paragraph(_esc(line[4:]), styles["Heading3"]))
        elif line.startswith("- "):
            content = _inline(line[2:])
            story.append(Paragraph(f"&bull;&nbsp;{content}", bullet_style))
        elif line.strip() in ("", "---", "---  "):
            story.append(Spacer(1, 3 * mm))
        else:
            story.append(Paragraph(_inline(line) or "&nbsp;", styles["Normal"]))

    _flush_md_table()
    _flush_code()
    doc.build(story)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# PDF — from a list of titled DataFrames (tables-only report)
# ---------------------------------------------------------------------------

def to_pdf_bytes_from_tables(
    title: str,
    sections: List[Dict],
    landscape: bool = False,
) -> Optional[bytes]:
    """
    Build a simple PDF from a list of titled tables.

    Parameters
    ----------
    title     : Document title shown at the top.
    sections  : list of dicts, each with:
                  "heading" (str, optional) — section heading
                  "df"      (pd.DataFrame)  — table data
    landscape : if True, use A4 landscape orientation (wide tables).

    Returns None if reportlab is not installed.
    """
    if not reportlab_available():
        return None

    import math
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.pagesizes import landscape as rlab_landscape
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
    )

    pagesize = rlab_landscape(A4) if landscape else A4
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=pagesize,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=15 * mm, bottomMargin=15 * mm,
    )
    styles = getSampleStyleSheet()
    story: list = [
        Paragraph(title, styles["Heading1"]),
        Spacer(1, 4 * mm),
    ]

    def _fmt_cell(v) -> str:
        """Format cell values: numbers with thousand separators, others as str."""
        if v is None:
            return ""
        if isinstance(v, float):
            import math as _math
            if _math.isnan(v):
                return "—"
            if v == round(v, 0):
                return f"{v:,.0f}"
            return f"{v:,.2f}"
        if isinstance(v, int):
            return f"{v:,}"
        return str(v)

    for sec in sections:
        heading = sec.get("heading", "")
        df = sec.get("df")
        if heading:
            story.append(Paragraph(heading, styles["Heading2"]))
            story.append(Spacer(1, 2 * mm))
        if df is not None and not df.empty:
            headers = list(df.columns)
            tdata = [headers]
            for _, row in df.iterrows():
                tdata.append([_fmt_cell(v) for v in row])
            tbl = Table(tdata, repeatRows=1, hAlign="LEFT")
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#3a3a3a")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1),
                 [colors.white, colors.HexColor("#f7f7f7")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 3),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                ("TOPPADDING", (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ]))
            story.append(tbl)
            story.append(Spacer(1, 5 * mm))

    doc.build(story)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    """Escape XML special chars for reportlab Paragraph."""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
    )


def _inline(text: str) -> str:
    """Convert simple inline markdown (bold, italic) to reportlab XML."""
    import re
    text = _esc(text)
    # **bold**
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    # _italic_  (not inside words)
    text = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"<i>\1</i>", text)
    # `code`
    text = re.sub(r"`(.+?)`", r"<font name='Courier'>\1</font>", text)
    return text
