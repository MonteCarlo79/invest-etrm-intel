# services/deal_committee/daf_builder.py
"""Build the DAF PDF (A4, Chinese, reportlab) from a CommitteeResult."""
from __future__ import annotations

import io
import re
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (
    Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

from services.deal_committee.charts import (
    chart_irr_distribution, chart_monthly_price, chart_revenue_distribution,
)
from services.deal_committee.orchestrator import CommitteeResult

_ACCENT = colors.HexColor("#1f4e79")
_GREY = colors.HexColor("#444444")


def split_synthesis(synthesis_md: str) -> dict[str, str]:
    """Split the synthesis markdown into its three mandated sections."""
    parts = {"交易摘要": "", "风险分析": "", "投资建议": ""}
    current = None
    for line in (synthesis_md or "").splitlines():
        m = re.match(r"^##\s*(交易摘要|风险分析|投资建议)\s*$", line.strip())
        if m:
            current = m.group(1)
            continue
        if current:
            parts[current] += line + "\n"
    return {k: v.strip() for k, v in parts.items()}


def _register_font() -> str:
    from services.hermes.export_utils import _register_cjk_font
    return _register_cjk_font()


def _styles(font: str) -> dict:
    ss = getSampleStyleSheet()
    return {
        "title": ParagraphStyle("daf_title", parent=ss["Title"], fontName=font,
                                fontSize=18, textColor=_ACCENT, spaceAfter=4),
        "h1": ParagraphStyle("daf_h1", parent=ss["Heading1"], fontName=font,
                             fontSize=13, textColor=_ACCENT, spaceBefore=12, spaceAfter=4),
        "body": ParagraphStyle("daf_body", parent=ss["Normal"], fontName=font,
                               fontSize=9.5, leading=14),
        "cell": ParagraphStyle("daf_cell", parent=ss["Normal"], fontName=font,
                               fontSize=8.5, leading=11),
        "caption": ParagraphStyle("daf_cap", parent=ss["Normal"], fontName=font,
                                  fontSize=8, textColor=_GREY, spaceAfter=6),
    }


def _esc(text: str) -> str:
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _md_inline(text: str) -> str:
    return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", _esc(text))


def _md_to_flowables(md: str, styles: dict) -> list:
    """Minimal markdown → flowables: headings, bullets, pipe tables, paragraphs."""
    flow, lines, i = [], (md or "").splitlines(), 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line.strip():
            i += 1
            continue
        if line.lstrip().startswith("|"):  # pipe table block
            rows = []
            while i < len(lines) and lines[i].lstrip().startswith("|"):
                cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                if not all(set(c) <= set(":- ") for c in cells):  # skip separator row
                    rows.append([Paragraph(_md_inline(c), styles["cell"]) for c in cells])
                i += 1
            if rows:
                ncol = max(len(r) for r in rows)
                rows = [r + [Paragraph("", styles["cell"])] * (ncol - len(r)) for r in rows]
                t = Table(rows, hAlign="LEFT")
                t.setStyle(TableStyle([
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#999999")),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8eef5")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ]))
                flow += [t, Spacer(1, 6)]
            continue
        if line.startswith("### "):
            flow.append(Paragraph(_md_inline(line[4:]), styles["h1"]))
        elif line.startswith("## "):
            flow.append(Paragraph(_md_inline(line[3:]), styles["h1"]))
        elif line.lstrip().startswith(("- ", "• ")):
            flow.append(Paragraph("• " + _md_inline(line.lstrip()[2:]), styles["body"]))
        else:
            flow.append(Paragraph(_md_inline(line), styles["body"]))
        i += 1
    return flow


def _png_flowable(png: bytes, width_cm: float = 15.5) -> Image:
    ir = ImageReader(io.BytesIO(png))
    w, h = ir.getSize()
    width = width_cm * cm
    return Image(io.BytesIO(png), width=width, height=width * h / w)


def _kv_table(pairs: list[tuple[str, str]], styles: dict) -> Table:
    rows = [[Paragraph(f"<b>{_esc(k)}</b>", styles["cell"]), Paragraph(_esc(v), styles["cell"])]
            for k, v in pairs]
    t = Table(rows, colWidths=[4.2 * cm, 11.8 * cm], hAlign="LEFT")
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#999999")),
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#e8eef5")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


def build_daf(result: CommitteeResult) -> bytes:
    font = _register_font()
    styles = _styles(font)
    brief = result.brief
    sections = {s.key: s for s in result.sections}
    syn = split_synthesis(result.synthesis)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm,
                            topMargin=2 * cm, bottomMargin=2 * cm,
                            title=f"DAF - {brief.deal_name or 'untitled'}")
    flow = [
        Paragraph("投资决策建议书 (Deal Advice Form)", styles["title"]),
        Paragraph(f"{brief.deal_name or '未命名项目'} · 生成时间 {datetime.now():%Y-%m-%d %H:%M}",
                  styles["caption"]),
        Spacer(1, 6),
    ]

    # 1. 交易概要表
    flow.append(Paragraph("一、交易概要", styles["h1"]))
    capex = f"¥{brief.capex_total_yuan/1e8:.2f} 亿" if brief.capex_total_yuan else "—"
    flow.append(_kv_table([
        ("项目名称", brief.deal_name or "—"),
        ("资产类型", brief.asset_type),
        ("省份 / 节点", f"{brief.province or '—'} / {brief.node or '—'}"),
        ("规模", f"{brief.capacity_mw:g} MW / {brief.capacity_mwh:g} MWh(储能)· "
                 f"{brief.installed_mw:g} MW(新能源装机)"),
        ("总投资", capex),
        ("期限 / 投运", f"{brief.tenor_years} 年 / {brief.commissioning_year}"),
        ("对手方", brief.counterparty or "—"),
        ("交易结构", brief.structure_notes or "—"),
        ("融资", f"负债率 {brief.debt_ratio:.0%} · 利率 {brief.loan_rate:.1%} · "
                 f"{brief.loan_term_years} 年"),
        ("投资结论", result.recommendation or "(未生成)"),
    ], styles))
    flow.append(Spacer(1, 6))

    # 2. 交易摘要
    flow.append(Paragraph("二、交易摘要", styles["h1"]))
    flow += _md_to_flowables(syn["交易摘要"] or "(未生成综合意见)", styles)

    # 3-6. section markdowns in fixed order
    section_headings = [
        ("market_background", "三、市场背景"),
        ("policy", "四、政策与规则环境"),
        ("economics", "五、经济性分析"),
        ("ops_mengxi", "六、运营实证"),
        ("ops_asset_risk", None),      # appended under 六
        ("ops_retail_risk", None),     # appended under 六
        ("risk", "七、风险数据基准"),
    ]
    for key, heading in section_headings:
        if heading:
            flow.append(Paragraph(heading, styles["h1"]))
        sec = sections.get(key)
        if sec is None:
            continue
        if sec.status != "ok":
            flow.append(Paragraph(f"〔本节数据缺失:{_esc(sec.error)}〕", styles["body"]))
            continue
        flow += _md_to_flowables(sec.markdown, styles)
        if key == "market_background" and result.economics and result.economics.monthly_price:
            flow.append(_png_flowable(chart_monthly_price(result.economics.monthly_price)))
        if key == "economics" and result.economics:
            flow.append(_png_flowable(chart_revenue_distribution(result.economics.mc.revenue_paths)))
            flow.append(_png_flowable(chart_irr_distribution(result.economics.mc.equity_irr_paths)))

    # 7. 风险分析 + 8. 投资建议
    flow.append(Paragraph("八、风险分析", styles["h1"]))
    flow += _md_to_flowables(syn["风险分析"] or "(未生成)", styles)
    flow.append(Paragraph("九、投资建议", styles["h1"]))
    flow += _md_to_flowables(syn["投资建议"] or "(未生成)", styles)

    # 10. 附录
    flow.append(Paragraph("十、附录", styles["h1"]))
    sources = "、".join(brief.source_files) or "手工录入"
    flow += _md_to_flowables(
        f"- 输入材料:{sources}\n"
        "- 数据来源:spot / bess-map / mengxi / asset-risk / retail-risk 无头代理,"
        "marketdata.rm_* 台账,libs/deal_models 经济性引擎(OU 模型)\n"
        f"- 生成时间:{datetime.now():%Y-%m-%d %H:%M} · 模型:claude-sonnet-4-6",
        styles)

    doc.build(flow)
    return buf.getvalue()
