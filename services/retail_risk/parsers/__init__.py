# services/retail_risk/parsers/__init__.py
"""Province parser registry."""
from services.retail_risk.parsers import (
    trades_anhui, trades_jinan, trades_shandong, trades_zhejiang,
    invoice_anhui, invoice_jinan, invoice_shandong_pdf, invoice_zhejiang,
    mtm_workbook, benchmark_infohub,
)

TRADES_PARSERS = {
    "冀南": trades_jinan.parse_jinan_trades,
    "浙江": trades_zhejiang.parse_zhejiang_trades,
    "山东": trades_shandong.parse_shandong_trades,      # needs ratio_df arg
    "安徽": trades_anhui.parse_anhui_trades,
}

LOAD_PARSERS = {
    "浙江": trades_zhejiang.parse_zhejiang_load,
    "山东": trades_shandong.parse_shandong_load,
}

INVOICE_GLOBS = {   # province -> (glob relative to root, parser)
    "冀南": ("冀南/月结算单/*.pdf", invoice_jinan.parse_jinan_invoice),
    "浙江": ("浙江/月结算单/*.pdf", invoice_zhejiang.parse_zhejiang_invoice),
    "山东": ("山东/2026*/景融/*结算单.pdf", invoice_shandong_pdf.parse_shandong_pdf_invoice),
    "安徽": ("安徽/结算单/*.pdf", invoice_anhui.parse_anhui_invoice),
}
