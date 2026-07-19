"""AI-based classifier: maps a filename to an OneDrive market-fundamentals subfolder.

Handles both Chinese provincial markets and international markets (GB, AU, ERCOT, etc.).
Auto-classify is now the default for all document files — no manual trigger needed.
"""
from __future__ import annotations

import logging
from typing import Optional
from shared.anthropic_client import make_client as _make_anthropic_client

logger = logging.getLogger(__name__)

_BASE_CN  = "etrm/bess-platform/data/market-fundamentals"
_BASE_INTL = "etrm/bess-platform/data/intl-markets"

# Document extensions that should be auto-classified
_DOC_EXTENSIONS = {
    "xlsx", "xls", "xlsm", "pdf", "docx", "doc",
    "pptx", "ppt", "csv", "txt", "md",
}

# ── Chinese province folders ──────────────────────────────────────────────────
_CN_FOLDER_MAP = """
【0全国】      → national / cross-province (no specific province, or mentions multiple provinces)
【0.区域级】   → regional grid (华北, 华东, 南方, 西南, etc.)
【1.北京】     → 北京 / Beijing
【2.天津】     → 天津 / Tianjin
【3.1冀北】    → 冀北 / Hebei-North
【3.2冀南】    → 冀南 / Hebei-South / 河北南
【3.河北】     → 河北 / Hebei (general)
【4.山西】     → 山西 / Shanxi
【5.1蒙西】    → 蒙西 / 内蒙古西 / Mengxi
【5.2蒙东】    → 蒙东 / 内蒙古东 / Mengdong
【6.辽宁】     → 辽宁 / Liaoning
【7.吉林】     → 吉林 / Jilin
【8.黑龙江】   → 黑龙江 / Heilongjiang
【10.江苏】    → 江苏 / Jiangsu
【11.浙江】    → 浙江 / Zhejiang
【12.安徽】    → 安徽 / Anhui
【13.福建】    → 福建 / Fujian
【14.江西】    → 江西 / Jiangxi
【15.山东】    → 山东 / Shandong
【16.河南】    → 河南 / Henan
【17.湖北】    → 湖北 / Hubei
【18.湖南】    → 湖南 / Hunan
【19.广东】    → 广东 / Guangdong
【20.广西】    → 广西 / Guangxi
【21.海南】    → 海南 / Hainan
【22.重庆】    → 重庆 / Chongqing
【23.四川】    → 四川 / Sichuan
【24.贵州】    → 贵州 / Guizhou
【25.云南】    → 云南 / Yunnan
【27.陕西】    → 陕西 / Shaanxi
【28.甘肃】    → 甘肃 / Gansu
【29.青海】    → 青海 / Qinghai
【30.宁夏】    → 宁夏 / Ningxia
【31.新疆】    → 新疆 / Xinjiang
【各省份装机数据】 → multi-province installed capacity data (装机, 容量, capacity)
【政策研究月报】   → monthly cross-market policy research reports
"""

_CN_SUBCAT_MAP = """
1-政策      → policy, market rules, regulations, notices, 通知, 规则, 政策, 意见稿, 细则
1-信息披露  → market disclosure, operational reports, 信息披露, 运行情况, 月报 (province-level)
"""

# ── International market folders ──────────────────────────────────────────────
_INTL_FOLDER_MAP = """
gb         → GB / Great Britain / UK / 英国 / National Grid ESO / Balancing Mechanism / CfD / capacity market
au         → AU / Australia / NEM / AEMO / 澳大利亚 / Queensland / Victoria / NSW / South Australia
ercot      → ERCOT / Texas / 德克萨斯
caiso      → CAISO / California / 加州
pjm        → PJM
ph         → Philippines / 菲律宾 / WESM
po         → Portugal / Iberia / 葡萄牙 / MIBEL / ENTSO-E
"""

_INTL_SUBCAT_MAP = """
data       → price data, capacity data, market results, settlement
reports    → market reports, analysis, research
policy     → regulations, rules, market rules, consultation papers
"""

_CLASSIFY_PROMPT = f"""You are a file routing assistant for an electricity market investment team.

Given a filename and optional hint, determine:
1. Is this file related to electricity/power markets (China or international)?
   - If NO → respond with exactly: NOT_MARKET
2. Which market does it belong to?
   - Chinese provincial market → use base path: {_BASE_CN}
   - International market → use base path: {_BASE_INTL}

Chinese province folder options:
{_CN_FOLDER_MAP}

Chinese subcategory folders (append after province folder):
{_CN_SUBCAT_MAP}

International market folder options (one folder name = market code):
{_INTL_FOLDER_MAP}

International subcategory folders (append after market code):
{_INTL_SUBCAT_MAP}

Rules:
- Output ONLY the full folder path — no explanation, no markdown.
- If the file mentions a specific Chinese province, use that province's numbered folder.
- For multi-province capacity files (装机/容量), use 【各省份装机数据】 (no subfolder).
- For cross-province monthly policy reports (月报), use 【政策研究月报】 (no subfolder).
- For international, pick the market code folder, then data/reports/policy subfolder.
- If clearly NOT electricity/power market related (personal files, photos, unrelated docs) → NOT_MARKET

Examples:
  山东现货市场运行情况2026年5月.pdf → {_BASE_CN}/【15.山东】/1-信息披露
  广东调频辅助服务市场实施细则.pdf  → {_BASE_CN}/【19.广东】/1-政策
  各省储能装机容量分析.xlsx          → {_BASE_CN}/【各省份装机数据】
  GB_BESS_Revenue_2026.xlsx         → {_BASE_INTL}/gb/data
  ERCOT_Market_Report_Q1_2026.pdf   → {_BASE_INTL}/ercot/reports
  AEMO_Quarterly_Energy_Dynamics.pdf → {_BASE_INTL}/au/reports
  meeting_notes_personal.docx       → NOT_MARKET
"""


def classify_to_market_fundamentals(
    filename: str,
    hint: str,
    api_key: str,
) -> Optional[str]:
    """Use Claude to pick the right OneDrive folder for a market file.

    Returns:
        folder path string — e.g. "etrm/bess-platform/data/market-fundamentals/【15.山东】/1-政策"
        None — if the file is not market-related (NOT_MARKET)
    Falls back to base CN path on error.
    """
    client = _make_anthropic_client(api_key)
    user_content = f"Filename: {filename}"
    if hint:
        user_content += f"\nUser hint: {hint}"

    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=160,
            system=_CLASSIFY_PROMPT,
            messages=[{"role": "user", "content": user_content}],
        )
        result = resp.content[0].text.strip().strip("/")
        if result == "NOT_MARKET":
            logger.info("market_classifier: '%s' → NOT_MARKET (skipping auto-route)", filename)
            return None
        logger.info("market_classifier: '%s' → '%s'", filename, result)
        return result
    except Exception as exc:
        logger.error("market_classifier: classification failed: %s", exc)
        return f"{_BASE_CN}/【0全国】/2-政策"


def is_document_file(filename: str) -> bool:
    """Return True if the file extension is a classifiable document type."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return ext in _DOC_EXTENSIONS
