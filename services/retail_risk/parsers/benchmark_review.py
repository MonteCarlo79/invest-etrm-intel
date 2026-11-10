# services/retail_risk/parsers/benchmark_review.py
"""全网 market benchmarks for trader alpha, from sources that exist for 2026
(the 信息汇总 workbooks stop at 2024/25):

  山东  信息披露月报 PDF 表2 (一)中长期累计交易情况 — product, 亿kWh, 加权平均电价.
        source='disclosure'.
  浙江  复盘 deck table 月份|年度长协|月度竞价|月度双边|上/中/下旬竞价|日前|实时 —
        monthly clearing prices; '（25）' rows are 2025; later decks win.
        source='review'.
  冀南  复盘 deck hourly table (24 rows) with a 全网/全市场 block (年度/月度/月内).
        Month from the slide title 'N月用户结算联动电价' (headers are copy-pasted
        and mislabelled). Collapsed to monthly by weighting each hour with OUR
        buy volume — then (benchmark - our buy VWAP) x volume equals the exact
        hour-matched alpha Σ_h v_h (mkt_h - p_h). source='review_hourly'.
"""
from __future__ import annotations

import datetime
import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_YI_KWH_TO_MWH = 100_000.0          # 1 亿kWh = 1e8 kWh = 1e5 MWh

# --- 山东 披露月报 -----------------------------------------------------------
_SD_LINE = re.compile(r"^(\S+?交易)\s+(-|[\d.]+)\s+(-|[\d.]+)\s*$")
_SD_ALIAS = {"连续撮合交易": "滚动撮合交易"}
_SD_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月份山东电力市场信息披露月报")


def parse_shandong_table2(text: str, month: datetime.date, source_file: str) -> pd.DataFrame:
    rows = []
    start = text.find("中长期累计交易情况")
    if start < 0:
        return pd.DataFrame(columns=schemas.BENCH_COLS)
    for line in text[start:].splitlines()[1:]:
        if line.startswith("（二）"):
            break
        m = _SD_LINE.match(line.strip())
        if not m or m.group(2) == "-" or m.group(3) == "-":
            continue
        rows.append(["山东", _SD_ALIAS.get(m.group(1), m.group(1)), month.isoformat(),
                     float(m.group(3)), float(m.group(2)) * _YI_KWH_TO_MWH,
                     "disclosure", source_file])
    return pd.DataFrame(rows, columns=schemas.BENCH_COLS)


def shandong_disclosure_files(root: str | Path) -> list[Path]:
    return sorted((Path(root) / "山东").glob("2026*/*信息披露月报*.pdf"))


def parse_shandong_disclosure(path: str | Path) -> pd.DataFrame:
    import pdfplumber
    path = Path(path)
    m = _SD_MONTH_RE.search(path.name)
    if not m:
        return pd.DataFrame(columns=schemas.BENCH_COLS)
    month = datetime.date(int(m.group(1)), int(m.group(2)), 1)
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages[:8]:
            # the STSong-Light watermark interleaves glyphs into table lines
            clean = page.filter(lambda o: o.get("object_type") != "char"
                                or "STSong" not in str(o.get("fontname", "")))
            text = clean.extract_text() or ""
            if "中长期累计交易情况" in text:
                return parse_shandong_table2(text, month, path.name)
    return pd.DataFrame(columns=schemas.BENCH_COLS)


# --- pptx helpers ------------------------------------------------------------
def _walk(shapes):
    for sh in shapes:
        if sh.shape_type == 6:                       # group
            yield from _walk(sh.shapes)
        else:
            yield sh


def _slides(path: Path):
    """-> [(slide_text, [table_rows])] with cell text stripped."""
    from pptx import Presentation
    out = []
    for slide in Presentation(str(path)).slides:
        texts, tables = [], []
        for sh in _walk(slide.shapes):
            if sh.has_text_frame:
                texts.append(sh.text_frame.text)
            if getattr(sh, "has_table", False) and sh.has_table:
                tables.append([[c.text.strip().replace("\n", "") for c in r.cells]
                               for r in sh.table.rows])
        out.append((" ".join(texts), tables))
    return out


def _num(v) -> float | None:
    try:
        return float(str(v).replace(",", ""))
    except ValueError:
        return None


# --- 浙江 复盘 ---------------------------------------------------------------
_ZJ_CHANNELS = ["年度长协", "月度竞价", "月度双边", "上旬竞价", "中旬竞价", "下旬竞价"]
_DECK_YEAR_RE = re.compile(r"(20\d{2})年")


def zhejiang_review_files(root: str | Path) -> list[Path]:
    return sorted((Path(root) / "浙江" / "月度复盘").glob("*.pptx"))


def parse_zhejiang_review(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    ym = _DECK_YEAR_RE.search(path.name)
    year = int(ym.group(1)) if ym else 2026
    rows = []
    for _, tables in _slides(path):
        for t in tables:
            hdr = t[0]
            if not hdr or hdr[0] != "月份" or "月度竞价" not in hdr:
                continue
            cols = {c: hdr.index(c) for c in _ZJ_CHANNELS if c in hdr}
            for r in t[1:]:
                m = re.match(r"(\d{1,2})月(（(\d{2})）)?", r[0])
                if not m:
                    continue
                y = 2000 + int(m.group(3)) if m.group(3) else year
                for ch, j in cols.items():
                    px = _num(r[j])
                    if px:
                        rows.append(["浙江", ch, f"{y}-{int(m.group(1)):02d}-01", px, None,
                                     "review", path.name])
    return pd.DataFrame(rows, columns=schemas.BENCH_COLS)


# --- 冀南 复盘 (hourly) ------------------------------------------------------
_JN_TITLE_RE = re.compile(r"(\d{1,2})月用户结算联动电价")
_JN_HDR_MONTH_RE = re.compile(r"(\d{1,2})月")
_JN_KINDS = [("月内", "月内合同"), ("月度", "月度合同"), ("年度", "年度合同")]   # 月内 before 年度
HOURLY_COLS = ["province", "channel", "month", "hour", "price", "source_file"]


def jinan_review_files(root: str | Path) -> list[Path]:
    return sorted((Path(root) / "冀南" / "复盘ppt").glob("*.pptx"))


def _hour_cols(t) -> list[int]:
    """Columns whose 24 data cells are exactly 1..24."""
    out = []
    for j in range(len(t[0])):
        vals = [_num(r[j]) for r in t[1:25]]
        if vals == [float(h) for h in range(1, 25)]:
            out.append(j)
    return out


def parse_jinan_review_hourly(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    ym = _DECK_YEAR_RE.search(path.name)
    year = int(ym.group(1)) if ym else 2026
    rows = []
    for text, tables in _slides(path):
        for t in tables:
            if len(t) < 25:
                continue
            hc = _hour_cols(t)
            if not hc:
                continue
            hdr = t[0]
            mt = _JN_TITLE_RE.search(text) or _JN_HDR_MONTH_RE.search(" ".join(hdr))
            if not mt:
                continue
            month = f"{year}-{int(mt.group(1)):02d}-01"
            bounds = hc + [len(hdr)]
            for b0, b1 in zip(bounds, bounds[1:]):
                block = hdr[b0:b1]
                if not any("全网" in h or "全市场" in h for h in block):
                    continue
                for j in range(b0 + 1, b1):
                    kind = next((k for key, k in _JN_KINDS if key in hdr[j]), None)
                    if kind is None or "现货" in hdr[j] or "传导" in hdr[j]:
                        continue
                    for r in t[1:25]:
                        px = _num(r[j])
                        if px is not None:
                            rows.append(["冀南", kind, month, int(_num(r[b0])) - 1, px, path.name])
    out = pd.DataFrame(rows, columns=HOURLY_COLS)
    return out.drop_duplicates(["channel", "month", "hour"], keep="last")


def benchmark_from_hourly(hourly: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    """Hourly 全网 prices -> monthly BENCH rows, weighted by our hourly BUY volume
    in the channels the benchmark maps to; flat mean when we bought nothing."""
    if hourly.empty:
        return pd.DataFrame(columns=schemas.BENCH_COLS)
    buys = trades[trades["direction"] == "buy"].copy() if not trades.empty else trades
    if not buys.empty:
        buys["month"] = pd.to_datetime(buys["delivery_date"]).dt.strftime("%Y-%m-01")
    rows = []
    for (prov, ch, month), g in hourly.groupby(["province", "channel", "month"]):
        ours = schemas.BENCH_CHANNEL_MAP.get(ch, [])
        w = (buys[(buys["month"] == month) & buys["channel"].isin(ours)]
             .groupby("hour")["volume_mwh"].sum() if not buys.empty else pd.Series(dtype=float))
        g = g.set_index("hour")["price"]
        w = w.reindex(g.index).fillna(0.0)
        px = float((g * w).sum() / w.sum()) if w.sum() > 0 else float(g.mean())
        rows.append([prov, ch, month, px, None, "review_hourly",
                     hourly["source_file"].iloc[0]])
    return pd.DataFrame(rows, columns=schemas.BENCH_COLS)
