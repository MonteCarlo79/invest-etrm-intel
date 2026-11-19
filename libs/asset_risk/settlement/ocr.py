"""OCR text for scanned settlement PDFs, laid out as reading-order lines.

Render with PyMuPDF at 200 dpi, recognise with RapidOCR (onnxruntime; no system
binaries), then rebuild rows: boxes whose vertical centres fall within ~0.6 of a
median box height form one line, ordered left→right. That restores bill rows such as
"（二）系统运行费用 623071.17" that the deterministic parsers expect.

Lazy imports: returns "" when the OCR stack is unavailable, so callers fall back.
"""
from __future__ import annotations

import functools
import os
import statistics


@functools.lru_cache(maxsize=16)
def _ocr_cached(path: str, mtime_ns: int, size: int, dpi: int) -> str:
    try:
        try:
            import pymupdf as fitz
        except ImportError:
            import fitz
        from rapidocr_onnxruntime import RapidOCR
    except Exception:  # noqa: BLE001 — OCR stack missing → caller falls back
        return ""
    engine = RapidOCR()
    doc = fitz.open(path)
    pages = []
    for page in doc:
        png = page.get_pixmap(dpi=dpi).tobytes("png")
        result, _ = engine(png)
        pages.append(_lines(result or []))
    return "\n".join(pages)


def _lines(result) -> str:
    boxes = []
    for pts, txt, _score in result:
        ys = [p[1] for p in pts]
        xs = [p[0] for p in pts]
        boxes.append(((min(ys) + max(ys)) / 2, max(ys) - min(ys), min(xs), txt.strip()))
    if not boxes:
        return ""
    tol = 0.6 * statistics.median(b[1] for b in boxes)
    boxes.sort(key=lambda b: b[0])
    rows, cur, cur_y = [], [], None
    for b in boxes:
        if cur and abs(b[0] - cur_y) > tol:
            rows.append(cur)
            cur = []
        cur.append(b)
        cur_y = sum(x[0] for x in cur) / len(cur)
    rows.append(cur)
    return "\n".join(" ".join(t for *_, t in sorted(r, key=lambda b: b[2])) for r in rows)


def ocr_text(path: str, dpi: int = 200) -> str:
    st = os.stat(path)
    return _ocr_cached(path, st.st_mtime_ns, st.st_size, dpi)
