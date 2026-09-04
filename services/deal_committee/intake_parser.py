"""Turn uploaded deal documents into plain text for brief extraction.

Wraps services.knowledge_pool.knowledge_docs._extract_pages (the same loaders the
knowledge pool uses) and joins pages into one truncated string.
"""
from __future__ import annotations

SUPPORTED_EXTS: tuple[str, ...] = ("docx", "pptx", "pdf", "xlsx", "xls", "txt")
MAX_CHARS = 30_000  # extraction prompt budget


def extract_text(file_bytes: bytes, filename: str, api_key: str | None = None) -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in SUPPORTED_EXTS:
        raise ValueError(f"不支持的文件类型: .{ext}(支持: {', '.join(SUPPORTED_EXTS)})")

    from services.knowledge_pool.knowledge_docs import _extract_pages  # lazy: heavy parsers

    pages = _extract_pages(file_bytes, filename, api_key=api_key)
    text = "\n\n".join(t.strip() for _, t in pages if t and t.strip())
    if not text:
        raise ValueError(f"无法从 {filename} 提取文本(可能是扫描件或无文字内容)")
    return text[:MAX_CHARS]
