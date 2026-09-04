"""
Embedding service for the knowledge pool.

Uses fastembed with BAAI/bge-small-zh-v1.5:
  - 512 dimensions
  - Optimised for Chinese text (electricity market docs)
  - ONNX runtime — fast on CPU, no GPU needed
  - Model is ~90 MB, downloaded on first use and cached

Usage:
    from services.knowledge_pool.embeddings import embed_texts, embed_one
    vecs = embed_texts(["山东2026年现货市场运行情况", "内蒙古电力交易规则"])
    # vecs: list[list[float]], each of length 512
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

MODEL_NAME = "BAAI/bge-small-zh-v1.5"
DIMS = 512

_model = None


def _get_model():
    global _model
    if _model is None:
        from fastembed import TextEmbedding
        logger.info("Loading embedding model %s (first use — may download ~90MB)", MODEL_NAME)
        _model = TextEmbedding(model_name=MODEL_NAME)
        logger.info("Embedding model loaded")
    return _model


def embed_texts(texts: list[str]) -> list[Optional[list[float]]]:
    """
    Embed a list of texts. Returns list of float vectors (length DIMS=512).
    Returns [None, ...] on model load failure so callers can skip gracefully.
    """
    if not texts:
        return []
    try:
        model = _get_model()
        return [e.tolist() for e in model.embed(texts)]
    except Exception as exc:
        logger.error("Embedding batch failed: %s", exc)
        return [None] * len(texts)


def embed_one(text: str) -> Optional[list[float]]:
    """Embed a single text. Returns None on failure."""
    results = embed_texts([text])
    return results[0] if results else None


def vec_to_pg(vec: list[float]) -> str:
    """Convert a float list to PostgreSQL vector literal: '[0.1,0.2,...]'"""
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
