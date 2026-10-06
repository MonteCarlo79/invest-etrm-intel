"""Market intel intake — batch hashing (extractor lands in a later task)."""
from __future__ import annotations

import hashlib


def batch_hash(pages_bytes: list[bytes]) -> str:
    """Order-independent content digest of a whole upload batch."""
    h = hashlib.sha256()
    for digest in sorted(hashlib.sha256(b).hexdigest() for b in pages_bytes):
        h.update(digest.encode())
    return h.hexdigest()
