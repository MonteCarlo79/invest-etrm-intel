"""Load sections of the distilled persona profile for prompt injection.

Stage-4 pattern: jobs read markdown at startup rather than embedding copies of
judgment text in N prompts. The canonical profile lives at
``skills/colleague/<slug>/work.md`` (distilly package, repo root). Each job
loads only the section it needs — never the whole package (token cost and
dilution). A missing profile degrades gracefully to an empty string: every
job must run unchanged with or without it.
"""
from __future__ import annotations

import functools
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# shared/persona_profile.py -> repo root (works both in the OneDrive checkout
# and in container images where the layout is /app/shared + /app/skills)
_REPO_ROOT = Path(__file__).resolve().parents[1]


@functools.lru_cache(maxsize=32)
def profile_section(heading_keyword: str, slug: str = "dipeng-chen", filename: str = "work.md") -> str:
    """Return the markdown of one ``## …`` section whose heading contains
    ``heading_keyword`` (case-insensitive), or "" when the file or section is
    absent. Cached per process — restart or clear the cache after /update-skill.
    """
    path = _REPO_ROOT / "skills" / "colleague" / slug / filename
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        logger.info("persona_profile: %s not found — running without profile guidance", path)
        return ""
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith("## ") and heading_keyword.lower() in line.lower():
            start = i
            break
    if start is None:
        logger.info("persona_profile: section %r not found in %s", heading_keyword, path)
        return ""
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("## "):
            end = j
            break
    return "\n".join(lines[start:end]).strip()
