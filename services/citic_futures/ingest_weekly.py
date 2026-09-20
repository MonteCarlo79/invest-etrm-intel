# services/citic_futures/ingest_weekly.py
"""CITIC Futures weekly report ingestor.

Scans data/zhongxin-futures/中信期货周报*/ folders and ingests each new
weekly folder's PDFs into the knowledge pool (staging.spot_knowledge_docs,
app='shared', category='futures_weekly'). Dedup is two-layer:

  1. State file (.ingest_state.json) — folders already fully digested are
     skipped entirely.
  2. KB sha256 dedup inside register_and_ingest — re-ingesting a file costs
     one cheap query and returns is_new=False.

Relevant-market filter (power/BESS read-across): energy chain, new materials,
basic metals. 黑色建材 / 农业 / 贵金属 are skipped by default — add the
keyword to _RELEVANT_KEYWORDS to include them later.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

CATEGORY = "futures_weekly"
_FOLDER_RE = re.compile(r"^中信期货周报\d{8}$")
_STATE_FILE = ".ingest_state.json"

# group → filename keywords (substring match, any hit = relevant)
_RELEVANT_KEYWORDS: dict[str, list[str]] = {
    "energy_chain": [
        "原油", "甲醇", "尿素", "LPG", "石脑油", "沥青", "燃油",
        "PX-TA-EG", "纯苯", "苯乙烯", "塑料", "丙烯", "PVC", "烧碱",
        "能源化工（板块策略）", "能源化工（原油",
    ],
    "new_materials": ["碳酸锂", "新能源金属", "（锂）"],
    "basic_metals": ["基本金属", "（锌）", "有色与新材料】"],
}


def relevant_group(filename: str) -> str | None:
    """Return the relevance group for a report filename, or None to skip."""
    for group, keywords in _RELEVANT_KEYWORDS.items():
        if any(k in filename for k in keywords):
            return group
    return None


def find_pending_weekly_folders(base_dir: Path) -> list[Path]:
    """Weekly folders not yet marked done in the state file."""
    done = set(_load_state(base_dir).get("done_folders", []))
    out = []
    for child in sorted(base_dir.iterdir()):
        if child.is_dir() and _FOLDER_RE.match(child.name) and child.name not in done:
            out.append(child)
    return out


def ingest_folder(folder: Path, api_key: str | None = None) -> dict:
    """Ingest all PDFs in one weekly folder into the KB pool.

    Returns {"ingested": int, "skipped_dup": int, "failed": list[str],
             "relevant_docs": [(doc_id, filename, group)]} — relevant_docs
    covers only NEW (non-duplicate) ingestions in the relevant groups.
    """
    from services.knowledge_pool.knowledge_docs import register_and_ingest

    ingested, skipped_dup, failed = 0, 0, []
    relevant_docs: list[tuple[int, str, str]] = []
    for pdf in sorted(folder.glob("*.pdf")):
        group = relevant_group(pdf.name)
        try:
            doc_id, is_new, _cat = register_and_ingest(
                pdf.read_bytes(), pdf.name,
                category_override=CATEGORY, app="shared", api_key=api_key,
            )
        except Exception as exc:
            logger.error("ingest failed for %s: %s", pdf.name, exc)
            failed.append(pdf.name)
            continue
        # Relevant docs are collected for BOTH new and duplicate files — a
        # crashed prior run (e.g. digest step failed) must still be able to
        # produce the digest on re-run. Send-once is guarded by the state
        # file, not by is_new.
        if group:
            relevant_docs.append((doc_id, pdf.name, group))
        if is_new:
            ingested += 1
        else:
            skipped_dup += 1
    return {"ingested": ingested, "skipped_dup": skipped_dup,
            "failed": failed, "relevant_docs": relevant_docs}


def mark_folder_done(base_dir: Path, folder: Path) -> None:
    state = _load_state(base_dir)
    done = state.setdefault("done_folders", [])
    if folder.name not in done:
        done.append(folder.name)
    _save_state(base_dir, state)


def _load_state(base_dir: Path) -> dict:
    p = base_dir / _STATE_FILE
    if not p.exists():
        return {"done_folders": []}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"done_folders": []}


def _save_state(base_dir: Path, state: dict) -> None:
    (base_dir / _STATE_FILE).write_text(
        json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
