from pathlib import Path

import yaml

from .concepts import parse_concept


def find_concept(root: Path, concept_id: str) -> Path:
    hits = sorted((Path(root) / "concepts").rglob(f"{concept_id}.md"))
    hits = [h for h in hits if not h.name.endswith(".zh.md")]
    if not hits:
        raise FileNotFoundError(concept_id)
    return hits[0]


def build_pack(root: Path, concept_id: str, max_excerpt: int = 4000) -> str:
    root = Path(root)
    path = find_concept(root, concept_id)
    fm, body = parse_concept(path)
    parts = [f"# Authoring pack: {concept_id}", "", "## Stub front matter", "```yaml"]
    parts += [yaml.safe_dump(fm, allow_unicode=True, sort_keys=False), "```",
              "", "## Stub body", body, ""]
    for src in fm.get("sources", []):
        sid, use = src["id"], src.get("use", "background")
        parts.append(f"## Source: {sid} ({use})")
        outline = root / "inventory" / f"{sid}.md"
        parts.append(outline.read_text(encoding="utf-8")
                     if outline.exists() else "(no outline)")
        cache = root / "cache" / f"{sid}.txt"
        if cache.exists():
            parts += ["", "### Excerpt", cache.read_text(encoding="utf-8")[:max_excerpt]]
        else:
            parts.append("(no cached text)")
        parts.append("")
    return "\n".join(parts)


import subprocess
import sys

from .concepts import render_concept

ORDER = ("stub", "drafted", "reviewed", "published")


def set_status(root, concept_id, status, labs_root=None) -> None:
    root = Path(root)
    path = find_concept(root, concept_id)
    fm, body = parse_concept(path)
    if status not in ORDER:
        raise ValueError(f"status must be one of {ORDER}")
    if ORDER.index(status) < ORDER.index(fm["status"]):
        raise ValueError(f"cannot move {fm['status']} -> {status}")
    if status == "reviewed":
        lab = Path(labs_root or root / "labs") / concept_id
        if not fm.get("no_lab_reason"):
            if not (lab / "test_lab.py").exists():
                raise ValueError("reviewed requires a lab (labs/<id>/test_lab.py) or no_lab_reason")
            r = subprocess.run([sys.executable, "-m", "pytest", str(lab), "-q"],
                               capture_output=True, text=True)
            if r.returncode != 0:
                raise ValueError("lab tests failing:\n" + r.stdout[-2000:])
    if status == "published" and not (fm.get("signoff") or {}).get("en"):
        raise ValueError("published requires signoff.en in front matter")
    fm["status"] = status
    path.write_text(render_concept(fm, body), encoding="utf-8")
