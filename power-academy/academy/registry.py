import os
import re
from pathlib import Path

from .io import dump_yaml, load_yaml
from .models import SourceEntry

DOC_EXT = {".pdf", ".ppt", ".pptx", ".doc", ".docx", ".txt"}
MODEL_EXT = {".xls", ".xlsx", ".xlsm", ".py", ".sql"}
SKIP_DIRS = {".git", ".metadata", ".settings", "__pycache__", "node_modules", ".idea"}


def slugify(name: str) -> str:
    s = re.sub(r"[^\w]+", "_", name.lower(), flags=re.UNICODE).strip("_")
    return (s or "source")[:60]


def _unique(slug: str, seen: set) -> str:
    cand, n = slug, 2
    while cand in seen:
        cand = f"{slug}_{n}"
        n += 1
    seen.add(cand)
    return cand


def walk_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS and not d.startswith("."))
        for f in sorted(filenames):
            if not f.startswith("."):
                yield Path(dirpath) / f


def register_library(root: Path, seen: set, skip_top=("workspace",)) -> list:
    root = Path(root)
    out = []
    for p in walk_files(root):
        if p.suffix.lower() not in DOC_EXT:
            continue
        if p.relative_to(root).parts[0] in skip_top:
            continue
        out.append(SourceEntry(
            id=_unique(slugify(p.stem), seen), path=str(p), title=p.stem,
            type=p.suffix.lower()[1:], source_class="library",
            cleared=False, license_risk="high"))
    return out


def register_practice(root: Path, seen: set) -> list:
    root = Path(root)
    out = []
    for d in sorted(root.iterdir()):
        if not d.is_dir() or d.name in SKIP_DIRS or d.name.startswith("."):
            continue
        out.append(SourceEntry(
            id=_unique(slugify(f"{root.name}_{d.name}"), seen), path=str(d),
            title=f"{root.name}/{d.name}", type="folder", source_class="practice",
            cleared=True, license_risk="low"))
    return out


def models_catalogue(root: Path) -> list:
    return [{"path": str(p), "ext": p.suffix.lower(), "size": p.stat().st_size}
            for p in walk_files(Path(root)) if p.suffix.lower() in MODEL_EXT]


def write_index(entries, path):
    dump_yaml({"sources": [e.to_dict() for e in entries]}, path)


def load_index(path) -> list:
    return [SourceEntry.from_dict(d) for d in load_yaml(path)["sources"]]
