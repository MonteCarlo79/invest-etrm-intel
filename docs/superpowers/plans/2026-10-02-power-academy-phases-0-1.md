# Power Academy Phases 0–1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the inventory pipeline (register → extract → LLM outline → coverage map) and the syllabus/concept-graph tooling (schema, validators, glossary, syllabus drafting) for a bilingual power-markets quant curriculum.

**Architecture:** A small Python package `power-academy/academy/` with one module per responsibility; markdown/YAML files are the only datastore. Local steps (register, extract, validate) run on the Mac; LLM steps (outline, coverage, syllabus) run in a one-off Fargate task because Anthropic is geo-blocked from the Mac. The LLM client is injected so every LLM-calling function is tested with a fake.

**Tech Stack:** Python 3 (venv `~/.venvs/bess-platform`), PyMuPDF (`fitz`), python-pptx, python-docx, PyYAML (new dev dep), anthropic, boto3, pytest.

**Spec:** `docs/superpowers/specs/2026-10-02-power-academy-design.md`

## Global Constraints

- New top-level folder `power-academy/`; no changes to `apps/` or `services/`.
- No copies of source PDFs/PPTs committed; `sources/index.yaml` holds paths only; extracted text lives in git-ignored `power-academy/cache/`.
- Outline prompts forbid summarising the author's argument: concept names + one-line scope only.
- Concept `originality` is `original | synthesized` (never `derived`); `status` is `stub | drafted | reviewed | published`; `translations.zh.status` is `none | drafted | reviewed`.
- `reviewed` requires a lab or `no_lab_reason`; `published` requires `signoff.en`; a present `signoff.zh` requires zh status `reviewed`.
- `practice` sources: `cleared: true` (owner statement 2026-10-02); `library` sources: `cleared: false`, `license_risk: high`.
- LLM models: outline = `claude-sonnet-4-6`, tagging/syllabus helper = `claude-haiku-4-5-20251001` (override via `ACADEMY_OUTLINE_MODEL`, `ACADEMY_TAG_MODEL`). Syllabus drafting uses the outline model.
- No silent drops: every pipeline stage emits counts (found / ok / no_text / unsupported / failed).
- OneDrive: read files through a throttled single-file loop; never bulk-touch the tree.
- Git: stage explicit paths only (never `infra/terraform/terraform.tfvars`); git is slow on the OneDrive tree — run `git commit`/`push` with `run_in_background`. Commit messages end with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Irreversible/outward actions (installing deps, AWS run-task, Anthropic API calls, S3 writes) require explicit in-session confirmation before running.
- Test command (from `power-academy/`): `~/.venvs/bess-platform/bin/python -m pytest tests -q`.

## Review Focus

1. Filenames with CJK, spaces, or identical stems in different folders → unique, stable, non-empty slugs, no overwritten entries. (Task 2)
2. Scanned/blank PDF with no text layer, and legacy `.ppt`/`.doc` → reported as `no_text` / `unsupported`, never dropped or crashed. (Task 3)
3. LLM returns fenced JSON, prose around JSON, or garbage → parsed or retried once, then recorded as a failure without aborting the batch. (Tasks 5, 6)
4. zh file whose English source changed after translation, or zh status set without any hash → flagged stale. (Task 4)
5. Practice folder containing only spreadsheets (no documents) or a very large doc set → `no_text` or capped at 200 000 chars, no crash. (Task 3)

---

### Task 1: Scaffold, models, YAML helpers

**Files:**
- Create: `power-academy/README.md`, `power-academy/requirements.txt`, `power-academy/pytest.ini`, `power-academy/.gitignore`
- Create: `power-academy/academy/__init__.py`, `power-academy/academy/models.py`, `power-academy/academy/io.py`
- Create: `power-academy/tests/__init__.py`, `power-academy/tests/test_models_io.py`

**Interfaces:**
- Produces: `SourceEntry` dataclass (`id, path, title, type, source_class, cleared, license_risk, year=None, market=None, level=None`; `.to_dict()` maps `source_class`→`class`; `SourceEntry.from_dict`), `load_yaml(path) -> Any`, `dump_yaml(obj, path) -> None`.

- [ ] **Step 1: Install dev dependency (confirm with owner first)**

Run: `uv pip install --python ~/.venvs/bess-platform/bin/python pyyaml`
Expected: `+ pyyaml` installed.

- [ ] **Step 2: Create scaffold files**

`power-academy/requirements.txt`:
```
pyyaml
pymupdf
python-pptx
python-docx
anthropic
boto3
pytest
```
`power-academy/pytest.ini`:
```
[pytest]
pythonpath = .
testpaths = tests
```
`power-academy/.gitignore`:
```
cache/
__pycache__/
*.pyc
bundle.tar.gz
results.tar.gz
```
`power-academy/README.md`:
```markdown
# Power Academy

Bilingual (EN/ZH) power-markets quant curriculum. Spec: `docs/superpowers/specs/2026-10-02-power-academy-design.md`.

Run from this folder with `PY=~/.venvs/bess-platform/bin/python`:

- `$PY -m academy.cli register` — index the library + practice folders into `sources/index.yaml`
- `$PY -m academy.cli extract` — extract text into git-ignored `cache/`
- `$PY -m academy.cli validate` — validate concepts, glossary, graph
- LLM stages (`outline`, `coverage`, `syllabus`) run on Fargate; see the plan Task 9.

Add a source: place it in the library, re-run `register`. Add a concept: create `concepts/<track>/<id>.md` (+ `<id>.zh.md`) following the schema in the spec §4, then `validate`.
```
`power-academy/academy/__init__.py`: empty file. `power-academy/tests/__init__.py`: empty file.

- [ ] **Step 3: Write the failing test**

`power-academy/tests/test_models_io.py`:
```python
from academy.io import dump_yaml, load_yaml
from academy.models import SourceEntry


def test_yaml_roundtrip_keeps_chinese(tmp_path):
    p = tmp_path / "x" / "a.yaml"
    dump_yaml({"term": "火花价差", "n": 1}, p)
    assert load_yaml(p) == {"term": "火花价差", "n": 1}
    assert "火花价差" in p.read_text(encoding="utf-8")


def test_source_entry_class_key_roundtrip():
    e = SourceEntry(id="a", path="/p", title="t", type="pdf",
                    source_class="library", cleared=False, license_risk="high")
    d = e.to_dict()
    assert d["class"] == "library" and "source_class" not in d
    assert SourceEntry.from_dict(d) == e
```

- [ ] **Step 4: Run test to verify it fails**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_models_io.py -q`
Expected: FAIL (`ModuleNotFoundError: academy.io`).

- [ ] **Step 5: Implement**

`power-academy/academy/io.py`:
```python
from pathlib import Path

import yaml


def load_yaml(path):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def dump_yaml(obj, path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(obj, allow_unicode=True, sort_keys=False), encoding="utf-8")
```
`power-academy/academy/models.py`:
```python
from dataclasses import asdict, dataclass


@dataclass
class SourceEntry:
    id: str
    path: str
    title: str
    type: str            # pdf|ppt|pptx|doc|docx|txt|folder
    source_class: str    # library|practice
    cleared: bool
    license_risk: str    # low|high
    year: int | None = None
    market: str | None = None
    level: str | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["class"] = d.pop("source_class")
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "SourceEntry":
        d = dict(d)
        d["source_class"] = d.pop("class")
        return cls(**d)
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_models_io.py -q`
Expected: 2 passed.

- [ ] **Step 7: Commit**

```bash
git add power-academy/README.md power-academy/requirements.txt power-academy/pytest.ini power-academy/.gitignore power-academy/academy/__init__.py power-academy/academy/models.py power-academy/academy/io.py power-academy/tests/__init__.py power-academy/tests/test_models_io.py
git commit -m "Scaffold power-academy package with source model and YAML helpers" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Source registrar

**Files:**
- Create: `power-academy/academy/registry.py`, `power-academy/tests/test_registry.py`

**Interfaces:**
- Consumes: `SourceEntry`, `dump_yaml`, `load_yaml` (Task 1).
- Produces: constants `DOC_EXT`, `MODEL_EXT`, `SKIP_DIRS`; `slugify(name) -> str`; `walk_files(root) -> Iterator[Path]`; `register_library(root: Path, seen: set[str], skip_top=("workspace",)) -> list[SourceEntry]`; `register_practice(root: Path, seen: set[str]) -> list[SourceEntry]` (one entry per immediate subfolder, id prefixed with `root.name`); `models_catalogue(root: Path) -> list[dict]` (`path, ext, size`); `write_index(entries, path)`; `load_index(path) -> list[SourceEntry]`.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_registry.py`:
```python
from academy.registry import (load_index, models_catalogue, register_library,
                              register_practice, slugify, write_index)


def _touch(p, data=b"x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def test_slugify_cjk_and_symbols():
    assert slugify("火花价差 notes (v2).pdf") == "火花价差_notes_v2_pdf"
    assert slugify("???") == "source"
    assert len(slugify("a" * 200)) == 60


def test_library_dedupes_same_stem_and_skips_noise(tmp_path):
    lib = tmp_path / "lib"
    _touch(lib / "Hedging" / "Plan.pdf")
    _touch(lib / "Valuation" / "Plan.pdf")          # same stem, other folder
    _touch(lib / "Swing" / "deck.ppt")
    _touch(lib / "Swing" / "pic.png")               # not a document
    _touch(lib / "workspace" / "docs" / "readme.txt")  # skipped top-level
    _touch(lib / "Hedging" / ".git" / "x.pdf")      # skipped dir
    entries = register_library(lib, set())
    ids = [e.id for e in entries]
    assert len(ids) == len(set(ids)) == 3
    assert sorted(e.type for e in entries) == ["pdf", "pdf", "ppt"]
    assert all(e.source_class == "library" and not e.cleared and e.license_risk == "high"
               for e in entries)


def test_practice_one_entry_per_folder_and_cross_root_unique(tmp_path):
    a, b = tmp_path / "power", tmp_path / "dipeng"
    (a / "Battery").mkdir(parents=True)
    (b / "battery").mkdir(parents=True)
    (b / ".hidden").mkdir()
    seen = set()
    ea = register_practice(a, seen)
    eb = register_practice(b, seen)
    assert [e.id for e in ea] == ["power_battery"]
    assert [e.id for e in eb] == ["dipeng_battery"]
    assert ea[0].type == "folder" and ea[0].cleared and ea[0].license_risk == "low"


def test_models_catalogue_and_index_roundtrip(tmp_path):
    _touch(tmp_path / "m" / "a.xlsx")
    _touch(tmp_path / "m" / "b.py")
    _touch(tmp_path / "m" / "c.pdf")
    cat = models_catalogue(tmp_path)
    assert sorted(c["ext"] for c in cat) == [".py", ".xlsx"]
    lib = tmp_path / "m"
    entries = register_library(lib, set())
    write_index(entries, tmp_path / "index.yaml")
    assert load_index(tmp_path / "index.yaml") == entries
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_registry.py -q`
Expected: FAIL (`ModuleNotFoundError: academy.registry`).

- [ ] **Step 3: Implement**

`power-academy/academy/registry.py`:
```python
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
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_registry.py -q`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/registry.py power-academy/tests/test_registry.py
git commit -m "Add source registrar for library and practice folders" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Text extraction with count report

**Files:**
- Create: `power-academy/academy/extract.py`, `power-academy/tests/test_extract.py`

**Interfaces:**
- Consumes: `SourceEntry`, `DOC_EXT`, `MODEL_EXT`, `walk_files` (Tasks 1–2).
- Produces: `Unsupported` exception; `extract_file(path: Path) -> str`; `extract_source(entry, cache_dir: Path) -> dict` (`{"id","status","chars","error"}`, status ∈ `ok|cached|no_text|unsupported|failed`; writes `cache_dir/<id>.txt` only for `ok`); `extract_all(entries, cache_dir, throttle_s=0.2) -> dict` (writes `cache_dir/_report.json`, returns `{"counts": {...}, "results": [...]}`); constants `MIN_CHARS=200`, `FOLDER_CAP=200_000`.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_extract.py`:
```python
import json

import docx
import fitz
from pptx import Presentation

from academy.extract import FOLDER_CAP, extract_all, extract_source
from academy.models import SourceEntry


def _entry(id_, path, type_="pdf", cls="library"):
    return SourceEntry(id=id_, path=str(path), title=id_, type=type_,
                       source_class=cls, cleared=cls == "practice",
                       license_risk="high" if cls == "library" else "low")


def _pdf(path, text):
    d = fitz.open()
    page = d.new_page()
    if text:
        page.insert_text((72, 72), text)
    d.save(path)


def test_pdf_ok_and_blank_pdf_is_no_text(tmp_path):
    _pdf(tmp_path / "a.pdf", "spark spread option " * 30)
    _pdf(tmp_path / "blank.pdf", "")
    cache = tmp_path / "cache"
    ok = extract_source(_entry("a", tmp_path / "a.pdf"), cache)
    blank = extract_source(_entry("blank", tmp_path / "blank.pdf"), cache)
    assert ok["status"] == "ok" and (cache / "a.txt").exists()
    assert blank["status"] == "no_text" and not (cache / "blank.txt").exists()


def test_pptx_docx_txt(tmp_path):
    prs = Presentation()
    s = prs.slides.add_slide(prs.slide_layouts[5])
    s.shapes.title.text = "tolling agreement " * 20
    prs.save(tmp_path / "d.pptx")
    dd = docx.Document()
    dd.add_paragraph("storage valuation " * 20)
    dd.save(tmp_path / "d.docx")
    (tmp_path / "d.txt").write_text("swing option " * 40, encoding="utf-8")
    cache = tmp_path / "cache"
    for stem, t in [("d", "pptx"), ("d", "docx"), ("d", "txt")]:
        e = _entry(f"id_{t}", tmp_path / f"{stem}.{t}", t)
        assert extract_source(e, cache)["status"] == "ok"


def test_legacy_ppt_unsupported_and_corrupt_failed(tmp_path):
    (tmp_path / "old.ppt").write_bytes(b"x")
    (tmp_path / "bad.pdf").write_bytes(b"not a pdf")
    cache = tmp_path / "cache"
    assert extract_source(_entry("old", tmp_path / "old.ppt", "ppt"), cache)["status"] == "unsupported"
    assert extract_source(_entry("bad", tmp_path / "bad.pdf"), cache)["status"] == "failed"


def test_practice_folder_docs_models_only_and_cap(tmp_path):
    f = tmp_path / "deal"
    f.mkdir()
    (f / "notes.txt").write_text("hedge " * 100, encoding="utf-8")
    (f / "old.ppt").write_bytes(b"x")
    cache = tmp_path / "cache"
    r = extract_source(_entry("deal", f, "folder", "practice"), cache)
    assert r["status"] == "ok"
    body = (cache / "deal.txt").read_text(encoding="utf-8")
    assert "[skipped: old.ppt" in body

    only_models = tmp_path / "models"
    only_models.mkdir()
    (only_models / "m.xlsx").write_bytes(b"x")
    assert extract_source(_entry("models", only_models, "folder", "practice"), cache)["status"] == "no_text"

    big = tmp_path / "big"
    big.mkdir()
    for i in range(3):
        (big / f"{i}.txt").write_text("x" * 150_000, encoding="utf-8")
    assert extract_source(_entry("big", big, "folder", "practice"), cache)["chars"] <= FOLDER_CAP + 500


def test_extract_all_counts_cached_and_report(tmp_path):
    _pdf(tmp_path / "a.pdf", "forward curve " * 40)
    (tmp_path / "old.ppt").write_bytes(b"x")
    es = [_entry("a", tmp_path / "a.pdf"), _entry("old", tmp_path / "old.ppt", "ppt")]
    cache = tmp_path / "cache"
    r1 = extract_all(es, cache, throttle_s=0)
    assert r1["counts"] == {"ok": 1, "unsupported": 1}
    r2 = extract_all(es, cache, throttle_s=0)
    assert r2["counts"] == {"cached": 1, "unsupported": 1}
    assert json.loads((cache / "_report.json").read_text())["counts"] == r2["counts"]
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_extract.py -q`
Expected: FAIL (`ModuleNotFoundError: academy.extract`).

- [ ] **Step 3: Implement**

`power-academy/academy/extract.py`:
```python
import json
import time
from pathlib import Path

import docx
import fitz
from pptx import Presentation

from .registry import DOC_EXT, MODEL_EXT, walk_files

MIN_CHARS = 200
FOLDER_CAP = 200_000


class Unsupported(Exception):
    pass


def extract_file(path: Path) -> str:
    ext = Path(path).suffix.lower()
    if ext == ".pdf":
        with fitz.open(path) as d:
            return "\n".join(page.get_text() for page in d)
    if ext == ".pptx":
        out = []
        for i, slide in enumerate(Presentation(str(path)).slides, 1):
            out.append(f"[slide {i}]")
            out.extend(sh.text_frame.text for sh in slide.shapes if sh.has_text_frame)
        return "\n".join(out)
    if ext == ".docx":
        return "\n".join(p.text for p in docx.Document(str(path)).paragraphs)
    if ext == ".txt":
        return Path(path).read_text(encoding="utf-8", errors="replace")
    raise Unsupported(ext)


def _extract_folder(folder: Path) -> str:
    parts, total, models = [], 0, []
    for p in walk_files(folder):
        ext = p.suffix.lower()
        if ext in MODEL_EXT:
            models.append(p.name)
            continue
        if ext not in DOC_EXT:
            continue
        try:
            body = extract_file(p)
        except Unsupported:
            parts.append(f"[skipped: {p.name} (unsupported {ext})]")
            continue
        except Exception as e:  # corrupt file: note and continue
            parts.append(f"[skipped: {p.name} ({type(e).__name__})]")
            continue
        chunk = f"=== {p.relative_to(folder)} ===\n{body}"
        parts.append(chunk)
        total += len(chunk)
        if total >= FOLDER_CAP:
            parts.append("[truncated: folder cap reached]")
            break
    if models:
        parts.append("[models] " + ", ".join(models))
    return "\n".join(parts)


def extract_source(entry, cache_dir: Path) -> dict:
    cache_dir = Path(cache_dir)
    out = cache_dir / f"{entry.id}.txt"
    res = {"id": entry.id, "status": "ok", "chars": 0, "error": None}
    if out.exists():
        res["status"] = "cached"
        res["chars"] = len(out.read_text(encoding="utf-8"))
        return res
    try:
        text = _extract_folder(Path(entry.path)) if entry.type == "folder" \
            else extract_file(Path(entry.path))
    except Unsupported as e:
        return {**res, "status": "unsupported", "error": f"unsupported {e}"}
    except Exception as e:
        return {**res, "status": "failed", "error": f"{type(e).__name__}: {e}"}
    res["chars"] = len(text.strip())
    if res["chars"] < MIN_CHARS:
        res["status"] = "no_text"
        return res
    cache_dir.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    return res


def extract_all(entries, cache_dir: Path, throttle_s: float = 0.2) -> dict:
    results, counts = [], {}
    for e in entries:
        r = extract_source(e, cache_dir)
        results.append(r)
        counts[r["status"]] = counts.get(r["status"], 0) + 1
        if r["status"] != "cached":
            time.sleep(throttle_s)
    report = {"counts": counts, "results": results}
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    (Path(cache_dir) / "_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    return report
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_extract.py -q`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/extract.py power-academy/tests/test_extract.py
git commit -m "Add throttled text extraction with per-status count report" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Concept schema validator, glossary checker, zh staleness

**Files:**
- Create: `power-academy/academy/concepts.py`, `power-academy/academy/glossary.py`, `power-academy/glossary/terms.yaml`
- Create: `power-academy/tests/test_concepts.py`, `power-academy/tests/test_glossary.py`

**Interfaces:**
- Consumes: `load_yaml`, `dump_yaml` (Task 1).
- Produces:
  - `concepts.parse_concept(path) -> tuple[dict, str]` (front matter, body)
  - `concepts.render_concept(fm: dict, body: str) -> str`
  - `concepts.body_hash(body: str) -> str` (first 16 hex of sha256 of stripped body)
  - `concepts.validate_concept(fm, labs_dir: Path | None = None) -> list[str]`
  - `concepts.zh_is_stale(fm_en: dict, en_body: str, fm_zh_translation: dict) -> bool` where the third arg is `fm_en["translations"]["zh"]`
  - `concepts.LEVELS`, `STATUSES`, `ORIGINALITY`, `MARKETS`, `TRANSLATION_STATUSES`
  - `glossary.load_glossary(path) -> list[dict]` (`en`, `zh`); `glossary.check_pair(en_body, zh_body, terms) -> list[str]`; `glossary.prompt_block(terms) -> str`

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_concepts.py`:
```python
from academy.concepts import (body_hash, parse_concept, render_concept,
                              validate_concept, zh_is_stale)


def _fm(**kw):
    fm = {"id": "spark_spread_option", "track": "asset_valuation", "level": "intermediate",
          "prerequisites": [], "markets": ["EU", "GB"], "status": "stub",
          "sources": [], "originality": "original",
          "translations": {"zh": {"status": "none", "en_hash": None}}}
    fm.update(kw)
    return fm


def test_valid_stub_passes_and_roundtrips(tmp_path):
    assert validate_concept(_fm()) == []
    p = tmp_path / "c.md"
    p.write_text(render_concept(_fm(), "## Intuition\nx\n"), encoding="utf-8")
    fm, body = parse_concept(p)
    assert fm["id"] == "spark_spread_option" and body.startswith("## Intuition")


def test_enum_and_required_errors():
    errs = validate_concept(_fm(level="expert", originality="derived", markets=["MARS"]))
    joined = " | ".join(errs)
    assert "level" in joined and "originality" in joined and "markets" in joined
    assert validate_concept({"id": "x"})  # missing fields reported


def test_reviewed_needs_lab_or_reason(tmp_path):
    labs = tmp_path / "labs"
    assert validate_concept(_fm(status="reviewed"), labs)  # no lab, no reason
    assert validate_concept(_fm(status="reviewed", no_lab_reason="pure theory"), labs) == []
    (labs / "spark_spread_option").mkdir(parents=True)
    (labs / "spark_spread_option" / "run.py").write_text("x")
    assert validate_concept(_fm(status="reviewed"), labs) == []


def test_published_needs_signoff_and_zh_signoff_needs_reviewed_zh():
    base = dict(status="published", no_lab_reason="n/a")
    assert validate_concept(_fm(**base))  # no signoff
    ok = _fm(**base, signoff={"en": "2026-10-02"})
    assert validate_concept(ok) == []
    bad = _fm(**base, signoff={"en": "2026-10-02", "zh": "2026-10-03"})
    assert any("zh" in e for e in validate_concept(bad))


def test_zh_staleness():
    en = "## Intuition\nforward curve\n"
    tr = {"status": "drafted", "en_hash": body_hash(en)}
    fm = _fm(translations={"zh": tr})
    assert zh_is_stale(fm, en, tr) is False
    assert zh_is_stale(fm, en + "changed", tr) is True
    tr_nohash = {"status": "drafted", "en_hash": None}
    assert zh_is_stale(fm, en, tr_nohash) is True      # drafted without a hash
    assert zh_is_stale(fm, en, {"status": "none", "en_hash": None}) is False
```
`power-academy/tests/test_glossary.py`:
```python
from academy.glossary import check_pair, load_glossary, prompt_block

TERMS = [{"en": "spark spread", "zh": "火花价差"}, {"en": "swing option", "zh": "摆动期权"}]


def test_check_pair_flags_missing_zh_term():
    assert check_pair("The Spark Spread is wide.", "火花价差很宽。", TERMS) == []
    errs = check_pair("The spark spread is wide.", "发电利润价差很宽。", TERMS)
    assert errs == ["'spark spread' should appear as '火花价差'"]
    assert check_pair("nothing relevant", "无关", TERMS) == []


def test_seed_glossary_loads_and_prompt_block():
    terms = load_glossary("glossary/terms.yaml")
    assert len(terms) >= 15
    assert all(t["en"] and t["zh"] for t in terms)
    block = prompt_block(TERMS)
    assert "spark spread = 火花价差" in block
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_concepts.py tests/test_glossary.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `concepts.py`**

`power-academy/academy/concepts.py`:
```python
import hashlib
from pathlib import Path

import yaml

LEVELS = ("foundation", "intermediate", "advanced")
STATUSES = ("stub", "drafted", "reviewed", "published")
ORIGINALITY = ("original", "synthesized")
MARKETS = ("EU", "GB", "US", "AU", "CN")
TRANSLATION_STATUSES = ("none", "drafted", "reviewed")
REQUIRED = ("id", "track", "level", "prerequisites", "markets", "status",
            "sources", "originality", "translations")


def parse_concept(path) -> tuple:
    text = Path(path).read_text(encoding="utf-8")
    _, fm, body = text.split("---\n", 2)
    return yaml.safe_load(fm), body


def render_concept(fm: dict, body: str) -> str:
    return "---\n" + yaml.safe_dump(fm, allow_unicode=True, sort_keys=False) + "---\n" + body


def body_hash(body: str) -> str:
    return hashlib.sha256(body.strip().encode("utf-8")).hexdigest()[:16]


def validate_concept(fm: dict, labs_dir=None) -> list:
    errs = [f"missing field: {k}" for k in REQUIRED if k not in fm]
    if errs:
        return errs
    if fm["level"] not in LEVELS:
        errs.append(f"level must be one of {LEVELS}")
    if fm["status"] not in STATUSES:
        errs.append(f"status must be one of {STATUSES}")
    if fm["originality"] not in ORIGINALITY:
        errs.append(f"originality must be one of {ORIGINALITY}")
    if not fm["markets"] or any(m not in MARKETS for m in fm["markets"]):
        errs.append(f"markets must be a non-empty subset of {MARKETS}")
    zh = (fm["translations"] or {}).get("zh", {})
    if zh.get("status", "none") not in TRANSLATION_STATUSES:
        errs.append(f"translations.zh.status must be one of {TRANSLATION_STATUSES}")
    if fm["status"] in ("reviewed", "published"):
        has_lab = False
        if labs_dir is not None:
            d = Path(labs_dir) / fm["id"]
            has_lab = d.is_dir() and any(d.iterdir())
        if not has_lab and not fm.get("no_lab_reason"):
            errs.append("reviewed/published needs a lab in labs/<id>/ or no_lab_reason")
    signoff = fm.get("signoff") or {}
    if fm["status"] == "published" and not signoff.get("en"):
        errs.append("published requires signoff.en")
    if signoff.get("zh") and zh.get("status") != "reviewed":
        errs.append("signoff.zh requires translations.zh.status == reviewed")
    return errs


def zh_is_stale(fm_en: dict, en_body: str, zh_tr: dict) -> bool:
    if zh_tr.get("status", "none") == "none":
        return False
    return zh_tr.get("en_hash") != body_hash(en_body)
```

- [ ] **Step 4: Implement `glossary.py` and the seed glossary**

`power-academy/academy/glossary.py`:
```python
from .io import load_yaml


def load_glossary(path) -> list:
    return load_yaml(path)["terms"]


def check_pair(en_body: str, zh_body: str, terms: list) -> list:
    low = en_body.lower()
    return [f"'{t['en']}' should appear as '{t['zh']}'"
            for t in terms if t["en"].lower() in low and t["zh"] not in zh_body]


def prompt_block(terms: list) -> str:
    lines = "\n".join(f"- {t['en']} = {t['zh']}" for t in terms)
    return "Use these approved EN=ZH terms exactly:\n" + lines
```
`power-academy/glossary/terms.yaml`:
```yaml
terms:
  - {en: spark spread, zh: 火花价差}
  - {en: dark spread, zh: 暗价差}
  - {en: clean spark spread, zh: 清洁火花价差}
  - {en: tolling agreement, zh: 委托加工协议}
  - {en: swing option, zh: 摆动期权}
  - {en: real option, zh: 实物期权}
  - {en: mean reversion, zh: 均值回归}
  - {en: forward curve, zh: 远期曲线}
  - {en: merit order, zh: 优先发电顺序}
  - {en: unit commitment, zh: 机组组合}
  - {en: rolling intrinsic, zh: 滚动内在价值}
  - {en: profit at risk, zh: 风险利润}
  - {en: value at risk, zh: 风险价值}
  - {en: imbalance price, zh: 不平衡价格}
  - {en: capacity market, zh: 容量市场}
  - {en: pumped storage, zh: 抽水蓄能}
  - {en: energy storage, zh: 储能}
  - {en: dispatch, zh: 调度}
  - {en: capture rate, zh: 捕获率}
  - {en: day-ahead market, zh: 日前市场}
```

- [ ] **Step 5: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_concepts.py tests/test_glossary.py -q`
Expected: 7 passed.

- [ ] **Step 6: Commit**

```bash
git add power-academy/academy/concepts.py power-academy/academy/glossary.py power-academy/glossary/terms.yaml power-academy/tests/test_concepts.py power-academy/tests/test_glossary.py
git commit -m "Add concept schema validator, EN/ZH glossary checker and zh staleness check" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 5: LLM JSON helper with fake client

**Files:**
- Create: `power-academy/academy/llm.py`, `power-academy/tests/fakes.py`, `power-academy/tests/test_llm.py`

**Interfaces:**
- Produces: `llm.parse_json(text) -> dict`; `llm.call_json(client, model, system, user, max_tokens=2000, retries=1) -> dict` (raises `ValueError("unparseable LLM output: …")` after retries); `llm.USAGE` dict `{"in": int, "out": int}` accumulated from `resp.usage` when present; `tests.fakes.FakeClient(replies: list[str])` with `.calls`.
- `client` is any object with `client.messages.create(model=, max_tokens=, system=, messages=)` returning an object with `.content[0].text`.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/fakes.py`:
```python
class _Block:
    def __init__(self, text):
        self.text = text


class _Resp:
    def __init__(self, text):
        self.content = [_Block(text)]


class FakeClient:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []
        self.messages = self

    def create(self, **kw):
        self.calls.append(kw)
        return _Resp(self.replies.pop(0))
```
`power-academy/tests/test_llm.py`:
```python
import pytest

from academy.llm import call_json, parse_json
from tests.fakes import FakeClient


def test_parse_json_handles_fences_and_prose():
    assert parse_json('```json\n{"a": 1}\n```') == {"a": 1}
    assert parse_json('Here you go: {"a": {"b": 2}} done') == {"a": {"b": 2}}


def test_parse_json_rejects_no_object():
    with pytest.raises(ValueError):
        parse_json("no json here")


def test_call_json_retries_once_then_succeeds():
    c = FakeClient(["garbage", '{"ok": true}'])
    assert call_json(c, "m", "sys", "user") == {"ok": True}
    assert len(c.calls) == 2


def test_call_json_raises_after_retries():
    c = FakeClient(["garbage", "still garbage"])
    with pytest.raises(ValueError, match="unparseable"):
        call_json(c, "m", "sys", "user")
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_llm.py -q`
Expected: FAIL (`ModuleNotFoundError: academy.llm`).

- [ ] **Step 3: Implement**

`power-academy/academy/llm.py`:
```python
import json

USAGE = {"in": 0, "out": 0}


def parse_json(text: str) -> dict:
    i, j = text.find("{"), text.rfind("}")
    if i == -1 or j <= i:
        raise ValueError("no json object")
    return json.loads(text[i:j + 1])


def call_json(client, model, system, user, max_tokens=2000, retries=1) -> dict:
    last = None
    for _ in range(retries + 1):
        resp = client.messages.create(
            model=model, max_tokens=max_tokens, system=system,
            messages=[{"role": "user", "content": user}])
        usage = getattr(resp, "usage", None)
        if usage is not None:
            USAGE["in"] += getattr(usage, "input_tokens", 0)
            USAGE["out"] += getattr(usage, "output_tokens", 0)
        try:
            return parse_json(resp.content[0].text)
        except (ValueError, json.JSONDecodeError) as e:
            last = e
    raise ValueError(f"unparseable LLM output: {last}")
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_llm.py -q`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/llm.py power-academy/tests/fakes.py power-academy/tests/test_llm.py
git commit -m "Add LLM JSON helper with retry and injectable client" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Outline generator

**Files:**
- Create: `power-academy/academy/outline.py`, `power-academy/tests/test_outline.py`

**Interfaces:**
- Consumes: `call_json` (Task 5), `SourceEntry` (Task 1), cache files from Task 3 (`cache/<id>.txt`).
- Produces: `SYSTEM_PROMPT: str`; `chunk_text(text, size=60_000, max_chunks=6) -> tuple[list[str], bool]` (chunks, truncated); `outline_source(client, model, entry, text) -> dict` (keys `topic, level, market, year, concepts[{name,scope}], methods, implied_prerequisites, has_worked_examples, has_code, truncated`); `render_outline_md(entry, outline) -> str`; `run_outlines(entries, cache_dir, out_dir, client, model, only=None) -> dict` (`{"ok":[ids], "failed":{id:err}, "skipped":{id:reason}}`; writes `out_dir/<id>.md` and merges into `out_dir/_outlines.json` `{id: outline}`; skips ids already present in `_outlines.json`).

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_outline.py`:
```python
import json

from academy.models import SourceEntry
from academy.outline import (SYSTEM_PROMPT, chunk_text, outline_source,
                             render_outline_md, run_outlines)
from tests.fakes import FakeClient


def _e(id_):
    return SourceEntry(id=id_, path="/x", title=id_.upper(), type="pdf",
                       source_class="library", cleared=False, license_risk="high")


def _reply(names, **kw):
    d = {"topic": "valuation", "level": "advanced", "market": "EU", "year": 2011,
         "concepts": [{"name": n, "scope": f"{n} scope"} for n in names],
         "methods": ["monte carlo"], "implied_prerequisites": ["option basics"],
         "has_worked_examples": False, "has_code": False}
    d.update(kw)
    return json.dumps(d)


def test_prompt_forbids_summarising():
    assert "do not summarise" in SYSTEM_PROMPT.lower()


def test_chunk_text_caps_and_flags_truncation():
    chunks, trunc = chunk_text("a" * 250, size=100, max_chunks=2)
    assert [len(c) for c in chunks] == [100, 100] and trunc is True
    chunks, trunc = chunk_text("a" * 50, size=100, max_chunks=2)
    assert len(chunks) == 1 and trunc is False


def test_outline_merges_chunks_dedupes_concepts():
    # 70 000 chars with the default 60 000 chunk size -> exactly two chunks
    c = FakeClient([_reply(["Spark Spread", "Tolling"]),
                    _reply(["spark spread", "Swing"], has_code=True)])
    out = outline_source(c, "m", _e("a"), "x" * 70_000)
    names = [x["name"] for x in out["concepts"]]
    assert names == ["Spark Spread", "Tolling", "Swing"]
    assert out["has_code"] is True and out["truncated"] is False


def test_render_md_has_concepts_and_no_body_text():
    out = json.loads(_reply(["Tolling"]))
    out["truncated"] = False
    md = render_outline_md(_e("a"), out)
    assert "Tolling" in md and "Tolling scope" in md and "A" in md


def test_run_outlines_isolates_failures_and_resumes(tmp_path):
    cache, outd = tmp_path / "cache", tmp_path / "inv"
    cache.mkdir()
    (cache / "good.txt").write_text("t" * 300, encoding="utf-8")
    (cache / "bad.txt").write_text("t" * 300, encoding="utf-8")
    es = [_e("good"), _e("bad"), _e("missing")]
    c = FakeClient([_reply(["A"]), "garbage", "garbage"])
    rep = run_outlines(es, cache, outd, c, "m")
    assert rep["ok"] == ["good"] and list(rep["failed"]) == ["bad"]
    assert rep["skipped"] == {"missing": "no cached text"}
    assert (outd / "good.md").exists()
    # resume: good already done, bad retried and now succeeds
    c2 = FakeClient([_reply(["B"])])
    rep2 = run_outlines(es, cache, outd, c2, "m")
    assert rep2["ok"] == ["bad"]
    assert set(json.loads((outd / "_outlines.json").read_text())) == {"good", "bad"}
```
- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_outline.py -q`
Expected: FAIL (`ModuleNotFoundError: academy.outline`).

- [ ] **Step 3: Implement**

`power-academy/academy/outline.py`:
```python
import json
from pathlib import Path

from .llm import call_json

SYSTEM_PROMPT = (
    "You are indexing a reference source for a power-markets quant curriculum. "
    "Return ONLY one JSON object with keys: "
    "topic (string); level (foundation|intermediate|advanced); "
    "market (string, e.g. EU, GB, US, DE, mixed); year (integer or null); "
    "concepts (list of {name, scope}: name is a concept name, scope is ONE sentence "
    "saying what the concept covers); methods (list of strings); "
    "implied_prerequisites (list of strings); has_worked_examples (bool); has_code (bool). "
    "Do not summarise the author's argument, results or prose, and never quote the source. "
    "Concept names and one-line scope only."
)


def chunk_text(text: str, size: int = 60_000, max_chunks: int = 6):
    chunks = [text[i:i + size] for i in range(0, len(text), size)]
    return chunks[:max_chunks], len(chunks) > max_chunks


def outline_source(client, model, entry, text) -> dict:
    chunks, truncated = chunk_text(text)
    merged, seen = None, set()
    for ch in chunks:
        part = call_json(client, model, SYSTEM_PROMPT,
                         f"Source title: {entry.title}\n\n{ch}", max_tokens=2000)
        if merged is None:
            merged = {k: part.get(k) for k in ("topic", "level", "market", "year")}
            merged.update(concepts=[], methods=[], implied_prerequisites=[],
                          has_worked_examples=False, has_code=False)
        for c in part.get("concepts", []):
            key = c["name"].strip().lower()
            if key not in seen:
                seen.add(key)
                merged["concepts"].append(c)
        for k in ("methods", "implied_prerequisites"):
            merged[k] += [x for x in part.get(k, []) if x not in merged[k]]
        merged["has_worked_examples"] |= bool(part.get("has_worked_examples"))
        merged["has_code"] |= bool(part.get("has_code"))
    merged["truncated"] = truncated
    return merged


def render_outline_md(entry, o: dict) -> str:
    lines = [f"# {entry.title}", "",
             f"- id: `{entry.id}` · class: {entry.source_class} · type: {entry.type}",
             f"- topic: {o['topic']} · level: {o['level']} · market: {o['market']} · year: {o['year']}",
             f"- worked examples: {o['has_worked_examples']} · code: {o['has_code']}"
             f"{' · TRUNCATED' if o['truncated'] else ''}", "", "## Concepts"]
    lines += [f"- **{c['name']}** — {c['scope']}" for c in o["concepts"]]
    lines += ["", "## Methods"] + [f"- {m}" for m in o["methods"]]
    lines += ["", "## Implied prerequisites"] + [f"- {m}" for m in o["implied_prerequisites"]]
    return "\n".join(lines) + "\n"


def run_outlines(entries, cache_dir, out_dir, client, model, only=None) -> dict:
    cache_dir, out_dir = Path(cache_dir), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    store = out_dir / "_outlines.json"
    done = json.loads(store.read_text(encoding="utf-8")) if store.exists() else {}
    rep = {"ok": [], "failed": {}, "skipped": {}}
    for e in entries:
        if only and e.id not in only:
            continue
        if e.id in done:
            continue
        txt = cache_dir / f"{e.id}.txt"
        if not txt.exists():
            rep["skipped"][e.id] = "no cached text"
            continue
        try:
            o = outline_source(client, model, e, txt.read_text(encoding="utf-8"))
        except Exception as ex:
            rep["failed"][e.id] = f"{type(ex).__name__}: {ex}"
            continue
        done[e.id] = o
        (out_dir / f"{e.id}.md").write_text(render_outline_md(e, o), encoding="utf-8")
        rep["ok"].append(e.id)
        store.write_text(json.dumps(done, ensure_ascii=False, indent=1), encoding="utf-8")
    return rep
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_outline.py -q`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/outline.py power-academy/tests/test_outline.py
git commit -m "Add LLM outline generator with chunk merge and resumable runs" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Track definitions and coverage map

**Files:**
- Create: `power-academy/academy/tracks.py`, `power-academy/academy/coverage.py`, `power-academy/tests/test_coverage.py`

**Interfaces:**
- Consumes: `call_json` (Task 5), outlines dict `{source_id: outline}` (Task 6).
- Produces:
  - `tracks.TRACKS: list[dict]` with keys `id`, `title` (8 entries, ids: `market_fundamentals, price_curve_modelling, asset_valuation, optimisation_dispatch, storage_flexibility, hedging_trading, risk_management, modern_markets`)
  - `coverage.tag_source(client, model, source_id, outline) -> dict[str, str | None]` (concept name → track id or None; invalid ids coerced to None)
  - `coverage.build_matrix(mappings: dict[str, dict]) -> dict[str, dict[str, int]]` (track id → {source_id: count})
  - `coverage.gaps(matrix, min_sources=2) -> list[str]`
  - `coverage.render_coverage_md(matrix, source_titles: dict[str,str], gap_ids: list[str]) -> str`
  - `coverage.run_coverage(outlines, client, model, out_dir, titles) -> dict` (writes `out_dir/_mappings.json` and `out_dir/coverage_map.md`; returns `{"gaps": [...], "failed": {id: err}}`)

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_coverage.py`:
```python
import json

from academy.coverage import (build_matrix, gaps, render_coverage_md,
                              run_coverage, tag_source)
from academy.tracks import TRACKS
from tests.fakes import FakeClient

OUT = {"concepts": [{"name": "Spark spread", "scope": "s"}, {"name": "Weird", "scope": "w"},
                    {"name": "Hedge ratio", "scope": "h"}]}


def test_tracks_are_the_eight_expected():
    assert [t["id"] for t in TRACKS] == [
        "market_fundamentals", "price_curve_modelling", "asset_valuation",
        "optimisation_dispatch", "storage_flexibility", "hedging_trading",
        "risk_management", "modern_markets"]


def test_tag_source_coerces_invalid_track_ids():
    reply = json.dumps({"mapping": {"Spark spread": "asset_valuation",
                                    "Weird": "not_a_track", "Hedge ratio": None}})
    m = tag_source(FakeClient([reply]), "m", "src", OUT)
    assert m == {"Spark spread": "asset_valuation", "Weird": None, "Hedge ratio": None}


def test_matrix_gaps_and_render():
    maps = {"a": {"x": "asset_valuation", "y": "asset_valuation"},
            "b": {"z": "asset_valuation", "w": None}}
    matrix = build_matrix(maps)
    assert matrix["asset_valuation"] == {"a": 2, "b": 1}
    g = gaps(matrix, min_sources=2)
    assert "asset_valuation" not in g and "modern_markets" in g and len(g) == 7
    md = render_coverage_md(matrix, {"a": "Paper A", "b": "Paper B"}, g)
    assert "Paper A" in md and "GAP" in md and "modern_markets" in md


def test_run_coverage_isolates_failures(tmp_path):
    outlines = {"a": OUT, "b": OUT}
    good = json.dumps({"mapping": {"Spark spread": "asset_valuation"}})
    c = FakeClient([good, "garbage", "garbage"])
    rep = run_coverage(outlines, c, "m", tmp_path, {"a": "A", "b": "B"})
    assert list(rep["failed"]) == ["b"]
    assert (tmp_path / "coverage_map.md").exists()
    assert set(json.loads((tmp_path / "_mappings.json").read_text())) == {"a"}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_coverage.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement**

`power-academy/academy/tracks.py`:
```python
TRACKS = [
    {"id": "market_fundamentals", "title": "Market fundamentals: price formation, merit order, market designs, products"},
    {"id": "price_curve_modelling", "title": "Price and curve modelling: mean reversion, spikes, forward curves, shaping"},
    {"id": "asset_valuation", "title": "Asset valuation: spark spreads, tolling, CCGT, real options"},
    {"id": "optimisation_dispatch", "title": "Optimisation and dispatch: LP/MIP, start-up value, unit commitment, switching"},
    {"id": "storage_flexibility", "title": "Storage and flexibility: hydro, pumped storage, swing, BESS"},
    {"id": "hedging_trading", "title": "Hedging and trading strategy: plant hedging, delta, rolling intrinsic"},
    {"id": "risk_management", "title": "Risk management: PaR, VaR, credit, limits, model risk"},
    {"id": "modern_markets", "title": "Modern markets: renewables-heavy, intraday, imbalance, China spot"},
]
```
`power-academy/academy/coverage.py`:
```python
import json
from pathlib import Path

from .llm import call_json
from .tracks import TRACKS

TRACK_IDS = [t["id"] for t in TRACKS]

TAG_SYSTEM = (
    "Map each concept to the single best-fitting curriculum track id, or null if none fits. "
    "Return ONLY JSON: {\"mapping\": {\"<concept name>\": \"<track id or null>\"}}. "
    "Track ids: " + ", ".join(f"{t['id']} ({t['title']})" for t in TRACKS)
)


def tag_source(client, model, source_id, outline) -> dict:
    names = "\n".join(f"- {c['name']}: {c['scope']}" for c in outline["concepts"])
    data = call_json(client, model, TAG_SYSTEM, f"Concepts from {source_id}:\n{names}",
                     max_tokens=2000)
    raw = data.get("mapping", {})
    return {c["name"]: (raw.get(c["name"]) if raw.get(c["name"]) in TRACK_IDS else None)
            for c in outline["concepts"]}


def build_matrix(mappings: dict) -> dict:
    matrix = {tid: {} for tid in TRACK_IDS}
    for sid, mp in mappings.items():
        for tid in mp.values():
            if tid:
                matrix[tid][sid] = matrix[tid].get(sid, 0) + 1
    return matrix


def gaps(matrix: dict, min_sources: int = 2) -> list:
    return [tid for tid, row in matrix.items() if len(row) < min_sources]


def render_coverage_md(matrix, titles, gap_ids) -> str:
    lines = ["# Coverage map (track × source)", "",
             "| Track | Sources | Concepts | Status |", "|---|---|---|---|"]
    for tid in TRACK_IDS:
        row = matrix[tid]
        srcs = ", ".join(f"{titles.get(s, s)} ({n})" for s, n in sorted(row.items()))
        lines.append(f"| {tid} | {srcs or '—'} | {sum(row.values())} | "
                     f"{'GAP' if tid in gap_ids else 'ok'} |")
    return "\n".join(lines) + "\n"


def run_coverage(outlines, client, model, out_dir, titles) -> dict:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    mappings, failed = {}, {}
    for sid, o in outlines.items():
        try:
            mappings[sid] = tag_source(client, model, sid, o)
        except Exception as e:
            failed[sid] = f"{type(e).__name__}: {e}"
    (out_dir / "_mappings.json").write_text(
        json.dumps(mappings, ensure_ascii=False, indent=1), encoding="utf-8")
    matrix = build_matrix(mappings)
    g = gaps(matrix)
    (out_dir / "coverage_map.md").write_text(
        render_coverage_md(matrix, titles, g), encoding="utf-8")
    return {"gaps": g, "failed": failed}
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_coverage.py -q`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/tracks.py power-academy/academy/coverage.py power-academy/tests/test_coverage.py
git commit -m "Add track definitions and track-by-source coverage map" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Graph validator and syllabus drafting

**Files:**
- Create: `power-academy/academy/graph.py`, `power-academy/academy/syllabus.py`
- Create: `power-academy/tests/test_graph.py`, `power-academy/tests/test_syllabus.py`

**Interfaces:**
- Consumes: `call_json` (Task 5), `TRACKS` (Task 7), `render_concept`/`validate_concept` (Task 4), `dump_yaml` (Task 1), outlines + mappings dicts (Tasks 6–7).
- Produces:
  - `graph.validate_graph(concepts: list[dict]) -> list[str]` — each dict has `id, level, prerequisites, sources, originality`; reports unknown prerequisite ids, cycles, orphans (chain ends at a root that is not `level == "foundation"`), and `synthesized` concepts with empty `sources`.
  - `syllabus.draft_track(client, model, track, items, source_ids) -> list[dict]` — items = `[{"source_id","name","scope"}]`; returns stubs `{id, track, level, prerequisites, markets, sources[{id,use}], originality, scope}`; invalid levels→`intermediate`, invalid markets dropped (empty→`["EU"]`), sources filtered to `source_ids`, `originality = "synthesized" if sources else "original"`.
  - `syllabus.stub_front_matter(stub) -> dict` (valid per `validate_concept`)
  - `syllabus.write_syllabus(stubs_by_track, root: Path) -> list[str]` — writes `syllabus/tracks.yaml`, `syllabus/graph.yaml` (`edges: [[prereq, id]]`), `concepts/<track>/<id>.md` stubs; returns graph validation errors.
  - `syllabus.render_review(stubs_by_track, errors, gap_ids) -> str` (includes per-track counts, errors, gaps, pilot suggestion `asset_valuation` + `hedging_trading`, and a sizing warning if total < 50 or > 120)
  - `syllabus.run_syllabus(outlines, mappings, client, model, root) -> dict` (`{"counts": {track: n}, "errors": [...]}`; writes `review/syllabus_review.md`)

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_graph.py`:
```python
from academy.graph import validate_graph


def c(id_, prereq=(), level="foundation", sources=("s",), orig="synthesized"):
    return {"id": id_, "level": level, "prerequisites": list(prereq),
            "sources": list(sources), "originality": orig}


def test_clean_graph_has_no_errors():
    g = [c("a"), c("b", ["a"], "intermediate"), c("d", ["b"], "advanced")]
    assert validate_graph(g) == []


def test_unknown_prerequisite_reported():
    assert validate_graph([c("a", ["zzz"])]) == ["a: unknown prerequisite 'zzz'"]


def test_cycle_reported():
    errs = validate_graph([c("a", ["b"]), c("b", ["a"])])
    assert any("cycle" in e for e in errs)


def test_orphan_root_must_be_foundation():
    errs = validate_graph([c("a", level="advanced")])
    assert errs == ["a: chain ends at non-foundation root 'a'"]


def test_synthesized_without_sources_reported_original_ok():
    assert validate_graph([c("a", sources=[])]) == ["a: synthesized concept has no sources"]
    assert validate_graph([c("a", sources=[], orig="original")]) == []
```
`power-academy/tests/test_syllabus.py`:
```python
import json

from academy.concepts import parse_concept, validate_concept
from academy.syllabus import (draft_track, render_review, run_syllabus,
                              stub_front_matter, write_syllabus)
from academy.tracks import TRACKS
from tests.fakes import FakeClient

AV = next(t for t in TRACKS if t["id"] == "asset_valuation")
REPLY = json.dumps({"concepts": [
    {"id": "option_basics", "scope": "calls and puts", "level": "foundation",
     "prerequisites": [], "markets": ["EU"], "sources": ["s1", "ghost"]},
    {"id": "spark_spread_option", "scope": "spread option", "level": "wizard",
     "prerequisites": ["option_basics"], "markets": ["MARS"], "sources": []},
]})


def test_draft_track_normalises_stubs():
    stubs = draft_track(FakeClient([REPLY]), "m", AV,
                        [{"source_id": "s1", "name": "x", "scope": "y"}], ["s1"])
    a, b = stubs
    assert a["sources"] == [{"id": "s1", "use": "background"}] and a["originality"] == "synthesized"
    assert b["level"] == "intermediate" and b["markets"] == ["EU"]
    assert b["sources"] == [] and b["originality"] == "original"
    assert a["track"] == "asset_valuation"
    assert validate_concept(stub_front_matter(a)) == []


def test_write_syllabus_files_and_errors(tmp_path):
    stubs = draft_track(FakeClient([REPLY]), "m", AV, [], ["s1"])
    errs = write_syllabus({"asset_valuation": stubs}, tmp_path)
    assert errs == []
    assert (tmp_path / "syllabus" / "tracks.yaml").exists()
    graph = (tmp_path / "syllabus" / "graph.yaml").read_text()
    assert "option_basics" in graph and "spark_spread_option" in graph
    fm, body = parse_concept(tmp_path / "concepts" / "asset_valuation" / "option_basics.md")
    assert fm["status"] == "stub" and "## Learning objectives" in body


def test_review_has_sizing_warning_pilot_and_gaps():
    md = render_review({"asset_valuation": [{"id": "a"}]}, ["x: bad"], ["modern_markets"])
    assert "asset_valuation: 1" in md and "x: bad" in md and "modern_markets" in md
    assert "fewer than 50" in md.lower() and "hedging_trading" in md


def test_run_syllabus_end_to_end(tmp_path):
    outlines = {"s1": {"concepts": [{"name": "Spark spread", "scope": "s"}]}}
    mappings = {"s1": {"Spark spread": "asset_valuation"}}
    replies = [json.dumps({"concepts": []}) for _ in TRACKS]
    replies[2] = REPLY
    rep = run_syllabus(outlines, mappings, FakeClient(replies), "m", tmp_path)
    assert rep["counts"]["asset_valuation"] == 2 and rep["errors"] == []
    assert (tmp_path / "review" / "syllabus_review.md").exists()
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_graph.py tests/test_syllabus.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `graph.py`**

`power-academy/academy/graph.py`:
```python
def validate_graph(concepts: list) -> list:
    by_id = {c["id"]: c for c in concepts}
    errs = []
    for c in concepts:
        for p in c["prerequisites"]:
            if p not in by_id:
                errs.append(f"{c['id']}: unknown prerequisite '{p}'")
        if c["originality"] == "synthesized" and not c["sources"]:
            errs.append(f"{c['id']}: synthesized concept has no sources")

    state = {}  # id -> 1 visiting, 2 done

    def visit(cid, path):
        if state.get(cid) == 2:
            return
        if state.get(cid) == 1:
            errs.append("cycle: " + " -> ".join(path + [cid]))
            return
        state[cid] = 1
        for p in by_id[cid]["prerequisites"]:
            if p in by_id:
                visit(p, path + [cid])
        state[cid] = 2

    for cid in by_id:
        visit(cid, [])

    for c in concepts:
        cur, hops = c, 0
        while cur["prerequisites"] and hops <= len(by_id):
            nxt = next((by_id[p] for p in cur["prerequisites"] if p in by_id), None)
            if nxt is None:
                break
            cur, hops = nxt, hops + 1
        if not cur["prerequisites"] and cur["level"] != "foundation":
            errs.append(f"{c['id']}: chain ends at non-foundation root '{cur['id']}'")
    return errs
```
(Behaviour: a chain that ends at a non-foundation root yields one error per descendant concept, naming the bad root.)

- [ ] **Step 4: Implement `syllabus.py`**

`power-academy/academy/syllabus.py`:
```python
import json
from pathlib import Path

from .concepts import LEVELS, MARKETS, render_concept
from .coverage import build_matrix, gaps
from .graph import validate_graph
from .io import dump_yaml
from .llm import call_json
from .tracks import TRACKS

SYSTEM = (
    "You are drafting a syllabus track for power-markets quants. Given concept names and "
    "scopes found in reference sources, propose 8-12 concept stubs for the track. "
    "Return ONLY JSON: {\"concepts\": [{\"id\": snake_case, \"scope\": one sentence, "
    "\"level\": foundation|intermediate|advanced, \"prerequisites\": [concept ids], "
    "\"markets\": subset of EU|GB|US|AU|CN, \"sources\": [source ids from the input]}]}. "
    "Include foundation concepts a learner needs even if no source covers them "
    "(empty sources). Do not summarise any source."
)

STUB_BODY = (
    "## Learning objectives\n\n## Intuition\n\n## Formal treatment\n\n"
    "## Worked example\n\n## Market variants\n\n## Common errors\n\n"
    "## Assessable questions\n"
)


def draft_track(client, model, track, items, source_ids) -> list:
    listing = "\n".join(f"- [{i['source_id']}] {i['name']}: {i['scope']}" for i in items) or "(none)"
    data = call_json(client, model, SYSTEM,
                     f"Track: {track['id']} — {track['title']}\n\nSource concepts:\n{listing}",
                     max_tokens=4000)
    stubs = []
    for c in data.get("concepts", []):
        srcs = [{"id": s, "use": "background"} for s in c.get("sources", []) if s in source_ids]
        markets = [m for m in c.get("markets", []) if m in MARKETS] or ["EU"]
        stubs.append({
            "id": c["id"], "track": track["id"], "scope": c.get("scope", ""),
            "level": c.get("level") if c.get("level") in LEVELS else "intermediate",
            "prerequisites": list(c.get("prerequisites", [])),
            "markets": markets, "sources": srcs,
            "originality": "synthesized" if srcs else "original"})
    return stubs


def stub_front_matter(stub: dict) -> dict:
    return {"id": stub["id"], "track": stub["track"], "level": stub["level"],
            "prerequisites": stub["prerequisites"], "markets": stub["markets"],
            "status": "stub", "sources": stub["sources"],
            "originality": stub["originality"],
            "translations": {"zh": {"status": "none", "en_hash": None}}}


def write_syllabus(stubs_by_track: dict, root: Path) -> list:
    root = Path(root)
    flat = [s for stubs in stubs_by_track.values() for s in stubs]
    titles = {t["id"]: t["title"] for t in TRACKS}
    dump_yaml({"tracks": [{"id": tid, "title": titles[tid], "concepts": stubs}
                          for tid, stubs in stubs_by_track.items()]},
              root / "syllabus" / "tracks.yaml")
    dump_yaml({"edges": [[p, s["id"]] for s in flat for p in s["prerequisites"]]},
              root / "syllabus" / "graph.yaml")
    for s in flat:
        d = root / "concepts" / s["track"]
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{s['id']}.md").write_text(
            render_concept(stub_front_matter(s), STUB_BODY), encoding="utf-8")
    return validate_graph([
        {"id": s["id"], "level": s["level"], "prerequisites": s["prerequisites"],
         "sources": [x["id"] for x in s["sources"]], "originality": s["originality"]}
        for s in flat])


def render_review(stubs_by_track, errors, gap_ids) -> str:
    total = sum(len(v) for v in stubs_by_track.values())
    lines = ["# Syllabus review", "", f"Total concept stubs: {total}", ""]
    if total < 50:
        lines += ["WARNING: fewer than 50 stubs — too coarse to generate games from.", ""]
    elif total > 120:
        lines += ["WARNING: more than 120 stubs — too granular to review.", ""]
    lines += ["## Concepts per track"] + [f"- {t}: {len(v)}" for t, v in stubs_by_track.items()]
    lines += ["", "## Coverage gaps (fewer than 2 sources)"] + [f"- {g}" for g in gap_ids]
    lines += ["", "## Validator errors"] + ([f"- {e}" for e in errors] or ["- none"])
    lines += ["", "## Proposed pilot", "- asset_valuation + hedging_trading "
              "(strongest sources, easiest labs)", "",
              "Owner gate: edit `syllabus/tracks.yaml` and approve before Phase 2."]
    return "\n".join(lines) + "\n"


def run_syllabus(outlines, mappings, client, model, root) -> dict:
    root = Path(root)
    source_ids = set(outlines)
    stubs_by_track = {}
    for t in TRACKS:
        items = [{"source_id": sid, "name": name, "scope": next(
                    c["scope"] for c in outlines[sid]["concepts"] if c["name"] == name)}
                 for sid, mp in mappings.items() for name, tid in mp.items() if tid == t["id"]]
        stubs_by_track[t["id"]] = draft_track(client, model, t, items, source_ids)
    errors = write_syllabus(stubs_by_track, root)
    gap_ids = gaps(build_matrix(mappings))
    (root / "review").mkdir(parents=True, exist_ok=True)
    (root / "review" / "syllabus_review.md").write_text(
        render_review(stubs_by_track, errors, gap_ids), encoding="utf-8")
    return {"counts": {t: len(v) for t, v in stubs_by_track.items()}, "errors": errors}
```
- [ ] **Step 5: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_graph.py tests/test_syllabus.py -q`
Expected: 9 passed.

- [ ] **Step 6: Commit**

```bash
git add power-academy/academy/graph.py power-academy/academy/syllabus.py power-academy/tests/test_graph.py power-academy/tests/test_syllabus.py
git commit -m "Add concept graph validator and syllabus drafting with review report" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 9: CLI, local validate, and bundle/push/pull plumbing

**Files:**
- Create: `power-academy/academy/cli.py`, `power-academy/tests/test_cli.py`

**Interfaces:**
- Consumes: everything from Tasks 1–8.
- Produces: `python -m academy.cli <command>` with commands `register`, `extract`, `validate`, `outline [--only ID,ID]`, `coverage`, `syllabus`, `bundle --bucket B --prefix P`, `push-results --bucket B --prefix P`, `pull-results --bucket B --prefix P`; `cli.validate_repo(root: Path) -> list[str]` (checks every `concepts/**/*.md` front matter via `validate_concept`, EN/ZH pairs via `check_pair` + `zh_is_stale`, and the whole graph via `validate_graph`); `cli.ROOT`, `cli.LIB`, `cli.PRACTICE_ROOTS`.

- [ ] **Step 1: Write the failing test**

`power-academy/tests/test_cli.py`:
```python
from academy.concepts import body_hash, render_concept
from academy.cli import validate_repo


def _fm(id_, prereq=(), level="foundation", zh=None):
    return {"id": id_, "track": "asset_valuation", "level": level,
            "prerequisites": list(prereq), "markets": ["EU"], "status": "stub",
            "sources": [{"id": "s", "use": "background"}], "originality": "synthesized",
            "translations": {"zh": zh or {"status": "none", "en_hash": None}}}


def _write(root, name, fm, body):
    p = root / "concepts" / "asset_valuation" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(render_concept(fm, body), encoding="utf-8")


def _glossary(root):
    g = root / "glossary"
    g.mkdir()
    (g / "terms.yaml").write_text("terms:\n  - {en: spark spread, zh: 火花价差}\n", encoding="utf-8")


def test_validate_repo_clean(tmp_path):
    _glossary(tmp_path)
    en = "The spark spread.\n"
    _write(tmp_path, "a.md", _fm("a", zh={"status": "drafted", "en_hash": body_hash(en)}), en)
    _write(tmp_path, "a.zh.md", {"id": "a"}, "火花价差。\n")
    assert validate_repo(tmp_path) == []


def test_validate_repo_reports_stale_glossary_and_graph(tmp_path):
    _glossary(tmp_path)
    en = "The spark spread.\n"
    _write(tmp_path, "a.md", _fm("a", zh={"status": "drafted", "en_hash": "stale"}), en)
    _write(tmp_path, "a.zh.md", {"id": "a"}, "发电价差。\n")
    _write(tmp_path, "b.md", _fm("b", ["missing"], "intermediate"), "x\n")
    errs = " | ".join(validate_repo(tmp_path))
    assert "a: zh translation is stale" in errs
    assert "spark spread" in errs
    assert "unknown prerequisite 'missing'" in errs


def test_zh_file_without_en_reported(tmp_path):
    _glossary(tmp_path)
    _write(tmp_path, "orphan.zh.md", {"id": "orphan"}, "x\n")
    assert any("orphan.zh.md" in e and "no English" in e for e in validate_repo(tmp_path))
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_cli.py -q`
Expected: FAIL (`ModuleNotFoundError: academy.cli`).

- [ ] **Step 3: Implement `cli.py`**

`power-academy/academy/cli.py`:
```python
import argparse
import json
import os
import tarfile
from pathlib import Path

from . import coverage as cv
from . import extract as ex
from . import llm, outline as ol, registry as rg, syllabus as sy
from .concepts import parse_concept, validate_concept, zh_is_stale
from .glossary import check_pair, load_glossary
from .graph import validate_graph
from .io import dump_yaml

ROOT = Path(__file__).resolve().parent.parent
_OD = Path.home() / "Library/CloudStorage/OneDrive-Personal"
LIB = _OD / "Structure/Asset Modelling/Power"
PRACTICE_ROOTS = [_OD / "company/SEE/power", _OD / "company/SEE/ote2/dipeng"]
OUTLINE_MODEL = os.environ.get("ACADEMY_OUTLINE_MODEL", "claude-sonnet-4-6")
TAG_MODEL = os.environ.get("ACADEMY_TAG_MODEL", "claude-haiku-4-5-20251001")
RESULT_DIRS = ["inventory", "syllabus", "review", "concepts"]


def validate_repo(root: Path) -> list:
    root = Path(root)
    errs, graph_in = [], []
    terms = load_glossary(root / "glossary" / "terms.yaml")
    cdir = root / "concepts"
    for en_path in sorted(p for p in cdir.rglob("*.md") if not p.name.endswith(".zh.md")):
        fm, body = parse_concept(en_path)
        errs += [f"{fm.get('id', en_path.name)}: {e}"
                 for e in validate_concept(fm, root / "labs")]
        if "id" not in fm:
            continue
        graph_in.append({"id": fm["id"], "level": fm["level"],
                         "prerequisites": fm["prerequisites"],
                         "sources": [s["id"] for s in fm["sources"]],
                         "originality": fm["originality"]})
        zh_path = en_path.with_name(en_path.stem + ".zh.md")
        if zh_path.exists():
            _, zh_body = parse_concept(zh_path)
            zh_tr = fm["translations"]["zh"]
            if zh_is_stale(fm, body, zh_tr):
                errs.append(f"{fm['id']}: zh translation is stale")
            errs += [f"{fm['id']}: {e}" for e in check_pair(body, zh_body, terms)]
    for zh_path in sorted(cdir.rglob("*.zh.md")):
        if not zh_path.with_name(zh_path.name[:-len(".zh.md")] + ".md").exists():
            errs.append(f"{zh_path.name}: no English source file")
    errs += validate_graph(graph_in) if graph_in else []
    return errs


def _entries():
    return rg.load_index(ROOT / "sources" / "index.yaml")


def _client():
    import anthropic
    return anthropic.Anthropic()


def cmd_register(_a):
    seen = set()
    entries = rg.register_library(LIB, seen)
    for r in PRACTICE_ROOTS:
        entries += rg.register_practice(r, seen)
    rg.write_index(entries, ROOT / "sources" / "index.yaml")
    dump_yaml({"models": rg.models_catalogue(LIB)}, ROOT / "sources" / "models_catalogue.yaml")
    by = {}
    for e in entries:
        by[(e.source_class, e.type)] = by.get((e.source_class, e.type), 0) + 1
    print("registered", len(entries), by)


def cmd_extract(_a):
    rep = ex.extract_all(_entries(), ROOT / "cache")
    print("extract counts", rep["counts"])


def cmd_validate(_a):
    errs = validate_repo(ROOT)
    print("\n".join(errs) or "ok")
    raise SystemExit(1 if errs else 0)


def cmd_outline(a):
    only = set(a.only.split(",")) if a.only else None
    rep = ol.run_outlines(_entries(), ROOT / "cache", ROOT / "inventory",
                          _client(), OUTLINE_MODEL, only)
    print({"ok": len(rep["ok"]), "failed": rep["failed"], "skipped": rep["skipped"]},
          "tokens", llm.USAGE)


def cmd_coverage(_a):
    outlines = json.loads((ROOT / "inventory" / "_outlines.json").read_text(encoding="utf-8"))
    titles = {e.id: e.title for e in _entries()}
    rep = cv.run_coverage(outlines, _client(), TAG_MODEL, ROOT / "inventory", titles)
    print(rep, "tokens", llm.USAGE)


def cmd_syllabus(_a):
    outlines = json.loads((ROOT / "inventory" / "_outlines.json").read_text(encoding="utf-8"))
    mappings = json.loads((ROOT / "inventory" / "_mappings.json").read_text(encoding="utf-8"))
    rep = sy.run_syllabus(outlines, mappings, _client(), OUTLINE_MODEL, ROOT)
    print(rep, "tokens", llm.USAGE)


def _s3():
    import boto3
    return boto3.client("s3")


def cmd_bundle(a):
    out = ROOT / "bundle.tar.gz"
    with tarfile.open(out, "w:gz") as t:
        t.add(ROOT / "academy", arcname="power-academy/academy")
        for d in ("cache", "sources", "glossary"):
            t.add(ROOT / d, arcname=f"power-academy/{d}")
    _s3().upload_file(str(out), a.bucket, f"{a.prefix}/bundle.tar.gz")
    print("uploaded", out.stat().st_size, "bytes")


def cmd_push_results(a):
    out = Path("/tmp/results.tar.gz")
    with tarfile.open(out, "w:gz") as t:
        for d in RESULT_DIRS:
            if (ROOT / d).exists():
                t.add(ROOT / d, arcname=d)
    _s3().upload_file(str(out), a.bucket, f"{a.prefix}/results.tar.gz")


def cmd_pull_results(a):
    out = ROOT / "results.tar.gz"
    _s3().download_file(a.bucket, f"{a.prefix}/results.tar.gz", str(out))
    with tarfile.open(out) as t:
        t.extractall(ROOT)


def main(argv=None):
    p = argparse.ArgumentParser(prog="academy")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name, fn in [("register", cmd_register), ("extract", cmd_extract),
                     ("validate", cmd_validate), ("coverage", cmd_coverage),
                     ("syllabus", cmd_syllabus)]:
        sub.add_parser(name).set_defaults(fn=fn)
    o = sub.add_parser("outline")
    o.add_argument("--only")
    o.set_defaults(fn=cmd_outline)
    for name, fn in [("bundle", cmd_bundle), ("push-results", cmd_push_results),
                     ("pull-results", cmd_pull_results)]:
        s = sub.add_parser(name)
        s.add_argument("--bucket", required=True)
        s.add_argument("--prefix", default="power-academy")
        s.set_defaults(fn=fn)
    a = p.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run to verify pass, then full suite**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests -q`
Expected: all tests pass (about 35).

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/cli.py power-academy/tests/test_cli.py
git commit -m "Add academy CLI, repo validator, and S3 bundle/results plumbing" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Run Phase 0 locally — register and extract (real library)

**Files:**
- Generated (commit): `power-academy/sources/index.yaml`, `power-academy/sources/models_catalogue.yaml`
- Generated (git-ignored): `power-academy/cache/*`

**Interfaces:**
- Consumes: CLI (Task 9). Produces: `sources/index.yaml` and populated `cache/` used by Task 11.

- [ ] **Step 1: Register**

Run (background, OneDrive can be slow): `cd power-academy && ~/.venvs/bess-platform/bin/python -m academy.cli register`
Expected: a line like `registered N {('library','pdf'): 36, ('practice','folder'): ~91, ...}`. Sanity: library PDFs ≈ 36, practice folders ≈ 26 + 65 (some `archived`, `New folder` type names are fine).

- [ ] **Step 2: Extract (throttled; run in background and poll the output file)**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m academy.cli extract`
Expected: `extract counts {...}` with `ok` ≫ `failed`. Read `cache/_report.json`: every `unsupported` (legacy `.ppt`/`.doc`) and `no_text`/`failed` id is listed.

- [ ] **Step 3: Triage non-ok sources with the owner**

Show the owner the `unsupported`, `no_text` and `failed` lists. For legacy `.ppt` (15 files), offer: convert with LibreOffice (`soffice --headless --convert-to pptx`) if installed, or skip. Do not convert or delete anything without the owner's decision.

- [ ] **Step 4: Commit generated index (no cache, no source copies)**

```bash
git add power-academy/sources/index.yaml power-academy/sources/models_catalogue.yaml
git commit -m "Register power library and practice folders into sources index" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Run LLM stages on Fargate (pilot of 3, then full)

**Files:**
- Generated (commit after owner review): `power-academy/inventory/*.md`, `inventory/_outlines.json`, `inventory/_mappings.json`, `inventory/coverage_map.md`, `syllabus/*`, `concepts/**`, `review/syllabus_review.md`

**Interfaces:**
- Consumes: CLI (Task 9), populated `cache/` (Task 10). Produces: Phase 0–1 deliverables.

**Confirmation gate:** this task sends text to the Anthropic API (including `practice` text) and writes to S3 and ECS. Get explicit in-session confirmation of: bucket, task definition, and the 3-source pilot cost before Step 3; get a second confirmation before the full run (Step 6).

- [ ] **Step 1: Choose the staging bucket and a task definition that already carries `ANTHROPIC_API_KEY`**

Run: `aws s3 ls --region ap-southeast-1` and pick the existing project bucket (staging is temporary, S3 is used only because the one-off task has no inbound channel).
Run: `aws ecs describe-task-definition --task-definition bess-platform-hermes --query 'taskDefinition.containerDefinitions[0].environment[].name' --region ap-southeast-1` and confirm `ANTHROPIC_API_KEY` is present; otherwise check `bess-spot-markets`. Use that family's *current service* tdArn: `aws ecs describe-services --cluster bess-platform-cluster --service <svc> --query 'services[0].taskDefinition' --output text --region ap-southeast-1`. Run-task only — never `update-service`.
Expected: a bucket name, a tdArn, container name noted.

- [ ] **Step 2: Upload the bundle**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m academy.cli bundle --bucket <BUCKET> --prefix power-academy`
Expected: `uploaded <N> bytes`.

- [ ] **Step 3: Pilot — outline 3 sources (pick 2 library + 1 practice from `sources/index.yaml`)**

Run (NAT-routed config copied from the nodal-writer probe; reuse the exact `--network-configuration` used for other one-off probes):
```bash
aws ecs run-task --cluster bess-platform-cluster --launch-type FARGATE --region ap-southeast-1 \
  --task-definition <TD_ARN> --network-configuration '<NAT network config>' \
  --overrides '{"containerOverrides":[{"name":"<CONTAINER>","command":["sh","-c","pip install -q pyyaml pymupdf python-pptx python-docx && python -c \"import boto3;boto3.client(\\\"s3\\\").download_file(\\\"<BUCKET>\\\",\\\"power-academy/bundle.tar.gz\\\",\\\"/tmp/b.tgz\\\")\" && mkdir -p /work && tar xzf /tmp/b.tgz -C /work && cd /work/power-academy && python -m academy.cli outline --only <ID1>,<ID2>,<ID3> && python -m academy.cli push-results --bucket <BUCKET> --prefix power-academy"]}]}'
```
Expected: task starts; watch CloudWatch logs for `{'ok': 3, ...} tokens {...}` and exit code 0 (`aws ecs describe-tasks`).

- [ ] **Step 4: Pull results and hand-check the 3 outlines against the originals**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m academy.cli pull-results --bucket <BUCKET>`
Check: each `inventory/<id>.md` names concepts with one-line scope and contains no paraphrased argument or quoted text. If outlines paraphrase, tighten `SYSTEM_PROMPT` in `academy/outline.py` (add a failing test asserting the new phrase first), rebuild the bundle, re-pilot.

- [ ] **Step 5: Cost check**

From the logged `tokens {"in": …, "out": …}` extrapolate to all outline-able sources (`ok` count from Task 10) at the model's list price. Report the projected full-run cost to the owner.

- [ ] **Step 6: Full run (after second confirmation)**

Same command as Step 3 but replace the command tail with `python -m academy.cli outline && python -m academy.cli coverage && python -m academy.cli syllabus && python -m academy.cli push-results --bucket <BUCKET> --prefix power-academy`. Then `pull-results`.
Expected: `inventory/coverage_map.md`, `syllabus/tracks.yaml`, `syllabus/graph.yaml`, `concepts/<track>/*.md`, `review/syllabus_review.md`.

- [ ] **Step 7: Validate locally**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m academy.cli validate`
Expected: `ok`, or a list of graph errors (unknown prerequisites across tracks are expected on first draft). Put the remaining errors in the owner review; do not hand-edit generated files before the owner gate.

- [ ] **Step 8: Commit generated inventory and syllabus drafts**

```bash
git add power-academy/inventory power-academy/syllabus power-academy/concepts power-academy/review
git commit -m "Add Phase 0 inventory outlines, coverage map and draft syllabus" -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 9: Owner gates**

Present `inventory/coverage_map.md` (Phase 0 gate) and `review/syllabus_review.md` (Phase 1 gate). Stop. Phase 2 needs its own spec.

---

## Self-Review

**Spec coverage:** §3 structure → Tasks 1, 2, 9, 11 (folders created by the CLI/pipeline; `labs/` is created in Phase 2, referenced by `validate_concept`). §4 schema + rules → Task 4. §5 bilingual: glossary, checker, staleness, independent gates → Tasks 4, 9 (`validate_repo`); EN-first/zh-origin authoring is a Phase 2 concern. §6 Phase 0 steps 1–5 → Tasks 2, 3, 6, 7, 10, 11; throttled hydration, no silent drops, no_text flag, cost pilot on 3 → Tasks 3, 10, 11. Fargate one-off + S3 justification → Tasks 9, 11. §7 Phase 1: tracks.yaml, graph.yaml, validator, review doc, sizing, pilot suggestion → Task 8, 11. §8 testing → every task. §9 risks → Review Focus + Task 11 gates. §11 deliverables incl. README → Task 1. Gap: spec's "spot-check 5 sources" is done as 3-source pilot hand-check in Task 11 Step 4; owner can extend to 5.

**Placeholder scan:** angle-bracket values in Task 11 (`<BUCKET>`, `<TD_ARN>`, `<CONTAINER>`, `<ID1>`) are runtime values discovered in Step 1 / Task 10, not plan gaps. No TBD/TODO text; all code steps contain code.

**Type consistency:** `SourceEntry` fields used identically in Tasks 2, 3, 6, 9. `call_json` signature consistent in Tasks 5–8. `run_outlines` writes `_outlines.json`, consumed by `cmd_coverage`/`cmd_syllabus`; `run_coverage` writes `_mappings.json`, consumed by `cmd_syllabus`; `run_syllabus` reads `outlines`/`mappings` dicts matching those shapes. `validate_graph` input keys (`id, level, prerequisites, sources, originality`) match both `write_syllabus` and `validate_repo`.
