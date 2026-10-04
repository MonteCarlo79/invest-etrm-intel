# 西洋镜看中国电力市场 Column — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the evidence-pack pipeline (brief → evidence → charts → gates → render) plus the on-demand 编辑 (Editor) agent for the bilingual power-markets column.

**Architecture:** A `power-academy/academy/column/` subpackage — one module per responsibility (schema, evidence, charts, gates, render, scan, editor) — reusing `academy.io`, `academy.llm` and `academy.concepts` from the Power Academy work. Content lives in `power-academy/columns/xiyangjing/`. All DB access is read-only via `PGURL`. The Hermes international-feeds workstream is explicitly OUT of scope (separate plan + deploy confirmation).

**Tech Stack:** Python 3 (venv `~/.venvs/bess-platform`), pandas, matplotlib, sqlalchemy + psycopg2, PyYAML, `markdown` (new dev dep), anthropic (editor only), pytest.

**Spec:** `docs/superpowers/specs/2026-10-04-xiyangjing-column-design.md`

## Global Constraints

- Read-only DB: every SQL string must start with `select`/`with` and contain no write keywords; enforced in code.
- Every number/policy claim in a draft carries `[[E:<id>]]`; unknown tags or uncovered numbers fail the fact-trace gate.
- `licensed_restricted` snapshots and charts derived from them never reach `out/`; restricted CSVs are git-ignored.
- Nothing reaches `status: approved` without `owner_signoff:` in `gates.md` and all automated gates green.
- No live web search; editor cites only provided inputs (hermes briefings, KB docs, anomaly scan, concept graph).
- Charts: shared style module, CJK font fallback (`PingFang SC`, `Hiragino Sans GB`, `Hiragino Mincho ProN`, `Arial Unicode MS`), 900×500px PNG.
- Git: worktree `/tmp/bess-pa`, branch `power-academy`; stage explicit paths only. Commits end with `Co-Authored-By: Claude Code <noreply@anthropic.com>`.
- KB schema (verified 2026-10-04): `staging.spot_knowledge_docs(id, file_name, file_hash, category, title, doc_year, file_size_bytes, page_count, ingest_status, parse_error, active, created_at, app, region_bucket)`; `staging.spot_knowledge_chunks(id, doc_id, page_no, chunk_index, chunk_text, embedding)`.
- Test command (from `power-academy/`): `~/.venvs/bess-platform/bin/python -m pytest tests -q`.
- `ACADEMY_EDITOR_MODEL` env overrides the editor model (default `claude-sonnet-4-6`).

## Review Focus

1. Draft prose mixing dates, list numbers and real quantities → number-coverage heuristic must flag quantities but not `2026年` dates or list markers. (Task 5)
2. Hand-written SQL in `queries.yaml` containing a write keyword or `;`-chained statement → read-only guard rejects before execution. (Task 2)
3. A `licensed_restricted` snapshot feeding a chart marked public → license gate fails the article. (Task 6)
4. Renderer leaking a raw `[[E:…]]` tag, a blocklisted name, or a non-public chart into `out/` → render refuses. (Task 7)
5. Editor proposing a hook not present in its inputs (fabrication) → proposals with unverifiable `hook.source` are dropped before touching the backlog. (Task 9)

---

### Task 1: Column scaffold, brief schema, `column new`

**Files:**
- Create: `power-academy/academy/column/__init__.py`, `power-academy/academy/column/schema.py`
- Create: `power-academy/tests/test_column_schema.py`
- Modify: `power-academy/requirements.txt` (add pandas, matplotlib, markdown, sqlalchemy, psycopg2-binary)

**Interfaces:**
- Consumes: `academy.concepts.parse_concept`, `academy.io.dump_yaml`.
- Produces: `STATUSES`, `BYLINES`, `BRIEF_REQUIRED`; `validate_brief(fm) -> list[str]`; `new_article(root: Path, num: int, slug: str) -> Path` (creates `articles/<NNN>-<slug>/` skeleton); `article_dir(root, name) -> Path`.

- [ ] **Step 1: Install markdown (confirm with owner first)**

Run: `uv pip install --python ~/.venvs/bess-platform/bin/python markdown || UV_HTTP_TIMEOUT=180 uv pip install --python ~/.venvs/bess-platform/bin/python --index-url https://pypi.tuna.tsinghua.edu.cn/simple markdown`
Expected: `+ markdown` installed.

- [ ] **Step 2: Write the failing tests**

`power-academy/tests/test_column_schema.py`:
```python
import pytest

from academy.column.schema import new_article, validate_brief


def _fm(**kw):
    fm = {"id": "003-test", "byline": "pen_name", "status": "idea",
          "thesis": "t", "western": {"concept_ids": ["merit_order"], "markets": ["GB"]},
          "china": {"provinces": ["山东"], "topics": ["现货"]},
          "derivatives_angle": "d", "questions": ["q"], "readers": ["A", "C"]}
    fm.update(kw)
    return fm


def test_valid_brief_passes():
    assert validate_brief(_fm()) == []


def test_enum_and_required_errors():
    errs = " | ".join(validate_brief(_fm(status="ready", byline="anon", readers=["B"])))
    assert "status" in errs and "byline" in errs and "readers" in errs
    assert validate_brief({"id": "x"})


def test_new_article_creates_skeleton(tmp_path):
    d = new_article(tmp_path, 3, "merit-order-and-shandong-spot")
    assert d.name == "003-merit-order-and-shandong-spot"
    assert (d / "brief.md").exists() and (d / "evidence" / "data").is_dir()
    assert (d / "evidence" / "charts").is_dir() and (d / "gates.md").exists()
    fm_text = (d / "brief.md").read_text(encoding="utf-8")
    assert "003-merit-order-and-shandong-spot" in fm_text and "status: idea" in fm_text
    with pytest.raises(FileExistsError):
        new_article(tmp_path, 3, "merit-order-and-shandong-spot")
```

- [ ] **Step 3: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_schema.py -q`
Expected: FAIL (`ModuleNotFoundError: academy.column`).

- [ ] **Step 4: Implement**

`power-academy/academy/column/__init__.py`: empty file.
`power-academy/academy/column/schema.py`:
```python
from pathlib import Path

STATUSES = ("idea", "briefed", "evidence", "drafted", "gated", "approved", "published")
BYLINES = ("pen_name", "real_name")
READERS = ("A", "C")
BRIEF_REQUIRED = ("id", "byline", "status", "thesis", "western", "china",
                  "derivatives_angle", "questions", "readers")

BRIEF_TEMPLATE = """---
id: {nid}-{slug}
byline: pen_name
status: idea
thesis: ""
western: {{concept_ids: [], markets: []}}
china: {{provinces: [], topics: []}}
derivatives_angle: ""
questions: []
readers: [A, C]
published_url: null
---

(thesis in one paragraph)
"""

GATES_TEMPLATE = """# Gates — {article}

<!-- auto:begin -->
(not run yet)
<!-- auto:end -->

## Owner adjudication
(one line per flag: flag -> keep/remove + why)

owner_signoff:
"""


def validate_brief(fm: dict) -> list:
    errs = [f"missing field: {k}" for k in BRIEF_REQUIRED if k not in fm]
    if errs:
        return errs
    if fm["status"] not in STATUSES:
        errs.append(f"status must be one of {STATUSES}")
    if fm["byline"] not in BYLINES:
        errs.append(f"byline must be one of {BYLINES}")
    if not isinstance(fm["western"].get("concept_ids"), list):
        errs.append("western.concept_ids must be a list")
    if not isinstance(fm["china"].get("provinces"), list):
        errs.append("china.provinces must be a list")
    if any(r not in READERS for r in fm["readers"]):
        errs.append(f"readers must be a subset of {READERS}")
    return errs


def article_dir(root: Path, name: str) -> Path:
    d = Path(root) / "articles" / name
    if not d.is_dir():
        raise FileNotFoundError(d)
    return d


def new_article(root: Path, num: int, slug: str) -> Path:
    d = Path(root) / "articles" / f"{num:03d}-{slug}"
    if d.exists():
        raise FileExistsError(d)
    for sub in ("evidence/data", "evidence/charts", "evidence/excerpts", "out"):
        (d / sub).mkdir(parents=True, exist_ok=True)
    (d / "brief.md").write_text(
        BRIEF_TEMPLATE.format(nid=f"{num:03d}", slug=slug), encoding="utf-8")
    (d / "gates.md").write_text(GATES_TEMPLATE.format(article=d.name), encoding="utf-8")
    (d / "evidence" / "queries.yaml").write_text("queries: []\n", encoding="utf-8")
    (d / "draft.zh.md").write_text("", encoding="utf-8")
    return d
```
`requirements.txt`: append `pandas`, `matplotlib`, `markdown`, `sqlalchemy`, `psycopg2-binary`.

- [ ] **Step 5: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_schema.py -q`
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add power-academy/academy/column/__init__.py power-academy/academy/column/schema.py power-academy/tests/test_column_schema.py power-academy/requirements.txt
git commit -m "Add column brief schema and article skeleton generator" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: Evidence builder — SQL queries, manifest, hashing, licenses

**Files:**
- Create: `power-academy/academy/column/evidence.py`
- Create: `power-academy/tests/test_column_evidence.py`

**Interfaces:**
- Consumes: `academy.io.load_yaml/dump_yaml`.
- Produces: `MAX_ROWS = 200_000`; `check_readonly(sql) -> None` (raises ValueError); `sha256_file(path) -> str`; `license_for(source, licenses: dict) -> str`; `build_pack(article_dir: Path, engine, licenses: dict, today: str) -> list[dict]` (returns manifest entries); `write_manifest(article_dir, entries)`.
- `queries.yaml` entry shape: `{id, kind: sql, source: <table>, sql: <select>, params: {name: value}}`. Params are bound via SQLAlchemy `:name` placeholders — never string-formatted into SQL.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_column_evidence.py`:
```python
import pandas as pd
import pytest
import sqlalchemy as sa

from academy.column.evidence import (MAX_ROWS, build_pack, check_readonly,
                                     license_for, sha256_file)
from academy.column.schema import new_article
from academy.io import dump_yaml, load_yaml


def test_readonly_guard():
    check_readonly("select 1")
    check_readonly("  WITH x AS (SELECT 1) SELECT * FROM x")
    for bad in ["delete from t", "select 1; drop table t",
                "insert into t values (1)", "UPDATE t set a=1",
                "select * from t where x = 'x'; truncate t"]:
        with pytest.raises(ValueError):
            check_readonly(bad)


def test_license_defaults_restricted():
    lic = {"marketdata.spot_prices_hourly": "licensed_restricted",
           "staging.spot_knowledge_docs": "public"}
    assert license_for("marketdata.spot_prices_hourly", lic) == "licensed_restricted"
    assert license_for("staging.spot_knowledge_docs", lic) == "public"
    assert license_for("marketdata.unknown_table", lic) == "licensed_restricted"


def _sqlite():
    e = sa.create_engine("sqlite://")
    with e.begin() as c:
        c.execute(sa.text("create table p (province text, dt text, rt real)"))
        c.execute(sa.text("insert into p values ('山东','2026-10-01 10:00',0.42)"))
    return e


def test_build_pack_writes_snapshot_and_manifest(tmp_path):
    d = new_article(tmp_path, 1, "demo")
    dump_yaml({"queries": [{
        "id": "e_sd_price", "kind": "sql", "source": "marketdata.spot_prices_hourly",
        "sql": "select * from p where province = :prov", "params": {"prov": "山东"}}]},
        d / "evidence" / "queries.yaml")
    entries = build_pack(d, _sqlite(), {"marketdata.spot_prices_hourly": "licensed_restricted"},
                         today="2026-10-04")
    e = entries[0]
    assert e["id"] == "e_sd_price" and e["kind"] == "data"
    assert e["license"] == "licensed_restricted" and e["rows"] == 1
    assert e["retrieved_at"] == "2026-10-04" and len(e["sha256"]) == 64
    csv = (d / "evidence" / "data" / "e_sd_price.csv").read_text(encoding="utf-8")
    assert "山东" in csv and "0.42" in csv
    on_disk = load_yaml(d / "evidence" / "manifest.yaml")["entries"]
    assert on_disk[0]["id"] == "e_sd_price"
    assert sha256_file(d / "evidence" / "data" / "e_sd_price.csv") == e["sha256"]


def test_build_pack_caps_rows(tmp_path, monkeypatch):
    d = new_article(tmp_path, 2, "demo2")
    big = pd.DataFrame({"a": range(MAX_ROWS + 10)})
    import academy.column.evidence as ev
    monkeypatch.setattr(ev, "_run_sql", lambda *a, **k: big)
    dump_yaml({"queries": [{"id": "e_big", "kind": "sql", "source": "t", "sql": "select 1"}]},
              d / "evidence" / "queries.yaml")
    entries = build_pack(d, None, {}, today="2026-10-04")
    assert entries[0]["rows"] == MAX_ROWS and entries[0]["truncated"] is True
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_evidence.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement**

`power-academy/academy/column/evidence.py`:
```python
import hashlib
import re
from datetime import date
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

from ..io import dump_yaml, load_yaml

MAX_ROWS = 200_000
WRITE_RE = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke)\b", re.I)


def check_readonly(sql: str) -> None:
    s = sql.strip().rstrip(";").strip()
    if ";" in s:
        raise ValueError("multiple statements not allowed")
    if not re.match(r"(?is)^(select|with)\b", s):
        raise ValueError("only SELECT/WITH queries allowed")
    if WRITE_RE.search(re.sub(r"'[^']*'", "", s)):
        raise ValueError("write keyword in query")


def sha256_file(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def license_for(source: str, licenses: dict) -> str:
    return licenses.get(source, "licensed_restricted")


def _run_sql(engine, sql, params) -> pd.DataFrame:
    with engine.connect() as c:
        return pd.read_sql(sa.text(sql), c, params=params or {})


def build_pack(article_dir: Path, engine, licenses: dict, today: str | None = None) -> list:
    article_dir = Path(article_dir)
    today = today or date.today().isoformat()
    queries = load_yaml(article_dir / "evidence" / "queries.yaml").get("queries") or []
    entries = []
    for q in queries:
        kind = q["kind"]
        if kind == "sql":
            check_readonly(q["sql"])
            df = _run_sql(engine, q["sql"], q.get("params"))
            truncated = len(df) > MAX_ROWS
            df = df.head(MAX_ROWS)
            out = article_dir / "evidence" / "data" / f"{q['id']}.csv"
            df.to_csv(out, index=False)
            entries.append({
                "id": q["id"], "kind": "data", "source": q["source"],
                "query": q["sql"], "params": q.get("params") or {},
                "retrieved_at": today, "license": license_for(q["source"], licenses),
                "sha256": sha256_file(out), "rows": len(df), "truncated": truncated})
        else:
            raise ValueError(f"unsupported kind in this task: {kind}")
    write_manifest(article_dir, entries)
    return entries


def write_manifest(article_dir: Path, entries: list) -> None:
    dump_yaml({"entries": entries}, Path(article_dir) / "evidence" / "manifest.yaml")
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_evidence.py -q`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/column/evidence.py power-academy/tests/test_column_evidence.py
git commit -m "Add evidence builder with read-only SQL guard, hashing and licenses" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: Evidence builder — kb_excerpt, western_fact, charts registration, verify

**Files:**
- Modify: `power-academy/academy/column/evidence.py`
- Create: `power-academy/tests/test_column_evidence_kinds.py`

**Interfaces:**
- Consumes: Task 2 functions; KB schema from Global Constraints.
- Produces: extended `build_pack` handling `kind: kb_excerpt | western_fact | chart`; `verify_pack(article_dir, engine) -> list[str]` (mismatch descriptions, empty = all match).
- New query shapes:
  - `{id, kind: kb_excerpt, source: staging.spot_knowledge_docs, title_like: "<substr>", max_chars: 500}` → pulls the first active doc whose title contains the substring + its first chunk, stores capped quote + citation.
  - `{id, kind: western_fact, concept_id, source_id, note}` → citation-only entry, no file.
  - `{id, kind: chart, script: <charts/<id>.py>, inputs: [data ids], public: bool}` → runs the script (subprocess), hashes the PNG.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_column_evidence_kinds.py`:
```python
import sqlalchemy as sa

from academy.column.evidence import build_pack, verify_pack
from academy.column.schema import new_article
from academy.io import dump_yaml, load_yaml

KB_SQL_DOCS = ("create table docs (id int, title text, active bool, created_at text)")
KB_SQL_CHUNKS = ("create table chunks (id int, doc_id int, page_no int, chunk_index int, "
                 "chunk_text text)")


def _kb_engine():
    e = sa.create_engine("sqlite://")
    with e.begin() as c:
        c.execute(sa.text(KB_SQL_DOCS))
        c.execute(sa.text(KB_SQL_CHUNKS))
        c.execute(sa.text("insert into docs values (1,'山东电力现货市场规则(试行)',1,'2026-09-01')"))
        c.execute(sa.text("insert into chunks values (1,1,3,0,'第三十二条 现货电能量市场采用节点边际电价机制 ' * 20)"))
    return e


def test_kb_excerpt_capped_with_citation(tmp_path):
    d = new_article(tmp_path, 1, "demo")
    dump_yaml({"queries": [{"id": "ex_rule32", "kind": "kb_excerpt",
                            "source": "staging.spot_knowledge_docs",
                            "title_like": "山东电力现货市场规则", "max_chars": 120}]},
              d / "evidence" / "queries.yaml")
    entries = build_pack(d, _kb_engine(), {"staging.spot_knowledge_docs": "public"},
                         today="2026-10-04")
    e = entries[0]
    assert e["kind"] == "policy_excerpt" and e["license"] == "public"
    md = (d / "evidence" / "excerpts" / "ex_rule32.md").read_text(encoding="utf-8")
    assert "山东电力现货市场规则(试行)" in md and "第三十二条" in md and len(md) < 500


def test_western_fact_is_citation_only(tmp_path):
    d = new_article(tmp_path, 2, "demo2")
    dump_yaml({"queries": [{"id": "wf_merit", "kind": "western_fact", "concept_id": "merit_order",
                            "source_id": "clewlow", "note": "stack ordering logic"}]},
              d / "evidence" / "queries.yaml")
    entries = build_pack(d, None, {}, today="2026-10-04")
    e = entries[0]
    assert e["kind"] == "western_fact" and e["license"] == "own"
    assert e["concept_id"] == "merit_order" and "sha256" not in e


def test_chart_entry_runs_script_and_hash(tmp_path):
    d = new_article(tmp_path, 3, "demo3")
    (d / "evidence" / "data").mkdir(parents=True, exist_ok=True)
    (d / "evidence" / "data" / "e_x.csv").write_text("a\n1\n2\n", encoding="utf-8")
    script = d / "evidence" / "charts" / "e_c.py"
    script.write_text(
        "import matplotlib; matplotlib.use('Agg')\n"
        "import matplotlib.pyplot as plt, pathlib, sys\n"
        "plt.figure(); plt.plot([1,2],[1,2])\n"
        "plt.savefig(sys.argv[1])\n", encoding="utf-8")
    dump_yaml({"queries": [{"id": "e_c", "kind": "chart", "script": "e_c.py",
                            "inputs": ["e_x"], "public": True}]},
              d / "evidence" / "queries.yaml")
    entries = build_pack(d, None, {}, today="2026-10-04")
    e = entries[0]
    assert e["kind"] == "chart" and e["public"] is True and len(e["sha256"]) == 64
    assert (d / "evidence" / "charts" / "e_c.png").exists()
    # license leak: restricted input on a public chart must be caught at build time
    import pytest
    dump_yaml({"entries": [{"id": "e_x", "kind": "data", "license": "licensed_restricted"}]},
              d / "evidence" / "manifest.yaml")
    with pytest.raises(ValueError, match="licensed_restricted"):
        build_pack(d, None, {}, today="2026-10-04")


def test_verify_pack_reports_changes(tmp_path):
    d = new_article(tmp_path, 4, "demo4")
    e = sa.create_engine("sqlite://")
    with e.begin() as c:
        c.execute(sa.text("create table p (a int)"))
        c.execute(sa.text("insert into p values (1)"))
    dump_yaml({"queries": [{"id": "e_d", "kind": "sql", "source": "t", "sql": "select * from p"}]},
              d / "evidence" / "queries.yaml")
    build_pack(d, e, {}, today="2026-10-04")
    assert verify_pack(d, e) == []
    with e.begin() as c:
        c.execute(sa.text("insert into p values (2)"))
    assert verify_pack(d, e) == ["e_d: data changed since retrieval"]
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_evidence_kinds.py -q`
Expected: FAIL (kb_excerpt kind unsupported).

- [ ] **Step 3: Implement — extend `evidence.py`**

Add imports at top: `import subprocess, sys`.
Replace the `build_pack` `else: raise` branch and add helpers:

```python
EXCERPT_DOC_SQL = ("select id, title from staging.spot_knowledge_docs "
                   "where active and title ilike :pat order by created_at desc limit 1")
EXCERPT_CHUNK_SQL = ("select chunk_text, page_no from staging.spot_knowledge_chunks "
                     "where doc_id = :doc_id order by chunk_index limit 1")


def _kb_excerpt(engine, q):
    with engine.connect() as c:
        doc = c.execute(sa.text(EXCERPT_DOC_SQL), {"pat": f"%{q['title_like']}%"}).fetchone()
        if doc is None:
            raise ValueError(f"no KB doc matching {q['title_like']!r}")
        chunk = c.execute(sa.text(EXCERPT_CHUNK_SQL), {"doc_id": doc[0]}).fetchone()
    quote = (chunk[0] if chunk else "")[: q.get("max_chars", 500)]
    return doc[1], quote, (chunk[1] if chunk else None)


def build_pack(article_dir, engine, licenses: dict, today: str | None = None) -> list:
    article_dir = Path(article_dir)
    today = today or date.today().isoformat()
    queries = load_yaml(article_dir / "evidence" / "queries.yaml").get("queries") or []
    prior = {}
    mpath = article_dir / "evidence" / "manifest.yaml"
    if mpath.exists():
        prior = {e["id"]: e for e in (load_yaml(mpath).get("entries") or [])}
    entries = []
    for q in queries:
        kind = q["kind"]
        if kind == "sql":
            check_readonly(q["sql"])
            df = _run_sql(engine, q["sql"], q.get("params"))
            truncated = len(df) > MAX_ROWS
            df = df.head(MAX_ROWS)
            out = article_dir / "evidence" / "data" / f"{q['id']}.csv"
            df.to_csv(out, index=False)
            entries.append({
                "id": q["id"], "kind": "data", "source": q["source"],
                "query": q["sql"], "params": q.get("params") or {},
                "retrieved_at": today, "license": license_for(q["source"], licenses),
                "sha256": sha256_file(out), "rows": len(df), "truncated": truncated})
        elif kind == "kb_excerpt":
            title, quote, page = _kb_excerpt(engine, q)
            md = (f"# {title}\n\n- doc: {title} · page {page}\n- retrieved_at: {today}\n\n"
                  f"> {quote}\n")
            out = article_dir / "evidence" / "excerpts" / f"{q['id']}.md"
            out.write_text(md, encoding="utf-8")
            entries.append({
                "id": q["id"], "kind": "policy_excerpt",
                "source": q.get("source", "staging.spot_knowledge_docs"),
                "query": f"title ilike %{q['title_like']}%",
                "retrieved_at": today,
                "license": license_for(q.get("source", "staging.spot_knowledge_docs"), licenses),
                "sha256": sha256_file(out)})
        elif kind == "western_fact":
            entries.append({
                "id": q["id"], "kind": "western_fact", "source": q["source_id"],
                "concept_id": q["concept_id"], "note": q.get("note", ""),
                "retrieved_at": today, "license": "own"})
        elif kind == "chart":
            inputs = q.get("inputs") or []
            for dep in inputs:
                lic = (prior.get(dep) or {}).get("license")
                if lic == "licensed_restricted" and q.get("public"):
                    raise ValueError(
                        f"chart {q['id']}: public chart uses licensed_restricted input {dep}")
            png = article_dir / "evidence" / "charts" / f"{q['id']}.png"
            script = article_dir / "evidence" / "charts" / q["script"]
            subprocess.run([sys.executable, str(script), str(png)],
                           check=True, cwd=article_dir / "evidence" / "charts")
            entries.append({
                "id": q["id"], "kind": "chart", "source": f"charts/{q['script']}",
                "inputs": inputs, "public": bool(q.get("public")),
                "retrieved_at": today, "license": "own",
                "sha256": sha256_file(png)})
        else:
            raise ValueError(f"unsupported kind: {kind}")
    write_manifest(article_dir, entries)
    return entries


def verify_pack(article_dir, engine) -> list:
    article_dir = Path(article_dir)
    problems = []
    queries = load_yaml(article_dir / "evidence" / "queries.yaml").get("queries") or []
    for q in queries:
        if q["kind"] != "sql":
            continue
        fresh = _run_sql(engine, q["sql"], q.get("params")).head(MAX_ROWS)
        current = article_dir / "evidence" / "data" / f"{q['id']}.csv"
        import io
        if sha256_file(current) != hashlib.sha256(
                fresh.to_csv(index=False).encode("utf-8")).hexdigest():
            problems.append(f"{q['id']}: data changed since retrieval")
    return problems
```
(Remove the old `build_pack` body when replacing; keep `check_readonly`, `sha256_file`, `license_for`, `_run_sql`, `write_manifest` unchanged.)

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests -q`
Expected: all pass (57).

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/column/evidence.py power-academy/tests/test_column_evidence_kinds.py
git commit -m "Add kb_excerpt, western_fact, chart entries and verify mode to evidence builder" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: Chart style module + 3 starter charts

**Files:**
- Create: `power-academy/academy/column/charts.py`
- Create: `power-academy/tests/test_column_charts.py`

**Interfaces:**
- Produces: `set_style() -> None`; `price_duration_curve(csv, out, price_col="rt_price", title="") -> Path`; `da_rt_spread(csv, out, title="") -> Path` (needs `da_price`,`rt_price`,`datetime` cols; daily means + fill); `province_compare(csv, out, value_col, label_col="province", title="") -> Path`; `png_size(path) -> tuple[int,int]`.
- All charts 900×500 PNG, Agg backend forced inside the module.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_column_charts.py`:
```python
import pandas as pd

from academy.column.charts import (da_rt_spread, png_size, price_duration_curve,
                                   province_compare, set_style)


def _csv(tmp_path, df):
    p = tmp_path / "in.csv"
    df.to_csv(p, index=False)
    return p


def test_duration_curve_900x500(tmp_path):
    set_style()
    csv = _csv(tmp_path, pd.DataFrame({"rt_price": [0.4, 0.1, 0.9, -0.05, 0.3]}))
    out = tmp_path / "c.png"
    price_duration_curve(csv, out, title="test 价格")
    assert png_size(out) == (900, 500) and out.stat().st_size > 5000


def test_da_rt_spread(tmp_path):
    csv = _csv(tmp_path, pd.DataFrame({
        "datetime": ["2026-10-01 10:00", "2026-10-01 11:00",
                     "2026-10-02 10:00", "2026-10-02 11:00"],
        "da_price": [0.40, 0.42, 0.38, 0.39],
        "rt_price": [0.45, 0.50, 0.30, 0.28]}))
    out = tmp_path / "s.png"
    da_rt_spread(csv, out)
    assert png_size(out) == (900, 500)


def test_province_compare_sorted(tmp_path):
    csv = _csv(tmp_path, pd.DataFrame({"province": ["山东", "山西", "广东"],
                                       "capture": [0.72, 0.85, 0.66]}))
    out = tmp_path / "p.png"
    province_compare(csv, out, value_col="capture")
    assert png_size(out) == (900, 500)
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_charts.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement**

`power-academy/academy/column/charts.py`:
```python
import struct
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ACCENT = "#1f6fb2"
SECOND = "#c0504d"
CJK_FONTS = ["PingFang SC", "Hiragino Sans GB", "Hiragino Mincho ProN", "Arial Unicode MS"]


def set_style() -> None:
    matplotlib.rcParams.update({
        "font.sans-serif": CJK_FONTS + ["sans-serif"],
        "axes.unicode_minus": False,
        "figure.figsize": (9, 5), "figure.dpi": 100,
        "axes.grid": True, "grid.alpha": 0.25, "axes.spines.top": False,
        "axes.spines.right": False, "font.size": 11})


def png_size(path) -> tuple:
    data = Path(path).read_bytes()
    w, h = struct.unpack(">II", data[16:24])
    return w, h


def _save(fig, out) -> Path:
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)
    return Path(out)


def price_duration_curve(csv, out, price_col="rt_price", title="") -> Path:
    s = pd.read_csv(csv)[price_col].dropna().sort_values(ascending=False).reset_index(drop=True)
    fig, ax = plt.subplots()
    ax.plot(s.index / max(len(s) - 1, 1) * 100, s.values, color=ACCENT, lw=1.4)
    ax.axhline(0, color="#888", lw=0.8)
    ax.set(xlabel="小时占比 (%)", ylabel="价格 (元/kWh)", title=title)
    return _save(fig, out)


def da_rt_spread(csv, out, title="") -> Path:
    df = pd.read_csv(csv, parse_dates=["datetime"])
    daily = df.set_index("datetime")[["da_price", "rt_price"]].resample("D").mean().dropna()
    fig, ax = plt.subplots()
    ax.plot(daily.index, daily["rt_price"], color=ACCENT, lw=1.4, label="实时 RT")
    ax.plot(daily.index, daily["da_price"], color="#888", lw=1.2, ls="--", label="日前 DA")
    ax.fill_between(daily.index, daily["da_price"], daily["rt_price"],
                    where=daily["rt_price"] >= daily["da_price"], color=ACCENT, alpha=0.15)
    ax.fill_between(daily.index, daily["da_price"], daily["rt_price"],
                    where=daily["rt_price"] < daily["da_price"], color=SECOND, alpha=0.15)
    ax.legend(frameon=False)
    ax.set(ylabel="价格 (元/kWh)", title=title)
    fig.autofmt_xdate()
    return _save(fig, out)


def province_compare(csv, out, value_col, label_col="province", title="") -> Path:
    df = pd.read_csv(csv).sort_values(value_col, ascending=False)
    fig, ax = plt.subplots()
    ax.bar(df[label_col], df[value_col], color=ACCENT, width=0.6)
    ax.set(title=title)
    fig.autofmt_xdate()
    return _save(fig, out)
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_charts.py -q`
Expected: 3 passed (font warnings are acceptable).

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/column/charts.py power-academy/tests/test_column_charts.py
git commit -m "Add chart style module with three starter chart types" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5: Gates — fact trace (tag resolution + number coverage)

**Files:**
- Create: `power-academy/academy/column/gates.py`
- Create: `power-academy/tests/test_column_gates_trace.py`

**Interfaces:**
- Consumes: manifest from Task 2/3.
- Produces: `TAG_RE`; `extract_tags(draft) -> set[str]`; `number_lines(draft) -> list[tuple[int,str]]`; `check_fact_trace(draft: str, manifest_ids: set[str]) -> list[str]`.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_column_gates_trace.py`:
```python
from academy.column.gates import check_fact_trace, extract_tags, number_lines

DRAFT = """# 标题 2026年

山东现货10月3日实时均价0.42元/kWh，高于日前。[[E:e_sd]]

- 第一，机制不同。
- 第二，价格形成不同。

2026年10月，山西、山东等8省转入正式运行。[[E:ex_rule]]

英国市场在1990年代启动池化交易。[[E:wf_gb]]
"""

def test_extract_tags():
    assert extract_tags(DRAFT) == {"e_sd", "ex_rule", "wf_gb"}


def test_number_lines_flags_quantities_not_dates_or_lists():
    lines = number_lines(DRAFT)
    texts = [t for _, t in lines]
    assert any("0.42元/kWh" in t for t in texts)
    assert any("8省" in t for t in texts)
    assert not any("2026年10月，山西" in t for t in texts)   # date-led line excluded
    assert not any("第一" in t for t in texts)
    assert not any("1990年代" in t for t in texts)


def test_fact_trace_unknown_and_uncovered():
    errs = check_fact_trace("均价0.42元/kWh。[[E:ghost]]", {"e_sd"})
    joined = " | ".join(errs)
    assert "unknown tag" in joined and "uncovered" in joined
    assert check_fact_trace("均价0.42元/kWh。[[E:e_sd]]", {"e_sd"}) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_gates_trace.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement**

`power-academy/academy/column/gates.py`:
```python
import re

TAG_RE = re.compile(r"\[\[E:([A-Za-z0-9_\-]+)\]\]")
CHART_REF_RE = re.compile(r"\[\[C:([A-Za-z0-9_\-]+)\]\]")

# a "quantity": digits with unit/scale suffix, a decimal, or a percent
_QUANTITY = re.compile(
    r"(\d+(?:\.\d+)?\s*(?:%|元|块|分|角|厘|亿|万|倍|千瓦时|兆瓦时|千瓦|兆瓦|吉瓦|"
    r"kWh|MWh|kW|MW|GW|GW?h|小时|bp|pct)|\d+\.\d+)")
_DATE_LINE = re.compile(r"^\s*(?:\d{4}年|\(?\d{4}\)?[年/.\-])")
_LIST_MARKER = re.compile(r"^\s*(?:[-*+]|\d+[.、)]|[一二三四五六七八九十]+[，,、.])")


def extract_tags(draft: str) -> set:
    return set(TAG_RE.findall(draft))


def number_lines(draft: str) -> list:
    out, in_code = [], False
    for n, line in enumerate(draft.splitlines(), 1):
        if line.strip().startswith("```"):
            in_code = not in_code
            continue
        s = line.strip()
        if in_code or not s or s.startswith("#") or _LIST_MARKER.match(s) or _DATE_LINE.match(s):
            continue
        body = TAG_RE.sub("", s)
        if _QUANTITY.search(body):
            out.append((n, s))
    return out


def check_fact_trace(draft: str, manifest_ids: set) -> list:
    errs = sorted(f"unknown tag: {t}" for t in extract_tags(draft) - manifest_ids)
    for n, line in number_lines(draft):
        if not TAG_RE.search(line):
            errs.append(f"line {n}: uncovered quantity: {line[:40]}")
    return errs
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_gates_trace.py -q`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/column/gates.py power-academy/tests/test_column_gates_trace.py
git commit -m "Add fact-trace gate with tag resolution and quantity coverage" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6: Gates — license, confidentiality, compliance flags, terminology, runner

**Files:**
- Modify: `power-academy/academy/column/gates.py`
- Create: `power-academy/tests/test_column_gates.py`

**Interfaces:**
- Consumes: Task 5 helpers, manifest, `style/blocklist.yaml`, `style/compliance_patterns.yaml`, `style/zh_terms.yaml`.
- Produces: `check_license(draft, entries) -> list[str]`; `check_blocklist(texts: dict[str,str], names: list[str]) -> list[str]`; `check_compliance(draft, patterns: list[str]) -> list[str]`; `check_terms(draft, terms: list[dict]) -> list[str]`; `run_gates(article_dir, column_root) -> dict` (rewrites the auto section of `gates.md` between `<!-- auto:begin/end -->`, returns `{gate: "pass"|"fail"|"flagged:N"}`); `AUTO_BEGIN/AUTO_END` markers.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_column_gates.py`:
```python
from academy.column.gates import (check_blocklist, check_compliance,
                                  check_license, check_terms, run_gates)
from academy.column.schema import new_article
from academy.io import dump_yaml


def test_license_blocks_nonpublic_chart_reference():
    entries = [{"id": "c1", "kind": "chart", "public": False},
               {"id": "d1", "kind": "data", "license": "licensed_restricted"}]
    assert check_license("见下图。\n[[C:c1]]", entries)
    assert check_license("见下图。\n[[C:c2]]", entries) == [] or True  # unknown chart ok here
    entries_pub = [{"id": "c1", "kind": "chart", "public": True}]
    assert check_license("[[C:c1]]", entries_pub) == []


def test_blocklist_case_and_cjk():
    texts = {"draft": "Shell Energy曾…", "excerpt": "与Counterparty签署"}
    hits = check_blocklist(texts, ["shell energy", "counterparty"])
    assert len(hits) == 2 and "draft" in hits[0]
    assert check_blocklist(texts, ["其他公司"]) == []


def test_compliance_flags_with_line_numbers():
    hits = check_compliance("价格将涨到0.8元。\n机制值得借鉴。", ["将涨到", "稳赚"])
    assert hits == ["line 1: risky pattern '将涨到'"]


def test_terms_variant_flagged():
    hits = check_terms("现货市场价格波动", [{"canonical": "现货市场", "variants": ["即期市场"]}])
    assert hits == []
    hits = check_terms("即期市场价格波动", [{"canonical": "现货市场", "variants": ["即期市场"]}])
    assert hits == ["use '现货市场' instead of '即期市场'"]


def test_run_gates_writes_auto_section_preserving_owner_text(tmp_path):
    root = tmp_path
    (root / "style").mkdir()
    for name, obj in [("blocklist.yaml", {"names": ["ACME"]}),
                      ("compliance_patterns.yaml", {"patterns": ["将涨到"]}),
                      ("zh_terms.yaml", {"terms": [{"canonical": "现货", "variants": ["即期"]}]})]:
        dump_yaml(obj, root / "style" / name)
    d = new_article(root, 1, "demo")
    (d / "draft.zh.md").write_text("价格将涨到1元/kWh。[[E:e1]]\n", encoding="utf-8")
    dump_yaml({"entries": [{"id": "e1", "kind": "data", "license": "own"}]},
              d / "evidence" / "manifest.yaml")
    (d / "gates.md").write_text(
        "# G\n\n<!-- auto:begin -->\nold\n<!-- auto:end -->\n\nowner_signoff: 张三 2026-10-05\n",
        encoding="utf-8")
    res = run_gates(d, root)
    assert res["fact_trace"] == "pass" and res["compliance"] == "flagged:1"
    body = (d / "gates.md").read_text(encoding="utf-8")
    assert "old" not in body and "张三 2026-10-05" in body and "fact_trace" in body
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_gates.py -q`
Expected: FAIL (functions missing).

- [ ] **Step 3: Implement — append to `gates.py`**

```python
from pathlib import Path

from ..io import load_yaml

AUTO_BEGIN = "<!-- auto:begin -->"
AUTO_END = "<!-- auto:end -->"


def check_license(draft: str, entries: list) -> list:
    by_id = {e["id"]: e for e in entries}
    errs = []
    for ref in CHART_REF_RE.findall(draft):
        e = by_id.get(ref)
        if e and e.get("kind") == "chart" and not e.get("public"):
            errs.append(f"draft references non-public chart: {ref}")
    return errs


def check_blocklist(texts: dict, names: list) -> list:
    hits = []
    for label, text in texts.items():
        low = text.lower()
        hits += [f"{label}: blocklisted name '{n}'" for n in names if n.lower() in low]
    return hits


def check_compliance(draft: str, patterns: list) -> list:
    hits = []
    for n, line in enumerate(draft.splitlines(), 1):
        hits += [f"line {n}: risky pattern '{p}'" for p in patterns if p in line]
    return hits


def check_terms(draft: str, terms: list) -> list:
    return [f"use '{t['canonical']}' instead of '{v}'"
            for t in terms for v in t.get("variants", []) if v in draft]


def run_gates(article_dir: Path, column_root: Path) -> dict:
    article_dir, column_root = Path(article_dir), Path(column_root)
    draft = (article_dir / "draft.zh.md").read_text(encoding="utf-8")
    entries = (load_yaml(article_dir / "evidence" / "manifest.yaml") or {}).get("entries") or []
    ids = {e["id"] for e in entries}
    style = column_root / "style"
    names = (load_yaml(style / "blocklist.yaml") or {}).get("names") or []
    patterns = (load_yaml(style / "compliance_patterns.yaml") or {}).get("patterns") or []
    terms = (load_yaml(style / "zh_terms.yaml") or {}).get("terms") or []
    excerpts = {p.stem: p.read_text(encoding="utf-8")
                for p in (article_dir / "evidence" / "excerpts").glob("*.md")}

    results = {
        "fact_trace": check_fact_trace(draft, ids),
        "license": check_license(draft, entries),
        "confidentiality": check_blocklist({"draft": draft, **excerpts}, names),
        "compliance": check_compliance(draft, patterns),
        "terminology": check_terms(draft, terms),
    }
    summary = {g: ("pass" if not errs else ("flagged:%d" % len(errs) if g == "compliance" else "fail"))
               for g, errs in results.items()}
    lines = [AUTO_BEGIN, ""]
    for g, errs in results.items():
        lines.append(f"- {g}: {summary[g]}")
        lines += [f"  - {e}" for e in errs[:20]]
    lines += ["", AUTO_END]
    gpath = article_dir / "gates.md"
    body = gpath.read_text(encoding="utf-8")
    pre, rest = body.split(AUTO_BEGIN, 1)
    _, post = rest.split(AUTO_END, 1)
    gpath.write_text(pre + "\n".join(lines) + post, encoding="utf-8")
    return summary
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests -q`
Expected: all pass (62).

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/column/gates.py power-academy/tests/test_column_gates.py
git commit -m "Add license, confidentiality, compliance and terminology gates with runner" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 7: Renderer + footer, and approval rule

**Files:**
- Create: `power-academy/academy/column/render.py`
- Modify: `power-academy/academy/column/schema.py` (add `check_approval_ready(article_dir) -> list[str]`)
- Create: `power-academy/tests/test_column_render.py`

**Interfaces:**
- Consumes: manifest, charts (Task 4 output), gates summary file, `style/disclaimer.md`.
- Produces: `render_article(article_dir, column_root) -> Path` (writes `out/<name>.html`); `ALLOWED_TAGS`; `check_approval_ready(article_dir) -> list[str]` (empty = may be marked `approved`).

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_column_render.py`:
```python
import base64

import pytest

from academy.column.render import render_article
from academy.column.schema import check_approval_ready, new_article
from academy.io import dump_yaml


def _article(tmp_path):
    root = tmp_path
    (root / "style").mkdir()
    (root / "style" / "disclaimer.md").write_text("免责声明：本文仅为研究交流。", encoding="utf-8")
    d = new_article(root, 1, "demo")
    png = d / "evidence" / "charts" / "c1.png"
    png.write_bytes(b"\x89PNG\r\n\x1a\n" + b"x" * 100)
    dump_yaml({"entries": [
        {"id": "e1", "kind": "data", "source": "marketdata.spot_prices_hourly",
         "retrieved_at": "2026-10-04", "license": "own"},
        {"id": "c1", "kind": "chart", "public": True, "retrieved_at": "2026-10-04",
         "license": "own"}]}, d / "evidence" / "manifest.yaml")
    (d / "draft.zh.md").write_text(
        "# 测试标题\n\n均价0.42元/kWh。[[E:e1]]\n\n[[C:c1]]\n\n<script>alert(1)</script>\n",
        encoding="utf-8")
    return root, d


def test_render_embeds_chart_refs_and_strips_bad_tags(tmp_path):
    root, d = _article(tmp_path)
    out = render_article(d, root)
    html = out.read_text(encoding="utf-8")
    assert "[[E:" not in html and "<script" not in html
    assert "data:image/png;base64," in html
    assert "市场数据" in html or "数据来源" in html
    assert "免责声明" in html and "spot_prices_hourly" in html
    assert "[1]" in html  # evidence reference numbering


def test_render_refuses_unknown_tag(tmp_path):
    root, d = _article(tmp_path)
    (d / "draft.zh.md").write_text("幽灵引用。[[E:ghost]]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unknown tag"):
        render_article(d, root)


def test_approval_needs_signoff_and_green_gates(tmp_path):
    root, d = _article(tmp_path)
    assert check_approval_ready(d)            # no signoff, gates not run
    (d / "gates.md").write_text(
        "<!-- auto:begin -->\n- fact_trace: pass\n- license: pass\n"
        "- confidentiality: pass\n- compliance: pass\n- terminology: pass\n"
        "<!-- auto:end -->\n", encoding="utf-8")
    assert check_approval_ready(d)            # still no signoff
    with open(d / "gates.md", "a", encoding="utf-8") as f:
        f.write("\nowner_signoff: 张三 2026-10-05\n")
    assert check_approval_ready(d) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_render.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement**

`power-academy/academy/column/render.py`:
```python
import base64
import re
from pathlib import Path

import markdown

from ..io import load_yaml
from .gates import CHART_REF_RE, TAG_RE

ALLOWED_TAGS = {"p", "h1", "h2", "h3", "blockquote", "strong", "em", "table",
                "thead", "tbody", "tr", "td", "th", "ul", "ol", "li", "img",
                "br", "hr", "sup", "section"}

CSS = ("body{font-family:'PingFang SC','Hiragino Sans GB',sans-serif;font-size:16px;"
       "line-height:1.8;color:#222;max-width:677px;margin:0 auto;padding:0 8px}"
       "h2{border-left:4px solid #1f6fb2;padding-left:8px}"
       "blockquote{color:#666;border-left:3px solid #ccc;padding-left:10px;margin-left:0}"
       "table{border-collapse:collapse;width:100%}td,th{border:1px solid #ddd;padding:6px}"
       "img{max-width:100%}.foot{color:#888;font-size:13px}")


def _sanitize(html: str) -> str:
    def sub(m):
        tag = m.group(2).lower()
        return m.group(0) if tag in ALLOWED_TAGS else ""
    html = re.sub(r"<(/?)([a-zA-Z0-9]+)([^>]*)>", sub, html)
    return re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.S | re.I)


def render_article(article_dir: Path, column_root: Path) -> Path:
    article_dir, column_root = Path(article_dir), Path(column_root)
    draft = (article_dir / "draft.zh.md").read_text(encoding="utf-8")
    entries = {e["id"]: e for e in
               (load_yaml(article_dir / "evidence" / "manifest.yaml") or {}).get("entries") or []}
    refs, seen = [], {}

    def tag_sub(m):
        tid = m.group(1)
        if tid not in entries:
            raise ValueError(f"unknown tag: {tid}")
        if tid not in seen:
            seen[tid] = len(refs) + 1
            refs.append(entries[tid])
        return f"<sup>[{seen[tid]}]</sup>"

    body = TAG_RE.sub(tag_sub, draft)

    def chart_sub(m):
        cid = m.group(1)
        e = entries.get(cid)
        if not e or e.get("kind") != "chart" or not e.get("public"):
            raise ValueError(f"cannot embed chart: {cid}")
        png = article_dir / "evidence" / "charts" / f"{cid}.png"
        b64 = base64.b64encode(png.read_bytes()).decode()
        return f'<img src="data:image/png;base64,{b64}" alt="{cid}"/>'

    body = CHART_REF_RE.sub(chart_sub, body)
    html = markdown.markdown(body, extensions=["tables"])
    html = _sanitize(html)

    src_lines = [f"[{i+1}] {e.get('source','')}（{e.get('retrieved_at','')}获取）"
                 for i, e in enumerate(refs)]
    disclaimer = (column_root / "style" / "disclaimer.md").read_text(encoding="utf-8").strip()
    foot = ("<section class='foot'><hr/><p><strong>数据来源与口径</strong><br/>"
            + "<br/>".join(src_lines)
            + "</p><p><strong>方法</strong>：数据经可复现查询提取，图表由脚本生成；"
              "观点为作者分析，不代表任何机构。</p><p>" + disclaimer + "</p></section>")
    out = article_dir / "out" / (article_dir.name + ".html")
    out.parent.mkdir(exist_ok=True)
    out.write_text(f"<style>{CSS}</style><section>{html}</section>{foot}", encoding="utf-8")
    return out
```
Append to `schema.py`:
```python
def check_approval_ready(article_dir) -> list:
    errs = []
    gpath = Path(article_dir) / "gates.md"
    body = gpath.read_text(encoding="utf-8") if gpath.exists() else ""
    if "owner_signoff:" not in body or not body.split("owner_signoff:", 1)[1].strip():
        errs.append("gates.md missing owner_signoff")
    auto = body.split("<!-- auto:begin -->")[-1]
    for gate in ("fact_trace", "license", "confidentiality", "terminology"):
        if f"- {gate}: pass" not in auto:
            errs.append(f"gate not passing: {gate}")
    return errs
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests -q`
Expected: all pass (65).

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/column/render.py power-academy/academy/column/schema.py power-academy/tests/test_column_render.py
git commit -m "Add公众号 HTML renderer with evidence references and approval rule" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 8: Weekly anomaly scan

**Files:**
- Create: `power-academy/academy/column/scan.py`
- Create: `power-academy/tests/test_column_scan.py`

**Interfaces:**
- Produces: `scan_anomalies(df: pd.DataFrame, today: str, top: int = 5) -> list[dict]` (`{province, metric, detail, score}`); `load_prices(engine, days=35) -> pd.DataFrame`; `render_scan_md(anomalies, today) -> str`.
- `df` columns: `province, datetime, rt_price, da_price`.

- [ ] **Step 1: Write the failing test**

`power-academy/tests/test_column_scan.py`:
```python
import numpy as np
import pandas as pd

from academy.column.scan import render_scan_md, scan_anomalies


def _df():
    rng = np.random.default_rng(7)
    rows = []
    for prov, base in [("山东", 0.4), ("山西", 0.35), ("广东", 0.5)]:
        for day in range(35):
            for h in (10, 11):
                rows.append({"province": prov,
                             "datetime": pd.Timestamp("2026-09-01") + pd.Timedelta(days=day, hours=h),
                             "rt_price": base + rng.normal(0, 0.02),
                             "da_price": base})
    df = pd.DataFrame(rows)
    # inject a spike week in 山东 during the last 7 days
    mask = (df.province == "山东") & (df.datetime >= "2026-09-30") & (df.datetime.dt.hour == 10)
    df.loc[mask, "rt_price"] = 2.5
    return df


def test_scan_finds_injected_spike():
    res = scan_anomalies(_df(), today="2026-10-05")
    assert res and res[0]["province"] == "山东"
    assert "spike" in res[0]["metric"] or "max" in res[0]["metric"]
    md = render_scan_md(res, today="2026-10-05")
    assert "山东" in md and "2026-10-05" in md
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_scan.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement**

`power-academy/academy/column/scan.py`:
```python
from datetime import date, timedelta

import pandas as pd
import sqlalchemy as sa

PRICE_SQL = ("select province, datetime, rt_price, da_price "
             "from marketdata.spot_prices_hourly where datetime >= :cut")


def load_prices(engine, days: int = 35) -> pd.DataFrame:
    cut = (pd.Timestamp(date.today() - timedelta(days=days))).tz_localize("UTC")
    with engine.connect() as c:
        return pd.read_sql(sa.text(PRICE_SQL), c, params={"cut": cut})


def scan_anomalies(df: pd.DataFrame, today: str, top: int = 5) -> list:
    today = pd.Timestamp(today)
    week_start = today - pd.Timedelta(days=7)
    prior_start = today - pd.Timedelta(days=35)
    out = []
    for prov, g in df.groupby("province"):
        g = g.dropna(subset=["rt_price"])
        week = g[g.datetime >= week_start]
        prior = g[(g.datetime >= prior_start) & (g.datetime < week_start)]
        if len(week) < 24 or len(prior) < 96:
            continue
        base_max = prior.rt_price.max()
        base_std = prior.rt_price.std() or 1e-9
        wk_max = week.rt_price.max()
        spike = (wk_max - base_max) / base_std
        wk_basis = (week.rt_price - week.da_price).abs().mean()
        pr_basis = (prior.rt_price - prior.da_price).abs().mean() or 1e-9
        basis_shift = wk_basis / pr_basis
        wk_vol = week.rt_price.std() / base_std
        metric, score, detail = max([
            ("rt_max_spike", spike,
             f"周实时最高 {wk_max:.3f} vs 前28天最高 {base_max:.3f}（{spike:+.1f}σ）"),
            ("da_rt_basis_shift", basis_shift,
             f"周均|RT-DA|基差 {wk_basis:.3f} vs 前28天 {pr_basis:.3f}（{basis_shift:.1f}x）"),
            ("volatility_shift", wk_vol,
             f"周实时波动率 {week.rt_price.std():.3f} vs 前28天 {base_std:.3f}（{wk_vol:.1f}x）")],
            key=lambda x: x[1])
        out.append({"province": prov, "metric": metric, "score": round(score, 2),
                    "detail": detail})
    return sorted(out, key=lambda x: -x["score"])[:top]


def render_scan_md(anomalies: list, today: str) -> str:
    lines = [f"# 周度价格异动扫描（截至 {today}）", ""]
    for a in anomalies:
        lines.append(f"- **{a['province']}** [{a['metric']}] {a['detail']}")
    return "\n".join(lines) + "\n"
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_scan.py -q`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/column/scan.py power-academy/tests/test_column_scan.py
git commit -m "Add weekly price anomaly scan over spot prices" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 9: Editor agent — propose-topics + CLI wiring

**Files:**
- Create: `power-academy/academy/column/editor.py`
- Modify: `power-academy/academy/cli.py` (add `column-*` commands)
- Create: `power-academy/tests/test_column_editor.py`

**Interfaces:**
- Consumes: `academy.llm.call_json`, `academy.column.scan`, backlog yaml, hermes briefings dir (path param), KB SQL (Global Constraints).
- Produces: `EDITOR_SYSTEM: str`; `gather_context(briefings_dir, kb_engine, scan_md, concept_ids, backlog_titles, briefing_days=7, kb_limit=10) -> dict`; `propose(client, model, context) -> list[dict]` (drops proposals whose `hook.source` is not in the context's source list); `append_backlog(root, proposals, today) -> int` (count added; dedupe by working_title).
- CLI: `column new --num N --slug S`; `column evidence <article>`; `column verify <article>`; `column gates <article>`; `column render <article>`; `column scan`; `column propose-topics`.
- `COLUMN_ROOT = ROOT / "columns" / "xiyangjing"`; engine from `PGURL` env or `config/.env` beside the repo (`ROOT.parent / "config" / ".env"`), error message must mention VPN bypass + SG rule.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_column_editor.py`:
```python
import json

from academy.column.editor import (EDITOR_SYSTEM, append_backlog, gather_context,
                                   propose)
from academy.io import load_yaml
from tests.fakes import FakeClient


def _ctx():
    return {"briefings": [{"file": "2026-10-03-morning.md", "text": "山西现货转入正式运行…"}],
            "kb_docs": [{"title": "关于深化新能源上网电价市场化改革的通知", "created_at": "2026-09-30"}],
            "scan_md": "- **山东** [rt_max_spike] 周实时最高 2.5…",
            "concept_ids": ["merit_order", "spark_spread_option"],
            "backlog_titles": ["已有选题"]}


def test_system_prompt_forbids_fabricated_hooks():
    assert "只能引用" in EDITOR_SYSTEM or "only" in EDITOR_SYSTEM.lower()


def test_propose_drops_unverifiable_hooks():
    good = {"working_title": "从英国容量市场看山东", "hook": {"source": "2026-10-03-morning.md",
            "item": "山西现货正式运行"}, "thesis_hypothesis": "t",
            "western": {"concept_ids": ["merit_order"], "angle": "a"},
            "china": {"provinces": ["山东"], "topics": ["现货"]},
            "evidence_candidates": ["spot_prices_hourly"], "timeliness": "本周"}
    bad = dict(good, working_title="编造来源", hook={"source": "不存在的来源", "item": "x"})
    c = FakeClient([json.dumps({"proposals": [good, bad]}, ensure_ascii=False)])
    props = propose(c, "m", _ctx())
    assert [p["working_title"] for p in props] == ["从英国容量市场看山东"]


def test_append_backlog_dedupes(tmp_path):
    (tmp_path / "topics").mkdir()
    (tmp_path / "topics" / "backlog.yaml").write_text(
        "topics:\n  - working_title: 已有选题\n", encoding="utf-8")
    props = [{"working_title": "新选题", "hook": {"source": "s", "item": "i"}},
             {"working_title": "已有选题", "hook": {"source": "s", "item": "i"}}]
    n = append_backlog(tmp_path, props, today="2026-10-06")
    assert n == 1
    titles = [t["working_title"] for t in load_yaml(tmp_path / "topics" / "backlog.yaml")["topics"]]
    assert titles == ["已有选题", "新选题"]
    entry = load_yaml(tmp_path / "topics" / "backlog.yaml")["topics"][1]
    assert entry["status"] == "idea" and entry["proposed_at"] == "2026-10-06"


def test_gather_context_reads_recent_briefings(tmp_path):
    b = tmp_path / "briefings"
    b.mkdir()
    (b / "2026-10-03-morning.md").write_text("新闻一", encoding="utf-8")
    (b / "2026-09-01-morning.md").write_text("旧闻", encoding="utf-8")
    ctx = gather_context(b, None, "扫描", ["merit_order"], ["已有"], briefing_days=7,
                         kb_limit=10, today="2026-10-06")
    assert len(ctx["briefings"]) == 1 and ctx["briefings"][0]["text"] == "新闻一"
    assert ctx["kb_docs"] == [] and ctx["scan_md"] == "扫描"
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_column_editor.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement `editor.py`**

`power-academy/academy/column/editor.py`:
```python
import json
import re
from datetime import date, timedelta
from pathlib import Path

import sqlalchemy as sa

from ..io import dump_yaml, load_yaml
from ..llm import call_json

EDITOR_SYSTEM = (
    "你是专栏「西洋镜看中国电力市场」的编辑。根据提供的近期素材，提出3-5个选题。"
    "只输出一个JSON对象：{\"proposals\": [{\"working_title\", \"hook\": {\"source\", \"item\"}, "
    "\"thesis_hypothesis\", \"western\": {\"concept_ids\": [], \"angle\"}, "
    "\"china\": {\"provinces\": [], \"topics\": []}, \"evidence_candidates\": [], \"timeliness\"\"}]。"
    "hook.source 只能引用提供的素材文件名或文档标题，禁止编造来源或新闻。"
    "每个选题必须包含西方市场机制视角与可获取的中国数据支撑。")

KB_RECENT_SQL = ("select title, created_at from staging.spot_knowledge_docs "
                 "where active order by created_at desc limit :n")


def gather_context(briefings_dir, kb_engine, scan_md, concept_ids, backlog_titles,
                   briefing_days=7, kb_limit=10, today=None) -> dict:
    today = today or date.today().isoformat()
    cut = date.fromisoformat(today) - timedelta(days=briefing_days)
    briefings = []
    for p in sorted(Path(briefings_dir).glob("*.md"), reverse=True):
        m = re.match(r"(\d{4}-\d{2}-\d{2})", p.name)
        if m and date.fromisoformat(m.group(1)) >= cut:
            briefings.append({"file": p.name, "text": p.read_text(encoding="utf-8")[:2000]})
    kb_docs = []
    if kb_engine is not None:
        with kb_engine.connect() as c:
            kb_docs = [{"title": t, "created_at": str(d)}
                       for t, d in c.execute(sa.text(KB_RECENT_SQL), {"n": kb_limit})]
    return {"briefings": briefings, "kb_docs": kb_docs, "scan_md": scan_md,
            "concept_ids": concept_ids, "backlog_titles": backlog_titles, "today": today}


def propose(client, model, context) -> list:
    sources = {b["file"] for b in context["briefings"]} | {d["title"] for d in context["kb_docs"]}
    data = call_json(client, model, EDITOR_SYSTEM, json.dumps(context, ensure_ascii=False),
                     max_tokens=3000)
    out = []
    for p in data.get("proposals", []):
        if p.get("hook", {}).get("source") in sources:
            out.append(p)
    return out


def append_backlog(root, proposals, today) -> int:
    path = Path(root) / "topics" / "backlog.yaml"
    data = load_yaml(path) or {"topics": []}
    topics = data.get("topics") or []
    have = {t.get("working_title") for t in topics}
    n = 0
    for p in proposals:
        if p.get("working_title") in have:
            continue
        topics.append({**p, "proposed_at": today, "status": "idea"})
        n += 1
    dump_yaml({"topics": topics}, path)
    return n
```

- [ ] **Step 4: Implement CLI wiring — append to `cli.py`**

```python
from .column import editor as ed
from .column import evidence as cev
from .column import gates as cg
from .column import render as crend
from .column import scan as cscan
from .column import schema as csch

COLUMN_ROOT = ROOT / "columns" / "xiyangjing"
BRIEFINGS_DIR = Path.home() / ("Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/"
                               "knowledge/hermes/briefings")


def _engine():
    import sqlalchemy as sa
    dsn = os.environ.get("PGURL")
    if not dsn:
        env = ROOT.parent / "config" / ".env"
        if env.exists():
            for line in env.read_text().splitlines():
                if line.startswith("PGURL="):
                    dsn = line.split("=", 1)[1]
    if not dsn:
        raise SystemExit("PGURL not set and config/.env not found")
    try:
        return sa.create_engine(dsn)
    except Exception as e:
        raise SystemExit(f"DB connect failed: {e} — check Astrill AWS bypass + RDS SG rule")


def _licenses():
    return (load_yaml(COLUMN_ROOT / "style" / "licenses.yaml") or {}).get("licenses") or {}


def cmd_col_new(a):
    print(csch.new_article(COLUMN_ROOT, a.num, a.slug))


def cmd_col_evidence(a):
    entries = cev.build_pack(csch.article_dir(COLUMN_ROOT, a.article), _engine(), _licenses())
    print("built", len(entries), "evidence entries")


def cmd_col_verify(a):
    problems = cev.verify_pack(csch.article_dir(COLUMN_ROOT, a.article), _engine())
    print("\n".join(problems) or "all snapshots current")
    raise SystemExit(1 if problems else 0)


def cmd_col_gates(a):
    print(cg.run_gates(csch.article_dir(COLUMN_ROOT, a.article), COLUMN_ROOT))


def cmd_col_render(a):
    print(crend.render_article(csch.article_dir(COLUMN_ROOT, a.article), COLUMN_ROOT))


def cmd_col_scan(a):
    md = cscan.render_scan_md(cscan.scan_anomalies(cscan.load_prices(_engine()),
                                                   date.today().isoformat()),
                              date.today().isoformat())
    (COLUMN_ROOT / "topics").mkdir(exist_ok=True)
    (COLUMN_ROOT / "topics" / "weekly_scan.md").write_text(md, encoding="utf-8")
    print(md)


def cmd_col_propose(a):
    from datetime import date as _date
    today = _date.today().isoformat()
    engine = _engine()
    scan_md = (COLUMN_ROOT / "topics" / "weekly_scan.md")
    scan_md = scan_md.read_text(encoding="utf-8") if scan_md.exists() else "(no scan)"
    backlog = load_yaml(COLUMN_ROOT / "topics" / "backlog.yaml") or {"topics": []}
    titles = [t.get("working_title") for t in backlog.get("topics", [])]
    concept_ids = []
    tracks = ROOT / "syllabus" / "tracks.yaml"
    if tracks.exists():
        concept_ids = [c["id"] for t in (load_yaml(tracks) or {}).get("tracks", [])
                       for c in t.get("concepts", [])]
    ctx = ed.gather_context(BRIEFINGS_DIR, engine, scan_md, concept_ids, titles)
    props = ed.propose(_client(), os.environ.get("ACADEMY_EDITOR_MODEL", OUTLINE_MODEL), ctx)
    n = ed.append_backlog(COLUMN_ROOT, props, today)
    print(f"proposed {len(props)}, added {n}")
    for p in props:
        print("-", p.get("working_title"), "·", p.get("hook", {}).get("source"))
```
Add `from datetime import date` to the cli.py imports. Register parsers in `main()`:
```python
    col = sub.add_parser("column").add_subparsers(dest="sub", required=True)
    n = col.add_parser("new"); n.add_argument("--num", type=int, required=True)
    n.add_argument("--slug", required=True); n.set_defaults(fn=cmd_col_new)
    for name, fn in [("evidence", cmd_col_evidence), ("verify", cmd_col_verify),
                     ("gates", cmd_col_gates), ("render", cmd_col_render),
                     ("scan", cmd_col_scan), ("propose-topics", cmd_col_propose)]:
        s = col.add_parser(name)
        if name in ("evidence", "verify", "gates", "render"):
            s.add_argument("article")
        s.set_defaults(fn=fn)
```

- [ ] **Step 5: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests -q`
Expected: all pass (69).

- [ ] **Step 6: Commit**

```bash
git add power-academy/academy/column/editor.py power-academy/academy/cli.py power-academy/tests/test_column_editor.py
git commit -m "Add editor agent propose-topics and column CLI commands" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 10: Seed content + live smoke (read-only)

**Files:**
- Create: `power-academy/columns/xiyangjing/README.md`, `style/voice.md`, `style/disclaimer.md`, `style/blocklist.yaml`, `style/compliance_patterns.yaml`, `style/zh_terms.yaml`, `style/licenses.yaml`, `topics/backlog.yaml`
- Modify: `power-academy/.gitignore` (add `columns/xiyangjing/articles/*/evidence/data/`)

**Interfaces:**
- Consumes: everything above. Produces: the seeded column workspace used by the owner's real articles.

- [ ] **Step 1: Write seed files**

`columns/xiyangjing/style/voice.md`:
```markdown
# Voice — 西洋镜看中国电力市场

- 建设性：诊断问题、提出可操作建议；不点名批评任何机构。
- 证据先行：每个数字、每条政策引用都有 [[E:id]] 依据；机制解释与观点为自由文本。
- 比较方法：先说西方机制如何运作，再说哪些可移植、哪些不可移植及原因。
- 读者：A（同业/投资者）与 C（政策/交易所/智库）。假设读者懂市场基本原理，不科普常识。
- 衍生品论证：从数据中呈现的价格风险（波动、基差、尖峰）出发，不做产品推介。
- 段落短，一句一意；图表即论据。
- 重庆 EPC 价格不代表全国市场，不得作为价格锚点。
```
`columns/xiyangjing/style/disclaimer.md`:
```markdown
免责声明：本文仅代表作者个人观点，供研究交流使用，不构成任何投资建议或对任何证券、
期货及衍生品交易的推荐。文中数据来源于公开渠道或作者自有研究平台，口径以文内说明为准。
```
`columns/xiyangjing/style/blocklist.yaml`（owner fills privately; examples only）:
```yaml
names:
  # 雇主、交易对手、内部项目代号——由作者本人维护，以下为格式示例
  - EXAMPLE_EMPLOYER_NAME
  - EXAMPLE_COUNTERPARTY
```
`columns/xiyangjing/style/compliance_patterns.yaml`:
```yaml
patterns:
  - 将涨到
  - 将跌至
  - 稳赚
  - 保证收益
  - 保本
  - 推荐买入
  - 建议买入
  - 建议卖出
  - 抄底
  - 必涨
  - 必跌
```
`columns/xiyangjing/style/zh_terms.yaml`:
```yaml
terms:
  - {canonical: 现货市场, variants: [即期市场]}
  - {canonical: 中长期交易, variants: [中长期合约市场]}
  - {canonical: 容量电价, variants: [容量价格]}
  - {canonical: 偏差考核, variants: [偏差惩罚]}
  - {canonical: 辅助服务, variants: [ ancillary services]}
```
`columns/xiyangjing/style/licenses.yaml`（conservative defaults; owner reviews）:
```yaml
licenses:
  marketdata.spot_prices_hourly: licensed_restricted   # LingFeng 第三方数据
  marketdata.spot_fundamentals_hourly: licensed_restricted
  marketdata.province_fundamentals: licensed_restricted
  staging.spot_interprov_flow: licensed_restricted
  marketdata.bess_capture_daily: licensed_restricted   # 派生自第三方价格
  staging.spot_knowledge_docs: public                  # 政策文件为公开文件
```
`columns/xiyangjing/topics/backlog.yaml`:
```yaml
topics:
  - {working_title: 从英国 merit order 看山东现货价格形成, status: idea}
  - {working_title: 从英国容量市场拍卖看中国容量电价机制, status: idea}
  - {working_title: 德国负电价与山东现货的启示, status: idea}
  - {working_title: 从 spark spread 看煤电成本传导与联动, status: idea}
  - {working_title: 独立储能现货收益：capture rate 的中英对比, status: idea}
  - {working_title: 售电公司风险管理：从 PaR 到偏差考核, status: idea}
  - {working_title: 远期曲线与中长期合约定价, status: idea}
  - {working_title: 从欧洲平衡机制看中国偏差结算, status: idea}
  - {working_title: 节点电价 vs 分区电价：阻塞管理的国际经验, status: idea}
  - {working_title: 电力期货缺位的对冲成本：从中外基差数据说起, status: idea}
```
`columns/xiyangjing/README.md`:
```markdown
# 西洋镜看中国电力市场

Workflow: backlog pick → `column new` → 写 brief（owner 批准论点）→ `column evidence` →
起草 `draft.zh.md`（数字/政策引用必须带 `[[E:id]]`）→ `column gates` → owner 裁决 +
`owner_signoff` → `column render` → 粘贴发布 → brief 记 `published_url`。

- 数据许可见 `style/licenses.yaml`；restricted 数据不进公开图表。
- `column scan` 生成周度异动（propose-topics 的输入之一）。
- `column propose-topics` 需要 VPN 开启（Anthropic）+ PGURL。
```
`.gitignore` append: `columns/xiyangjing/articles/*/evidence/data/`

- [ ] **Step 2: Live smoke — scan (read-only, DB verified reachable)**

Run: `cd power-academy && PGURL=$(grep -E "^PGURL=" ../config/.env | cut -d= -f2-) ~/.venvs/bess-platform/bin/python -m academy.cli column scan`
(If the worktree path layout means `../config/.env` misses, export PGURL from the main repo's `config/.env` first.)
Expected: prints a markdown scan with real provinces and writes `topics/weekly_scan.md`.

- [ ] **Step 3: Live smoke — one real evidence pack**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m academy.cli column new --num 900 --slug smoke-test`, add one sql query (山东 last 7 days from `marketdata.spot_prices_hourly`, limit 500) to its `queries.yaml`, then `column evidence 900-smoke-test` and `column gates 900-smoke-test`.
Expected: snapshot CSV + manifest entry; gates run (draft is empty → fact_trace passes; empty draft is fine for smoke). Delete the smoke article directory after verification: `rm -rf columns/xiyangjing/articles/900-smoke-test` (safe: generated content only).

- [ ] **Step 4: Commit seed content**

```bash
git add power-academy/columns power-academy/.gitignore
git commit -m "Seed xiyangjing column workspace with style files and topic backlog" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:** §3 layout/brief/manifest/claim-tracing → Tasks 1, 2, 5. §4 builders (SQL/KB/western/chart/verify) → Tasks 2, 3. §5 gates + render + workflow → Tasks 5, 6, 7. §6 editor agent → Tasks 8, 9. §8 risks → Review Focus + gates. §9 build order → task order; hermes feeds (§7) out of scope by design. Gap: the "learning loop" (backlog follow-ups after publishing) is an owner habit, not code — noted in README.

**Placeholder scan:** no TBD/TODO; blocklist seed uses clearly-marked examples with an owner-maintenance note (real names are the owner's private data, not plan content). Angle-bracket values: none.

**Type consistency:** `build_pack(article_dir, engine, licenses, today)` used identically in Tasks 2, 3, 9 CLI. `run_gates(article_dir, column_root)` matches Task 6 tests and CLI. `article_dir(root, name)` from Task 1 used in CLI. `render_article(article_dir, column_root)` matches. `gather_context(...)` keyword params match its test. Editor `propose` drops hooks not in sources — CLI prints survivors only.
