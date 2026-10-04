import hashlib
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

from ..io import dump_yaml, load_yaml

MAX_ROWS = 200_000
WRITE_RE = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke)\b", re.I)
KB_DOCS_TABLE = "staging.spot_knowledge_docs"
KB_CHUNKS_TABLE = "staging.spot_knowledge_chunks"


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


def _kb_excerpt(engine, q):
    doc_sql = (f"select id, title from {KB_DOCS_TABLE} "
               "where active and title like :pat order by created_at desc limit 1")
    chunk_sql = (f"select chunk_text, page_no from {KB_CHUNKS_TABLE} "
                 "where doc_id = :doc_id order by chunk_index limit 1")
    with engine.connect() as c:
        doc = c.execute(sa.text(doc_sql), {"pat": f"%{q['title_like']}%"}).fetchone()
        if doc is None:
            raise ValueError(f"no KB doc matching {q['title_like']!r}")
        chunk = c.execute(sa.text(chunk_sql), {"doc_id": doc[0]}).fetchone()
    quote = (chunk[0] if chunk else "")[: q.get("max_chars", 500)]
    return doc[1], quote, (chunk[1] if chunk else None)


def build_pack(article_dir: Path, engine, licenses: dict, today: str | None = None) -> list:
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
                "query": f"title like %{q['title_like']}%",
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


def write_manifest(article_dir: Path, entries: list) -> None:
    dump_yaml({"entries": entries}, Path(article_dir) / "evidence" / "manifest.yaml")


def verify_pack(article_dir, engine) -> list:
    article_dir = Path(article_dir)
    problems = []
    queries = load_yaml(article_dir / "evidence" / "queries.yaml").get("queries") or []
    for q in queries:
        if q["kind"] != "sql":
            continue
        fresh = _run_sql(engine, q["sql"], q.get("params")).head(MAX_ROWS)
        current = article_dir / "evidence" / "data" / f"{q['id']}.csv"
        if sha256_file(current) != hashlib.sha256(
                fresh.to_csv(index=False).encode("utf-8")).hexdigest():
            problems.append(f"{q['id']}: data changed since retrieval")
    return problems
