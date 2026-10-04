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
