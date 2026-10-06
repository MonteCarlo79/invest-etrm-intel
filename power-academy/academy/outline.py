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
                         f"Source title: {entry.title}\n\n{ch}", max_tokens=4000)
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
