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
