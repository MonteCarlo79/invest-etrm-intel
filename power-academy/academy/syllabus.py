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
