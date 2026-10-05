import argparse
import json
import os
import tarfile
from datetime import date
from pathlib import Path

from . import coverage as cv
from . import extract as ex
from . import llm, outline as ol, registry as rg, syllabus as sy
from . import authoring as auth
from .column import editor as ed
from .column import evidence as cev
from .column import gates as cg
from .column import render as crend
from .column import scan as cscan
from .column import schema as csch
from .concepts import parse_concept, validate_concept, zh_is_stale
from .glossary import check_pair, load_glossary
from .graph import validate_graph
from .io import dump_yaml, load_yaml

ROOT = Path(__file__).resolve().parent.parent
_OD = Path.home() / "Library/CloudStorage/OneDrive-Personal"
LIB = _OD / "Structure/Asset Modelling/Power"
PRACTICE_ROOTS = [_OD / "company/SEE/power", _OD / "company/SEE/ote2/dipeng"]
OUTLINE_MODEL = os.environ.get("ACADEMY_OUTLINE_MODEL", "claude-sonnet-4-6")
TAG_MODEL = os.environ.get("ACADEMY_TAG_MODEL", "claude-haiku-4-5-20251001")
RESULT_DIRS = ["inventory", "syllabus", "review", "concepts"]
COLUMN_ROOT = ROOT / "columns" / "xiyangjing"
BRIEFINGS_DIR = Path.home() / ("Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/"
                               "knowledge/hermes/briefings")


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
    rep = ex.extract_all(_entries(), ROOT / "cache", convert_dir=ROOT / "cache" / "_converted", ocr=True)
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
        e = sa.create_engine(dsn)
        with e.connect():
            pass
        return e
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
    today = date.today().isoformat()
    engine = _engine()
    scan_path = COLUMN_ROOT / "topics" / "weekly_scan.md"
    scan_md = scan_path.read_text(encoding="utf-8") if scan_path.exists() else "(no scan)"
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
    col = sub.add_parser("column").add_subparsers(dest="sub", required=True)
    n = col.add_parser("new")
    n.add_argument("--num", type=int, required=True)
    n.add_argument("--slug", required=True)
    n.set_defaults(fn=cmd_col_new)
    for name, fn in [("evidence", cmd_col_evidence), ("verify", cmd_col_verify),
                     ("gates", cmd_col_gates), ("render", cmd_col_render),
                     ("scan", cmd_col_scan), ("propose-topics", cmd_col_propose)]:
        s = col.add_parser(name)
        if name in ("evidence", "verify", "gates", "render"):
            s.add_argument("article")
        s.set_defaults(fn=fn)
    con = sub.add_parser("concept").add_subparsers(dest="sub2", required=True)
    pk = con.add_parser("pack")
    pk.add_argument("concept_id")
    pk.set_defaults(fn=lambda a: print(auth.build_pack(ROOT, a.concept_id)))
    a = p.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()
