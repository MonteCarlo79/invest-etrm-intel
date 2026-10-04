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
