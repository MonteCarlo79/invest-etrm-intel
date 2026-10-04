import re

TAG_RE = re.compile(r"\[\[E:([A-Za-z0-9_\-]+)\]\]")
CHART_REF_RE = re.compile(r"\[\[C:([A-Za-z0-9_\-]+)\]\]")

# a "quantity": digits with unit/scale suffix, a decimal, or a percent
_QUANTITY = re.compile(
    r"(\d+(?:\.\d+)?\s*(?:%|元|块|分|角|厘|亿|万|倍|千瓦时|兆瓦时|千瓦|兆瓦|吉瓦|省|"
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
