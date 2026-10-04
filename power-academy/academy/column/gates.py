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
