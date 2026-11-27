"""Filter kb_diff's MISSING list to the agreed subset (owner decision 2026-11-27):
policy/rules files (folder path contains 政策 or 规则, any year) + 2025–2026 quarterly,
half-year and annual disclosure reports. Per-month detail sheets are excluded.

Usage: python select_subset.py <missing.json> <out.json> [<already_exported_results.jsonl> ...]
"""
import json
import re
import sys

REPORT = re.compile(r"季度|季报|上半年|年度|年报")
YEAR = re.compile(r"20(2[5-6])")


def wanted(x: dict) -> str | None:
    if re.search(r"政策|规则", x["path"]):
        return "policy"
    if REPORT.search(x["name"]) and YEAR.search(x["name"]):
        return "report"
    return None


def main() -> None:
    missing = json.load(open(sys.argv[1]))
    done = set()
    for f in sys.argv[3:]:
        for line in open(f):
            r = json.loads(line)
            if r["result"].startswith("OK") or "no Download" in r["result"]:
                done.add(r["url"])
    out = [dict(x, subset=w) for x in missing if (w := wanted(x)) and x["url"] not in done]
    json.dump(out, open(sys.argv[2], "w"), ensure_ascii=False, indent=1)
    n = sum(1 for x in out if x["subset"] == "policy")
    print(f"selected {len(out)} (policy {n}, reports {len(out) - n}); {len(done)} already handled")


if __name__ == "__main__":
    main()
