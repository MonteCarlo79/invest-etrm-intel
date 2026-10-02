import hashlib
from pathlib import Path

import yaml

LEVELS = ("foundation", "intermediate", "advanced")
STATUSES = ("stub", "drafted", "reviewed", "published")
ORIGINALITY = ("original", "synthesized")
MARKETS = ("EU", "GB", "US", "AU", "CN")
TRANSLATION_STATUSES = ("none", "drafted", "reviewed")
REQUIRED = ("id", "track", "level", "prerequisites", "markets", "status",
            "sources", "originality", "translations")


def parse_concept(path) -> tuple:
    text = Path(path).read_text(encoding="utf-8")
    _, fm, body = text.split("---\n", 2)
    return yaml.safe_load(fm), body


def render_concept(fm: dict, body: str) -> str:
    return "---\n" + yaml.safe_dump(fm, allow_unicode=True, sort_keys=False) + "---\n" + body


def body_hash(body: str) -> str:
    return hashlib.sha256(body.strip().encode("utf-8")).hexdigest()[:16]


def validate_concept(fm: dict, labs_dir=None) -> list:
    errs = [f"missing field: {k}" for k in REQUIRED if k not in fm]
    if errs:
        return errs
    if fm["level"] not in LEVELS:
        errs.append(f"level must be one of {LEVELS}")
    if fm["status"] not in STATUSES:
        errs.append(f"status must be one of {STATUSES}")
    if fm["originality"] not in ORIGINALITY:
        errs.append(f"originality must be one of {ORIGINALITY}")
    if not fm["markets"] or any(m not in MARKETS for m in fm["markets"]):
        errs.append(f"markets must be a non-empty subset of {MARKETS}")
    zh = (fm["translations"] or {}).get("zh", {})
    if zh.get("status", "none") not in TRANSLATION_STATUSES:
        errs.append(f"translations.zh.status must be one of {TRANSLATION_STATUSES}")
    if fm["status"] in ("reviewed", "published"):
        has_lab = False
        if labs_dir is not None:
            d = Path(labs_dir) / fm["id"]
            has_lab = d.is_dir() and any(d.iterdir())
        if not has_lab and not fm.get("no_lab_reason"):
            errs.append("reviewed/published needs a lab in labs/<id>/ or no_lab_reason")
    signoff = fm.get("signoff") or {}
    if fm["status"] == "published" and not signoff.get("en"):
        errs.append("published requires signoff.en")
    if signoff.get("zh") and zh.get("status") != "reviewed":
        errs.append("signoff.zh requires translations.zh.status == reviewed")
    return errs


def zh_is_stale(fm_en: dict, en_body: str, zh_tr: dict) -> bool:
    if zh_tr.get("status", "none") == "none":
        return False
    return zh_tr.get("en_hash") != body_hash(en_body)
