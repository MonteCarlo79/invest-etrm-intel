"""Diff a Feishu folder listing against the knowledge base; emit the documents the KB lacks.

Usage: python kb_diff.py <tree.json> <out_missing.json> [PGURL]
KB metadata is read from PGURL (default: local replica). A Feishu file counts as PRESENT when
  1. its normalised name equals a KB file name / title, or
  2. a KB doc has the same (province, period, report type), or
  3. (policy files) a KB doc carries the same 文号 or a near-identical 《title》.
Anything uncertain counts as MISSING; ingest-time sha256 dedup catches byte-identical copies.
"""
from __future__ import annotations

import difflib
import json
import os
import re
import sys

import psycopg2

PROVINCES = [
    "蒙东", "蒙西", "内蒙古东", "内蒙古西", "上海", "北京", "首都", "天津", "冀北", "河北", "山西", "山东",
    "辽宁", "吉林", "黑龙江", "江苏", "浙江", "安徽", "福建", "江西", "河南", "湖北", "湖南", "重庆",
    "四川", "陕西", "甘肃", "青海", "宁夏", "新疆", "西藏", "广东", "广西", "云南", "贵州", "海南", "南方",
]
ALIAS = {"内蒙古东": "蒙东", "内蒙古西": "蒙西", "首都": "北京"}
TYPES = [  # first match wins — order from specific to generic
    ("两个细则", r"两个细则"), ("辅助服务", r"辅助服务|调频|备用"), ("绿电", r"绿电|绿证|绿色电力"),
    ("不平衡", r"不平衡"), ("装机", r"装机"), ("用电", r"用电"), ("结算", r"结算"),
    ("代购", r"代购|代理购电"), ("季报", r"季度|季报|上半年"), ("年报", r"年度|年报"),
    ("月报", r"披露|月报|交易信息|市场信息"),
]
EXT = re.compile(r"\.(pdf|docx?|xlsx?|pptx?|wps|txt|html?|csv|png|jpe?g|webp|zip|rar|svg)$", re.I)
UNSUPPORTED = re.compile(r"\.(zip|rar|wps|svg|doc|xls|ppt)$", re.I)  # KB extractors cannot read these


def norm(name: str) -> str:
    s = EXT.sub("", name)
    s = re.sub(r"\(\d+\)|（\d+）|\d{14}|\s+|[-_—·.,，、()（）【】\[\]《》\"“”'：:]", "", s)
    return s.lower()


def province(text: str) -> str | None:
    for p in PROVINCES:
        if p in text:
            return ALIAS.get(p, p)
    return None


def period(text: str) -> str | None:
    t = text.replace(" ", "")
    m = re.search(r"(20\d{2})[-年._]?(\d{1,2})[月-]?(?!\d)", t) or re.search(r"(20\d{2})(\d{2})(?!\d)", t)
    q = re.search(r"(20\d{2})[^0-9]{0,4}(?:第)?([1-4一二三四])(?:季度|季)", t) or re.search(r"(20\d{2})-0?([1-4])季度", t)
    if q:
        return f"{q.group(1)}Q{'一二三四'.find(q.group(2)) + 1 if q.group(2) in '一二三四' else q.group(2)}"
    if "上半年" in t and (y := re.search(r"20\d{2}", t)):
        return f"{y.group(0)}H1"
    if m and 1 <= int(m.group(2)) <= 12:
        return f"{m.group(1)}-{int(m.group(2)):02d}"
    if re.search(r"年度|年报", t) and (y := re.search(r"20\d{2}", t)):
        return y.group(0)
    return None


def rtype(text: str) -> str | None:
    for name, pat in TYPES:
        if re.search(pat, text):
            return name
    return None


def wenhao(text: str) -> str | None:
    m = re.search(r"[一-龥]{1,8}[〔\[（(](20\d{2})[〕\]）)]\s*\d+\s*号", text)
    return re.sub(r"\s", "", m.group(0)) if m else None


def booktitle(text: str) -> str | None:
    m = re.search(r"《([^》]{6,})》", text)
    return norm(m.group(1)) if m else None


def main() -> None:
    tree, out = sys.argv[1], sys.argv[2]
    dsn = sys.argv[3] if len(sys.argv) > 3 else "postgresql://postgres@localhost:5433/marketdata"
    with psycopg2.connect(dsn) as c, c.cursor() as cur:
        cur.execute("select coalesce(file_name,''), coalesce(title,''), coalesce(province,'') "
                    "from staging.spot_knowledge_docs where active")
        kb = cur.fetchall()
    names, keys, whs, titles = set(), set(), set(), []
    for fn, title, prov in kb:
        for t in (fn, title):
            if t:
                names.add(norm(t))
        txt = f"{fn} {title}"
        p = province(txt) or (ALIAS.get(prov, prov) or None)
        if (per := period(txt)) and (rt := rtype(txt)) and p:
            keys.add((p, per, rt))
        if w := wenhao(txt):
            whs.add(w)
        if b := booktitle(txt):
            titles.append(b)

    items = [x for x in json.load(open(tree)) if x["kind"] != "folder"]
    missing, present, skipped = [], {"name": 0, "key": 0, "policy": 0}, []
    for x in items:
        name, path = x["name"], x["path"]
        if UNSUPPORTED.search(name):
            skipped.append(x)
            continue
        ctx = f"{path} {name}"
        if norm(name) in names:
            present["name"] += 1
            continue
        p, per, rt = province(ctx), period(name), rtype(name)
        if p and per and rt and (p, per, rt) in keys:
            present["key"] += 1
            continue
        w, b = wenhao(name), booktitle(name)
        if (w and w in whs) or (b and any(difflib.SequenceMatcher(None, b, t).ratio() > 0.9 for t in titles)):
            present["policy"] += 1
            continue
        missing.append(x)
    json.dump(missing, open(out, "w"), ensure_ascii=False, indent=1)
    json.dump(skipped, open(out.replace(".json", "_unsupported.json"), "w"), ensure_ascii=False, indent=1)
    print(f"listed files {len(items)} | present: {present} | unsupported format {len(skipped)} | MISSING {len(missing)}")


if __name__ == "__main__":
    main()
