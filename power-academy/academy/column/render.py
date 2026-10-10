import base64
import re
from pathlib import Path

import markdown

from ..io import load_yaml
from .gates import CHART_REF_RE, TAG_RE, check_blocklist
from .schema import check_approval_ready

ALLOWED_TAGS = {"p", "h1", "h2", "h3", "blockquote", "strong", "em", "table",
                "thead", "tbody", "tr", "td", "th", "ul", "ol", "li", "img",
                "br", "hr", "sup", "section"}

CSS = ("body{font-family:'PingFang SC','Hiragino Sans GB',sans-serif;font-size:16px;"
       "line-height:1.8;color:#222;max-width:677px;margin:0 auto;padding:0 8px}"
       "h2{border-left:4px solid #1f6fb2;padding-left:8px}"
       "blockquote{color:#666;border-left:3px solid #ccc;padding-left:10px;margin-left:0}"
       "table{border-collapse:collapse;width:100%}td,th{border:1px solid #ddd;padding:6px}"
       "img{max-width:100%}.foot{color:#888;font-size:13px}")


def _sanitize(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.S | re.I)

    def sub(m):
        tag = m.group(2).lower()
        return m.group(0) if tag in ALLOWED_TAGS else ""
    return re.sub(r"<(/?)([a-zA-Z0-9]+)([^>]*)>", sub, html)


def render_article(article_dir: Path, column_root: Path) -> Path:
    article_dir, column_root = Path(article_dir), Path(column_root)
    problems = check_approval_ready(article_dir)
    if problems:
        raise ValueError("article not approved: " + "; ".join(problems))
    draft = (article_dir / "draft.zh.md").read_text(encoding="utf-8")
    bl_path = column_root / "style" / "blocklist.yaml"
    names = (load_yaml(bl_path) or {}).get("names") or [] if bl_path.exists() else []
    hits = check_blocklist({"draft": draft}, names)
    if hits:
        raise ValueError("blocklisted names in draft: " + "; ".join(hits))
    entries = {e["id"]: e for e in
               (load_yaml(article_dir / "evidence" / "manifest.yaml") or {}).get("entries") or []}
    refs, seen = [], {}

    def tag_sub(m):
        tid = m.group(1)
        if tid not in entries:
            raise ValueError(f"unknown tag: {tid}")
        if tid not in seen:
            seen[tid] = len(refs) + 1
            refs.append(entries[tid])
        return f"<sup>[{seen[tid]}]</sup>"

    from .gates import load_claim_map
    claim_map = load_claim_map(article_dir)
    if claim_map:
        body = draft
        seen_c = {}
        for c in claim_map:
            for eid in c.get("evidence", []):
                if eid in entries and eid not in seen_c:
                    seen_c[eid] = len(refs) + 1
                    refs.append(entries[eid])
    else:
        body = TAG_RE.sub(tag_sub, draft)

    def chart_sub(m):
        cid = m.group(1)
        e = entries.get(cid)
        if not e or e.get("kind") != "chart" or not e.get("public"):
            raise ValueError(f"cannot embed chart: {cid}")
        png = article_dir / "evidence" / "charts" / f"{cid}.png"
        b64 = base64.b64encode(png.read_bytes()).decode()
        return f'<img src="data:image/png;base64,{b64}" alt="{cid}"/>'

    body = CHART_REF_RE.sub(chart_sub, body)
    if not claim_map and ("[[E:" in body or "[[C:" in body):
        raise ValueError("malformed or unconverted tag in draft")
    html = markdown.markdown(body, extensions=["tables"])
    html = _sanitize(html)

<<<<<<< main
    src_lines = [f"[{i+1}] {e.get('source','')}（{e.get('retrieved_at','')}获取）"
=======
    GENERIC_SOURCE = {"policy_excerpt": "公开政策文件", "western_fact": "公开文献",
                      "chart": "作者测算", "news": "公开报道", "owner_note": "作者注记",
                      "data_own": "作者自有经营数据",
                      "data_public": "公开市场数据", "data_licensed_restricted": "授权市场数据"}

    def _generic(e):
        if e.get("kind") != "data":
            return GENERIC_SOURCE[e.get("kind", "data_public")]
        return GENERIC_SOURCE["data_" + e.get("license", "public")]

    src_lines = [f"[{i+1}] {_generic(e)}（{e.get('retrieved_at','')}获取）"
>>>>>>> pa-work
                 for i, e in enumerate(refs)]
    disclaimer = (column_root / "style" / "disclaimer.md").read_text(encoding="utf-8").strip()
    foot = ("<section class='foot'><hr/><p><strong>数据来源与口径</strong><br/>"
            + "<br/>".join(src_lines)
            + "</p><p><strong>方法</strong>：数据经可复现查询提取，图表由脚本生成；"
              "观点为作者分析，不代表任何机构。</p><p>" + disclaimer + "</p></section>")
    out = article_dir / "out" / (article_dir.name + ".html")
    out.parent.mkdir(exist_ok=True)
    out.write_text(f"<style>{CSS}</style><section>{html}</section>{foot}", encoding="utf-8")
    return out
