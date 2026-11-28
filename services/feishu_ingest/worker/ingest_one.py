"""Ingest one S3 object into the shared KB (child process, so an OOM only loses this file)."""
import io, json, os, sys, boto3
import services.knowledge_pool.knowledge_docs as kd
from services.knowledge_pool.db import get_conn
FIXED = None  # optional: source of fixed extractor functions to patch over an older image

if FIXED:
    ns = {"io": io}; exec(compile(FIXED, "fixed_extractors", "exec"), ns)
    kd._extract_pages_docx = ns["_extract_pages_docx"]; kd._extract_pages_xlsx = ns["_extract_pages_xlsx"]
# Very large sheets (row-level project lists etc.) would produce tens of thousands of chunks:
# keep the first ROW_CAP rows per sheet plus a note with the true size.
ROW_CAP, BIG_SHEET, ROW_BUDGET = 2000, 10000, 8000  # per-sheet cap, trigger, whole-workbook budget
_orig_xlsx = kd._extract_pages_xlsx
def _capped_xlsx(file_bytes):
    pages = _orig_xlsx(file_bytes)
    total = sum(t.count("\n") + 1 for _, t in pages)
    if total <= BIG_SHEET:
        return pages
    cap = min(ROW_CAP, max(150, ROW_BUDGET // max(1, len(pages))))
    out = []
    for no, text in pages:
        lines = text.split("\n")
        if len(lines) > cap + 1:
            text = "\n".join(lines[:cap + 1]) + f"\n[truncated for indexing: {len(lines) - 1} rows in this sheet]"
        out.append((no, text))
    return out
kd._extract_pages_xlsx = _capped_xlsx
# The KB tables exist. init_knowledge_tables() runs ALTER TABLE ... ADD COLUMN IF NOT EXISTS, which takes
# an AccessExclusiveLock every call and deadlocks parallel ingest batches - skip it here.
kd.init_knowledge_tables = lambda: None

B, key, path = sys.argv[1], sys.argv[2], sys.argv[3]
PROV = ["云南","山西","新疆","江苏","海南","甘肃","陕西","青海","上海","广西","河北南网","河北","河南","江西","内蒙","蒙西","蒙东","吉林","山东","广东","浙江","安徽","湖南","湖北","四川","贵州","宁夏","辽宁","黑龙江","福建"]
name = key.rsplit("/", 1)[1]
data = boto3.client("s3").get_object(Bucket=B, Key=key)["Body"].read()
api = os.environ.get("ANTHROPIC_API_KEY")
doc_id, is_new, cat = kd.register_and_ingest(data, name, app="shared", api_key=api, synthesize=False)
kd._embed_chunks_for_doc(doc_id)
syn = "-"
if is_new:
    try:
        from services.knowledge_pool.synthesis import synthesize_on_ingest
        synthesize_on_ingest(doc_id, api); syn = "ok"
    except Exception as e:
        syn = "skipped: " + str(e)[:80]
    prov = next((p for p in PROV if p in path.split("/")), None) or next((p for p in PROV if p in name), None)
    with get_conn() as c, c.cursor() as cur:
        cur.execute("update staging.spot_knowledge_docs set source_name=%s, province=coalesce(province,%s) where id=%s",
                    ("feishu:" + path, prov, doc_id))
        c.commit()
with get_conn() as c, c.cursor() as cur:
    cur.execute("select count(*), count(embedding) from staging.spot_knowledge_chunks where doc_id=%s", (doc_id,))
    n, e = cur.fetchone()
print("RESULT " + json.dumps({"file": path + "/" + name, "doc_id": doc_id, "new": is_new, "category": cat,
      "chunks": n, "embedded": e, "synthesis": syn}, ensure_ascii=False), flush=True)
