"""
Batch-ingest all PDF/PPTX/DOCX/XLSX/images from data/market-fundamentals
into the knowledge pool (strategist app).

Run from the repo root:
    python scripts/ingest_market_fundamentals.py

Requires env vars:
    PGURL              — PostgreSQL connection string (same as HERMES_DB_URL)
    ANTHROPIC_API_KEY  — for image/chart vision descriptions
"""
import os
import sys
import pathlib

# Add repo root to path so services.* imports work
_repo = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo))

# Load .env if present
try:
    from dotenv import load_dotenv
    for _env in [_repo / "config" / ".env", _repo / ".env"]:
        if _env.exists():
            load_dotenv(_env)
except ImportError:
    pass

from services.knowledge_pool.knowledge_docs import register_and_ingest

EXTENSIONS = {".pdf", ".pptx", ".docx", ".jpg", ".jpeg", ".png"}
BASE = _repo / "data" / "market-fundamentals"

api_key = os.environ.get("ANTHROPIC_API_KEY", "")
pgurl   = os.environ.get("PGURL", os.environ.get("HERMES_DB_URL", ""))

if not pgurl:
    sys.exit("ERROR: Set PGURL (or HERMES_DB_URL) before running.")

# Patch PGURL so knowledge_pool.db picks it up
os.environ.setdefault("PGURL", pgurl)

files = [f for f in BASE.rglob("*") if f.is_file() and f.suffix.lower() in EXTENSIONS]
print(f"Found {len(files)} files under {BASE}\n")

ok = dup = err = 0
for f in sorted(files):
    try:
        doc_id, is_new, cat = register_and_ingest(
            file_bytes=f.read_bytes(),
            filename=f.name,
            app="strategist",
            api_key=api_key,
            synthesize=False,   # run synthesis separately after bulk load
        )
        tag = "NEW" if is_new else "DUP"
        if is_new:
            ok += 1
        else:
            dup += 1
        print(f"[{tag}] {f.name}  →  {cat}  (doc_id={doc_id})")
    except Exception as exc:
        err += 1
        print(f"[ERR] {f.name}  →  {exc}")

print(f"\nDone: {ok} new, {dup} duplicate, {err} errors")
print("Run synthesis next:  python scripts/run_synthesis.py")
