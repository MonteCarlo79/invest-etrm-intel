"""
Run synthesis pipeline on all un-synthesized knowledge docs.
Generates summary + Q&A pairs for each document so Hermes can answer questions.

Run after ingest_market_fundamentals.py:
    python scripts/run_synthesis.py
"""
import os
import sys
import pathlib

_repo = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo))

try:
    from dotenv import load_dotenv
    for _env in [_repo / "config" / ".env", _repo / ".env"]:
        if _env.exists():
            load_dotenv(_env)
except ImportError:
    pass

pgurl   = os.environ.get("PGURL", os.environ.get("HERMES_DB_URL", ""))
api_key = os.environ.get("ANTHROPIC_API_KEY", "")

if not pgurl:
    sys.exit("ERROR: Set PGURL (or HERMES_DB_URL) before running.")
if not api_key:
    sys.exit("ERROR: Set ANTHROPIC_API_KEY before running.")

os.environ.setdefault("PGURL", pgurl)

from services.knowledge_pool.synthesis import SynthesisPipeline

pipeline = SynthesisPipeline(api_key=api_key, workers=2, delay_between_calls=3.0)
print("Running synthesis (this may take a while for large document sets)…\n")
result = pipeline.run(app_filter="strategist", verbose=True)
print(f"\nDone: {result['ok']} OK, {result['error']} errors")
