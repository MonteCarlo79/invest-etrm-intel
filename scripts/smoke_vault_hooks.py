"""Live smoke test for vault hooks. Run: python scripts/smoke_vault_hooks.py
Requires config/.env (PGURL, ONEDRIVE_*). Writes one scratch note to the vault
inbox and reads back one existing note."""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))
load_dotenv(_REPO / "config" / ".env")

from services.knowledge_pool import vault_reader, vault_writer

print("[1] read vault index …")
idx = vault_reader.read_note("spot_market/04_indices/index.md", max_chars=400)
print("    OK" if idx else "    FAIL — empty")

print("[2] search 山东 …")
hits = vault_reader.search_notes(query="山东现货市场")
print("   ", [h["path"] for h in hits] or "FAIL — no hits")

print("[3] write scratch note …")
path = vault_writer.write_insight_note(
    category="smoke_test", content="vault hooks smoke test — safe to delete",
    source_app="manual", confidence="low",
)
print("   ", path or "FAIL — write returned None")

print("[4] read scratch back …")
if path:
    back = vault_reader._client().read_file_by_path(
        f"{vault_writer.VAULT_ROOT}/{path}").decode("utf-8")
    print("    OK" if "smoke test" in back else "    FAIL — content mismatch")
print("DONE")
