"""services/deal_engine/scenario_store.py — JSON-file scenario persistence."""
from __future__ import annotations
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

_STORE_DIR = Path(__file__).parent / "scenarios"


def _ensure_dir() -> None:
    _STORE_DIR.mkdir(parents=True, exist_ok=True)


def save_scenario(scenario_id: str, data: dict[str, Any]) -> Path:
    """Persist a named scenario (inputs + MC results) to JSON. Returns file path."""
    _ensure_dir()
    data["_saved_at"] = datetime.utcnow().isoformat()
    path = _STORE_DIR / f"{scenario_id}.json"
    path.write_text(json.dumps(data, default=str), encoding="utf-8")
    return path


def load_scenario(scenario_id: str) -> dict[str, Any]:
    """Load a scenario by ID. Raises FileNotFoundError if not found."""
    path = _STORE_DIR / f"{scenario_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"Scenario {scenario_id!r} not found in {_STORE_DIR}")
    return json.loads(path.read_text(encoding="utf-8"))


def list_scenarios() -> list[str]:
    """Return list of saved scenario IDs (without .json extension)."""
    _ensure_dir()
    return [p.stem for p in sorted(_STORE_DIR.glob("*.json"))]


def delete_scenario(scenario_id: str) -> None:
    path = _STORE_DIR / f"{scenario_id}.json"
    if path.exists():
        path.unlink()
