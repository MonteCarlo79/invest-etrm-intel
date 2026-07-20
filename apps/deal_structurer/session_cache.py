"""Persist deal-structurer session state to disk so results survive page refreshes."""
from __future__ import annotations
import pickle
import pathlib

_CACHE = pathlib.Path("/tmp/deal_structurer_session.pkl")

# Keys to persist (ordered by pipeline step)
_KEYS = [
    "price_paths",
    "price_sim_req",
    "dispatch_result",
    "last_dispatch_req",
    "last_financials",
    "last_cf_result",
    "mc_result",
    "dp_result",
    "agent_messages",
    "agent_display",
]


def save(state) -> None:
    """Snapshot current session state keys to disk."""
    try:
        data = {k: state[k] for k in _KEYS if k in state}
        _CACHE.write_bytes(pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL))
    except Exception:
        pass  # never crash the UI over a cache write failure


def load(state) -> None:
    """Restore persisted keys into session state (only if the slot is still at its default)."""
    if not _CACHE.exists():
        return
    try:
        data = pickle.loads(_CACHE.read_bytes())
        for k, v in data.items():
            current = state.get(k)
            # Only restore if the slot holds a default (None or empty list)
            if current is None or current == []:
                state[k] = v
    except Exception:
        pass  # corrupt cache — ignore, user will re-run


def clear() -> None:
    """Delete the cache file."""
    try:
        _CACHE.unlink(missing_ok=True)
    except Exception:
        pass
